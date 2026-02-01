# -*- coding: utf-8 -*-
import logging
from typing import Any, Dict, List, Optional

from ..executor import ExecutionContext, StandardQueryExecution
from ..result_set import ResultSet
from ..sql.query_builder import QueryExecutionPlan
from .detector import SubqueryDetector
from .query_db_sqlite import QueryDBSQLite

_logger = logging.getLogger(__name__)


class SupersetExecution(StandardQueryExecution):
    """Two-stage execution strategy for subquery-based queries using intermediate RDBMS.

    Uses a QueryDatabase backend (SQLite3 by default) to handle complex
    SQL operations that MongoDB cannot perform natively.

    Attributes:
        _query_db_factory: Callable that creates QueryDatabase instances
    """

    def __init__(self, query_db_factory: Optional[Any] = None) -> None:
        """
        Initialize SupersetExecution with optional custom database backend.

        Args:
            query_db_factory: Callable that returns QueryDatabase instance.
                             Defaults to SQLiteBridge if not provided.
        """
        self._query_db_factory = query_db_factory or QueryDBSQLite
        self._execution_plan: Optional[QueryExecutionPlan] = None

    @property
    def execution_plan(self) -> QueryExecutionPlan:
        return self._execution_plan

    def supports(self, context: ExecutionContext) -> bool:
        """Support queries with subqueries, only SELECT statments is supported in this mode."""
        normalized = context.query.lstrip().upper()
        return "superset" in context.execution_mode.lower() and normalized.startswith("SELECT")

    def execute(
        self,
        context: ExecutionContext,
        connection: Any,
        parameters: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Execute query in two stages: MongoDB for subquery, intermediate DB for outer query"""
        _logger.debug(f"Using subquery execution for query: {context.query[:100]}")

        # Detect if query is a subquery or simple SELECT
        query_info = SubqueryDetector.detect(context.query)

        # If no subquery detected, fall back to standard execution
        if not query_info.has_subquery:
            _logger.debug("No subquery detected, falling back to standard execution")
            return super().execute(context, connection, parameters)

        # Stage 1: Execute MongoDB subquery
        mongo_query = query_info.subquery_text
        _logger.debug(f"Stage 1: Executing MongoDB subquery: {mongo_query}")

        mongo_execution_plan = self._parse_sql(mongo_query)
        mongo_result = self._execute_find_plan(mongo_execution_plan, connection)

        # Extract result set from MongoDB
        mongo_result_set = ResultSet(
            command_result=mongo_result,
            execution_plan=mongo_execution_plan,
            database=connection.database,
        )

        # Fetch all MongoDB results and convert to list of dicts
        mongo_rows = mongo_result_set.fetchall()
        _logger.debug(f"Stage 1 complete: Got {len(mongo_rows)} rows from MongoDB")

        # Convert tuple rows to dictionaries using column names
        column_names = [desc[0] for desc in mongo_result_set.description] if mongo_result_set.description else []
        mongo_dicts = []

        for row in mongo_rows:
            if column_names:
                mongo_dicts.append(dict(zip(column_names, row)))
            else:
                # Fallback if no description available
                mongo_dicts.append({"result": row})

        # Stage 2: Load results into intermediate DB and execute outer query
        db_name = self._query_db_factory.__name__ if hasattr(self._query_db_factory, "__name__") else "QueryDB"
        _logger.debug(f"Stage 2: Loading {len(mongo_dicts)} rows into {db_name}")

        query_db = self._query_db_factory()

        try:
            # Create temporary table with MongoDB results
            querydb_query, table_name = SubqueryDetector.extract_outer_query(context.query)
            if querydb_query is None or table_name is None:
                # Fallback to original query if extraction fails
                querydb_query = context.query
                table_name = "virtual_table"

            # Use cursor description for schema instead of inferring from values
            if mongo_result_set.description:
                query_db.insert_records_with_description(table_name, mongo_dicts, mongo_result_set.description)
            else:
                # Fallback to value-based inference if no description available
                query_db.insert_records(table_name, mongo_dicts)

            # Execute outer query against intermediate DB
            _logger.debug(f"Stage 2: Executing QueryDBSQLite query: {querydb_query}")

            querydb_rows = query_db.execute_query(querydb_query)
            _logger.debug(f"Stage 2 complete: Got {len(querydb_rows)} rows from {db_name}")

            # Build column mapping from outer query to MongoDB description
            # This preserves type information from projection functions
            mongo_description_map = {}
            if mongo_result_set.description:
                for desc in mongo_result_set.description:
                    col_name = desc[0]
                    type_code = desc[1]
                    mongo_description_map[col_name] = type_code

            # Convert SQLite TEXT values back to their proper types based on type codes
            if mongo_description_map and querydb_rows:
                querydb_rows = self._convert_row_values(querydb_rows, mongo_description_map)

            # Build command result dict from intermediate DB results
            command_result = self._create_result_set_from_db(querydb_rows, querydb_query)

            # Build final execution plan that includes projection information
            # The MongoDB result set description already has the correct column names with type codes
            projection_stage = {}

            if querydb_rows and isinstance(querydb_rows[0], dict):
                # Extract column names from first result row
                for col_name in querydb_rows[0].keys():
                    projection_stage[col_name] = 1  # 1 means included in projection
            else:
                # If no rows, get column names from the SQLite query directly
                try:
                    cursor = query_db.execute_query_cursor(querydb_query)
                    if cursor.description:
                        # Extract column names from cursor description
                        for col_desc in cursor.description:
                            col_name = col_desc[0]
                            projection_stage[col_name] = 1
                except Exception as e:
                    _logger.warning(f"Could not extract column names from empty result: {e}")

            # Create execution plan with projection_output built from MongoDB stage description
            # This preserves type information for all output columns
            final_execution_plan = QueryExecutionPlan(
                collection="query_db_result",
                projection_stage=projection_stage,
            )

            # Mark that rows are from SQLite intermediate storage (already formatted)
            # This tells the cursor to use PreProcessedResultSet instead of regular ResultSet
            final_execution_plan.from_intermediate_storage = True

            # Extract projection_output from MongoDB result set description
            # This provides the correct mapping from outer query column names to type codes
            if mongo_result_set.description:
                projection_output = []
                for col_name, type_code, *_ in mongo_result_set.description:
                    # Find the corresponding function if any
                    func_info = None
                    # Try to find it in projection_functions by output name from the MongoDB stage
                    if hasattr(mongo_execution_plan, "projection_output") and mongo_execution_plan.projection_output:
                        for proj in mongo_execution_plan.projection_output:
                            if proj.get("output_name") == col_name:
                                func_info = proj.get("function")
                                break

                    projection_output.append({"output_name": col_name, "function": func_info})

                final_execution_plan.projection_output = projection_output

            self._execution_plan = final_execution_plan

            # Return the command result dict (not a ResultSet)
            # The cursor will create the appropriate ResultSet based on the execution plan
            return command_result

        finally:
            query_db.close()

    def _create_result_set_from_db(self, rows: List[Dict[str, Any]], query: str) -> Dict[str, Any]:
        """
        Create a command result from query database results.

        Args:
            rows: List of dictionaries from query database
            query: Original SQL query

        Returns:
            Dictionary with command result format
        """
        # Create a mock command result structure compatible with ResultSet
        command_result = {
            "cursor": {
                "id": 0,  # No pagination for query DB results
                "firstBatch": rows,
            }
        }

        return command_result

    def _convert_row_values(self, rows: List[Dict[str, Any]], type_codes_map: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        Convert string values from SQLite back to their proper types based on type codes.

        Args:
            rows: List of dictionaries from SQLite
            type_codes_map: Mapping of column names to type codes from MongoDB stage

        Returns:
            List of dictionaries with converted values
        """
        import datetime
        import re

        from bson import Timestamp

        converted_rows = []
        for row in rows:
            converted_row = {}
            for col_name, value in row.items():
                # Always skip None values
                if value is None:
                    converted_row[col_name] = value
                    continue

                if col_name not in type_codes_map:
                    converted_row[col_name] = value
                    continue

                type_code = type_codes_map[col_name]

                # Convert based on type code string
                if isinstance(type_code, str):
                    type_code_lower = type_code.lower()

                    if type_code_lower == "float" and isinstance(value, str):
                        try:
                            converted_row[col_name] = float(value)
                        except (ValueError, TypeError):
                            converted_row[col_name] = value
                    elif type_code_lower == "datetime" and isinstance(value, str):
                        # First check if it's a Timestamp string representation
                        try:
                            match = re.match(r"Timestamp\((\d+),\s*(\d+)\)", value)
                            if match:
                                time_t = int(match.group(1))
                                inc = int(match.group(2))
                                converted_row[col_name] = Timestamp(time_t, inc)
                                continue
                        except Exception:
                            pass

                        # Try to parse as datetime first, then as date
                        try:
                            # Try ISO format first
                            converted_row[col_name] = datetime.datetime.fromisoformat(value)
                        except (ValueError, TypeError):
                            try:
                                # Try common datetime formats
                                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"]:
                                    try:
                                        converted_row[col_name] = datetime.datetime.strptime(value, fmt)
                                        break
                                    except ValueError:
                                        continue
                                else:
                                    # Try as date if datetime parsing failed
                                    try:
                                        converted_row[col_name] = datetime.datetime.strptime(value, "%Y-%m-%d").date()
                                    except ValueError:
                                        converted_row[col_name] = value
                            except Exception:
                                converted_row[col_name] = value
                    elif type_code_lower == "timestamp" and isinstance(value, str):
                        # Try to parse Timestamp string representation: Timestamp(time_t, inc)
                        try:
                            match = re.match(r"Timestamp\((\d+),\s*(\d+)\)", value)
                            if match:
                                time_t = int(match.group(1))
                                inc = int(match.group(2))
                                converted_row[col_name] = Timestamp(time_t, inc)
                            else:
                                converted_row[col_name] = value
                        except Exception:
                            converted_row[col_name] = value
                    elif type_code_lower == "date" and isinstance(value, str):
                        try:
                            # Try ISO format first
                            converted_row[col_name] = datetime.date.fromisoformat(value)
                        except (ValueError, TypeError):
                            try:
                                # Try common date formats
                                for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"]:
                                    try:
                                        converted_row[col_name] = datetime.datetime.strptime(value, fmt).date()
                                        break
                                    except ValueError:
                                        continue
                                else:
                                    converted_row[col_name] = value
                            except Exception:
                                converted_row[col_name] = value
                    elif type_code_lower == "int" and isinstance(value, str):
                        try:
                            converted_row[col_name] = int(value)
                        except (ValueError, TypeError):
                            converted_row[col_name] = value
                    elif type_code_lower == "bool" and isinstance(value, str):
                        converted_row[col_name] = value.lower() in ("true", "1", "yes")
                    else:
                        converted_row[col_name] = value
                else:
                    # If type_code is not a string, keep the value as-is
                    converted_row[col_name] = value

            converted_rows.append(converted_row)

        return converted_rows
