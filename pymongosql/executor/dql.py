# -*- coding: utf-8 -*-
import logging
from typing import Any, Dict, Optional, Sequence, Union

from pymongo.errors import PyMongoError

from ..error import DatabaseError, OperationalError, ProgrammingError, SqlSyntaxError
from ..helper import SQLHelper
from ..sql.parser import SQLParser
from ..sql.query_builder import QueryExecutionPlan
from .base import ExecutionContext, ExecutionStrategy

_logger = logging.getLogger(__name__)


class StandardQueryExecution(ExecutionStrategy):
    """Standard execution strategy for simple SELECT queries without subqueries"""

    @property
    def execution_plan(self) -> QueryExecutionPlan:
        """Return standard execution plan"""
        return self._execution_plan

    def supports(self, context: ExecutionContext) -> bool:
        """Support simple queries without subqueries"""
        normalized = context.query.lstrip().upper()
        return "standard" in context.execution_mode.lower() and normalized.startswith("SELECT")

    def _parse_sql(self, sql: str) -> QueryExecutionPlan:
        """Parse SQL statement and return QueryExecutionPlan"""
        try:
            parser = SQLParser(sql)
            execution_plan = parser.get_execution_plan()

            if not execution_plan.validate():
                raise SqlSyntaxError("Generated query plan is invalid")

            return execution_plan

        except SqlSyntaxError:
            raise
        except Exception as e:
            _logger.error(f"SQL parsing failed: {e}")
            raise SqlSyntaxError(f"Failed to parse SQL: {e}")

    def _replace_placeholders(self, obj: Any, parameters: Sequence[Any]) -> Any:
        """Recursively replace ? placeholders with parameter values in filter/projection dicts"""
        return SQLHelper.replace_placeholders_generic(obj, parameters, "qmark")

    def _execute_find_plan(
        self,
        execution_plan: QueryExecutionPlan,
        connection: Any = None,
        parameters: Optional[Sequence[Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Execute a QueryExecutionPlan against MongoDB using db.command

        Args:
            execution_plan: QueryExecutionPlan to execute
            connection: Connection object (for session and database access)
            parameters: Parameters for placeholder replacement
        """
        try:
            # Get database from connection
            if not connection:
                raise OperationalError("No connection provided")

            db = connection.database

            # Get database
            if not execution_plan.collection:
                raise ProgrammingError("No collection specified in query")

            # Replace placeholders with parameters in filter_stage only (not in projection)
            filter_stage = execution_plan.filter_stage or {}

            if parameters:
                # Positional parameters with ? (named parameters are converted to positional in execute())
                filter_stage = self._replace_placeholders(filter_stage, parameters)

            projection_stage = execution_plan.projection_stage or {}

            # Build MongoDB find command
            find_command = {"find": execution_plan.collection, "filter": filter_stage}

            # Apply projection if specified
            if projection_stage:
                find_command["projection"] = projection_stage

            # Apply sort if specified
            if execution_plan.sort_stage:
                sort_spec = {}
                for sort_dict in execution_plan.sort_stage:
                    for field_name, direction in sort_dict.items():
                        sort_spec[field_name] = direction
                find_command["sort"] = sort_spec

            # Apply skip if specified
            if execution_plan.skip_stage:
                find_command["skip"] = execution_plan.skip_stage

            # Apply limit if specified
            if execution_plan.limit_stage:
                find_command["limit"] = execution_plan.limit_stage

            _logger.debug(f"Executing MongoDB command: {find_command}")

            # Execute find command with session if in transaction
            if connection and connection.session and connection.session.in_transaction:
                result = db.command(find_command, session=connection.session)
            else:
                result = db.command(find_command)

            # Create command result
            return result

        except PyMongoError as e:
            _logger.error(f"MongoDB command execution failed: {e}")
            raise DatabaseError(f"Command execution failed: {e}")
        except Exception as e:
            _logger.error(f"Unexpected error during command execution: {e}")
            raise OperationalError(f"Command execution error: {e}")

    def _execute_aggregate_plan(
        self,
        execution_plan: QueryExecutionPlan,
        connection: Any = None,
        parameters: Optional[Sequence[Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Execute a QueryExecutionPlan with aggregate() call.

        Args:
            execution_plan: QueryExecutionPlan with aggregate_pipeline and aggregate_options
            connection: Connection object (for database access)
            parameters: Parameters for placeholder replacement

        Returns:
            Command result with aggregation results
        """
        try:
            import json

            # Get database from connection
            if not connection:
                raise OperationalError("No connection provided")

            db = connection.database

            if not execution_plan.collection:
                raise ProgrammingError("No collection specified in aggregate query")

            # Parse pipeline and options from JSON strings
            try:
                pipeline = json.loads(execution_plan.aggregate_pipeline or "[]")
                options = json.loads(execution_plan.aggregate_options or "{}")
            except json.JSONDecodeError as e:
                raise ProgrammingError(f"Invalid JSON in aggregate pipeline or options: {e}")

            _logger.debug(f"Executing aggregate on collection {execution_plan.collection}")
            _logger.debug(f"Pipeline: {pipeline}")
            _logger.debug(f"Options: {options}")

            # Get collection and call aggregate()
            collection = db[execution_plan.collection]

            # Execute aggregate with options
            cursor = collection.aggregate(pipeline, **options)

            # Convert cursor to list
            results = list(cursor)

            # Apply additional filters if specified (from WHERE clause)
            if execution_plan.filter_stage:
                _logger.debug(f"Applying additional filter: {execution_plan.filter_stage}")
                # Would need to filter results in Python, as aggregate already ran
                # For now, log that we're applying filters
                results = self._filter_results(results, execution_plan.filter_stage)

            # Apply sorting if specified
            if execution_plan.sort_stage:
                for sort_dict in reversed(execution_plan.sort_stage):
                    for field_name, direction in sort_dict.items():
                        reverse = direction == -1
                        results = sorted(results, key=lambda x: x.get(field_name), reverse=reverse)

            # Apply skip and limit
            if execution_plan.skip_stage:
                results = results[execution_plan.skip_stage :]

            if execution_plan.limit_stage:
                results = results[: execution_plan.limit_stage]

            # Apply projection if specified
            if execution_plan.projection_stage:
                results = self._apply_projection(results, execution_plan.projection_stage)

            # Return in command result format
            return {
                "cursor": {"firstBatch": results},
                "ok": 1,
            }

        except (ProgrammingError, OperationalError):
            raise
        except PyMongoError as e:
            _logger.error(f"MongoDB aggregate execution failed: {e}")
            raise DatabaseError(f"Aggregate execution failed: {e}")
        except Exception as e:
            _logger.error(f"Unexpected error during aggregate execution: {e}")
            raise OperationalError(f"Aggregate execution error: {e}")

    @staticmethod
    def _filter_results(results: list, filter_conditions: dict) -> list:
        """Apply MongoDB filter conditions to Python results"""
        # Basic filtering implementation
        # This is a simplified version - can be enhanced with full MongoDB query operators
        filtered = []
        for doc in results:
            if StandardQueryExecution._matches_filter(doc, filter_conditions):
                filtered.append(doc)
        return filtered

    @staticmethod
    def _matches_filter(doc: dict, filter_conditions: dict) -> bool:
        """Check if a document matches the filter conditions"""
        for field, condition in filter_conditions.items():
            if field == "$and":
                return all(StandardQueryExecution._matches_filter(doc, cond) for cond in condition)
            elif field == "$or":
                return any(StandardQueryExecution._matches_filter(doc, cond) for cond in condition)
            elif isinstance(condition, dict):
                # Handle operators like $eq, $gt, etc.
                for op, value in condition.items():
                    if op == "$eq":
                        if doc.get(field) != value:
                            return False
                    elif op == "$ne":
                        if doc.get(field) == value:
                            return False
                    elif op == "$gt":
                        if not (doc.get(field) > value):
                            return False
                    elif op == "$gte":
                        if not (doc.get(field) >= value):
                            return False
                    elif op == "$lt":
                        if not (doc.get(field) < value):
                            return False
                    elif op == "$lte":
                        if not (doc.get(field) <= value):
                            return False
            else:
                if doc.get(field) != condition:
                    return False
        return True

    @staticmethod
    def _apply_projection(results: list, projection_stage: dict) -> list:
        """Apply projection to results"""
        projected = []
        include_fields = {k for k, v in projection_stage.items() if v == 1}
        exclude_fields = {k for k, v in projection_stage.items() if v == 0}

        for doc in results:
            if include_fields:
                # Include mode: only include specified fields
                projected_doc = (
                    {"_id": doc.get("_id")} if "_id" in include_fields or "_id" not in projection_stage else {}
                )
                for field in include_fields:
                    if field != "_id" and field in doc:
                        projected_doc[field] = doc[field]
                projected.append(projected_doc)
            else:
                # Exclude mode: exclude specified fields
                projected_doc = {k: v for k, v in doc.items() if k not in exclude_fields}
                projected.append(projected_doc)

        return projected

    def execute(
        self,
        context: ExecutionContext,
        connection: Any,
        parameters: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Execute standard query directly against MongoDB"""
        _logger.debug(f"Using standard execution for query: {context.query[:100]}")

        # Preprocess query to convert named parameters to positional
        processed_query = context.query
        processed_params = parameters
        if isinstance(parameters, dict):
            # Convert :param_name to ? for parsing
            import re

            param_names = re.findall(r":(\w+)", context.query)
            # Convert dict parameters to list in order of appearance
            processed_params = [parameters[name] for name in param_names]
            # Replace :param_name with ?
            processed_query = re.sub(r":(\w+)", "?", context.query)

        # Parse the query
        self._execution_plan = self._parse_sql(processed_query)

        # Route to appropriate execution plan handler
        if hasattr(self._execution_plan, "is_aggregate_query") and self._execution_plan.is_aggregate_query:
            return self._execute_aggregate_plan(self._execution_plan, connection, processed_params)
        else:
            return self._execute_find_plan(self._execution_plan, connection, processed_params)
