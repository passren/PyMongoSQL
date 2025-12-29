# -*- coding: utf-8 -*-
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence, Union

from pymongo.errors import PyMongoError

from .error import DatabaseError, OperationalError, ProgrammingError, SqlSyntaxError
from .helper import SQLHelper
from .sql.delete_builder import DeleteExecutionPlan
from .sql.insert_builder import InsertExecutionPlan
from .sql.parser import SQLParser
from .sql.query_builder import QueryExecutionPlan
from .sql.update_builder import UpdateExecutionPlan

_logger = logging.getLogger(__name__)


@dataclass
class ExecutionContext:
    """Manages execution context for a single query"""

    query: str
    execution_mode: str = "standard"
    parameters: Optional[Union[Sequence[Any], Dict[str, Any]]] = None

    def __repr__(self) -> str:
        return f"ExecutionContext(mode={self.execution_mode}, " f"query={self.query})"


class ExecutionStrategy(ABC):
    """Abstract base class for query execution strategies"""

    @property
    @abstractmethod
    def execution_plan(self) -> Union[QueryExecutionPlan, InsertExecutionPlan]:
        """Name of the execution plan"""
        pass

    @abstractmethod
    def execute(
        self,
        context: ExecutionContext,
        connection: Any,
        parameters: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Execute query and return result set.

        Args:
            context: ExecutionContext with query and subquery info
            connection: MongoDB connection
            parameters: Sequence for positional (?) or Dict for named (:param) parameters

        Returns:
            command_result with query results
        """
        pass

    @abstractmethod
    def supports(self, context: ExecutionContext) -> bool:
        """Check if this strategy supports the given context"""
        pass


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

    def _execute_execution_plan(
        self,
        execution_plan: QueryExecutionPlan,
        db: Any,
        parameters: Optional[Sequence[Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Execute a QueryExecutionPlan against MongoDB using db.command"""
        try:
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

            # Execute find command directly
            result = db.command(find_command)

            # Create command result
            return result

        except PyMongoError as e:
            _logger.error(f"MongoDB command execution failed: {e}")
            raise DatabaseError(f"Command execution failed: {e}")
        except Exception as e:
            _logger.error(f"Unexpected error during command execution: {e}")
            raise OperationalError(f"Command execution error: {e}")

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

        return self._execute_execution_plan(self._execution_plan, connection.database, processed_params)


class InsertExecution(ExecutionStrategy):
    """Execution strategy for INSERT statements."""

    @property
    def execution_plan(self) -> InsertExecutionPlan:
        return self._execution_plan

    def supports(self, context: ExecutionContext) -> bool:
        return context.query.lstrip().upper().startswith("INSERT")

    def _parse_sql(self, sql: str) -> InsertExecutionPlan:
        try:
            parser = SQLParser(sql)
            plan = parser.get_execution_plan()

            if not isinstance(plan, InsertExecutionPlan):
                raise SqlSyntaxError("Expected INSERT execution plan")

            if not plan.validate():
                raise SqlSyntaxError("Generated insert plan is invalid")

            return plan
        except SqlSyntaxError:
            raise
        except Exception as e:
            _logger.error(f"SQL parsing failed: {e}")
            raise SqlSyntaxError(f"Failed to parse SQL: {e}")

    def _replace_placeholders(
        self,
        documents: Sequence[Dict[str, Any]],
        parameters: Optional[Union[Sequence[Any], Dict[str, Any]]],
        style: Optional[str],
    ) -> Sequence[Dict[str, Any]]:
        return SQLHelper.replace_placeholders_generic(documents, parameters, style)

    def _execute_execution_plan(
        self,
        execution_plan: InsertExecutionPlan,
        db: Any,
        parameters: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        try:
            if not execution_plan.collection:
                raise ProgrammingError("No collection specified in insert")

            docs = execution_plan.insert_documents or []
            docs = self._replace_placeholders(docs, parameters, execution_plan.parameter_style)

            command = {"insert": execution_plan.collection, "documents": docs}

            _logger.debug(f"Executing MongoDB insert command: {command}")

            return db.command(command)
        except PyMongoError as e:
            _logger.error(f"MongoDB insert failed: {e}")
            raise DatabaseError(f"Insert execution failed: {e}")
        except (ProgrammingError, DatabaseError, OperationalError):
            # Re-raise our own errors without wrapping
            raise
        except Exception as e:
            _logger.error(f"Unexpected error during insert execution: {e}")
            raise OperationalError(f"Insert execution error: {e}")

    def execute(
        self,
        context: ExecutionContext,
        connection: Any,
        parameters: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        _logger.debug(f"Using insert execution for query: {context.query[:100]}")

        self._execution_plan = self._parse_sql(context.query)

        return self._execute_execution_plan(self._execution_plan, connection.database, parameters)


class DeleteExecution(ExecutionStrategy):
    """Strategy for executing DELETE statements."""

    @property
    def execution_plan(self) -> Any:
        return self._execution_plan

    def supports(self, context: ExecutionContext) -> bool:
        return context.query.lstrip().upper().startswith("DELETE")

    def _parse_sql(self, sql: str) -> Any:
        try:
            parser = SQLParser(sql)
            plan = parser.get_execution_plan()

            if not isinstance(plan, DeleteExecutionPlan):
                raise SqlSyntaxError("Expected DELETE execution plan")

            if not plan.validate():
                raise SqlSyntaxError("Generated delete plan is invalid")

            return plan
        except SqlSyntaxError:
            raise
        except Exception as e:
            _logger.error(f"SQL parsing failed: {e}")
            raise SqlSyntaxError(f"Failed to parse SQL: {e}")

    def _execute_execution_plan(
        self,
        execution_plan: Any,
        db: Any,
        parameters: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        try:
            if not execution_plan.collection:
                raise ProgrammingError("No collection specified in delete")

            filter_conditions = execution_plan.filter_conditions or {}

            # Replace placeholders in filter if parameters provided
            if parameters and filter_conditions:
                filter_conditions = SQLHelper.replace_placeholders_generic(
                    filter_conditions, parameters, execution_plan.parameter_style
                )

            command = {"delete": execution_plan.collection, "deletes": [{"q": filter_conditions, "limit": 0}]}

            _logger.debug(f"Executing MongoDB delete command: {command}")

            return db.command(command)
        except PyMongoError as e:
            _logger.error(f"MongoDB delete failed: {e}")
            raise DatabaseError(f"Delete execution failed: {e}")
        except (ProgrammingError, DatabaseError, OperationalError):
            # Re-raise our own errors without wrapping
            raise
        except Exception as e:
            _logger.error(f"Unexpected error during delete execution: {e}")
            raise OperationalError(f"Delete execution error: {e}")

    def execute(
        self,
        context: ExecutionContext,
        connection: Any,
        parameters: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        _logger.debug(f"Using delete execution for query: {context.query[:100]}")

        self._execution_plan = self._parse_sql(context.query)

        return self._execute_execution_plan(self._execution_plan, connection.database, parameters)


class UpdateExecution(ExecutionStrategy):
    """Strategy for executing UPDATE statements."""

    @property
    def execution_plan(self) -> Any:
        return self._execution_plan

    def supports(self, context: ExecutionContext) -> bool:
        return context.query.lstrip().upper().startswith("UPDATE")

    def _parse_sql(self, sql: str) -> Any:
        try:
            parser = SQLParser(sql)
            plan = parser.get_execution_plan()

            if not isinstance(plan, UpdateExecutionPlan):
                raise SqlSyntaxError("Expected UPDATE execution plan")

            if not plan.validate():
                raise SqlSyntaxError("Generated update plan is invalid")

            return plan
        except SqlSyntaxError:
            raise
        except Exception as e:
            _logger.error(f"SQL parsing failed: {e}")
            raise SqlSyntaxError(f"Failed to parse SQL: {e}")

    def _execute_execution_plan(
        self,
        execution_plan: Any,
        db: Any,
        parameters: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        try:
            if not execution_plan.collection:
                raise ProgrammingError("No collection specified in update")

            if not execution_plan.update_fields:
                raise ProgrammingError("No fields to update specified")

            filter_conditions = execution_plan.filter_conditions or {}
            update_fields = execution_plan.update_fields or {}

            # Replace placeholders if parameters provided
            # Note: We need to replace both update_fields and filter_conditions in one pass
            # to maintain correct parameter ordering (SET clause first, then WHERE clause)
            if parameters:
                # Combine structures for replacement in correct order
                combined = {"update_fields": update_fields, "filter_conditions": filter_conditions}
                replaced = SQLHelper.replace_placeholders_generic(combined, parameters, execution_plan.parameter_style)
                update_fields = replaced["update_fields"]
                filter_conditions = replaced["filter_conditions"]

            # MongoDB update command format
            # https://www.mongodb.com/docs/manual/reference/command/update/
            command = {
                "update": execution_plan.collection,
                "updates": [
                    {
                        "q": filter_conditions,  # query filter
                        "u": {"$set": update_fields},  # update document using $set operator
                        "multi": True,  # update all matching documents (like SQL UPDATE)
                        "upsert": False,  # don't insert if no match
                    }
                ],
            }

            _logger.debug(f"Executing MongoDB update command: {command}")

            return db.command(command)
        except PyMongoError as e:
            _logger.error(f"MongoDB update failed: {e}")
            raise DatabaseError(f"Update execution failed: {e}")
        except (ProgrammingError, DatabaseError, OperationalError):
            # Re-raise our own errors without wrapping
            raise
        except Exception as e:
            _logger.error(f"Unexpected error during update execution: {e}")
            raise OperationalError(f"Update execution error: {e}")

    def execute(
        self,
        context: ExecutionContext,
        connection: Any,
        parameters: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        _logger.debug(f"Using update execution for query: {context.query[:100]}")

        self._execution_plan = self._parse_sql(context.query)

        return self._execute_execution_plan(self._execution_plan, connection.database, parameters)


class ExecutionPlanFactory:
    """Factory for creating appropriate execution strategy based on query context"""

    _strategies = [StandardQueryExecution(), InsertExecution(), UpdateExecution(), DeleteExecution()]

    @classmethod
    def get_strategy(cls, context: ExecutionContext) -> ExecutionStrategy:
        """Get appropriate execution strategy for context"""
        for strategy in cls._strategies:
            if strategy.supports(context):
                _logger.debug(f"Selected strategy: {strategy.__class__.__name__}")
                return strategy

        # Fallback to standard execution
        return StandardQueryExecution()

    @classmethod
    def register_strategy(cls, strategy: ExecutionStrategy) -> None:
        """
        Register a custom execution strategy.

        Args:
            strategy: ExecutionStrategy instance
        """
        cls._strategies.append(strategy)
        _logger.debug(f"Registered strategy: {strategy.__class__.__name__}")
