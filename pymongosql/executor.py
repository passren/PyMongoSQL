# -*- coding: utf-8 -*-
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence, Union

from pymongo.errors import PyMongoError

from .error import DatabaseError, OperationalError, ProgrammingError, SqlSyntaxError
from .sql.parser import SQLParser
from .sql.query_builder import QueryExecutionPlan

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
    def execution_plan(self) -> QueryExecutionPlan:
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


class StandardExecution(ExecutionStrategy):
    """Standard execution strategy for simple SELECT queries without subqueries"""

    @property
    def execution_plan(self) -> QueryExecutionPlan:
        """Return standard execution plan"""
        return self._execution_plan

    def supports(self, context: ExecutionContext) -> bool:
        """Support simple queries without subqueries"""
        return "standard" in context.execution_mode.lower()

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
        param_index = [0]  # Use list to allow modification in nested function

        def replace_recursive(value: Any) -> Any:
            if isinstance(value, str):
                # Replace ? with the next parameter value
                if value == "?":
                    if param_index[0] < len(parameters):
                        result = parameters[param_index[0]]
                        param_index[0] += 1
                        return result
                    else:
                        raise ProgrammingError(
                            f"Not enough parameters provided: expected at least {param_index[0] + 1}"
                        )
                return value
            elif isinstance(value, dict):
                return {k: replace_recursive(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [replace_recursive(item) for item in value]
            else:
                return value

        return replace_recursive(obj)

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


class ExecutionPlanFactory:
    """Factory for creating appropriate execution strategy based on query context"""

    _strategies = [StandardExecution()]

    @classmethod
    def get_strategy(cls, context: ExecutionContext) -> ExecutionStrategy:
        """Get appropriate execution strategy for context"""
        for strategy in cls._strategies:
            if strategy.supports(context):
                _logger.debug(f"Selected strategy: {strategy.__class__.__name__}")
                return strategy

        # Fallback to standard execution
        return StandardExecution()

    @classmethod
    def register_strategy(cls, strategy: ExecutionStrategy) -> None:
        """
        Register a custom execution strategy.

        Args:
            strategy: ExecutionStrategy instance
        """
        cls._strategies.append(strategy)
        _logger.debug(f"Registered strategy: {strategy.__class__.__name__}")
