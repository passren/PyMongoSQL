# -*- coding: utf-8 -*-
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple, TypeVar

from pymongo.cursor import Cursor as MongoCursor
from pymongo.errors import PyMongoError

from .common import BaseCursor, CursorIterator
from .error import DatabaseError, OperationalError, ProgrammingError, SqlSyntaxError
from .result_set import ResultSet
from .sql.builder import ExecutionPlan
from .sql.parser import SQLParser

if TYPE_CHECKING:
    from .connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore
_T = TypeVar("_T", bound="Cursor")


class Cursor(BaseCursor, CursorIterator):
    """SQL-compatible cursor that translates SQL to MongoDB operations"""

    NO_RESULT_SET = "No result set."

    def __init__(self, connection: "Connection", **kwargs) -> None:
        super().__init__(
            connection=connection,
            **kwargs,
        )
        self._kwargs = kwargs
        self._result_set: Optional[ResultSet] = None
        self._result_set_class = ResultSet
        self._current_execution_plan: Optional[ExecutionPlan] = None
        self._mongo_cursor: Optional[MongoCursor] = None
        self._is_closed = False

    @property
    def result_set(self) -> Optional[ResultSet]:
        return self._result_set

    @result_set.setter
    def result_set(self, val: ResultSet) -> None:
        self._result_set = val

    @property
    def has_result_set(self) -> bool:
        return self._result_set is not None

    @property
    def result_set_class(self) -> Optional[type]:
        return self._result_set_class

    @result_set_class.setter
    def result_set_class(self, val: type) -> None:
        self._result_set_class = val

    @property
    def rowcount(self) -> int:
        return self._result_set.rowcount if self._result_set else -1

    @property
    def rownumber(self) -> Optional[int]:
        return self._result_set.rownumber if self._result_set else None

    @property
    def description(
        self,
    ) -> Optional[List[Tuple[str, str, None, None, None, None, None]]]:
        return self._result_set.description if self._result_set else None

    @property
    def errors(self) -> List[Dict[str, str]]:
        return self._result_set.errors if self._result_set else []

    def _check_closed(self) -> None:
        """Check if cursor is closed"""
        if self._is_closed:
            raise ProgrammingError("Cursor is closed")

    def _parse_sql(self, sql: str) -> ExecutionPlan:
        """Parse SQL statement and return ExecutionPlan"""
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

    def _execute_execution_plan(self, execution_plan: ExecutionPlan) -> None:
        """Execute an ExecutionPlan against MongoDB using db.command"""
        try:
            # Get database
            if not execution_plan.collection:
                raise ProgrammingError("No collection specified in query")

            db = self.connection.database

            # Build MongoDB find command
            find_command = {"find": execution_plan.collection, "filter": execution_plan.filter_stage or {}}

            # Apply projection if specified (already in MongoDB format)
            if execution_plan.projection_stage:
                find_command["projection"] = execution_plan.projection_stage

            # Apply sort if specified
            if execution_plan.sort_stage:
                sort_spec = {}
                for sort_dict in execution_plan.sort_stage:
                    for field, direction in sort_dict.items():
                        sort_spec[field] = direction
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

            # Create result set from command result
            self._result_set = self._result_set_class(
                command_result=result, execution_plan=execution_plan, **self._kwargs
            )

            _logger.info(f"Query executed successfully on collection '{execution_plan.collection}'")

        except PyMongoError as e:
            _logger.error(f"MongoDB command execution failed: {e}")
            raise DatabaseError(f"Command execution failed: {e}")
        except Exception as e:
            _logger.error(f"Unexpected error during command execution: {e}")
            raise OperationalError(f"Command execution error: {e}")

    def execute(self: _T, operation: str, parameters: Optional[Dict[str, Any]] = None) -> _T:
        """Execute a SQL statement

        Args:
            operation: SQL statement to execute
            parameters: Parameters for the SQL statement (not yet implemented)

        Returns:
            Self for method chaining
        """
        self._check_closed()

        if parameters:
            _logger.warning("Parameter substitution not yet implemented, ignoring parameters")

        try:
            # Parse SQL to ExecutionPlan
            self._current_execution_plan = self._parse_sql(operation)

            # Execute the execution plan
            self._execute_execution_plan(self._current_execution_plan)

            return self

        except (SqlSyntaxError, DatabaseError, OperationalError, ProgrammingError):
            # Re-raise known errors
            raise
        except Exception as e:
            _logger.error(f"Unexpected error during execute: {e}")
            raise DatabaseError(f"Execute failed: {e}")

    def executemany(
        self,
        operation: str,
        seq_of_parameters: List[Optional[Dict[str, Any]]],
    ) -> None:
        """Execute a SQL statement multiple times with different parameters

        Note: This is not yet fully implemented for MongoDB operations
        """
        self._check_closed()

        # For now, just execute once and ignore parameters
        _logger.warning("executemany not fully implemented, executing once without parameters")
        self.execute(operation)

    def execute_transaction(self) -> None:
        """Execute transaction (MongoDB has limited transaction support)"""
        self._check_closed()

        # MongoDB transactions are complex and require specific setup
        # For now, this is a placeholder
        raise NotImplementedError("Transaction support not yet implemented")

    def flush(self) -> None:
        """Flush any pending operations"""
        # In MongoDB context, this might involve ensuring writes are acknowledged
        # For now, this is a no-op
        pass

    def fetchone(self) -> Optional[Sequence[Any]]:
        """Fetch the next row from the result set"""
        self._check_closed()

        if not self.has_result_set:
            raise ProgrammingError(self.NO_RESULT_SET)

        return self._result_set.fetchone()

    def fetchmany(self, size: Optional[int] = None) -> List[Sequence[Any]]:
        """Fetch multiple rows from the result set"""
        self._check_closed()

        if not self.has_result_set:
            raise ProgrammingError(self.NO_RESULT_SET)

        return self._result_set.fetchmany(size)

    def fetchall(self) -> List[Sequence[Any]]:
        """Fetch all remaining rows from the result set"""
        self._check_closed()

        if not self.has_result_set:
            raise ProgrammingError(self.NO_RESULT_SET)

        return self._result_set.fetchall()

    def close(self) -> None:
        """Close the cursor and free resources"""
        try:
            if self._mongo_cursor:
                # Close MongoDB cursor
                try:
                    self._mongo_cursor.close()
                except Exception as e:
                    _logger.warning(f"Error closing MongoDB cursor: {e}")
                finally:
                    self._mongo_cursor = None

            if self._result_set:
                # Close result set
                try:
                    self._result_set.close()
                except Exception as e:
                    _logger.warning(f"Error closing result set: {e}")
                finally:
                    self._result_set = None

            self._is_closed = True

            # Remove from connection's cursor pool
            try:
                self.connection.cursor_pool.remove(self)
            except (ValueError, AttributeError):
                pass  # Cursor not in pool or connection gone

            _logger.debug("Cursor closed successfully")

        except Exception as e:
            _logger.error(f"Error during cursor close: {e}")

    def __del__(self):
        """Destructor to ensure resources are cleaned up"""
        if not self._is_closed:
            try:
                self.close()
            except Exception:
                pass  # Ignore errors during cleanup
