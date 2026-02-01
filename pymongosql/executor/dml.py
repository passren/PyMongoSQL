# -*- coding: utf-8 -*-
import logging
from typing import Any, Dict, Optional, Sequence, Union

from pymongo.errors import PyMongoError

from ..error import DatabaseError, OperationalError, ProgrammingError, SqlSyntaxError
from ..helper import SQLHelper
from ..sql.delete_builder import DeleteExecutionPlan
from ..sql.insert_builder import InsertExecutionPlan
from ..sql.parser import SQLParser
from ..sql.update_builder import UpdateExecutionPlan
from .base import ExecutionContext, ExecutionStrategy

_logger = logging.getLogger(__name__)


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
        connection: Any = None,
        parameters: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        try:
            # Get database from connection
            if not connection:
                raise OperationalError("No connection provided")

            db = connection.database

            if not execution_plan.collection:
                raise ProgrammingError("No collection specified in insert")

            docs = execution_plan.insert_documents or []
            docs = self._replace_placeholders(docs, parameters, execution_plan.parameter_style)

            command = {"insert": execution_plan.collection, "documents": docs}

            _logger.debug(f"Executing MongoDB insert command: {command}")

            # Execute with session if in transaction
            if connection and connection.session and connection.session.in_transaction:
                return db.command(command, session=connection.session)
            else:
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

        return self._execute_execution_plan(self._execution_plan, connection, parameters)


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
        connection: Any = None,
        parameters: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        try:
            # Get database from connection
            if not connection:
                raise OperationalError("No connection provided")

            db = connection.database

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

            # Execute with session if in transaction
            if connection and connection.session and connection.session.in_transaction:
                return db.command(command, session=connection.session)
            else:
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

        return self._execute_execution_plan(self._execution_plan, connection, parameters)


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
        connection: Any = None,
        parameters: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        try:
            # Get database from connection
            if not connection:
                raise OperationalError("No connection provided")

            db = connection.database

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

            # Execute with session if in transaction
            if connection and connection.session and connection.session.in_transaction:
                return db.command(command, session=connection.session)
            else:
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

        return self._execute_execution_plan(self._execution_plan, connection, parameters)
