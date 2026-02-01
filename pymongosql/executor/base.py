# -*- coding: utf-8 -*-
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Union

from ..sql.insert_builder import InsertExecutionPlan
from ..sql.query_builder import QueryExecutionPlan

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


class ExecutionPlanFactory:
    """Factory for creating appropriate execution strategy based on query context"""

    _strategies: Optional[List[ExecutionStrategy]] = None

    @classmethod
    def _ensure_strategies(cls) -> None:
        if cls._strategies is None:
            from .dml import DeleteExecution, InsertExecution, UpdateExecution
            from .dql import StandardQueryExecution

            cls._strategies = [StandardQueryExecution(), InsertExecution(), UpdateExecution(), DeleteExecution()]

    @classmethod
    def get_strategy(cls, context: ExecutionContext) -> ExecutionStrategy:
        """Get appropriate execution strategy for context"""
        cls._ensure_strategies()

        for strategy in cls._strategies or []:
            if strategy.supports(context):
                _logger.debug(f"Selected strategy: {strategy.__class__.__name__}")
                return strategy

        from .dql import StandardQueryExecution

        return StandardQueryExecution()

    @classmethod
    def register_strategy(cls, strategy: ExecutionStrategy) -> None:
        """
        Register a custom execution strategy.

        Args:
            strategy: ExecutionStrategy instance
        """
        cls._ensure_strategies()
        if cls._strategies is None:
            cls._strategies = []
        cls._strategies.append(strategy)
        _logger.debug(f"Registered strategy: {strategy.__class__.__name__}")
