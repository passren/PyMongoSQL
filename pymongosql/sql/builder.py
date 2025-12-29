# -*- coding: utf-8 -*-
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

_logger = logging.getLogger(__name__)


@dataclass
class ExecutionPlan:
    """Base class for execution plans (query, insert, etc.).

    Provides common attributes and shared validation helpers.
    """

    collection: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert plan to a serializable dictionary. Must be implemented by subclasses."""
        raise NotImplementedError()

    def validate_base(self) -> list[str]:
        """Common validation checks for all plans.

        Returns a list of error messages for the caller to aggregate and log.
        """
        errors: list[str] = []
        if not self.collection:
            errors.append("Collection name is required")
        return errors


class BuilderFactory:
    """Factory for creating builders for different operations."""

    @staticmethod
    def create_query_builder():
        """Create a builder for SELECT queries"""
        # Local import to avoid circular dependency during module import
        from .query_builder import MongoQueryBuilder

        return MongoQueryBuilder()

    @staticmethod
    def create_insert_builder():
        """Create a builder for INSERT queries"""
        # Local import to avoid circular dependency during module import
        from .insert_builder import MongoInsertBuilder

        return MongoInsertBuilder()

    @staticmethod
    def create_delete_builder():
        """Create a builder for DELETE queries"""
        # Local import to avoid circular dependency during module import
        from .delete_builder import MongoDeleteBuilder

        return MongoDeleteBuilder()

    @staticmethod
    def create_update_builder():
        """Create a builder for UPDATE queries"""
        # Local import to avoid circular dependency during module import
        from .update_builder import MongoUpdateBuilder

        return MongoUpdateBuilder()


__all__ = [
    "ExecutionPlan",
    "BuilderFactory",
]
