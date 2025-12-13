# -*- coding: utf-8 -*-
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Iterator, Union

from pymongo.cursor import Cursor as MongoCursor
from pymongo.errors import PyMongoError

from .error import ProgrammingError, DatabaseError
from .common import CursorIterator
from .sql.builder import QueryPlan

if TYPE_CHECKING:
    from .connection import Connection

_logger = logging.getLogger(__name__)


class ResultSet(CursorIterator):
    """Result set wrapper for MongoDB cursor results"""

    def __init__(
        self,
        mongo_cursor: MongoCursor,
        query_plan: QueryPlan,
        arraysize: int = None,
        **kwargs,
    ) -> None:
        super().__init__(arraysize=arraysize or self.DEFAULT_FETCH_SIZE, **kwargs)
        self._mongo_cursor = mongo_cursor
        self._query_plan = query_plan
        self._is_closed = False
        self._cached_results: List[Dict[str, Any]] = []
        self._cache_exhausted = False
        self._total_fetched = 0
        self._description: Optional[
            List[Tuple[str, str, None, None, None, None, None]]
        ] = None
        self._errors: List[Dict[str, str]] = []

        # Build description from projection
        self._build_description()

    def _build_description(self) -> None:
        """Build column description from query plan projection"""
        if not self._query_plan.projection_stage:
            # No projection specified, description will be built dynamically
            self._description = None
            return

        # Build description from projection
        description = []
        for field_name, alias in self._query_plan.projection_stage.items():
            # SQL cursor description format: (name, type_code, display_size, internal_size, precision, scale, null_ok)
            column_name = alias if alias != field_name else field_name
            description.append((column_name, "VARCHAR", None, None, None, None, None))

        self._description = description

    def _ensure_results_available(self, count: int = 1) -> None:
        """Ensure we have at least 'count' results available in cache"""
        if self._is_closed:
            raise ProgrammingError("ResultSet is closed")

        if self._cache_exhausted:
            return

        # Fetch more results if needed
        while len(self._cached_results) < count and not self._cache_exhausted:
            try:
                # Iterate through cursor without calling limit() again
                batch = []
                for i, doc in enumerate(self._mongo_cursor):
                    if i >= self.arraysize:
                        break
                    batch.append(doc)

                if not batch:
                    self._cache_exhausted = True
                    break

                # Process results through projection mapping
                processed_batch = [self._process_document(doc) for doc in batch]
                self._cached_results.extend(processed_batch)
                self._total_fetched += len(batch)

            except PyMongoError as e:
                self._errors.append({"error": str(e), "type": type(e).__name__})
                raise DatabaseError(f"Error fetching results: {e}")

    def _process_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Process a MongoDB document according to projection mapping"""
        if not self._query_plan.projection_stage:
            # No projection, return document as-is (including _id)
            return dict(doc)

        # Apply projection mapping
        processed = {}
        for field_name, alias in self._query_plan.projection_stage.items():
            if field_name in doc:
                output_name = alias if alias != field_name else field_name
                processed[output_name] = doc[field_name]
            elif field_name != "_id":  # _id might be excluded by MongoDB
                # Field not found, set to None
                output_name = alias if alias != field_name else field_name
                processed[output_name] = None

        return processed

    @property
    def errors(self) -> List[Dict[str, str]]:
        return self._errors.copy()

    @property
    def rowcount(self) -> int:
        """Return number of rows fetched so far (not total available)"""
        return self._total_fetched

    @property
    def description(
        self,
    ) -> Optional[List[Tuple[str, str, None, None, None, None, None]]]:
        """Return column description"""
        if self._description is None and not self._cache_exhausted:
            # Try to fetch one result to build description dynamically
            try:
                self._ensure_results_available(1)
                if self._cached_results:
                    # Build description from first result
                    first_result = self._cached_results[0]
                    self._description = [
                        (col_name, "VARCHAR", None, None, None, None, None)
                        for col_name in first_result.keys()
                    ]
            except Exception as e:
                _logger.warning(f"Could not build dynamic description: {e}")

        return self._description

    def fetchone(self) -> Optional[Dict[str, Any]]:
        """Fetch the next row from the result set"""
        if self._is_closed:
            raise ProgrammingError("ResultSet is closed")

        # Ensure we have at least one result
        self._ensure_results_available(1)

        if not self._cached_results:
            return None

        # Return and remove first result
        result = self._cached_results.pop(0)
        self._rownumber = (self._rownumber or 0) + 1
        return result

    def fetchmany(self, size: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch up to 'size' rows from the result set"""
        if self._is_closed:
            raise ProgrammingError("ResultSet is closed")

        fetch_size = size or self.arraysize

        # Ensure we have enough results
        self._ensure_results_available(fetch_size)

        # Return requested number of results
        results = self._cached_results[:fetch_size]
        self._cached_results = self._cached_results[fetch_size:]

        # Update row number
        self._rownumber = (self._rownumber or 0) + len(results)

        return results

    def fetchall(self) -> List[Dict[str, Any]]:
        """Fetch all remaining rows from the result set"""
        if self._is_closed:
            raise ProgrammingError("ResultSet is closed")

        # Fetch all remaining results
        all_results = []

        # Add cached results
        all_results.extend(self._cached_results)
        self._cached_results.clear()

        # Fetch remaining from cursor
        try:
            if not self._cache_exhausted:
                # Iterate through all remaining documents in the cursor
                remaining_docs = list(self._mongo_cursor)
                if remaining_docs:
                    # Process results through projection mapping
                    processed_docs = [
                        self._process_document(doc) for doc in remaining_docs
                    ]
                    all_results.extend(processed_docs)
                    self._total_fetched += len(remaining_docs)

                self._cache_exhausted = True

        except PyMongoError as e:
            self._errors.append({"error": str(e), "type": type(e).__name__})
            raise DatabaseError(f"Error fetching all results: {e}")

        # Update row number
        self._rownumber = (self._rownumber or 0) + len(all_results)

        return all_results

    @property
    def is_closed(self) -> bool:
        return self._is_closed

    def close(self) -> None:
        """Close the result set and free resources"""
        if not self._is_closed:
            try:
                if self._mongo_cursor:
                    self._mongo_cursor.close()
            except Exception as e:
                _logger.warning(f"Error closing MongoDB cursor: {e}")
            finally:
                self._is_closed = True
                self._mongo_cursor = None
                self._cached_results.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# For backward compatibility
MongoResultSet = ResultSet
