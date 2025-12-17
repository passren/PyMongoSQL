# -*- coding: utf-8 -*-
import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pymongo.cursor import Cursor as MongoCursor
from pymongo.errors import PyMongoError

from .common import CursorIterator
from .error import DatabaseError, ProgrammingError
from .sql.builder import ExecutionPlan

_logger = logging.getLogger(__name__)


class ResultSet(CursorIterator):
    """Result set wrapper for MongoDB command results"""

    def __init__(
        self,
        command_result: Optional[Dict[str, Any]] = None,
        mongo_cursor: Optional[MongoCursor] = None,
        execution_plan: ExecutionPlan = None,
        arraysize: int = None,
        **kwargs,
    ) -> None:
        super().__init__(arraysize=arraysize or self.DEFAULT_FETCH_SIZE, **kwargs)

        # Handle both command results and legacy mongo cursor for backward compatibility
        if command_result is not None:
            self._command_result = command_result
            self._mongo_cursor = None
            # Extract cursor info from command result
            self._result_cursor = command_result.get("cursor", {})
            self._raw_results = self._result_cursor.get("firstBatch", [])
            self._cached_results: List[Sequence[Any]] = []
        elif mongo_cursor is not None:
            self._mongo_cursor = mongo_cursor
            self._command_result = None
            self._raw_results = []
            self._cached_results: List[Sequence[Any]] = []
        else:
            raise ProgrammingError("Either command_result or mongo_cursor must be provided")

        self._execution_plan = execution_plan
        self._is_closed = False
        self._cache_exhausted = False
        self._total_fetched = 0
        self._description: Optional[List[Tuple[str, str, None, None, None, None, None]]] = None
        self._column_names: Optional[List[str]] = None  # Track column order for sequences
        self._errors: List[Dict[str, str]] = []

        # Process firstBatch immediately if available (after all attributes are set)
        if command_result is not None and self._raw_results:
            processed_batch = [self._process_document(doc) for doc in self._raw_results]
            # Convert dictionaries to sequences for DB API 2.0 compliance
            sequence_batch = [self._dict_to_sequence(doc) for doc in processed_batch]
            self._cached_results.extend(sequence_batch)

        # Build description from projection
        self._build_description()

    def _build_description(self) -> None:
        """Build column description from execution plan projection"""
        if not self._execution_plan.projection_stage:
            # No projection specified, description will be built dynamically
            self._description = None
            return

        # Build description from projection (now in MongoDB format {field: 1})
        description = []
        for field_name, include_flag in self._execution_plan.projection_stage.items():
            # SQL cursor description format: (name, type_code, display_size, internal_size, precision, scale, null_ok)
            if include_flag == 1:  # Field is included in projection
                description.append((field_name, "VARCHAR", None, None, None, None, None))

        self._description = description

    def _ensure_results_available(self, count: int = 1) -> None:
        """Ensure we have at least 'count' results available in cache"""
        if self._is_closed:
            raise ProgrammingError("ResultSet is closed")

        if self._cache_exhausted:
            return

        if self._command_result is not None:
            # For command results, we already have all data in firstBatch
            # No additional fetching needed
            self._cache_exhausted = True
            return

        elif self._mongo_cursor is not None:
            # Fetch more results if needed (legacy mongo cursor support)
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
                    # Convert dictionaries to sequences for DB API 2.0 compliance
                    sequence_batch = [self._dict_to_sequence(doc) for doc in processed_batch]
                    self._cached_results.extend(sequence_batch)
                    self._total_fetched += len(batch)

                except PyMongoError as e:
                    self._errors.append({"error": str(e), "type": type(e).__name__})
                    raise DatabaseError(f"Error fetching results: {e}")

    def _process_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Process a MongoDB document according to projection mapping"""
        if not self._execution_plan.projection_stage:
            # No projection, return document as-is (including _id)
            return dict(doc)

        # Apply projection mapping (now using MongoDB format {field: 1})
        processed = {}
        for field_name, include_flag in self._execution_plan.projection_stage.items():
            if include_flag == 1:  # Field is included in projection
                if field_name in doc:
                    processed[field_name] = doc[field_name]
                elif field_name != "_id":  # _id might be excluded by MongoDB
                    # Field not found, set to None
                    processed[field_name] = None

        return processed

    def _dict_to_sequence(self, doc: Dict[str, Any]) -> Tuple[Any, ...]:
        """Convert document dictionary to sequence according to column order"""
        if self._column_names is None:
            # First time - establish column order
            self._column_names = list(doc.keys())

        # Return values in consistent column order
        return tuple(doc.get(col_name) for col_name in self._column_names)

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
                if self._column_names:
                    # Build description from established column names
                    self._description = [
                        (col_name, "VARCHAR", None, None, None, None, None) for col_name in self._column_names
                    ]
            except Exception as e:
                _logger.warning(f"Could not build dynamic description: {e}")

        return self._description

    def fetchone(self) -> Optional[Sequence[Any]]:
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

    def fetchmany(self, size: Optional[int] = None) -> List[Sequence[Any]]:
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

    def fetchall(self) -> List[Sequence[Any]]:
        """Fetch all remaining rows from the result set"""
        if self._is_closed:
            raise ProgrammingError("ResultSet is closed")

        # Fetch all remaining results
        all_results = []

        try:
            if self._command_result is not None:
                # Handle command result (db.command)
                if not self._cache_exhausted:
                    # Results are already processed in constructor, just extend
                    all_results.extend(self._cached_results)
                    self._total_fetched += len(self._cached_results)
                    self._cache_exhausted = True

            elif self._mongo_cursor is not None:
                # Handle legacy mongo cursor (for backward compatibility)
                # Add cached results
                all_results.extend(self._cached_results)
                self._cached_results.clear()

                # Fetch remaining from cursor
                if not self._cache_exhausted:
                    # Iterate through all remaining documents in the cursor
                    remaining_docs = list(self._mongo_cursor)
                    if remaining_docs:
                        # Process results through projection mapping
                        processed_docs = [self._process_document(doc) for doc in remaining_docs]
                        # Convert dictionaries to sequences for DB API 2.0 compliance
                        sequence_docs = [self._dict_to_sequence(doc) for doc in processed_docs]
                        all_results.extend(sequence_docs)
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
                # No special cleanup needed for command results
            except Exception as e:
                _logger.warning(f"Error closing MongoDB cursor: {e}")
            finally:
                self._is_closed = True
                self._mongo_cursor = None
                self._command_result = None
                self._cached_results.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# For backward compatibility
MongoResultSet = ResultSet
