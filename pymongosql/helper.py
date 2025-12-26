# -*- coding: utf-8 -*-
"""
Connection helper utilities for PyMongoSQL.

Handles connection string parsing and mode detection.
"""

import logging
from typing import Optional, Tuple
from urllib.parse import urlparse

_logger = logging.getLogger(__name__)


class ConnectionHelper:
    """Helper class for connection string parsing and mode detection.

    Supports connection string patterns:
    - mongodb://host:port/database - Core driver (no subquery support)
    - mongodb+superset://host:port/database - Superset driver with subquery support
    """

    @staticmethod
    def parse_connection_string(connection_string: str) -> Tuple[str, str, Optional[str], int, Optional[str]]:
        """
        Parse PyMongoSQL connection string and determine driver mode.
        """
        try:
            if not connection_string:
                return "standard", None

            parsed = urlparse(connection_string)
            scheme = parsed.scheme

            if not parsed.scheme:
                return "standard", connection_string

            base_scheme = "mongodb"
            mode = "standard"

            # Determine mode from scheme
            if "+" in scheme:
                base_scheme = scheme.split("+")[0].lower()
                mode = scheme.split("+")[-1].lower()

            host = parsed.hostname or "localhost"
            port = parsed.port or 27017
            database = parsed.path.lstrip("/") if parsed.path else None

            # Build normalized connection string with mongodb scheme (removing any +mode)
            # Reconstruct netloc with credentials if present
            netloc = host
            if parsed.username:
                creds = parsed.username
                if parsed.password:
                    creds += f":{parsed.password}"
                netloc = f"{creds}@{host}"
            netloc += f":{port}"

            query_part = f"?{parsed.query}" if parsed.query else ""
            normalized_connection_string = f"{base_scheme}://{netloc}/{database or ''}{query_part}"

            _logger.debug(f"Parsed connection string - Mode: {mode}, Host: {host}, Port: {port}, Database: {database}")

            return mode, normalized_connection_string

        except Exception as e:
            _logger.error(f"Failed to parse connection string: {e}")
            raise ValueError(f"Invalid connection string format: {e}")
