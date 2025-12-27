# -*- coding: utf-8 -*-
"""
Connection helper utilities for PyMongoSQL.

Handles connection string parsing and mode detection.
"""

import logging
from typing import Optional, Tuple
from urllib.parse import parse_qs, urlparse

_logger = logging.getLogger(__name__)


class ConnectionHelper:
    """Helper class for connection string parsing and mode detection.

    Supports connection string patterns:
    - mongodb://host:port/database - Core driver (no subquery support)
    - mongodb+srv://host:port/database - Cloud/SRV connection string
    - mongodb://host:port/database?mode=superset - Superset driver with subquery support
    - mongodb+srv://host:port/database?mode=superset - Cloud SRV with superset mode

    Mode is specified via query parameter (?mode=superset) and defaults to "standard" if not specified.
    """

    @staticmethod
    def parse_connection_string(connection_string: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Parse MongoDB connection string and extract driver mode from query parameters.

        Mode is extracted from the 'mode' query parameter and removed from the normalized
        connection string. Database name is extracted from the path. If mode is not specified,
        it defaults to "standard".

        Supports all standard MongoDB connection string patterns:
        mongodb://[username:password@]host1[:port1][,host2[:port2]...][/[defaultauthdb]?options]

        Args:
            connection_string: MongoDB connection string

        Returns:
            Tuple of (mode, database_name, normalized_connection_string)
            - mode: "standard" (default) or other mode values specified via ?mode= parameter
            - database_name: extracted database name from path, or None if not specified
            - normalized_connection_string: connection string without the mode parameter
        """
        try:
            if not connection_string:
                return "standard", None, None

            parsed = urlparse(connection_string)

            if not parsed.scheme:
                return "standard", None, connection_string

            # Extract mode from query parameters (defaults to "standard" if not specified)
            query_params = parse_qs(parsed.query, keep_blank_values=True) if parsed.query else {}
            mode = query_params.get("mode", ["standard"])[0]

            # Extract database name from path
            database_name = None
            if parsed.path:
                # Remove leading slash and trailing slashes
                path_parts = parsed.path.strip("/").split("/")
                if path_parts and path_parts[0]:  # Get the first path segment as database name
                    database_name = path_parts[0]

            # Remove mode from query parameters
            query_params.pop("mode", None)

            # Rebuild query string without mode parameter
            query_string = (
                "&".join(f"{k}={v}" if v else k for k, v_list in query_params.items() for v in v_list)
                if query_params
                else ""
            )

            # Reconstruct the connection string without mode parameter
            if query_string:
                if parsed.path:
                    normalized_connection_string = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{query_string}"
                else:
                    normalized_connection_string = f"{parsed.scheme}://{parsed.netloc}?{query_string}"
            else:
                if parsed.path:
                    normalized_connection_string = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                else:
                    normalized_connection_string = f"{parsed.scheme}://{parsed.netloc}"

            _logger.debug(f"Parsed connection string - Mode: {mode}, Database: {database_name}")

            return mode, database_name, normalized_connection_string

        except Exception as e:
            _logger.error(f"Failed to parse connection string: {e}")
            raise ValueError(f"Invalid connection string format: {e}")
