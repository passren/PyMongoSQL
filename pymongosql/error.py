# -*- coding: utf-8 -*-
__all__ = [
    "Error",
    "Warning",
    "SqlSyntaxError",
    "InterfaceError",
    "DatabaseError",
    "InternalError",
    "OperationalError",
    "ProgrammingError",
    "DataError",
    "NotSupportedError",
]


class Error(Exception): ...


class Warning(Exception): ...


class SqlSyntaxError(Error): ...


class InterfaceError(Error): ...


class DatabaseError(Error): ...


class InternalError(DatabaseError): ...


class OperationalError(DatabaseError): ...


class ProgrammingError(DatabaseError): ...


class IntegrityError(DatabaseError): ...


class DataError(DatabaseError): ...


class NotSupportedError(DatabaseError): ...
