# -*- coding: utf-8 -*-
import logging
from typing import Any

_logger = logging.getLogger(__name__)  # type: ignore

class Command(object):
    def __init__(self, command: Any, value: Any, type: str) -> None:
        self._command: Any = command
        self._value: Any = value
        self._type: str = type

    @property
    def command(self) -> Any:
        return self._command
    
    @property
    def value(self) -> Any:
        return self._value
    
    @property
    def type(self) -> str:
        return self._type
