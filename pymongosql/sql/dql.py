# -*- coding: utf-8 -*-
import logging
from .ast import BaseParser
from .base import Command

_logger = logging.getLogger(__name__)  # type: ignore

class DqlTransformer:
    def __init__(self, context: BaseParser.QueryDqlContext):
        self._context = context
        

    def transform(self) -> Command:
        _logger.debug(f"Transforming DQL query: {self._context.getText()}")

        return None