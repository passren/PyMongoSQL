# -*- coding: utf-8 -*-
from pymongosql.sql.parser import SQLParser

class TestDqlParser:
    
    def test_dql_parser_select(self):
        sql = "SELECT name, age FROM users WHERE age > 30"
        executor = SQLParser(sql).get_executor()
        assert executor.type == "QUERY"
