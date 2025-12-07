# -*- coding: utf-8 -*-
from pymongosql.sql.parser import SQLParser

class TestDqlParser:
    
    def test_dql_parser_select(self):
        sql = "SELECT * FROM users WHERE age > 30"
        parser = SQLParser(sql)
        command = parser.get_command()
        assert command.type == "DQL"
        assert command.command == "SELECT"
        assert command.value == {
            "fields": ["*"],
            "table": "users",
            "condition": {"age": {">": 30}}
        }