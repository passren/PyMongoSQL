# -*- coding: utf-8 -*-
from .partiql.PartiQLLexer import PartiQLLexer
from .partiql.PartiQLParser import PartiQLParser
from .partiql.PartiQLParserVisitor import PartiQLParserVisitor

class MongoSQLLexer(PartiQLLexer):
    ...


class MongoSQLParser(PartiQLParser):
    ...


class MongoSQLParserVisitor(PartiQLParserVisitor):
    def visitRoot(self, ctx:PartiQLParser.RootContext):
        print("Visiting root")
        return self.visitChildren(ctx)

    def visitDql(self, ctx:PartiQLParser.DqlContext):
        return self.visitChildren(ctx)
