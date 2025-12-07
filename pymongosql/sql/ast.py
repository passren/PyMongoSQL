# -*- coding: utf-8 -*-
from pymongosql.sql.base import MongoSQL
from pymongosql.sql.dql import MongoQuery
from .partiql.PartiQLLexer import PartiQLLexer
from .partiql.PartiQLParser import PartiQLParser
from .partiql.PartiQLParserVisitor import PartiQLParserVisitor


class MongoSQLLexer(PartiQLLexer): ...


class MongoSQLParser(PartiQLParser): ...


class MongoSQLParserVisitor(PartiQLParserVisitor):

    def __init__(self) -> None:
        super().__init__()
        self._mongo_sql: MongoSQL = None

    @property
    def mongo_sql(self) -> MongoSQL:
        return self._mongo_sql

    def visitRoot(self, ctx: PartiQLParser.RootContext):

        return self.visitChildren(ctx)

    def visitDql(self, ctx: PartiQLParser.DqlContext):
        self._mongo_sql = MongoQuery()
        return self.visitChildren(ctx)

    def visitSelectAll(self, ctx: PartiQLParser.SelectAllContext):
        return super().visitSelectAll(ctx)

    def visitProjectionItem(self, ctx: PartiQLParser.ProjectionItemContext):
        return self.visitChildren(ctx)

    def visitSelectItems(self, ctx: PartiQLParser.SelectItemsContext):
        projection = {}

        for item in ctx.projectionItems().projectionItem():
            projection[item.getText()] = 1
        self._mongo_sql.projection = projection

    def visitFromClause(self, ctx: PartiQLParser.FromClauseContext):
        self._mongo_sql.collection = ctx.tableReference().getText()

    def visitWhereClauseSelect(self, ctx: PartiQLParser.WhereClauseSelectContext):
        self._mongo_sql.filter = ctx.exprSelect().getText()
