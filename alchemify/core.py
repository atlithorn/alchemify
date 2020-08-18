import operator
from collections import defaultdict

from sqlalchemy import MetaData, Table, Column, ForeignKey
from sqlalchemy.sql import ClauseElement, select, cast
from sqlalchemy.sql.elements import Cast, Label
from sqlalchemy.sql.expression import BinaryExpression, UnaryExpression, literal, and_, or_
from sqlalchemy.types import Integer, String

from .grammar import select_parser, insert_parser, update_parser
from .grammar import SelectTransformer, TemplateTransformer, InsertTransformer, UpdateTransformer, DeleteTransformer

class Alchemify:

    def __init__(self, engine, metadata=None):
        self.engine = engine
        self.metadata = metadata if metadata else MetaData(bind=engine)
        assert(self.metadata.bind is not None), "Alchemify only works with bound metadata"
    
    def _tabularize(self, table):
        if type(table) == str:
            return Table(table, self.metadata, autoload=True)
        return table

    def _conditional_returning(self, table, parsed_query_string, result):
        """
        deal with returning - problem is that it's dialect specific
        so if it is supported just get the template and generate the output
        otherwise one could create a new select statement based off the returned id
        but who needs the hassle, just let the client handle it
        """
        if self.engine.dialect.implicit_returning:
            template = self.get_template(table, parsed_query_string=parsed_query_string)
            if template:
                return self.generate(template, result)
        return None
     
    def select_statement(self, table, query_string=None, parsed_query_string=None):
        if parsed_query_string is None:
            parsed_query_string = select_parser.parse(query_string)
        stmt = SelectTransformer(self._tabularize(table), self.metadata).transform(parsed_query_string)
        return stmt
    
    def insert_statement(self, table, rows, query_string=None, parsed_query_string=None):
        if parsed_query_string is None:
            parsed_query_string = insert_parser.parse(query_string)
        stmt = InsertTransformer(self._tabularize(table), self.metadata, rows).transform(parsed_query_string)
        return stmt

    def update_statement(self, table, rows, query_string=None, parsed_query_string=None):
        if parsed_query_string is None:
            parsed_query_string = insert_parser.parse(query_string)
        stmt = UpdateTransformer(self._tabularize(table), self.metadata, rows).transform(parsed_query_string)
        return stmt

    def delete_statement(self, table, query_string=None, parsed_query_string=None):
        # same dsl for update and delete
        if parsed_query_string is None:
            parsed_query_string = update_parser.parse(query_string)
        stmt = DeleteTransformer(self._tabularize(table), self.metadata).transform(parsed_query_string)
        return stmt

    def get_template(self, table, query_string=None, parsed_query_string=None):
        if parsed_query_string is None:
            parsed_query_string = select_parser.parse(query_string)
        template = TemplateTransformer(self._tabularize(table), self.metadata).transform(parsed_query_string)
        return template

    def generate(self, template, rows):
        output = list()
        for row in rows:
            # zip silently ignores this otherwise
            assert(len(row) == len(template)), "Template and rows are of unequal length"
            value = defaultdict(dict)
            for v, t in zip(row, template):
                if len(t) == 1: 
                    value[t[0]] = v
                else:
                    value[t[0]].update({t[1]:v})
            output.append(value)
        return output

    def select(self, table, query_string):
        table = self._tabularize(table)
        parsed_query_string = select_parser.parse(query_string)
        stmt = self.select_statement(table, parsed_query_string=parsed_query_string)
        with self.engine.connect() as connection:
            result = connection.execute(stmt)
            template = self.get_template(table, parsed_query_string=parsed_query_string)
            return self.generate(template, result)        

    def insert(self, table, query_string, rows):        
        table = self._tabularize(table)
        parsed_query_string = insert_parser.parse(query_string)        
        stmt = self.insert_statement(table, rows, parsed_query_string=parsed_query_string)    
        with self.engine.connect() as connection:
            result = connection.execute(stmt)
            return self._conditional_returning(table, parsed_query_string, result)
        
    def update(self, table, query_string, rows):
        table = self._tabularize(table)
        parsed_query_string = update_parser.parse(query_string)        
        stmt = self.update_statement(table, rows, parsed_query_string=parsed_query_string)        
        with self.engine.connect() as connection:
            result = connection.execute(stmt)
            return self._conditional_returning(table, parsed_query_string, result)

    def delete(self, table, query_string):
        table = self._tabularize(table)
        parsed_query_string = update_parser.parse(query_string)        
        stmt = self.delete_statement(table, parsed_query_string=parsed_query_string)
        with self.engine.connect() as connection:
            result = connection.execute(stmt)
            return self._conditional_returning(table, parsed_query_string, result)
    
    def open_api(self):
        #todo
        return {}