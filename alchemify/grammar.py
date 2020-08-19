import operator
import warnings

from lark import Lark, Transformer, v_args

from sqlalchemy import select, insert, update, delete
from sqlalchemy import Table, Integer, String
from sqlalchemy.sql import cast
from sqlalchemy.sql.expression import BinaryExpression, UnaryExpression, literal, and_, or_

_imports = """
%import common.CNAME
%import common.NUMBER
%import common.ESCAPED_STRING
%import common.WS
%ignore WS
"""

_select = """
select: "select="_selector("," _selector)*
_selector: column
         | foreigner
         | all 
column: name[":"label]["::"cast]
foreigner : foreign_definition"("_foreign_selector(","_foreign_selector)*")"
foreign_definition: [alias":"]title
_foreign_selector: column
                 | all
all: "*"
name: CNAME  
label: CNAME  
cast: CNAME
title: CNAME
alias: CNAME  
"""

_modifiers = """
order: "order="ordering("," ordering)*
ordering: reference["."direction]
direction: "asc" -> asc
         | "desc" -> desc
limit: "limit="NUMBER
offset: "offset="NUMBER
"""

_columns = """
columns: "columns="CNAME(","CNAME)*
"""

_whereclause = """
whereclause: expression
expression: _left"."operator"."_right
          | _left"="operator"."_right
          | _list_expression
_list_expression: and_list_expression
                | or_list_expression
and_list_expression: "and=("_expression_list")"
                   | "and("_expression_list")"
or_list_expression: "or=("_expression_list")"
                  | "or("_expression_list")"
_expression_list: expression("," expression)+
operator: "eq"     -> eq
        | "gte"    -> ge
        | "gt"     -> gt
        | "lte"    -> le
        | "lt"     -> lt        
        | "neq"    -> ne
        | "not."operator -> not_
reference: CNAME("."CNAME)*
_left: reference
_right: reference    
      | _literal
_literal: literal_string
        | literal_number
literal_string: ESCAPED_STRING
literal_number: NUMBER
"""


class BaseTransformer(Transformer):

    def select(self, args):
        # args is a list of list of dicts that represent columns   
        column_list = list()
        for sublist in args:
            for item in sublist:
                _table = item.get('table')
                if _table is None:
                    _table = self.table
                # default is just select all
                col = _table
                _name = item.get('name')
                if _name:
                    # cast and label does not apply for 'all'
                    col = _table.c[_name]
                    _cast = item.get('cast')
                    if _cast:
                        col = cast(col, _cast)
                    _label = item.get('label')
                    if _label:
                        col = col.label(_label)
                column_list.append(col)
        return self.select.__name__, column_list

    def columns(self, args):
        return self.columns.__name__, [arg.value for arg in args]

    def limit(self, args):        
        return self.limit.__name__, args[0].value

    def offset(self, args):
        return self.offset.__name__, args[0].value

    def order(self, args):        
        orderings = list()
        for arg in args:
            ref = arg.children[0]
            if len(arg.children) > 1 and not arg.children[1]:
                # ascending is default
                ref = ref.desc()
            orderings.append(ref)
        return self.order.__name__, orderings

    def asc(self, args):
        return True
    def desc(self, args):
        return False

    def column(self, args):
        response = dict(name=args[0].children[0].value)
        for arg in args[1:]:
            if arg.data == 'cast':
                response['cast'] = dict(int=Integer, string=String)[arg.children[0].value]
            elif arg.data == 'label':
                response['label'] = str(arg.children[0].value)
        return [response]

    def foreigner(self, args):
        table = Table(args[0], self.metadata, autoload=True)
        cols = list()
        for sublist in args[1:]:
            for item in sublist:
                item['table'] = table
                cols.append(item)
        return cols

    def foreign_definition(self, args):
        # for select purposes we only need the name
        # templating uses the alias
        return args[-1].children[0].value

    def all(self, args):
        return [{}]

    def whereclause(self, args):
        return self.whereclause.__name__, args[0]  

    def expression(self, args):
        if len(args) == 1:
            # it's a list expression
            return args[0]
        left, op, right = args
        inverse = False
        if type(op) is tuple:
            op = op[0]
            inverse = True
        exp = BinaryExpression(left, right, op)
        if inverse:
            exp = UnaryExpression(exp, operator=operator.inv)
        return exp

    def and_list_expression(self, args):
        return and_(*args)
    def or_list_expression(self, args):
        return or_(*args)

    def literal_string(self, args):
        return literal(args[0].value[1:-1], type_=String)    
    def literal_number(self, args):        
        return literal(args[0].value, type_=Integer)

    def reference(self, args):
        ref = self.table
        # check the first value for validity in self.table
        if not ref.c.has_key(args[0].value):
            ref = Table(args.pop(0).value, self.metadata, autoload=True)
        for arg in args:
            ref = ref.c[arg.value]
        return ref

    def not_(self, args):
        return args[0], True
    def eq(self, args):
        return operator.eq
    def ge(self, args):
        return operator.ge
    def gt(self, args):
        return operator.gt
    def le(self, args):
        return operator.le
    def lt(self, args):
        return operator.lt
    def ne(self, args):
        return operator.ne

select_grammar = f"""
start: [_pair("&"_pair)*]
_pair: select
     | order
     | limit
     | offset
     | whereclause
{_select}
{_modifiers}
{_whereclause}
{_imports}
"""

select_parser = Lark(select_grammar)

class SelectTransformer(BaseTransformer):

    def __init__(self, table, metadata):
        self.table = table
        self.metadata = metadata

    def start(self, args):    
        columns = [self.table]
        whereclauses = []
        order = None
        limit = None
        offset = None
        for key,val in args:
            if key == SelectTransformer.whereclause.__name__:
                whereclauses.append(val)
            elif key == SelectTransformer.select.__name__:
                columns = val
            elif key == SelectTransformer.order.__name__:
                order = val
            elif key == SelectTransformer.limit.__name__:
                limit = val
            elif key == SelectTransformer.offset.__name__:
                offset = val
        stmt = select(columns)
        if whereclauses:
            stmt = stmt.where(and_(*whereclauses))
        if limit is not None:
            stmt = stmt.limit(limit)
        if offset is not None:
            stmt = stmt.offset(offset)
        if order is not None:
            stmt = stmt.order_by(*order)

        return stmt


insert_grammar = f"""
start: [_pair("&"_pair)*]
_pair: select
     | columns
{_select}
{_columns}
{_imports}
"""

insert_parser = Lark(insert_grammar)

def _filter_values(values, columns=None):
    if columns:
        # filter self.values via columns
        column_set = set(columns)
        return {k:v for k,v in values.items() if k in column_set}
    return values


class InsertTransformer(BaseTransformer):

    def __init__(self, table, metadata, values=None):
        self.table = table
        self.metadata = metadata
        self.values = values

    def start(self, args):
        select = None
        columns = None
        for key, val in args:
            if key == InsertTransformer.select.__name__:
                select = val
            elif key == InsertTransformer.columns.__name__:
                columns = val 
        stmt = insert(self.table)
        if self.values:
            stmt = stmt.values(_filter_values(self.values, columns))
        if select:
            # add returning to statement
            if self.metadata.bind.dialect.implicit_returning:
                stmt = stmt.returning(*select)
            else:
                warnings.warn(f"{self.metadata.bind.dialect} does not support returning, ignoring")
        return stmt


update_grammar = f"""
start: [_pair("&"_pair)*]
_pair: select
     | columns
     | whereclause
{_select}
{_columns}
{_whereclause}
{_imports}
"""

update_parser = Lark(update_grammar)

class UpdateTransformer(BaseTransformer):

    def __init__(self, table, metadata, values=None):
        self.table = table
        self.metadata = metadata
        self.values = values

    def start(self, args):
        whereclauses = list()
        select = None
        columns = None
        for key, val in args:
            if key == UpdateTransformer.whereclause.__name__:
                whereclauses.append(val)
            elif key == UpdateTransformer.select.__name__:
                select = val
            elif key == UpdateTransformer.columns.__name__:
                columns = val 
        stmt = update(self.table)
        if whereclauses:
            stmt = stmt.where(*whereclauses)
        if self.values:
            stmt = stmt.values(_filter_values(self.values, columns))
        if select:
            # add returning to statement
            if self.metadata.bind.dialect.implicit_returning:
                stmt = stmt.returning(*select)
            else:
                warnings.warn(f"{self.metadata.bind.dialect} does not support returning from insert/update/delete statements, ignoring")
        return stmt


class DeleteTransformer(BaseTransformer):

    def __init__(self, table, metadata):
        self.table = table
        self.metadata = metadata

    def start(self, args):
        whereclauses = list()
        select = None
        for key, val in args:
            if key == UpdateTransformer.whereclause.__name__:
                whereclauses.append(val)
            elif key == UpdateTransformer.select.__name__:
                select = val
        stmt = delete(self.table)
        if whereclauses:
            stmt = stmt.where(*whereclauses)
        if select:
            # add returning to statement
            if self.metadata.bind.dialect.implicit_returning:
                stmt = stmt.returning(*select)
            else:
                warnings.warn(f"{self.metadata.bind.dialect} does not support returning, ignoring")
        return stmt


class TemplateTransformer(Transformer):

    def __init__(self, table, metadata):
        self.table = table
        self.metadata = metadata

    def _expand_table(self, tbl):
        output_list = list()
        for c in tbl.c:
            if self.table == tbl:
                output_list.append((c.name,))
            else:
                output_list.append((item.get('table_key'), c.name))
        return output_list
    
    def start(self, args):
        # only support output from select        
        for arg in args:
            if type(arg) == tuple and arg[0] == TemplateTransformer.select.__name__:
                 return arg[1]
        # if no select output that means we're returning all items from table
        return self._expand_table(self.table)

    def select(self, args):
        # args is a list of list of dicts that represent columns   
        output_list = list()
        for sublist in args:
            for item in sublist:
                _table = item.get('table')
                if _table is None:
                    _table = self.table
                # default is just select all
                col = _table
                tmplt = None
                _name = item.get('name')
                if _name:
                    tmplt_key = _name
                    col = _table.c[_name]
                    # label does not apply for 'all'
                    # ignoring cast for now, think it can be handled by json serializer
                    _label = item.get('label')
                    if _label:
                        tmplt_key = _label 
                    if self.table == _table:
                        output_list.append((tmplt_key,))
                    else:
                        output_list.append((item.get('table_key'), tmplt_key))
                else:
                    # we need to extract template for 'all' 
                    output_list.extend(self._expand_table(col))
        return TemplateTransformer.select.__name__, output_list

    def columns(self, args):
        """
        columns keyword only offers top level support so this is relatively simple
        """
        return [(arg.value,) for arg in args]

    def column(self, args):
        response = dict(name=args[0].children[0].value)
        for arg in args[1:]:
            if arg.data == 'label':
                response['label'] = str(arg.children[0].value)
        return [response]

    def foreigner(self, args):
        definition = args[0]
        table = Table(definition['title'], self.metadata, autoload=True)
        cols = list()
        for sublist in args[1:]:
            for item in sublist:
                item['table'] = table
                item['table_key'] =  definition.get('alias', definition.get('title'))
                cols.append(item)
        return cols

    def foreign_definition(self, args):
        return {arg.data:arg.children[0].value for arg in args}

    def all(self, args):
        return [{}]