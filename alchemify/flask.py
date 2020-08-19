import json
from urllib.parse import unquote

from flask import current_app, request, make_response
from flask.views import MethodView

from alchemify import Alchemify

def dumps(input):
    # quick and dirty support for whatever objects that serialize nicely to str
    response = make_response(json.dumps(input, indent=4, default=str))
    response.headers["Content-Type"] = "application/json; charset=utf-8"
    return response

class AlchemifiedView(MethodView):

    def get(self, table):
        rows = current_app.alchemify.select(table, unquote(request.query_string.decode("utf-8")))        
        return dumps(rows), 200

    def post(self, table):
        rows = current_app.alchemify.insert(table, unquote(request.query_string.decode("utf-8")), request.json)        
        if rows:
            return dumps(rows), 201
        return '', 204

    def put(self, table):        
        # put and patch are equal from alchemify's perspective, ie both go to .update
        # but from http perspective we should assert on difference, ie put only deals with single objects
        rows = current_app.alchemify.update(table, unquote(request.query_string.decode("utf-8")), request.json)
        if rows:
            return dumps(rows), 200
        return '', 204


    def patch(self, table):        
        rows = current_app.alchemify.update(table, unquote(request.query_string.decode("utf-8")), request.json)
        if rows:
            return dumps(rows), 200
        return '', 204
  
    def delete(self, table):
        rows = current_app.alchemify.delete(table, unquote(request.query_string.decode("utf-8")))
        if rows:
            return dumps(rows), 200
        return '', 204


class AlchemicallyEnhancedView(MethodView):
    
    def get(self, table):

        query_string = unquote(request.query_string.decode("utf-8"))
        stmt = current_app.alchemify.select_stmt(table, query_string)
        # eg enrich statement with queries that are too complex for the query_string dsl
        tmplt = current_app.alchemify.get_template(table, query_string)
        # correspondingly update template
        rows = current_app.alchemify.generate(tmplt, current_app.alchemify.connection.execute(stmt))
        # eg enrich rows with external data
        return dumps(rows)   

class AlchemicallyAssistedView(AlchemifiedView):
    
    def put(self, table):        
        rows = super().put(table)
        # eg create some other side effects but don't fiddle with output        
        return dumps(rows)