# Alchemify

Alchemify is a little tool that I have been playing around with that is written on top of SQLAlchemy.  
It parses http requests as defined by [PostgREST](https://postgrest.org) (I think PostgREST is amazing but I'm too lazy and/or stupid to learn Haskell). The parsed requests then generate sql statements via [SQLAlchemy](https://www.sqlalchemy.org).  
It's early days but I have the basic stuff working.  
### Flask example
Create a database.

    % sqlite3 fawlty.db
    CREATE TABLE users (
        id INTEGER NOT NULL, 
        name VARCHAR, 
        fullname VARCHAR, 
        PRIMARY KEY (id)
    );
    CREATE TABLE addresses (
        id INTEGER NOT NULL, 
        user_id INTEGER, 
        email_address VARCHAR NOT NULL, 
        PRIMARY KEY (id), 
        FOREIGN KEY(user_id) REFERENCES users (id)
    );

Hello Alchemify. (Example from hello-flask.py)

    from flask import Flask

    from sqlalchemy import create_engine

    from alchemify import Alchemify
    from alchemify.flask import AlchemifiedView

    engine = create_engine('sqlite:///fawlty.db', echo=True)

    app = Flask(__name__)
    app.alchemify = Alchemify(engine)

    app.add_url_rule('/api/<table>', view_func=AlchemifiedView.as_view('api'))

Run it.

    % export FLASK_APP=hello-flask.py 
    % flask run

Try it.

    % curl -X POST -H "Content-Type: application/json" "http://localhost:5000/api/users" -d '{"name":"Basil", "fullname": "Basil Fawlty"}'
    % curl -X POST -H "Content-Type: application/json" "http://localhost:5000/api/users" -d '{"name":"Sybil", "fullname": "Sybil Fawlty"}'

    % curl -X POST -H "Content-Type: application/json" "http://localhost:5000/api/addresses" -d '[{"user_id":1, "email_address": "basil@fawlty.co.uk"}, {"user_id":2, "email_address": "reception@fawlty.co.uk"}]'

    % curl  "http://localhost:5000/api/users?select=id,name,fullname,addresses(email_address)&id=eq.addresses.user_id&order=name"
    [
        {
            "id": 1, 
            "name": "Basil", 
            "fullname": "Basil Fawlty", 
            "addresses": {
                "email_address": "basil@fawlty.co.uk"
            }
        }, 
        {
            "id": 2, 
            "name": "Sybil", 
            "fullname": "Sybil Fawlty", 
            "addresses": {
                "email_address": "reception@fawlty.co.uk"
            }
        }
    ]
        
    % curl -X PUT -H "Content-Type: application/json" "http://localhost:5000/api/addresses?user_id=eq.2" -d '{"email_address":"sybil@fawlty.co.uk"}'
    
    % curl "http://localhost:5000/api/addresses?select=email_address,user:users(name)&id=lt.5&user_id=eq.users.id&order=users.fullname.desc"
    [
        {
            "email_address": "sybil@fawlty.co.uk",
            "user": {
                "name": "Sybil"
            }
        },
        {
            "email_address": "basil@fawlty.co.uk",
            "user": {
                "name": "Basil"
            }
        }
    ]

    % curl -X POST -H "Content-Type: application/json" "http://localhost:5000/api/users" -d '{"name":"Manuel", "fullname": "Manuel"}'

    % curl -X DELETE 'http://localhost:5000/api/users?name=eq."Manuel"' 


Nice, right? But what about grouping?  
Just like PostgREST, let the database handle this, try to keep the interface simple. 

    % sqlite3 fawlty.db
    CREATE VIEW user_addresses AS 
       SELECT users.*, group_concat(addresses.email_address) AS emails 
       FROM users JOIN addresses ON users.id = addresses.user_id 
       GROUP BY users.id, users.name, users.fullname;

Unfortunately sqlite doesn't support array types but you get the idea.  
From there it's just:

    % curl -X POST -H "Content-Type: application/json" "http://localhost:5000/api/addresses" -d '{"user_id":2, "email_address": "reception@fawlty.co.uk"}'
    
    % curl  "http://localhost:5000/api/user_addresses?select=*&limit=2" 
    [
        {
            "id": 1,
            "name": "Basil",
            "fullname": "Basil Fawlty",
            "emails": "basil@fawlty.co.uk"
        },
        {
            "id": 2,
            "name": "Sybil",
            "fullname": "Sybil Fawlty",
            "emails": "sybil@fawlty.co.uk,reception@fawlty.co.uk"
        }
    ]



### Why? 
Again, I think PostgREST is mindblowingly amazing.  
But an API generated from your database is only going to get you so far.  
You're going to need to handle more complicated queries and side effects.  
PostgREST encourages you to write more complicated queries as views or stored procedures.  
Which makes sense but sometimes feels a little heavy handed when all you want to do is add a field to the output or add a single join to your query.  
Or even just use some of the data from your queries in side effects without having to perform an extra deserialize and serialize operation.  
I guess I could have extended PostgREST in Haskell to similar effect but that's where my laziness/stupidity kicks in.  
I'm no Haskell programmer and there is this amazing tool called SQLAlchemy that already does anything you would want to do with a database and more.

With Alchemify you can step in to your generated API whereever you see fit.  
Whether you're enhancing the query to the database, adding data to the output, providing unrelated data to a third party service via a side effect or whatever you want.  
Alchemify allows you to do that in Python.  
It only requires access to the SQLAlchemy engine you're using.  

A side effect of allowing you to step in where you see fit is that Alchemify doesn't need to be as opinionated as PostgREST.  
If you want to enforce permissions via the database - knock yourself out!  
If you prefer to use simple cookies instead of jwt for authentication - feel free!  
The idea is not to tell you how to write your application but to get you running faster while not being a hindering factor that you will have to refactor around (our out) when your project grows.

### Todos
* Tests
* Add FastAPI support (other frameworks?)
* Documentation
* Support OpenAPI
* Add more operators and features.

