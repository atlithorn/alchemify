from flask import Flask

from sqlalchemy import create_engine

from alchemify import Alchemify
from alchemify.flask import AlchemifiedView

engine = create_engine('sqlite:///fawlty.db', echo=True)

app = Flask(__name__)
app.alchemify = Alchemify(engine)

@app.route('/')
def hello_world():
    return 'Hello, World!'

app.add_url_rule('/api/<table>', view_func=AlchemifiedView.as_view('api'))
