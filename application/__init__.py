from flask import Flask
# from flask_cors import CORS

app = Flask(__name__)
app.secret_key = 'inkpl,n,28675837'
# CORS(app)

from application import routes
