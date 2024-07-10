from flask import render_template
from flask import Flask
from kubequery import queries

app = Flask(__name__)

@app.route("/", methods=['GET'])
def index():
    return render_template('index.html')

@app.route("/clusters")
def get_clusters():
    return queries.get_clusters()