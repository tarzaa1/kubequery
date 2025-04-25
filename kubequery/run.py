from kubequery.routes import app, neo4j
from neo4j import basic_auth

from kubequery.conf import (
    NEO4J_URI,
    NEO4J_AUTH,
    FLASK_HOST,
    FLASK_PORT
)

if __name__ == '__main__':
    # Commented out for when benchmarking is done, as db not always running
    # username, password = NEO4J_AUTH.split('/')
    # auth = basic_auth(username, password)
    # neo4j.connect(NEO4J_URI, auth)


    app.run(host=FLASK_HOST, port=FLASK_PORT, debug=False)