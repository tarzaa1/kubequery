from kubequery.routes import app, neo4j

from kubequery.conf import (
    NEO4J_URI,
    NEO4J_AUTH,
    FLASK_HOST,
    FLASK_PORT
)

if __name__ == '__main__':
    
    neo4j.connect(NEO4J_URI, NEO4J_AUTH)
    app.run(host=FLASK_HOST, port=FLASK_PORT, debug=False)