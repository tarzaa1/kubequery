from kubequery.routes import app, neo4j

if __name__ == '__main__':
    URI = "bolt://localhost:7687"
    AUTH = ("neo4j", "password")
    
    neo4j.connect(URI, AUTH)
    app.run(host="0.0.0.0", port=5000, debug=True)