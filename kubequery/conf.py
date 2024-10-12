from decouple import config

#Neo4j
NEO4J_URI = config('NEO4J_URI', default='bolt://localhost:7687')
auth_string = config('NEO4J_AUTH')
NEO4J_AUTH = tuple(auth_string.split('/'))

FLASK_HOST = config('FLASK_HOST', default='localhost')
FLASK_PORT = config('FLASK_PORT', default=5000)