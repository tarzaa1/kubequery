from decouple import config

#Neo4j
NEO4J_URI = config('NEO4J_URI', default='bolt://localhost:7687')

NEO4J_AUTH_STRING = config("NEO4J_AUTH")
NEO4J_AUTH = tuple(NEO4J_AUTH_STRING.split('/'))

MEMGRAPH_AUTH_STRING = config("MEMGRAPH_AUTH")
MEMGRAPH_AUTH = tuple(MEMGRAPH_AUTH_STRING.split('/'))

NEO4J_PID = config("NEO4J_PID")
MEMGRAPH_PID = config("MEMGRAPH_PID")

# JANUSGRAPH_PID = config("JANUSGRAPH_PID")
# JANUSGRAPH_AUTH = config("JANUSGRAPH_AUTH")

FLASK_HOST = config('FLASK_HOST', default='localhost')
FLASK_PORT = config('FLASK_PORT', default=5000)

#Directories
KUBEINSIGHTS_DIR = config("KUBEINSIGHTS_DIR", default="")
KUBEGRAPHER_DIR = config("KUBEGRAPHER_DIR", default="")