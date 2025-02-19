from flask import Flask, render_template, jsonify, abort, send_from_directory
from kubequery.utils.graph import Neo4j
from kubequery.queries import *
from flask_swagger_ui import get_swaggerui_blueprint

app = Flask(__name__)
neo4j = Neo4j()

# Swagger UI
SWAGGER_URL = '/swagger'  # URL for accessing Swagger UI
API_URL = '/static/swagger.yaml'

swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,          # Swagger UI endpoint
    API_URL,              # Swagger spec file
    config={
        'app_name': "KubeQuery API"
    }
)
# Register the swagger blueprint
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)


@app.route("/", methods=['GET'])
def index():
    neo4j.execute_read(subgraph)
    return render_template('index.html')

@app.route("/clusters", methods=['GET'])
def list_clusters():
    try:
        clusters = neo4j.execute_read(clusters_info)
    except Exception as e:
        abort(500, description=f"An unexpected error occurred: {str(e)}")
    if not clusters:
        abort(404, description=f"No cluster found")
    return jsonify(clusters), 200

@app.route("/stats", methods=['GET'])
def list_stats():
    try:
        stats = neo4j.execute_read(distinct_labels)
    except Exception as e:
        abort(500, description=f"An unexpected error occurred: {str(e)}")
    return jsonify(stats), 200

@app.route("/clusters/<string:clusterId>/nodes", methods=['GET'])
def list_nodes(clusterId):
    if not clusterId:
        abort(400, description="Missing clusterId")
    try:
        nodes = neo4j.execute_read(nodes_info, clusterId)
    except Exception as e:
        abort(500, description=f"An unexpected error occurred: {str(e)}")
    if not nodes:
        abort(404, description=f"No nodes found for clusterId {clusterId}")
    return jsonify(nodes), 200

@app.route("/clusters/<string:clusterId>/pods", methods=['GET'])
def list_pods_on_cluster(clusterId):
    if not clusterId:
        abort(400, description="Missing clusterId")
    try:
        pods = neo4j.execute_read(pods_info_by_cluster, clusterId)
    except Exception as e:
        abort(500, description=f"An unexpected error occurred: {str(e)}")
    if not pods:
        abort(404, description=f"No pods found for clusterId {clusterId}")
    return jsonify(pods), 200

@app.route("/clusters/<string:clusterId>/<string:nodeId>/pods")
def list_pods_on_node(clusterId, nodeId):
    if not clusterId or not nodeId:
        abort(400, description="Missing clusterId or nodeId")
    try:
        pods = neo4j.execute_read(pods_info, clusterId, nodeId)
    except Exception as e:
        abort(500, description=f"An unexpected error occurred: {str(e)}")
    if not pods:
        abort(404, description=f"No pods matched")
    return jsonify(pods), 200

@app.route("/clusters/<string:clusterId>/<string:nodeId>/resources")
def list_allocated_resources_on_node(clusterId, nodeId):
    if not clusterId or not nodeId:
        abort(400, description="Missing clusterId or nodeId")
    try:
        resources = neo4j.execute_read(node_resources, clusterId, nodeId)
    except Exception as e:
        abort(500, description=f"An unexpected error occurred: {str(e)}")
    if not resources:
        abort(404, description=f"No resources available on node")
    return jsonify(resources), 200

@app.route("/clusters/<string:clusterId>/pods/resources")
def list_pods_usage_on_cluster(clusterId):
    if not clusterId:
        abort(400, description="Missing clusterId")
    try:
        resources = neo4j.execute_read(pods_resources, clusterId)
    except Exception as e:
        abort(500, description=f"An unexpected error occurred: {str(e)}")
    if not resources:
        abort(404, description=f"No resources available")
    return jsonify(resources), 200

# Serve the swagger.yaml file in the static directory
@app.route("/static/swagger.yaml")
def swagger_yaml():
    return send_from_directory('static', 'swagger.yaml')