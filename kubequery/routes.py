from flask import Flask, render_template, jsonify
from kubequery.utils.graph import Neo4j
from kubequery.queries import *

app = Flask(__name__)
neo4j = Neo4j()

@app.route("/", methods=['GET'])
def index():
    neo4j.execute_read(subgraph)
    return render_template('index.html')

@app.route("/clusters", methods=['GET'])
def list_clusters():
    """ Retrieve all available clusters

    Response:
    [
        {"clusterId": "cluster1", "name": "Cluster 1"},
        {"clusterId": "cluster2", "name": "Cluster 2"}
    ]
    """
    clusters = neo4j.execute_read(clusters_info)
    return jsonify(clusters)

@app.route("/stats", methods=['GET'])
def list_stats():
    stats = neo4j.execute_read(distinct_labels)
    return jsonify(stats)

@app.route("/clusters/<string:clusterId>/nodes", methods=['GET'])
def list_nodes(clusterId):
    """ Retreive all nodes in a specified cluster

    Response:
    [
        {"nodeId": "node1", "hostname": "node1-host", "status": "Ready"},
        {"nodeId": "node2", "hostname": "node2-host", "status": "NotReady"}
    ]
    """
    nodes = neo4j.execute_read(nodes_info, clusterId)
    return jsonify(nodes)

@app.route("/clusters/<string:clusterId>/pods", methods=['GET'])
def list_pods_on_cluster(clusterId):
    """ Retreive all pods in a specified cluster

    Response:
    [
        {"podId": "pod1", "name": "pod1", "status": "Running"},
        {"podId": "pod2", "name": "pod2", "status": "Pending"}
    ]
    """
    nodes = neo4j.execute_read(pods_info_by_cluster, clusterId)
    return jsonify(nodes)

@app.route("/clusters/<string:clusterId>/<string:nodeId>/pods")
def list_pods_on_node(clusterId, nodeId):
    """ Retreive all pods scheduled on a specified node

    Response:
    [
        {"podId": "pod1", "name": "pod1", "status": "Running"},
        {"podId": "pod2", "name": "pod2", "status": "Pending"}
    ]
    """
    pods = neo4j.execute_read(pods_info, clusterId, nodeId)
    return jsonify(pods)

@app.route("/clusters/<string:clusterId>/<string:nodeId>/resources")
def list_allocated_resources_on_node(clusterId, nodeId):
    """ Retreive allocated resources statistics on a specified node
    """
    resources = neo4j.execute_read(node_resources_info, clusterId, nodeId)
    return jsonify(resources)
