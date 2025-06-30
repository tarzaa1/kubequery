import threading
import time
# Flask imports
from flask_swagger_ui import get_swaggerui_blueprint
from flask import Flask, render_template, jsonify,abort, send_from_directory, Response, request, stream_with_context
from flask import Response, request
from flask_swagger_ui import get_swaggerui_blueprint
import json
# Kubequery imports
from kubequery.conf import (NEO4J_URI,NEO4J_AUTH,MEMGRAPH_AUTH,NEO4J_AUTH_STRING, MEMGRAPH_AUTH_STRING)
from kubequery.queries import *
from kubequery.benchmarking.runBenchmarking import generate_test_id, load_config, restart_database, restart_kafka, run_sequence
from kubequery.benchmarking.k8s import check_deployments_ready, k8s_main
from kubequery.benchmarking.benchmarking import setup_test
from kubequery.utils.graph import Neo4j
from kubequery.utils.benchmark.plot_benchmark import analyse_queries, plot_create_graph, plot_kafka_cg_duration, plot_query_latency, plot_query_throughput, queries_boxplot
from kubequery.utils.benchmark.helpers import get_all_tests
from kubequery.utils.benchmark.docker_utils import monitor_neo4j_process
from kubequery.utils.benchmark.dash_app import create_dash_app

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


@app.route("/metrics")
def metrics():
    # Load the configuration
    clusters_list, nodes_list, pods_list, queries, databases = load_config()
    
    # Build presets for each node/pod combination
    presets = []
    for num_clusters in clusters_list:
        for num_nodes in nodes_list:
            for num_pods in pods_list:
                presets.append({
                    'desc': f"{num_clusters}-clusters-{num_nodes}-nodes-with-{num_pods}-pods",
                    'num_clusters': num_clusters,
                    'num_nodes': num_nodes,
                    'num_pods': num_pods
                })
    
    # Pass the raw configuration as well
    return render_template("metrics.html",
                           queries=queries,
                           presets=presets,
                           clusters_list=clusters_list,
                           nodes_list=nodes_list,
                           pods_list=pods_list,
                           databases=databases
                           )


@app.route("/run_benchmark")
def run_benchmark():
    # Get list values from query parameters for nodes and pods.
    clusters_list = request.args.getlist('num_clusters')
    nodes_list = request.args.getlist('num_nodes')
    pods_list = request.args.getlist('num_pods')
    queries = request.args.getlist('queries')
    databases = request.args.getlist('db')

    # Log the values for debugging.
    print("Clusters:", clusters_list)
    print("Nodes:", nodes_list)
    print("Pods:", pods_list)
    print("Queries:", queries)
    print("Databases:", databases)
    
    # Error handling: ensure that nodes, pods, queries, and databases are provided.
    if not clusters_list or all(c.strip() == "" for c in clusters_list):
        return Response("Error: No nodes provided.", status=400)
    if not nodes_list or all(n.strip() == "" for n in nodes_list):
        return Response("Error: No nodes provided.", status=400)
    if not pods_list or all(p.strip() == "" for p in pods_list):
        return Response("Error: No pods provided.", status=400)
    if not databases:
        return Response("Error: No databases provided.", status=400)
    

    # Initially, diagram data, dash and deployment-progress should be hidden

    @stream_with_context
    def generate():        
        yield f"data: {json.dumps({'type': 'toggleDiv', 'div': "all"})}\n\n"
        for num_clusters in clusters_list: # I only tested for 1, but I have given the option
            for num_nodes in nodes_list:
                for num_pods in pods_list:

                    # Create kubernetes clusters, nodes, deployments and pods as specified
                    # The same generated k8's data is used for each database
                    yield "data: Generating nodes and clusters...\n\n"
                    yield from k8s_main(int(num_clusters), int(num_nodes), int(num_pods))

                    for db_name in databases: 
                        if num_nodes == "250" and db_name == "neo4j":
                            continue
                        # Each test has its own id and decription
                        test_id = generate_test_id()
                        description = f"{num_nodes}-nodes-with-{num_pods}-pods"

                        # Restart Kafka
                        yield from restart_kafka()
                        # If connecting to real data
                        
                        # Starts up next database, tears down last
                        yield from restart_database(db_name)
                        
                        # Connect to the database.
                        neo4j = Neo4j()  # Instantiate database class.

                        print("Connecting to", db_name)
                        if db_name == "neo4j":
                            AUTH_STRING = NEO4J_AUTH_STRING
                            AUTH = NEO4J_AUTH
                        elif db_name == "memgraph":
                            AUTH_STRING = MEMGRAPH_AUTH_STRING
                            AUTH = MEMGRAPH_AUTH
                        
                        else:
                            yield f"data: Unsupported database type: {db_name}\n\n"
                            continue  # Skip unsupported database types.

                        for attempt in range(1, 5 + 1):
                            try:
                                neo4j.connect(NEO4J_URI, AUTH)
                                break  # Success, exit the loop
                            except Exception as e:
                                print(f"Attempt {attempt} failed: {e}")
                                if attempt == 5:
                                    raise ValueError("Database connection failed after multiple attempts.")
                                time.sleep(60)

                        yield "data: Clearing any existing data in the database...\n\n"

                        neo4j.execute_write(delete_all) # This can't fail

                        # Start the monitoring thread for cpu/memory metrics collection.
                        print("Starting monitoring thread")
                        stop_event = threading.Event()
                        yield "data: Starting monitoring thread...\n\n"
                        monitor_thread = threading.Thread(
                            target=monitor_neo4j_process,
                            args=(test_id, stop_event, db_name)
                        )
                        monitor_thread.start()
        
                        # Push to kafka, kubeGrapher.
                        total_num_pods = int(num_pods) * int(num_nodes)
                        yield from run_sequence(test_id, total_num_pods, num_nodes, num_pods, description, AUTH_STRING, db_name)
                        
                        # Run the benchmark queries.
                        yield "data: Benchmarking queries...\n\n"
                        yield from setup_test(neo4j, test_id, description, queries, db_name)

                        # Stop the monitoring thread.
                        stop_event.set()
                        monitor_thread.join()

                        # clear the screen 
                        yield f"data: {json.dumps({'type': 'toggleDiv', 'div': "diagramData"})}\n\n"
                        yield f"data: {json.dumps({'type': 'toggleDiv', 'div': "dash"})}\n\n"

        
                        yield f"data: Benchmarking finished for {num_clusters} clusters {num_nodes} nodes and {num_pods} pods...\n\n"
 
                        # Provided you are not on the last db in list of databases
                        if db_name != databases[-1]:
                            deploy_ready = check_deployments_ready() # Check that deployments are still in ready state
                            if deploy_ready: # If all deployments ready, it's fine to push to kafka
                                continue
                            else: # Otherwise, need to remake the cluser
                                yield from k8s_main(int(num_clusters), int(num_nodes), int(num_pods)) 
 
                
        yield "data: All benchmarks completed.\n\n"
    
    return Response(generate(), mimetype="text/event-stream")

@app.route('/load_tests')
def load_tests(): 
    tests = get_all_tests()
    queries = ['test_get_all_nodes', 'test_get_all_node_annotations', 'test_get_all_node_labels', 'test_get_all_node_taint', 
                'test_get_all_nodes_pods', 'test_get_all_pods_labels', 'test_get_all_pods_annotations', 'test_get_all_pods_containers', 'test_get_all_pods_replicasets', 
                'test_get_all_pods_replicasets_labels', 'test_get_all_pods_replicasets_annotations', 'test_get_all_pods_deployments', 
                 'test_get_all_pods_deployments_annotations', 'test_get_all_pods_deployments_labels', 
                 
                #  'test_get_pods_for_specific_node', 
                #   'get_labels_for_specific_node', 'get_taint_for_specific_node', 'get_annotation_for_specific_node', 'get_pod_labels_for_specific_node', 
                #   'get_pod_annotations_for_specific_node', 'get_pod_container_for_specific_node', 'get_pod_replicaset_for_specific_node', 
                #  'get_pod_deployment_for_specific_node', 'get_pod_replicaset_label_for_specific_node', 
                #     'get_pod_replicaset_annotation_for_specific_node', 'get_pod_deployment_label_for_specific_node', 'get_pod_deployment_annotation_for_specific_node', 


                    'get_pod_labels_for_specific_pod', 'get_pod_annotations_for_specific_pod', 'get_pod_container_for_specific_pod', 'get_pod_replicaset_for_specific_pod', 
                    'get_pod_deployment_for_specific_pod', 'get_pod_replicaset_label_for_specific_pod', 'get_pod_replicaset_annotation_for_specific_pod', 
                    'get_pod_deployment_label_for_specific_pod', 
                    'get_pod_deployment_annotation_for_specific_pod', 'get_pods_for_replicaset', 'get_deployment_for_replicaset',
                      'get_pod_labels_for_replicaset', 'get_pod_annotations_for_replicaset', 'get_pod_container_for_replicaset', 'get_deployment_label_for_replicaset', 
                      'get_deployment_annotation_for_replicaset']
    # Input queries here, so users can select queries for their results
    return render_template("new_load_tests.html", tests=tests, queries=queries)


@app.route('/fetch_test_data', methods=['GET'])
def fetch_test_data():
    # Output type and data about selected test(s) 
    outputType = request.args.get("outputType")
    testIDs = request.args.getlist("testIDs")
    queries = request.args.getlist("queries")

    # Necessary to further data type
    percentile = request.args.get("percentile")
    hop = request.args.get("hops")
    aggregate_hops=False

    if outputType == "cg_plots":
        result = plot_create_graph(testIDs, "cpu")
        result2 = plot_create_graph(testIDs, "mem")
        result3 = plot_kafka_cg_duration(testIDs)

        if result and result2 and result3:
            return jsonify({outputType: "Created plots for create graph"}), 200
    
    else: # Else its an analyse query type
        if outputType =="analyse_bar_and_hops":
            aggregate_hops = True

        # Fetch analysis data necessary for all output types
        analysis_data = analyse_queries(testIDs, queries, hop, aggregate_hops)
        
        # Maybe add error handling for empty returned analysis data
        if outputType == "analyse_boxplot":
            queries_boxplot(testIDs, queries)
        if outputType == "analyse_bar_and_hops":
            plot_query_latency(analysis_data, percentile, True)
            return jsonify({outputType: "Created bar plots for num hops"}), 200
        if outputType == "analyse_bar_plots":
            plot_query_latency(analysis_data, percentile)
            return jsonify({outputType: "Created bar plots for queries"}), 200
        
        if outputType == "analyse_throughput":
            plot_query_throughput(analysis_data)
            return  jsonify({outputType: "Created plots for throughput"}), 200 # Flask will return it as application/json

        # Return just the table data in the case of analyse
        analysis_data_serialized = [df.to_dict(orient='records') for df in analysis_data]
        return {outputType: analysis_data_serialized}, 200  # Flask will return it as application/json


    print("No data found")
    return jsonify({"Error": "No data found for selected fields"}), 400


# Serve the swagger.yaml file in the static directory
@app.route("/static/swagger.yaml")
def swagger_yaml():
    return send_from_directory('static', 'swagger.yaml')
    

# Import and initialize the Dash app (mounted at '/dash/')

# Uncomment for live metrics on metrics page
# dash_app = create_dash_app(app) # If the prints from this is annoying you, comment it out.

if __name__ == '__main__':
    app.run(debug=True)
