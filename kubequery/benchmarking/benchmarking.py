from kubequery.utils.benchmark.test_queries import *
from kubequery.queries import *
from kubequery.utils.benchmark.docker_utils import sequential_benchmark_query, throughput_benchmark_query
from kubequery.conf import NEO4J_URI, NEO4J_AUTH  # Docker container name here
import random, time



# Global Variables

CSV_PATH = "kubequery/static/data/benchmark.csv"
CONTINUOUS_DOCKER_STATS_CSV = "kubequery/static/data/continuous_docker_stats.csv"
IMAGE_DIR = "kubequery/static/images/"
SLEEP_TIME = 2

################################################################################
# Main Benchmarking Logic
################################################################################

def setup_test(neo4j, test_id, desc, user_queries, db_name):
    """
    Gather cluster/node/pod info, then run benchmark queries.
    - This has to use real queries
    """
    stats = neo4j.execute_read(distinct_labels)
    print(stats)
    num_clusters = stats.get("Cluster", 0)
    num_nodes = stats.get("K8sNode", 0)


    # Subtract one node if one is reserved for maintaining cluster state
    num_nodes = num_nodes - 1  
    num_pods = stats.get("Pod", 0)

    cluster_info = neo4j.execute_read(clusters_info)
    for cluster in cluster_info:
        cluster_id = cluster["id"]
        node_ids = [node["id"] for node in neo4j.execute_read(nodes_info, cluster_id)
                    if node["name"] != "kwok-kwok-control-plane"]
        pod_ids = [pod["id"] for pod in neo4j.execute_read(pods_info, cluster_id, random.choice(node_ids))]
        replicaset_ids = [rs["id"] for rs in neo4j.execute_read(get_replicaset)]

        # Error handling, ensure node_ids and pod_ids ar not empty, else try again/raise error
        if node_ids is []:
            yield f"data: Graph did not populate properly: No nodes found in graph...\n\n"
            raise ValueError("Graph did not populate properly: No nodes found in graph")
        if pod_ids is []:
            yield f"data: Graph did not populate properly: No pods found in graph...\n\n"
            raise ValueError("Graph did not populate properly: No pods found in graph") # Problem with kubegrapher not putting the data into the graph
        yield from benchmark(neo4j, test_id, cluster_id, node_ids, replicaset_ids, pod_ids, num_clusters, num_nodes, num_pods, desc, db_name, user_queries)



def benchmark(neo4j, test_id, cluster_id, node_ids, replicaset_ids, pod_ids, num_clusters, num_nodes, num_pods, desc, db_name, user_queries=None):
    """
    Run benchmark sequential and throughput tests on different sets of queries
    """
    # 1) List of info queries

    # Choose a random node_id for testing, randomness promotes fairness.
    # If the code fails here, the graph wasnt populated correctly
    node_id = random.choice(node_ids)
    pod_id = random.choice(pod_ids) 
    replicaset_id = random.choice(replicaset_ids)

    # Full list of all available queries, with the input paramters
    queries = [
        (test_get_all_nodes, [cluster_id], 1, ),  # 1 hop, graphy
        (test_get_all_node_annotations, [cluster_id], 1),  # 1 hop, graphy
        (test_get_all_node_labels, [cluster_id], 1),  # 1 hop, graphy
        (test_get_all_node_taint, [cluster_id], 1),  # 1 hop, graphy
        (test_get_all_node_images, [cluster_id], 1),  # 1 hop, graphy

        (test_get_all_nodes_pods, [cluster_id], 2),  # 2 hop, graphy

        (test_get_all_pods_labels, [cluster_id], 3),  # 3 hop, graphy
        (test_get_all_pods_annotations, [cluster_id], 3),  # 3 hop, graphy
        (test_get_all_pods_containers, [cluster_id], 3),  # 3 hop, graphy
        (test_get_all_pods_replicasets, [cluster_id], 3),  # 3 hop, graphy

        (test_get_all_pods_replicasets_labels, [cluster_id], 4),  # 4 hop, graphy
        (test_get_all_pods_replicasets_annotations, [cluster_id], 4),  # 4 hop, graphy
        (test_get_all_pods_deployments, [cluster_id], 4),  # 4 hop, graphy
        (test_get_all_pods_images, [cluster_id], 4),  # 4 hop, graphy
        (test_get_all_container_configmaps, [cluster_id], 4),  # 4 hop, graphy

        (test_get_all_pods_deployments_annotations, [cluster_id], 5),  # 5 hop, graphy
        (test_get_all_pods_deployments_labels, [cluster_id], 5),  # 5 hop, graphy

        (test_get_pods_for_specific_node, [node_id], 1),  # 1 hop, graphy
        (get_labels_for_specific_node, [node_id], 1),  # 1 hop, graphy
        (get_taint_for_specific_node, [node_id], 1),  # 1 hop, graphy
        (get_annotation_for_specific_node, [node_id], 1),  # 1 hop, graphy

        (get_pod_labels_for_specific_node, [node_id], 2),  # 2 hop, graphy
        (get_pod_annotations_for_specific_node, [node_id], 2),  # 2 hop, graphy
        (get_pod_container_for_specific_node, [node_id], 2),  # 2 hop, graphy
        (get_pod_replicaset_for_specific_node, [node_id], 2),  # 2 hop, graphy

        (get_pod_configmap_for_specific_node, [node_id], 3),  # 3 hop, graphy
        (get_pod_image_for_specific_node, [node_id], 3),  # 3 hop, graphy
        (get_pod_deployment_for_specific_node, [node_id], 3),  # 3 hop, graphy
        (get_pod_replicaset_label_for_specific_node, [node_id], 3),  # 3 hop, graphy
        (get_pod_replicaset_annotation_for_specific_node, [node_id], 3),  # 3 hop, graphy

        (get_pod_deployment_label_for_specific_node, [node_id], 4),  # 4 hop, graphy
        (get_pod_deployment_annotation_for_specific_node, [node_id], 4),  # 4 hop, graphy

        (get_pod_labels_for_specific_pod, [pod_id], 1),  # 1 hop, graphy
        (get_pod_annotations_for_specific_pod, [pod_id], 1),  # 1 hop, graphy
        (get_pod_container_for_specific_pod, [pod_id], 1),  # 1 hop, graphy
        (get_pod_replicaset_for_specific_pod, [pod_id], 1),  # 1 hop, graphy

        (get_pod_deployment_for_specific_pod, [pod_id], 2),  # 2 hop, graphy
        (get_pod_replicaset_label_for_specific_pod, [pod_id], 2),  # 2 hop, graphy
        (get_pod_replicaset_annotation_for_specific_pod, [pod_id], 2),  # 2 hop, graphy
        (get_pod_configmap_for_specific_pod, [pod_id], 2),  # 2 hop, graphy
        (get_pod_image_for_specific_pod, [pod_id], 2),  # 2 hop, graphy

        (get_pod_deployment_label_for_specific_pod, [pod_id], 3),  # 3 hop, graphy
        (get_pod_deployment_annotation_for_specific_pod, [pod_id], 3),  # 3 hop, graphy

        (get_pods_for_replicaset, [replicaset_id], 1),  # 1 hop, graphy
        (get_deployment_for_replicaset, [replicaset_id], 1),  # 1 hop, graphy

        (get_pod_labels_for_replicaset, [replicaset_id], 2),  # 2 hop, graphy
        (get_pod_annotations_for_replicaset, [replicaset_id], 2),  # 2 hop, graphy
        (get_pod_container_for_replicaset, [replicaset_id], 2),  # 2 hop, graphy
        
        (get_deployment_label_for_replicaset, [replicaset_id], 3),  # 3 hop, graphy
        (get_deployment_annotation_for_replicaset, [replicaset_id], 3),  # 3 hop, graphy

    ]


    #  Filter queries if specified by user, else do all
    if user_queries:
        queries = [(q, args, num_hops) for q, args, num_hops, in queries if q.__name__ in user_queries]
    else:
        user_queries = queries

    test = []
    test.append([q.__name__ for q, _, _ in queries])
    print(test)



    yield f"data: {json.dumps({'type': 'toggleDiv', 'div': "diagramData"})}\n\n"
    time.sleep(SLEEP_TIME)
    print("Info queries to run:", [q.__name__ for q, _, _ in queries])
    for query_func, args, num_hops in queries:

        raw_query = query_func.query_template
        diagram_data = extract_diagram_data_from_query(raw_query)

        # Update the frontend with representation of the query (open day code)
        # Yield an SSE message with a diagram indicator so the front end knows this is diagram data.
        yield f"data: {json.dumps({'type': 'diagram', 'data': (diagram_data, query_func.__name__)})}\n\n"


        yield f"data: Loading next query: {query_func.__name__}...\n\n"
        time.sleep(SLEEP_TIME)

        yield f"data: Executing sequential benchmark for {query_func.__name__}...\n\n"
        try:
            sequential_benchmark_query(
                neo4j, test_id, query_func.__name__, num_clusters, num_nodes, num_pods,
                desc, query_func, num_hops, db_name, *args
            )
        except Exception as e:
            print(f"Unexpected error in sequential benchmark for {query_func.__name__}: {e}")

        yield f"data: Waiting {SLEEP_TIME} seconds for throughput benchmarking...\n\n"
        time.sleep(SLEEP_TIME)
        
        yield f"data: Executing throughput for {query_func.__name__}...\n\n"
        try:
            throughput_benchmark_query(
                neo4j, test_id, query_func.__name__, num_clusters, num_nodes, num_pods,
                desc, query_func, num_hops, db_name, *args
            )
        except Exception as e:
            print(f"Unexpected error in throughput benchmark for {query_func.__name__}: {e}")

    print("All queries finished. Waiting 2 seconds before ending benchmark...")
    time.sleep(10)
    neo4j.execute_write(delete_all)

    return user_queries

def get_node_count_temp():
    from kubequery.utils.graph import Neo4j

    neo4j = Neo4j()  # Instantiate database class.
    from kubequery.conf import (NEO4J_URI,NEO4J_AUTH,MEMGRAPH_AUTH,NEO4J_AUTH_STRING, MEMGRAPH_AUTH_STRING)

    AUTH_STRING = MEMGRAPH_AUTH_STRING
    AUTH = MEMGRAPH_AUTH

    neo4j.connect(NEO4J_URI, AUTH)

    neo4j.execute_read(get_node_count)
    neo4j.execute_read(get_relationship_count)


get_node_count_temp()