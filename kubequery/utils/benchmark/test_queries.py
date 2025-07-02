
import re

def get_node_count(tx):
    query = """
    MATCH (n)
    RETURN count(n) AS node_count
    """
    result = tx.run(query)
    try:
        record = result.single()
        return record["node_count"]
    except Exception as e:
        raise
    
def get_relationship_count(tx):
    query = """
    MATCH ()-[r]->()
    RETURN count(r) AS rel_count
    """
    result = tx.run(query)
    try:
        record = result.single()
        return record["rel_count"]
    except Exception as e:
        raise

def query_template(template):
    def decorator(func):
        func.query_template = template.strip()
        return func
    return decorator

def extract_diagram_data_from_query(raw_query):
    """
    Given a raw Cypher query string, extract diagram data as JSON.
    This simple parser finds node definitions in MATCH patterns and
    extracts relationship patterns. Note that this approach assumes
    a consistent query format.
    
    Example input query:
    
      MATCH (C:Cluster)<-[:BELONGS_TO]-(N:K8sNode)
      WHERE C.id = "{cluster_id}"
      RETURN C, N

    Produces:
      {
         "nodes": [
            {"id": "c", "label": "Cluster"},
            {"id": "n", "label": "K8sNode"}
         ],
         "edges": [
            {"source": "n", "target": "c", "label": "BELONGS_TO"}
         ]
      }
    """
    # Regex pattern to match nodes like (C:Cluster)
    node_pattern = r'\(\s*([A-Za-z0-9_]+)\s*:\s*([A-Za-z0-9_]+)\s*\)'
    found_nodes = re.findall(node_pattern, raw_query)
    
    # Build a dictionary mapping variable name (lowercase) to its label.
    node_dict = {}
    for var, label in found_nodes:
        node_dict[var.lower()] = label

    edges = []
    
    # Pattern for relationships with a left arrow, e.g. (C:Cluster)<-[:BELONGS_TO]-(N:K8sNode)
    pattern_left = r'(?=(\(\s*([A-Za-z0-9_]+)?(?:\s*:\s*[A-Za-z0-9_]+)?\s*\)\s*<-\s*\[\s*:\s*([A-Za-z0-9_]+)\s*\]\s*-\s*\(\s*([A-Za-z0-9_]+)?(?:\s*:\s*[A-Za-z0-9_]+)?\s*\)))'
    for match in re.finditer(pattern_left, raw_query):
        # Capture the left variable, relationship label, and right variable.
        left_var = match.group(2)
        # Use .strip() to remove any extra spaces; provide a default if needed.
        rel = match.group(3).strip() if match.group(3) and match.group(3).strip() else "unknown_rel"
        right_var = match.group(4)
        left_id = left_var.lower() if left_var else "unknown_left"
        right_id = right_var.lower() if right_var else "unknown_right"
        edges.append({
            "source": right_id,  # For left-arrow relationships, source is on the right.
            "target": left_id,   # Target is on the left.
            "label": rel
        })

    # Pattern for relationships with a right arrow, e.g. (A:Annotation)-[:HAS_ANNOTATION]->(B:Something)
    pattern_right = r'(?=(\(\s*([A-Za-z0-9_]+)?(?:\s*:\s*[A-Za-z0-9_]+)?\s*\)\s*-\s*\[\s*:\s*([A-Za-z0-9_]+)\s*\]\s*->\s*\(\s*([A-Za-z0-9_]+)?(?:\s*:\s*[A-Za-z0-9_]+)?\s*\)))'
    for match in re.finditer(pattern_right, raw_query):
        # match.group(1) is the whole pattern, then groups for source, rel, and target follow.
        source_var = match.group(2)
        rel = match.group(3)
        target_var = match.group(4)
        source = source_var.lower() if source_var else "unknown_source"
        target = target_var.lower() if target_var else "unknown_target"
        edges.append({
            "source": source,  # For right arrow, the left node is the source.
            "target": target,  # The right node is the target.
            "label": rel
        })


    # Build the list of nodes from the dictionary.
    nodes_list = [{"id": var, "label": label} for var, label in node_dict.items()]

    diagram_data = {
        "nodes": nodes_list,
        "edges": edges
    }
    return diagram_data

# STARTING FROM CLUSTER
# -------------------
# Node -> 1 Hop
# -------------------
@query_template("""
MATCH (C:Cluster)<-[:BELONGS_TO]-(N:K8sNode)
WHERE C.id = "{cluster_id}"
RETURN C, N
""")
def test_get_all_nodes(tx, cluster_id: str):
    # Use the template and format it with parameters.
    query = test_get_all_nodes.query_template.format(cluster_id=cluster_id)
    return tx.run(query)

# -------------------
# Node -> 2 Hops
# -------------------
@query_template("""
    MATCH (C:Cluster)<-[:BELONGS_TO]-(N:K8sNode)
    WHERE C.id = "{cluster_id}"
    MATCH (N)-[:HAS_ANNOTATION]->(A:Annotation)
    RETURN C, N, A
""")
def test_get_all_node_annotations(tx, cluster_id: str):
    """
    Get all nodes and their annotations for a given cluster_id.
    1 Hop query, + 1 -> 2 hop Traversal
    """
    query = test_get_all_node_annotations.query_template.format(cluster_id=cluster_id)
    return tx.run(query)

@query_template("""
    MATCH (C:Cluster)<-[:BELONGS_TO]-(N:K8sNode)
    WHERE C.id = "{cluster_id}"
    MATCH (N)-[:HAS_LABEL]->(L:Label)
    RETURN C, N, L
""")
def test_get_all_node_labels(tx, cluster_id: str):
    """
    Get all nodes and their annotations for a given cluster_id.
    1 Hop query, + 1 -> 2 hop Traversal
    """
    query = test_get_all_node_labels.query_template.format(cluster_id=cluster_id)
    return tx.run(query)

@query_template("""
    MATCH (C:Cluster)<-[:BELONGS_TO]-(N:K8sNode)
    WHERE C.id = "{cluster_id}"
    MATCH (N)-[:HAS_TAINT]->(T:Taint)
    RETURN C, N, T
""")
def test_get_all_node_taint(tx, cluster_id: str):
    """
    Get all nodes and their annotations for a given cluster_id.
    1 Hop query, + 1 -> 2 hop Traversal
    """
    query = test_get_all_node_taint.query_template.format(cluster_id=cluster_id)
    return tx.run(query)

@query_template("""
    MATCH (C:Cluster)<-[:BELONGS_TO]-(N:K8sNode)
    WHERE C.id = "{cluster_id}"
    MATCH (N)-[:STORES]->(I:Image)
    RETURN C, N, I
""")
def test_get_all_node_images(tx, cluster_id: str):
    """
    USING KWOK, NO IMAGES WILL BE FOUND
    Get all nodes and their annotations for a given cluster_id.
    1 Hop query, + 1 -> 2 hop Traversal
    """
    query = test_get_all_node_images.query_template.format(cluster_id=cluster_id)
    return tx.run(query)


# -------------------
# POD 2 Hops
# -------------------
@query_template("""
    MATCH (P:Pod)-[:SCHEDULED_ON]->(N:K8sNode)-[:BELONGS_TO]->(C:Cluster) 
    WHERE C.id = "{cluster_id}"
    Return C, N, P
""")
def test_get_all_nodes_pods(tx, cluster_id: str):
    """
    Get all nodes and pods
    - 2 Hops
    """
    
    query = test_get_all_nodes_pods.query_template.format(cluster_id=cluster_id)
    return tx.run(query)

# -------------------
# POD 3 Hops
# -------------------
@query_template("""
    MATCH (P:Pod)-[:SCHEDULED_ON]->(N:K8sNode)-[:BELONGS_TO]->(C:Cluster)
    WHERE C.id = "{cluster_id}"
    MATCH (P)-[:HAS_LABEL]->(L:Label)
    RETURN C, N, P, L
""")
def test_get_all_pods_labels(tx, cluster_id: str):
    """
    Get all labels for all pods
    - 3 Hops
    """
    query = test_get_all_pods_labels.query_template.format(cluster_id=cluster_id)
    return tx.run(query)

@query_template("""
    MATCH (P:Pod)-[:SCHEDULED_ON]->(N:K8sNode)-[:BELONGS_TO]->(C:Cluster)
    WHERE C.id = "{cluster_id}"
    MATCH (P)-[:HAS_ANNOTATION]->(A:Annotation)
    RETURN C, N, P, A
""")
def test_get_all_pods_annotations(tx, cluster_id: str):
    """
    Get all labels for all pods
    - 3 Hops
    """
    query = test_get_all_pods_annotations.query_template.format(cluster_id=cluster_id)
    return tx.run(query)

@query_template("""
    MATCH (P:Pod)-[:SCHEDULED_ON]->(N:K8sNode)-[:BELONGS_TO]->(C:Cluster)
    WHERE C.id = "{cluster_id}"
    MATCH (P)-[:RUNS_CONTAINER]->(Cont:Container)
    RETURN C, N, P, Cont
""")
def test_get_all_pods_containers(tx, cluster_id: str):
    """
    Get all containers for all pods
    - 3 Hops
    """
    query = test_get_all_pods_containers.query_template.format(cluster_id=cluster_id)
    return tx.run(query)

@query_template("""
    MATCH (P:Pod)-[:SCHEDULED_ON]->(N:K8sNode)-[:BELONGS_TO]->(C:Cluster)
    WHERE C.id = "{cluster_id}"
    MATCH (P)-[:MANAGED_BY]->(R:ReplicaSet)
    RETURN C, N, P, R
""")
def test_get_all_pods_replicasets(tx, cluster_id: str):
    """
    Get all containers for all pods
    - 3 Hops
    """
    query = test_get_all_pods_replicasets.query_template.format(cluster_id=cluster_id)
    return tx.run(query)

# -------------------
# POD 4 Hops
# -------------------
@query_template("""
    MATCH (P:Pod)-[:SCHEDULED_ON]->(N:K8sNode)-[:BELONGS_TO]->(C:Cluster)
    WHERE C.id = "{cluster_id}"
    MATCH (P)-[:MANAGED_BY]->(R:ReplicaSet)-[:HAS_LABEL]->(L:Label)
    RETURN C, N, P, R, L
""")
def test_get_all_pods_replicasets_labels(tx, cluster_id: str):
    """
    Get all containers for all pods
    - 4 Hops
    """
    query = test_get_all_pods_replicasets_labels.query_template.format(cluster_id=cluster_id)
    return tx.run(query)

@query_template("""
    MATCH (P:Pod)-[:SCHEDULED_ON]->(N:K8sNode)-[:BELONGS_TO]->(C:Cluster)
    WHERE C.id = "{cluster_id}"
    MATCH (P)-[:MANAGED_BY]->(R:ReplicaSet)-[:HAS_ANNOTATION]->(A:Annotation)
    RETURN C, N, P, R, A
""")
def test_get_all_pods_replicasets_annotations(tx, cluster_id: str):
    """
    Get all containers for all pods
    - 4 Hops
    """
    query = test_get_all_pods_replicasets_annotations.query_template.format(cluster_id=cluster_id)
    return tx.run(query)

@query_template("""
    MATCH (P:Pod)-[:SCHEDULED_ON]->(N:K8sNode)-[:BELONGS_TO]->(C:Cluster)
    WHERE C.id = "{cluster_id}"
    MATCH (P)-[:MANAGED_BY]->(R:ReplicaSet)-[:MANAGED_BY]->(D:Deployment)
    RETURN C, N, P, R, D
""")
def test_get_all_pods_deployments(tx, cluster_id: str):
    """
    Get all containers for all pods
    - 4 Hops
    """
    query = test_get_all_pods_deployments.query_template.format(cluster_id=cluster_id)
    return tx.run(query)

@query_template("""
    MATCH (P:Pod)-[:SCHEDULED_ON]->(N:K8sNode)-[:BELONGS_TO]->(C:Cluster)
    WHERE C.id = "{cluster_id}"
    MATCH (P)-[:RUNS_CONTAINER]->(Cont:Container)-[:INSTANTIATES]->(I:Image)
    RETURN C, N, P, Cont, I
""")
def test_get_all_pods_images(tx, cluster_id: str):
    """
    USING KWOK, NO IMAGES WILL BE FOUND
    Get all images, for all containers of all pods
    - 4 Hops
    """
    query = test_get_all_pods_images.query_template.format(cluster_id=cluster_id)
    return tx.run(query)

@query_template("""
    MATCH (P:Pod)-[:SCHEDULED_ON]->(N:K8sNode)-[:BELONGS_TO]->(C:Cluster)
    WHERE C.id = "{cluster_id}"
    MATCH (P)-[:RUNS_CONTAINER]->(Cont:Container)-[:CONFIGMAP_REF]->(Conf:ConfigMap)
    RETURN C, N, P, Cont, Conf
""")
def test_get_all_container_configmaps(tx, cluster_id: str):
    """
    USING KWOK, NO CONFIGMAPS WILL BE FOUND
    Get all containers for all pods
    - 4 Hops
    """
    query = test_get_all_container_configmaps.query_template.format(cluster_id=cluster_id)
    return tx.run(query)

# -------------------
# POD 5 Hops
# -------------------
@query_template("""
    MATCH (P:Pod)-[:SCHEDULED_ON]->(N:K8sNode)-[:BELONGS_TO]->(C:Cluster)
    WHERE C.id = "{cluster_id}"
    MATCH (P)-[:MANAGED_BY]->(R:ReplicaSet)-[:MANAGED_BY]->(D:Deployment)-[:HAS_ANNOTATION]->(A:Annotation)
    RETURN C, N, P, R, D, A
""")
def test_get_all_pods_deployments_annotations(tx, cluster_id: str):
    """
    Get all Annotations for the deployments via the replicaset for every pod of every node
    - 5 Hops
    """
    query = test_get_all_pods_deployments_annotations.query_template.format(cluster_id=cluster_id)
    return tx.run(query)

@query_template("""
    MATCH (P:Pod)-[:SCHEDULED_ON]->(N:K8sNode)-[:BELONGS_TO]->(C:Cluster)
    WHERE C.id = "{cluster_id}"
    MATCH (P)-[:MANAGED_BY]->(R:ReplicaSet)-[:MANAGED_BY]->(D:Deployment)-[:HAS_LABEL]->(L:Label)
    RETURN C, N, P, R, D, L
""")
def test_get_all_pods_deployments_labels(tx, cluster_id: str):
    """
    Get all containers for all pods
    - 5 Hops
    """
    query = test_get_all_pods_deployments_labels.query_template.format(cluster_id=cluster_id)
    return tx.run(query)

# STARTING FROM NODE
# -------------------
# 1 Hop
# -------------------
@query_template("""
    MATCH (N:K8sNode)
    WHERE N.id = "{node_id}"
    MATCH (N)<-[:SCHEDULED_ON]-(P:Pod)
    RETURN N, P
""")
def test_get_pods_for_specific_node(tx, node_id):
    """
    1-hop
    Get pods for node
    """
    query = test_get_pods_for_specific_node.query_template.format(node_id=node_id)
    return tx.run(query, node_id=node_id)

@query_template("""
    MATCH (N:K8sNode)
    WHERE N.id = "{node_id}"
    MATCH (N)-[:HAS_LABEL]->(L:Label)
    RETURN N, L
""")
def get_labels_for_specific_node(tx, node_id):
    """
    1-hop
    Get pods for node
    """
    query = get_labels_for_specific_node.query_template.format(node_id=node_id)
    return tx.run(query, node_id=node_id)

@query_template("""
    MATCH (N:K8sNode)
    WHERE N.id = "{node_id}"
    MATCH (N)-[:HAS_TAINT]->(T:Taint)
    RETURN N, T
""")
def get_taint_for_specific_node(tx, node_id):
    """
    1-hop
    Get pods for node
    """
    query = get_taint_for_specific_node.query_template.format(node_id=node_id)
    return tx.run(query, node_id=node_id)

@query_template("""
    MATCH (N:K8sNode)
    WHERE N.id = "{node_id}"
    MATCH (N)-[:HAS_ANNOTATION]->(A:Annotation)
    RETURN N, A
""")
def get_annotation_for_specific_node(tx, node_id):
    """
    1-hop
    Get pods for node
    """
    query = get_annotation_for_specific_node.query_template.format(node_id=node_id)
    return tx.run(query, node_id=node_id)
# -------------------
# 2 Hop
# -------------------
@query_template("""
    MATCH (N:K8sNode)
    WHERE N.id = "{node_id}"
    MATCH (N)<-[:SCHEDULED_ON]-(P:Pod)
    MATCH (P)-[:HAS_LABEL]->(L:Label)
    RETURN N, P, L
""")
def get_pod_labels_for_specific_node(tx, node_id):
    """
    2-hop
    Get pods for node
    """
    query = get_pod_labels_for_specific_node.query_template.format(node_id=node_id)
    return tx.run(query, node_id=node_id)

@query_template("""
    MATCH (N:K8sNode)
    WHERE N.id = "{node_id}"
    MATCH (N)<-[:SCHEDULED_ON]-(P:Pod)
    MATCH (P)-[:HAS_ANNOTATION]->(A:Annotation)
    RETURN N, P, A
""")
def get_pod_annotations_for_specific_node(tx, node_id):
    """
    2-hop
    Get pods for node
    """
    query = get_pod_annotations_for_specific_node.query_template.format(node_id=node_id)
    return tx.run(query, node_id=node_id)

@query_template("""
    MATCH (N:K8sNode)
    WHERE N.id = "{node_id}"
    MATCH (N)<-[:SCHEDULED_ON]-(P:Pod)
    MATCH (P)-[:RUNS_CONTAINER]->(Cont:Container)
    RETURN N, P, Cont
""")
def get_pod_container_for_specific_node(tx, node_id):
    """
    2-hop
    Get pods for node
    """
    query = get_pod_container_for_specific_node.query_template.format(node_id=node_id)
    return tx.run(query, node_id=node_id)

@query_template("""
    MATCH (N:K8sNode)
    WHERE N.id = "{node_id}"
    MATCH (N)<-[:SCHEDULED_ON]-(P:Pod)
    MATCH (P)-[:MANAGED_BY]->(R:ReplicaSet)
    RETURN N, P, R
""")
def get_pod_replicaset_for_specific_node(tx, node_id):
    """
    2-hop
    Get pods for node
    """
    query = get_pod_replicaset_for_specific_node.query_template.format(node_id=node_id)
    return tx.run(query, node_id=node_id)

# -------------------
# 3 Hop
# -------------------
@query_template("""
    MATCH (N:K8sNode)
    WHERE N.id = "{node_id}"
    MATCH (N)<-[:SCHEDULED_ON]-(P:Pod)
    MATCH (P)-[:RUNS_CONTAINER]->(Cont:Container)-[:CONFIGMAP_REF]->(Conf:ConfigMap)
    RETURN N, P, Cont, Conf
""")
def get_pod_configmap_for_specific_node(tx, node_id):
    """
    USING KWOK, WONT BE FOUND
    3-hop
    """
    query = get_pod_configmap_for_specific_node.query_template.format(node_id=node_id)
    return tx.run(query, node_id=node_id)

@query_template("""
    MATCH (N:K8sNode)
    WHERE N.id = "{node_id}"
    MATCH (N)<-[:SCHEDULED_ON]-(P:Pod)
    MATCH (P)-[:RUNS_CONTAINER]->(Cont:Container)-[:INSTANTIATES]->(I:Image)
    RETURN N, P, Cont, I
""")
def get_pod_image_for_specific_node(tx, node_id):
    """
    USING KWOK, WONT BE FOUND
    3-hop
    """
    query = get_pod_image_for_specific_node.query_template.format(node_id=node_id)
    return tx.run(query, node_id=node_id)

@query_template("""
    MATCH (N:K8sNode)
    WHERE N.id = "{node_id}"
    MATCH (N)<-[:SCHEDULED_ON]-(P:Pod)
    MATCH (P)-[:MANAGED_BY]->(R:ReplicaSet)-[:MANAGED_BY]->(D:Deployment)
    RETURN N, P, R, D
""")
def get_pod_deployment_for_specific_node(tx, node_id):
    """
    3-hop
    """
    query = get_pod_deployment_for_specific_node.query_template.format(node_id=node_id)
    return tx.run(query, node_id=node_id)

@query_template("""
    MATCH (N:K8sNode)
    WHERE N.id = "{node_id}"
    MATCH (N)<-[:SCHEDULED_ON]-(P:Pod)
    MATCH (P)-[:MANAGED_BY]->(R:ReplicaSet)-[:HAS_LABEL]->(L:Label)
    RETURN N, P, R, L
""")
def get_pod_replicaset_label_for_specific_node(tx, node_id):
    """
    3-hop
    Get pods for node
    """
    query = get_pod_replicaset_label_for_specific_node.query_template.format(node_id=node_id)
    return tx.run(query, node_id=node_id)

@query_template("""
    MATCH (N:K8sNode)
    WHERE N.id = "{node_id}"
    MATCH (N)<-[:SCHEDULED_ON]-(P:Pod)
    MATCH (P)-[:MANAGED_BY]->(R:ReplicaSet)-[:HAS_ANNOTATION]->(A:Annotation)
    RETURN N, P, R, A
""")
def get_pod_replicaset_annotation_for_specific_node(tx, node_id):
    """
    3-hop
    Get pods for node
    """
    query = get_pod_replicaset_annotation_for_specific_node.query_template.format(node_id=node_id)
    return tx.run(query, node_id=node_id)
# -------------------
# 4 Hop
# -------------------
@query_template("""
    MATCH (N:K8sNode)
    WHERE N.id = "{node_id}"
    MATCH (N)<-[:SCHEDULED_ON]-(P:Pod)
    MATCH (P)-[:MANAGED_BY]->(R:ReplicaSet)-[:MANAGED_BY]->(D:Deployment)-[:HAS_LABEL]->(L:Label)
    RETURN N, P, R, D, L
""")
def get_pod_deployment_label_for_specific_node(tx, node_id):
    """
    4-hop
    """
    query = get_pod_deployment_label_for_specific_node.query_template.format(node_id=node_id)
    return tx.run(query, node_id=node_id)

@query_template("""
    MATCH (N:K8sNode)
    WHERE N.id = "{node_id}"
    MATCH (N)<-[:SCHEDULED_ON]-(P:Pod)
    MATCH (P)-[:MANAGED_BY]->(R:ReplicaSet)-[:MANAGED_BY]->(D:Deployment)-[:HAS_ANNOTATION]->(A:Annotation)
    RETURN N, P, R, D, A
""")
def get_pod_deployment_annotation_for_specific_node(tx, node_id):
    """
    3-hop
    """
    query = get_pod_deployment_annotation_for_specific_node.query_template.format(node_id=node_id)
    return tx.run(query, node_id=node_id)

# STARTING FROM POD
# -------------------
# 1 Hop
# -------------------
@query_template("""
    MATCH (P:Pod)
    WHERE P.id = "{pod_id}"
    MATCH (P)-[:HAS_LABEL]->(L:Label)
    RETURN P, L
""")
def get_pod_labels_for_specific_pod(tx, pod_id):
    """
    1-hop: Get labels for a specific pod.
    """
    query = get_pod_labels_for_specific_pod.query_template.format(pod_id=pod_id)
    return tx.run(query, pod_id=pod_id)

@query_template("""
    MATCH (P:Pod)
    WHERE P.id = "{pod_id}"
    MATCH (P)-[:HAS_ANNOTATION]->(A:Annotation)
    RETURN P, A
""")
def get_pod_annotations_for_specific_pod(tx, pod_id):
    """
    1-hop: Get annotations for a specific pod.
    """
    query = get_pod_annotations_for_specific_pod.query_template.format(pod_id=pod_id)
    return tx.run(query, pod_id=pod_id)

@query_template("""
    MATCH (P:Pod)
    WHERE P.id = "{pod_id}"
    MATCH (P)-[:RUNS_CONTAINER]->(Cont:Container)
    RETURN P, Cont
""")
def get_pod_container_for_specific_pod(tx, pod_id):
    """
    1-hop: Get containers for a specific pod.
    """
    query = get_pod_container_for_specific_pod.query_template.format(pod_id=pod_id)
    return tx.run(query, pod_id=pod_id)

@query_template("""
    MATCH (P:Pod)
    WHERE P.id = "{pod_id}"
    MATCH (P)-[:MANAGED_BY]->(R:ReplicaSet)
    RETURN P, R
""")
def get_pod_replicaset_for_specific_pod(tx, pod_id):
    """
    1-hop: Get ReplicaSet managing a specific pod.
    """
    query = get_pod_replicaset_for_specific_pod.query_template.format(pod_id=pod_id)
    return tx.run(query, pod_id=pod_id)

# -------------------
# 2 Hop Queries Starting from Pod
# -------------------
@query_template("""
    MATCH (P:Pod)
    WHERE P.id = "{pod_id}"
    MATCH (P)-[:MANAGED_BY]->(R:ReplicaSet)-[:MANAGED_BY]->(D:Deployment)
    RETURN P, R, D
""")
def get_pod_deployment_for_specific_pod(tx, pod_id):
    """
    2-hop: Get the Deployment managing a specific pod via its ReplicaSet.
    """
    query = get_pod_deployment_for_specific_pod.query_template.format(pod_id=pod_id)
    return tx.run(query, pod_id=pod_id)

@query_template("""
    MATCH (P:Pod)
    WHERE P.id = "{pod_id}"
    MATCH (P)-[:MANAGED_BY]->(R:ReplicaSet)-[:HAS_LABEL]->(L:Label)
    RETURN P, R, L
""")
def get_pod_replicaset_label_for_specific_pod(tx, pod_id):
    """
    1-hop: Get ReplicaSet managing a specific pod.
    """
    query = get_pod_replicaset_label_for_specific_pod.query_template.format(pod_id=pod_id)
    return tx.run(query, pod_id=pod_id)

@query_template("""
    MATCH (P:Pod)
    WHERE P.id = "{pod_id}"
    MATCH (P)-[:MANAGED_BY]->(R:ReplicaSet)-[:HAS_ANNOTATION]->(A:Annotation)
    RETURN P, R, A
""")
def get_pod_replicaset_annotation_for_specific_pod(tx, pod_id):
    """
    1-hop: Get ReplicaSet managing a specific pod.
    """
    query = get_pod_replicaset_annotation_for_specific_pod.query_template.format(pod_id=pod_id)
    return tx.run(query, pod_id=pod_id)


@query_template("""
    MATCH (P:Pod)
    WHERE P.id = "{pod_id}"
    MATCH (P)-[:RUNS_CONTAINER]->(Cont:Container)-[:CONFIGMAP_REF]->(Conf:ConfigMap)
    RETURN P, Cont, Conf
""")
def get_pod_configmap_for_specific_pod(tx, pod_id):
    """
    USING KWOK, WONT BE FOUND
    2-hop: Get ConfigMaps referenced by containers of a specific pod.
    """
    query = get_pod_configmap_for_specific_pod.query_template.format(pod_id=pod_id)
    return tx.run(query, pod_id=pod_id)

@query_template("""
    MATCH (P:Pod)
    WHERE P.id = "{pod_id}"
    MATCH (P)-[:RUNS_CONTAINER]->(Cont:Container)-[:INSTANTIATES]->(I:Image)
    RETURN P, Cont, I
""")
def get_pod_image_for_specific_pod(tx, pod_id):
    """
    USING KWOK, WONT BE FOUND
    2-hop: Get Images instantiated by containers of a specific pod.
    """
    query = get_pod_image_for_specific_pod.query_template.format(pod_id=pod_id)
    return tx.run(query, pod_id=pod_id)
# -------------------
# 3 Hop Queries Starting from Pod
# -------------------
@query_template("""
    MATCH (P:Pod)
    WHERE P.id = "{pod_id}"
    MATCH (P)-[:MANAGED_BY]->(R:ReplicaSet)-[:MANAGED_BY]->(D:Deployment)-[:HAS_LABEL]->(L:Label)
    RETURN P, R, D, L
""")
def get_pod_deployment_label_for_specific_pod(tx, pod_id):
    """
    2-hop: Get the Deployment managing a specific pod via its ReplicaSet.
    """
    query = get_pod_deployment_label_for_specific_pod.query_template.format(pod_id=pod_id)
    return tx.run(query, pod_id=pod_id)

@query_template("""
    MATCH (P:Pod)
    WHERE P.id = "{pod_id}"
    MATCH (P)-[:MANAGED_BY]->(R:ReplicaSet)-[:MANAGED_BY]->(D:Deployment)-[:HAS_ANNOTATION]->(A:Annotation)
    RETURN P, R, D, A
""")
def get_pod_deployment_annotation_for_specific_pod(tx, pod_id):
    """
    2-hop: Get the Deployment managing a specific pod via its ReplicaSet.
    """
    query = get_pod_deployment_annotation_for_specific_pod.query_template.format(pod_id=pod_id)
    return tx.run(query, pod_id=pod_id)

# STARTING FROM REPLICASET
# -------------------
# 1 Hop
# -------------------
@query_template("""
    MATCH (R:ReplicaSet)
    WHERE R.id = "{replicaset_id}"
    MATCH (P:Pod)-[:MANAGED_BY]->(R)
    RETURN R, P
""")
def get_pods_for_replicaset(tx, replicaset_id):
    """
    1-hop: Get replicaset for pods
    """
    query = get_pods_for_replicaset.query_template.format(replicaset_id=replicaset_id)
    return tx.run(query, replicaset_id=replicaset_id)


@query_template("""
    MATCH (R:ReplicaSet)
    WHERE R.id = "{replicaset_id}"
    MATCH (R)-[:MANAGED_BY]->(D:Deployment)
    RETURN R, D
""")
def get_deployment_for_replicaset(tx, replicaset_id):
    """
    1-hop: Get replicaset for pods
    """
    query = get_deployment_for_replicaset.query_template.format(replicaset_id=replicaset_id)
    return tx.run(query, replicaset_id=replicaset_id)

# -------------------
# 2 Hop
# -------------------
@query_template("""
    MATCH (R:ReplicaSet)
    WHERE R.id = "{replicaset_id}"
    MATCH (P:Pod)-[:MANAGED_BY]->(R)
    MATCH (P)-[:HAS_LABEL]->(L:Label)
    RETURN R, P, L
""")
def get_pod_labels_for_replicaset(tx, replicaset_id):
    """
    2-hop: Get replicaset for pods
    """
    query = get_pod_labels_for_replicaset.query_template.format(replicaset_id=replicaset_id)
    return tx.run(query, replicaset_id=replicaset_id)

@query_template("""
    MATCH (R:ReplicaSet)
    WHERE R.id = "{replicaset_id}"
    MATCH (P:Pod)-[:MANAGED_BY]->(R)
    MATCH (P)-[:HAS_ANNOTATION]->(A:Annotation)
    RETURN R, P, A
""")
def get_pod_annotations_for_replicaset(tx, replicaset_id):
    """
    2-hop: Get replicaset for pods
    """
    query = get_pod_annotations_for_replicaset.query_template.format(replicaset_id=replicaset_id)
    return tx.run(query, replicaset_id=replicaset_id)

@query_template("""
    MATCH (R:ReplicaSet)
    WHERE R.id = "{replicaset_id}"
    MATCH (P:Pod)-[:MANAGED_BY]->(R)
    MATCH (P)-[:RUNS_CONTAINER]->(Cont:Container)
    RETURN R, P, Cont
""")
def get_pod_container_for_replicaset(tx, replicaset_id):
    """
    2-hop: Get replicaset for pods
    """
    query = get_pod_container_for_replicaset.query_template.format(replicaset_id=replicaset_id)
    return tx.run(query, replicaset_id=replicaset_id)

# -------------------
# 3 Hop
# -------------------
@query_template("""
    MATCH (R:ReplicaSet)
    WHERE R.id = "{replicaset_id}"
    MATCH (R)-[:MANAGED_BY]->(D:Deployment)
    MATCH (D)-[:HAS_LABEL]->(L:Label)
    RETURN R, D, L
""")
def get_deployment_label_for_replicaset(tx, replicaset_id):
    """
    3-hop: Get replicaset for pods
    """
    query = get_deployment_label_for_replicaset.query_template.format(replicaset_id=replicaset_id)
    return tx.run(query, replicaset_id=replicaset_id)

@query_template("""
    MATCH (R:ReplicaSet)
    WHERE R.id = "{replicaset_id}"
    MATCH (R)-[:MANAGED_BY]->(D:Deployment)
    MATCH (D)-[:HAS_ANNOTATION]->(A:Annotation)
    RETURN R, D, A
""")
def get_deployment_annotation_for_replicaset(tx, replicaset_id):
    """
    3-hop: Get replicaset for pods
    """
    query = get_deployment_annotation_for_replicaset.query_template.format(replicaset_id=replicaset_id)
    return tx.run(query, replicaset_id=replicaset_id)



