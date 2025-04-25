import json
import re


def count(tx, type):
    query = f"""
        MATCH (nodes:{type})
        RETURN count(nodes)
        """
    result = tx.run(query)
    count = None
    try:
        count = result.single().data()['count(nodes)']
    except AttributeError:
        pass
    except Exception as e:
        raise
    return count

def distinct_labels(tx):
    query = f"""
            MATCH (n) 
            RETURN distinct labels(n), count(*)
            """
    result = tx.run(query)
    stats_dict = {}
    try:
        for record in result:
            record_data = record.data()
            stats_dict[record_data.get('labels(n)')[0]
                    ] = record_data.get('count(*)')
    except AttributeError:
        pass
    except Exception as e:
        raise
    return stats_dict


def clusters_info(tx):
    query = f"""
            MATCH (n:Cluster) 
            RETURN n
            """
    result = tx.run(query)
    cluster_lst = []
    try:
        for record in result:
            record_data = record.data()
            cluster_lst.append(record_data.get('n'))
    except AttributeError:
        pass
    except Exception as e:
        raise
    return cluster_lst


def nodes_info(tx, cluster_id: str):
    query = f"""
        MATCH (nodes:K8sNode) -[BELONGS_TO]-> (cluster:Cluster)
        WHERE cluster.id = "{cluster_id}"
        RETURN nodes
        """
    result = tx.run(query)
    node_lst = []
    try:
        for record in result:
            record_data = record.data()
            node_lst.append(record_data.get('nodes'))
    except AttributeError:
        pass
    except Exception as e:
        raise
    return node_lst


def pods_info(tx, cluster_id: str, node_id: str):
    query = f"""
        MATCH (pods:Pod) -[SCHEDULED_ON]-> (node:K8sNode) -[BELONGS_TO]-> (cluster:Cluster)
        WHERE cluster.id = "{cluster_id}" AND node.id = "{node_id}"
        RETURN pods
        """
    result = tx.run(query)
    pod_lst = []
    try:
        for record in result:
            record_data = record.data()
            pod_lst.append(record_data.get('pods'))
    except AttributeError:
        pass
    except Exception as e:
        raise
    return pod_lst


def node_resources(tx, cluster_id: str, node_id: str):
    query = f"""
        MATCH (pod:Pod)-[:SCHEDULED_ON]->(node:K8sNode)-[BELONGS_TO]->(cluster:Cluster)
        WHERE cluster.id = "{cluster_id}" AND node.id = "{node_id}"
        MATCH (pod)-[:RUNS_CONTAINER]->(c:Container)
        RETURN node, sum(c.request_cpu) AS requestedCPU, sum(c.request_memory) AS requestedMemory, sum(c.limit_cpu) AS limitCPU, sum(c.limit_memory) AS limitMemory
        """
    resources = {}
    result = tx.run(query)
    try:
        data = result.single().data()
        node_data = data["node"]
        resources["name"] = node_data["hostname"]
        resources["requests"] = {"cpu": data["requestedCPU"]/1000,
                                "memory": data["requestedMemory"],
                                }
        resources["limits"] = {"cpu": data["limitCPU"]/1000,
                            "memory": data["limitMemory"],
                            }
        resources["allocatable"] = {"cpu": node_data["allocatable_cpu"],
                                    "memory": extract_number(node_data["allocatable_memory"])/1000,
                                    "ephemeral_storage": node_data["allocatable_ephemeral_storage"]
                                    }
        resources["utilization"] = {
            "cpu": extract_number(node_data["usage_cpu"])/100000000,
            "memory": extract_number(node_data["usage_memory"])/1000
        }
    except AttributeError:
        pass
    except Exception as e:
        raise
    return resources


def pods_info_by_cluster(tx, cluster_id: str):
    query = f"""
        MATCH (pods:Pod) -[SCHEDULED_ON]-> (node:K8sNode) -[BELONGS_TO]-> (cluster:Cluster)
        WHERE cluster.id = "{cluster_id}"
        RETURN pods
        """
    result = tx.run(query)
    pod_lst = []
    try:
        for record in result:
            record_data = record.data()
            pod_lst.append(record_data.get('pods'))
    except AttributeError:
        pass
    except Exception as e:
        raise
    return pod_lst


def subgraph(tx):

    nodes = []
    edges = []
    ids = []

    result = get_nodes(tx, 'K8sNode')
    for record in result:
        node = record['node']
        if node.element_id not in ids:
            nodes.append(
                {'id': node.element_id, 'name': node['name'], 'type': 'K8sNode'})
            ids.append(node.element_id)

    result = get_nodes(tx, 'Pod')
    for record in result:
        node = record['node']
        if node.element_id not in ids:
            nodes.append(
                {'id': node.element_id, 'name': node['name'], 'type': 'Pod'})
            ids.append(node.element_id)

    result = get_nodes(tx, 'Container')
    for record in result:
        node = record['node']
        if node.element_id not in ids:
            nodes.append(
                {'id': node.element_id, 'name': node['name'], 'type': 'Container'})
            ids.append(node.element_id)

    result = get_images(tx)
    for record in result:
        node = record['node']
        if node.element_id not in ids:
            nodes.append(
                {'id': node.element_id, 'name': node['name'], 'type': 'Image'})
            ids.append(node.element_id)
        r = record['r']
        if r.element_id not in ids:
            edges.append({'id': r.element_id, 'start': r.start_node.element_id,
                         'end': r.end_node.element_id, 'label': r.type})
            ids.append(r.element_id)

    result = get_edges(tx, "K8sNode", "Pod")
    for record in result:
        r = record['r']
        if r.element_id not in ids:
            edges.append({'id': r.element_id, 'start': r.start_node.element_id,
                         'end': r.end_node.element_id, 'label': r.type})
            ids.append(r.element_id)

    result = get_edges(tx, "Container", "Pod")
    for record in result:
        r = record['r']
        if r.element_id not in ids:
            edges.append({'id': r.element_id, 'start': r.start_node.element_id,
                         'end': r.end_node.element_id, 'label': r.type})
            ids.append(r.element_id)

    result = get_edges(tx, "Container", "Image")
    for record in result:
        r = record['r']
        if r.element_id not in ids:
            edges.append({'id': r.element_id, 'start': r.start_node.element_id,
                         'end': r.end_node.element_id, 'label': r.type})
            ids.append(r.element_id)

    subgraph = {'nodes': nodes, 'edges': edges}
    with open('kubequery/static/data/subgraph.json', 'w') as fp:
        json.dump(subgraph, fp, indent=4)


def get_nodes(tx, type):
    query = f"""
        MATCH (node:{type})
        RETURN node
        """
    return tx.run(query)


def get_images(tx):
    query = f"""
        MATCH (node:Image)--(:Container)
        MATCH (node)-[r]-()
        RETURN node, r
        """
    return tx.run(query)


def get_edges(tx, start, end):
    query = f"""
        MATCH (:{start})-[r]-(:{end})
        RETURN r
        """
    return tx.run(query)

def extract_number(string):
    match = re.match(r"(\d+)", string)
    if match:
        return int(match.group(1))
    else:
        return None


def delete_all(tx):
    query = """MATCH (n)
                DETACH DELETE n
            """
    return tx.run(query)

def get_replicaset(tx):
    query = """
    MATCH (r:ReplicaSet)
    RETURN r
    """
    result =  tx.run(query)
    rs_list = []
    try:
        for record in result:
            record_data = record.data()
            rs_list.append(record_data.get('replicasets')) # Make sure this is correct
    except AttributeError:
        pass
    except Exception as e:
        raise
    return rs_list
