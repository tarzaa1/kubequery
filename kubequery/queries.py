import json

def count(tx, type):
    query = f"""
        MATCH (nodes:{type})
        RETURN count(nodes)
        """
    result = tx.run(query)
    return result.single().data()['count(nodes)']

def distinct_labels(tx):
    query = f"""
            MATCH (n) 
            RETURN distinct labels(n), count(*)
            """
    result = tx.run(query)
    # TODO error handeling
    stats_dict = {}
    for record in result:
        record_data = record.data()
        stats_dict[record_data.get('labels(n)')[0]] = record_data.get('count(*)')
    return stats_dict

def clusters_info(tx):
    query = f"""
            MATCH (n:Cluster) 
            RETURN n
            """
    result = tx.run(query)
    cluster_lst = []
    for record in result:
        record_data = record.data()
        cluster_lst.append(record_data.get('n'))
    return cluster_lst

def nodes_info(tx, cluster_id : str):
    query = f"""
        MATCH (nodes:K8sNode) -[BELONGS_TO]-> (cluster:Cluster)
        WHERE cluster.id = "{cluster_id}"
        RETURN nodes
        """
    result = tx.run(query)
    node_lst = []
    for record in result:
        record_data = record.data()
        node_lst.append(record_data.get('nodes'))
    return node_lst

def pods_info(tx, cluster_id : str, node_id : str):
    query = f"""
        MATCH (pods:Pod) -[SCHEDULED_ON]-> (node:K8sNode) -[BELONGS_TO]-> (cluster:Cluster)
        WHERE cluster.id = "{cluster_id}" AND node.id = "{node_id}"
        RETURN pods
        """
    result = tx.run(query)
    pod_lst = []
    for record in result:
        record_data = record.data()
        pod_lst.append(record_data.get('pods'))
    return pod_lst

def subgraph(tx):

    nodes = []
    edges = []
    ids = []

    result = get_nodes(tx, 'K8sNode')
    for record in result:
        node = record['node']
        if node.element_id not in ids:
            nodes.append({'id': node.element_id, 'name': node['name'], 'type': 'K8sNode'})
            ids.append(node.element_id)

    result = get_nodes(tx, 'Pod')
    for record in result:
        node = record['node']
        if node.element_id not in ids:
            nodes.append({'id': node.element_id, 'name': node['name'], 'type': 'Pod'})
            ids.append(node.element_id)

    result = get_nodes(tx, 'Container')
    for record in result:
        node = record['node']
        if node.element_id not in ids:
            nodes.append({'id': node.element_id, 'name': node['name'], 'type': 'Container'})
            ids.append(node.element_id)

    result = get_images(tx)
    for record in result:
        node = record['node']
        if node.element_id not in ids:
            nodes.append({'id': node.element_id, 'name': node['name'], 'type': 'Image'})
            ids.append(node.element_id)
        r = record['r']
        if r.element_id not in ids:
            edges.append({'id': r.element_id, 'start': r.start_node.element_id, 'end':r.end_node.element_id, 'label': r.type})
            ids.append(r.element_id)
    
    result = get_edges(tx, "K8sNode", "Pod")
    for record in result:
        r = record['r']
        if r.element_id not in ids:
            edges.append({'id': r.element_id, 'start': r.start_node.element_id, 'end':r.end_node.element_id, 'label': r.type})
            ids.append(r.element_id)

    result = get_edges(tx, "Container", "Pod")
    for record in result:
        r = record['r']
        if r.element_id not in ids:
            edges.append({'id': r.element_id, 'start': r.start_node.element_id, 'end':r.end_node.element_id, 'label': r.type})
            ids.append(r.element_id)

    result = get_edges(tx, "Container", "Image")
    for record in result:
        r = record['r']
        if r.element_id not in ids:
            edges.append({'id': r.element_id, 'start': r.start_node.element_id, 'end':r.end_node.element_id, 'label': r.type})
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