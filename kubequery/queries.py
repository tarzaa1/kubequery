import json

def count(tx, type):
    query = f"""
        MATCH (nodes:{type})
        RETURN count(nodes)
        """
    result = tx.run(query)
    print(f"{type}s: {result.single().data()['count(nodes)']}")

def stats(tx):
    print('\n')
    count(tx, 'K8sNode')
    count(tx, 'Pod')
    count(tx, 'Deployment')
    count(tx, 'ReplicaSet')
    count(tx, 'Label')
    count(tx, 'Annotation')
    count(tx, 'Image')
    count(tx, 'Container')
    count(tx, 'Taint')
    count(tx, 'Service')
    count(tx, 'ConfigMap')

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