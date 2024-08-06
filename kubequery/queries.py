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

def node_resources_info(tx, cluster_id: str, node_id: str):
    query = f"""
        MATCH (ps:Pod) -[SCHEDULED_ON]-> (n:K8sNode) -[BELONGS_TO]-> (c:Cluster)
        WHERE c.id = "{cluster_id}" AND n.id = "{node_id}"
        RETURN ps, n
        """
    request_cpu_lst = []
    request_memory_lst = []
    limit_cpu_lst = []
    limit_memory_lst = []
    node_data = {}
    result = tx.run(query)
    for record in result:
        pod_data = record.data()["ps"]
        if not node_data:
            node_data = record.data()["n"]
        request_cpu_lst.append(pod_data.get('request_cpu'))
        request_memory_lst.append(pod_data.get('request_memory'))
        limit_cpu_lst.append(pod_data.get('limit_cpu'))
        limit_memory_lst.append(pod_data.get('limit_memory'))
    request_cpu =  _sum_string_list(request_cpu_lst)
    request_memory = _sum_string_list(request_memory_lst)
    limit_cpu = _sum_string_list(limit_cpu_lst)
    limit_memory = _sum_string_list(limit_memory_lst)
    resources = {}
    resources["requests"] = {}
    resources["limits"] = {}
    resources["requests"]["cpu"] = _get_percentage(request_cpu, node_data["allocatable_cpu"] )
    resources["requests"]["memory"] = _get_percentage(request_memory, node_data["allocatable_memory"] )
    resources["limits"]["cpu"] = _get_percentage(limit_cpu, node_data["allocatable_cpu"] )
    resources["limits"]["memory"] = _get_percentage(limit_memory, node_data["allocatable_memory"])
    return resources

def pods_info_by_cluster(tx, cluster_id : str):
    query = f"""
        MATCH (pods:Pod) -[SCHEDULED_ON]-> (node:K8sNode) -[BELONGS_TO]-> (cluster:Cluster)
        WHERE cluster.id = "{cluster_id}"
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

def _sum_string_list(str_list):
    if not str_list:
        return ""
    total = 0
    suffix = None
    for item in str_list:
        # break and return "" if contains None
        if not item:
            return ""
        # Separate numeric value and suffix
        num = ""
        for char in item:
            if char.isdigit():
                num += char
            else:
                suffix = item[len(num):]
                break
        total += int(num)
    return f"{total}{suffix}"

def _get_percentage(a1, a2):
    if not a1 or not a2:
        return "0%"
    num1 = num2 = suffix1 = suffix2 = ""
    for char in a1:
        if char.isdigit():
            num1 += char
        else:
            suffix1 = a1[len(num1):]
            break
    for char in a2:
        if char.isdigit():
            num2 += char
        else:
            suffix2 = a2[len(num2):]
            break
    num1 = int(num1)
    num2 = int(num2)
    if suffix1 == "Mi" and suffix2 == "Ki":
        result = (num1 * 1024 / num2) * 100
    elif suffix1 == "m" and suffix2 == "":
        result =  (num1 / num2 / 1000) * 100
    else:
        raise NotImplementedError
    return f"{result:.2f}%"
