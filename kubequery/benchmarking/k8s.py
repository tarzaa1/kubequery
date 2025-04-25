import json
import os
import time
import yaml
import subprocess
from copy import deepcopy
from kubernetes import client, config
from kubequery.utils.benchmark.yaml_utils import load_yaml_template, update_deployment_template, apply_deployment_yaml

# Configuration directory for YAML templates
config_dir = "kubequery/static/k8sConfig/"

# Paths to the template files
node_template_file = os.path.join(config_dir, "node.yaml")
deployment_template_file = os.path.join(config_dir, "deployment.yaml")

metrics_file = os.path.join(config_dir, "metrics-usage.yaml")
usage_file = os.path.join(config_dir, "usage-from-annotation.yaml")
resource_file = os.path.join(config_dir, "metrics-resource.yaml")
fast_workflow = os.path.join(config_dir, "workflow-fast.yaml")
# Load Kubernetes Configuration
try:
    config.load_kube_config()
except:
    pass

# Initialize API Clients
v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()

# Creating/Deleting cluster
def delete_kwok_clusters():
    """Deletes all KWOK clusters."""
    print("Deleting KWOK clusters...")
    os.system("kwokctl delete cluster --all")
    print("All clusters have been removed.")

def clear_kwok_cluster():
    """Clears all pods and nodes except the control-plane node."""
    # Delete all delpoyments
    os.system("kubectl delete deploy --all")

    # Delete all nodes except 'kwok-kwok-control-plane'
    os.system("kubectl delete node $(kubectl get nodes --no-headers | grep -v 'kwok-kwok-control-plane' | awk '{print $1}')")
    print("Cluster cleared (control plane node preserved).")
    os.system("kubectl delete pod --all")

def generate_kwok_cluster(cluster_num):
    """
    Deploys a KWOK cluster with Metrics Server and Argo Workflows.
    The cluster will be named 'kwok-{cluster_num}'.
    """
    # Ensure required files exist before proceeding.
    required_files = [metrics_file, usage_file, resource_file, fast_workflow]
    for file in required_files:
        if not os.path.isfile(file):
            print(f"Error: Missing configuration file {file}.")
            return

    cluster_name = f"kwok-{cluster_num}"
    print(f"Creating KWOK cluster {cluster_name}...")
    os.system(f"kwokctl create cluster --name {cluster_name} --enable-metrics-server -c {fast_workflow} -c {metrics_file} -c {resource_file}")

    print(f"KWOK simulated cluster {cluster_name} with Metrics Server and Argo Workflows is ready.")


def batch_scale_kwok_nodes_with_deployment(num_nodes=1, num_pods=1, cluster_name=""):
    """
    Creates nodes and a deployment per node for scaling pods.
    Node names are prefixed with the cluster name to ensure uniqueness.
    """
    nodes_list = []
    for i in range(1, num_nodes + 1):
        # Prefix node names with the cluster name.
        node_name = f'{cluster_name}-node-{i}' if cluster_name else f'node-{i}'
        node_obj = deepcopy(load_yaml_template(node_template_file))
        node_obj["metadata"]["name"] = node_name
        node_obj["metadata"]["annotations"]["metrics.k8s.io/resource-metrics-path"] = f"/metrics/nodes/{node_name}/metrics/resource"
        nodes_list.append(node_obj)

    scaled_nodes_file = os.path.join(config_dir, f"scaled_nodes_{cluster_name}.yaml")
    with open(scaled_nodes_file, "w") as f:
        yaml.dump_all(nodes_list, f, default_flow_style=False)
    subprocess.run(["kubectl", "apply", "-f", scaled_nodes_file])

    # Create a deployment for each node.
    for i in range(1, num_nodes + 1):
        node_name = f'{cluster_name}-node-{i}' if cluster_name else f'node-{i}'

        deployment_template = deepcopy(load_yaml_template(deployment_template_file))
        updated_deployment = update_deployment_template(deployment_template, node_name, replicas=num_pods)
        apply_deployment_yaml(updated_deployment)

        # Calculate progress percentage.
        progress_percent = int((i / num_nodes) * 100)
        # Yield the structured progress update event.
        yield f"data: {json.dumps({'type': 'scaleDeployments', 'data': {
                "node": node_name,
                "current": i,
                "total": num_nodes,
                "pods_per_node": num_pods,
                "progress": progress_percent
            }})}\n\n"
        if i % 50 == 0:
            wait_for_deployments_ready()

    print(f"Successfully applied {num_nodes} nodes and deployments with {num_pods} pods per node for cluster {cluster_name}.")

def wait_for_deployments_ready(interval=1):
    """
    Waits until all deployments in the cluster have all their replicas ready.
    """
    while True:
        output = subprocess.run(
            "kubectl get deployments --no-headers",
            shell=True, capture_output=True, text=True
        ).stdout.strip().split()

        if not output:
            print("No deployments found.")
            return

        rows = [output[i:i+5] for i in range(0, len(output), 5)]
        all_ready = True

        for row in rows:
            dep_name = row[0]
            replicas_status = row[1]  # e.g., "200/200"
            try:
                desired, available = replicas_status.split("/")
                if desired != available:
                    all_ready = False
                    print(f"Deployment {dep_name}: {replicas_status} not ready")
            except Exception as e:
                print(f"Error parsing replicas for deployment {dep_name}: '{replicas_status}'. Error: {e}")
                all_ready = False

        if all_ready:
            print("All deployments are fully ready.")
            return

        print("Waiting for deployments to become ready...")
        time.sleep(interval)

def check_deployments_ready():
    """
    Checks if all deployments in the cluster have all their replicas ready.
    Returns True if all deployments are fully ready, otherwise returns False.
    """
    result = subprocess.run(
        "kubectl get deployments --no-headers",
        shell=True, capture_output=True, text=True
    )
    output_lines = result.stdout.strip().splitlines()

    if not output_lines:
        print("No deployments found.")
        return False

    all_ready = True

    # Process each line of output.
    for line in output_lines:
        parts = line.split()
        if len(parts) < 2:
            print(f"Unexpected output format: '{line}'")
            all_ready = False
            continue

        dep_name = parts[0]
        replicas_status = parts[1]  # e.g., "200/200"
        try:
            desired, available = replicas_status.split("/")
            if desired != available:
                all_ready = False
                print(f"Deployment {dep_name}: {replicas_status} not ready")
        except Exception as e:
            print(f"Error parsing replicas for deployment {dep_name}: '{replicas_status}'. Error: {e}")
            all_ready = False

    if all_ready:
        print("All deployments are fully ready.")
    else:
        print("Some deployments are not ready.")
    return all_ready

def k8s_main(num_clusters, num_nodes, num_pods):
    """
    Main execution function that creates multiple clusters.
    Each cluster is created with its own name (e.g. kwok-1, kwok-2, etc.)
    and is scaled with the specified number of nodes and pods.
    """
    yield "data: Deleting previous KWOK clusters...\n\n"
    delete_kwok_clusters() #Its easier and faster to just delete entire large clusters

    for i in range(1, num_clusters + 1):
        cluster_name = f"kwok-{i}"
        yield f"data: Generating new KWOK cluster {cluster_name} with Metrics Server...\n\n"
        generate_kwok_cluster(i)

        yield f"data: Scaling nodes and pods for cluster {cluster_name}...\n\n"

        yield f"data: {json.dumps({'type': 'toggleDiv', 'div': "deployment-progress"})}\n\n"
        yield from batch_scale_kwok_nodes_with_deployment(num_nodes, num_pods, cluster_name=cluster_name)

        yield f"data: Cluster creation complete\n\n"
        time.sleep(5)
        yield f"data: {json.dumps({'type': 'toggleDiv', 'div': "deployment-progress"})}\n\n"




