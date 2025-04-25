import os, yaml, subprocess, tempfile

config_dir = "kubequery/static/k8sConfig/"


########## Yaml Management for Kubernetes Deployments ##########
def load_yaml_template(file_path):
    """Load yaml file based on file_path"""
    with open(file_path, "r") as f:
        return yaml.safe_load(f)

def update_deployment_template(deployment_obj, node_name, replicas):
    """
    Updates the deployment object with the given node name and number of replicas.
    """
    # Update metadata.name
    deployment_obj["metadata"]["name"] = f"deployment-for-{node_name}"
    
    # Update selector and pod template labels to reflect the node name
    deployment_obj["metadata"]["labels"]["app"] = f"{node_name}-pods"
    deployment_obj["spec"]["selector"]["matchLabels"]["app"] = f"{node_name}-pods"
    deployment_obj["spec"]["template"]["metadata"]["labels"]["app"] = f"{node_name}-pods"
    
    # Update the replica count
    deployment_obj["spec"]["replicas"] = replicas
    
    # Update nodeName in the pod template
    deployment_obj["spec"]["template"]["spec"]["nodeName"] = node_name
    
    # Update the metrics annotation in the pod template metadata
    deployment_obj["spec"]["template"]["metadata"]["annotations"]["metrics.k8s.io/resource-metrics-path"] = f"/metrics/nodes/{node_name}/metrics/resource"
    
    return deployment_obj

def apply_deployment_yaml(deployment_obj):
    """Writes the updated deployment object to a temporary file and applies it."""
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as tmp:
        # Get the temporary file name
        temp_file = tmp.name
        yaml.dump(deployment_obj, tmp, default_flow_style=False)
        # Ensure the data is written before we call kubectl
        tmp.flush()
    subprocess.run(["kubectl", "apply", "-f", temp_file])
    os.remove(temp_file)



