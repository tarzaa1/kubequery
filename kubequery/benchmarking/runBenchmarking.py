from datetime import datetime, timedelta, timezone
from configparser import ConfigParser
import json
import random, string, time
from dotenv import dotenv_values
import subprocess
import os, select
# Kubequery imports
from kubequery.conf import KUBEINSIGHTS_DIR, KUBEGRAPHER_DIR
from kubequery.utils.benchmark.docker_utils import export_query

# Config files
CONFIG_FILE = "kubequery/config.ini"
CONTINUOUS_DOCKER_STATS_CSV = "kubequery/static/data/continuous_docker_stats.csv"

def generate_test_id():
    """Generate a unique test ID (5-character random string)."""
    return f"{''.join(random.choices(string.ascii_letters + string.digits, k=5))}"

def generate_kafka_topic():
    """Generate a random kafka topic for each test."""
    random_digits = "".join(random.choices(string.digits, k=5))
    return "cluster" + random_digits




def load_config():
    """Parses config.ini and returns a list of nodes, pods and queries specifications with default values if not set."""
    config = ConfigParser()
    config.read(CONFIG_FILE)

    # Fetch values, ensuring they are strings, and handle empty cases
    clusters_str = config.get("Experiments", "clusters", fallback="").strip()

    nodes_str = config.get("Experiments", "nodes", fallback="").strip()
    pods_str = config.get("Experiments", "pods", fallback="").strip()
    queries_str = config.get("Experiments", "queries", fallback="").strip()
    databases_str = config.get("Experiments", "databases", fallback="").strip()


    # Convert values, setting defaults if empty
    clusters_list = [int(x.strip()) for x in clusters_str.split(",") if x.strip().isdigit()] if clusters_str else [1]
    nodes_list = [int(x.strip()) for x in nodes_str.split(",") if x.strip().isdigit()] if nodes_str else [10]
    pods_list = [int(x.strip()) for x in pods_str.split(",") if x.strip().isdigit()] if pods_str else [100]

    queries = [x.strip() for x in queries_str.split(",") if x.strip()] if queries_str else ['test_get_all_nodes', 'test_get_all_node_annotations', 'test_get_all_node_labels', 'test_get_all_node_taint', 
                'test_get_all_nodes_pods', 'test_get_all_pods_labels', 'test_get_all_pods_annotations', 'test_get_all_pods_containers', 'test_get_all_pods_replicasets', 
                'test_get_all_pods_replicasets_labels', 'test_get_all_pods_replicasets_annotations', 'test_get_all_pods_deployments', 'test_get_all_pods_images',
                   'test_get_all_pods_deployments_annotations', 'test_get_all_pods_deployments_labels', 'test_get_pods_for_specific_node', 
                  'get_labels_for_specific_node', 'get_taint_for_specific_node', 'get_annotation_for_specific_node', 'get_pod_labels_for_specific_node', 
                  'get_pod_annotations_for_specific_node', 'get_pod_container_for_specific_node', 'get_pod_replicaset_for_specific_node',
                   'get_pod_deployment_for_specific_node', 'get_pod_replicaset_label_for_specific_node', 
                    'get_pod_replicaset_annotation_for_specific_node', 'get_pod_deployment_label_for_specific_node', 'get_pod_deployment_annotation_for_specific_node', 
                    'get_pod_labels_for_specific_pod', 'get_pod_annotations_for_specific_pod', 'get_pod_container_for_specific_pod', 'get_pod_replicaset_for_specific_pod', 
                    'get_pod_deployment_for_specific_pod', 'get_pod_replicaset_label_for_specific_pod', 'get_pod_replicaset_annotation_for_specific_pod', 
                      'get_pod_deployment_label_for_specific_pod', 
                    'get_pod_deployment_annotation_for_specific_pod', 'get_pods_for_replicaset', 'get_deployment_for_replicaset',
                      'get_pod_labels_for_replicaset', 'get_pod_annotations_for_replicaset', 'get_pod_container_for_replicaset', 'get_deployment_label_for_replicaset', 
                      'get_deployment_annotation_for_replicaset']
    databases = [x.strip() for x in databases_str.split(",") if x.strip()] if databases_str else [ # Default vals
        "neo4j", "memgraph"
    ]

    return clusters_list, nodes_list, pods_list, queries, databases


# Pass new env variables to kubegrapher if necessary
def wait_for_kubegrapher(kafka_topic, auth, db_name):
    """
    Launches kubeGrapher from its virtual environment and waits until either:
      - A status block starting with "Clusters:" is repeated 5 times consecutively (indicating stable output), or
      - No new messages appear for 1 minute.
    """
    # Load environment variables from kubeGrapher's .env file.
    kubegrapher_env = dotenv_values(f"{KUBEGRAPHER_DIR}/.env")
    env = os.environ.copy()


    # Update the AUTH and DB_NAME variables.
    kubegrapher_env["AUTH"] = auth
    kubegrapher_env["DB_NAME"] = db_name
    kubegrapher_env["KAFKA_TOPIC"] = kafka_topic

    env.update(kubegrapher_env)

    # Launch kubeGrapher with unbuffered output.
    cmd = [
        f"{KUBEGRAPHER_DIR}/venv/bin/python",
        "-u",
        "-m",
        "kubegrapher.run"
    ]

    process = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1  # line buffering
    )

    print("Waiting for kubeGrapher output...")

    # We only want to analyze "status blocks" that start with "Clusters:".
    in_status_block = False  # Are we currently collecting a status block?
    current_block_lines = [] # Accumulator for the current status block.
    previous_block = None    # Last complete status block we saw.
    repetition_count = 0     # Count of consecutive identical status blocks.
    required_repetitions = 5 # How many repetitions to consider output stable.
    timeout = 60             # Timeout in seconds for no output.

    while True:
        # Wait for up to 'timeout' seconds for output.
        ready, _, _ = select.select([process.stdout], [], [], timeout)
        if ready:
            line = process.stdout.readline()
            if not line:
                print("kubeGrapher terminated before a complete status block was found.")
                break

            stripped = line.strip()
            print("kubeGrapher output:", repr(stripped))

            # Check if this line marks the start of a status block.
            if stripped.startswith("Clusters:"):
                # Start a new status block.
                in_status_block = True
                current_block_lines = [stripped]
            elif in_status_block:
                if stripped == "":
                    # End of the status block.
                    current_block = "\n".join(current_block_lines)
                    print("Completed status block:\n", current_block)
                    # Compare to the previous block.
                    if previous_block is not None and current_block == previous_block:
                        repetition_count += 1
                        print(f"Status block repeated {repetition_count} time(s) in a row.")
                    else:
                        previous_block = current_block
                        repetition_count = 1
                        print("New status block received. Repetition counter reset.")

                    # If we have seen the same status block 5 times consecutively, exit.
                    if repetition_count >= required_repetitions:
                        print("Stable status block detected 5 times. Stopping.")
                        return "All nodes completed"
                    # Reset status block state.
                    in_status_block = False
                    current_block_lines = []
                else:
                    # Still within a status block; add the line.
                    current_block_lines.append(stripped)
            # Else: if not in a status block, we simply ignore the line.
        else:
            # No new messages within the timeout period.
            print("No new messages for 1 minute, continuing...")
            process.kill()
            return "No messages after 1 minute"

    print("Broken free")
    process.kill()
    return "All nodes completed"



def wait_for_kubeinsights(kafka_topic):
    """
    Launches kubeInsights using 'go run ./cmd/main/main.go' in its project directory
    and waits until a log line containing the required text is output.
    """
    # Load environment variables from kubeInsights' .env file.
    kubeinsights_env = dotenv_values(f"{KUBEINSIGHTS_DIR}/.env")
    env = os.environ.copy()    
    kubeinsights_env["KAFKA_TOPIC"] = kafka_topic

    env.update(kubeinsights_env)

    # Command to run kubeInsights.
    cmd = ["go", "run", "./cmd/main/main.go"]
    
    # Set the working directory so the relative paths in the command work correctly.
    cwd = KUBEINSIGHTS_DIR
    
    process = subprocess.Popen(
        cmd,
        env=env,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1  # Line-buffered
    )
    
    target_text = "Sending Event: Update NodeMetrics"
    print("Waiting for kubeInsights to output:", target_text)
    
    while True:
        line = process.stdout.readline()
        if not line:
            print("kubeInsights terminated before target text was found.")
            break
        print("kubeInsights:", line.strip())
        if target_text in line:
            print("kubeInsights target output detected, finishing...")
            time.sleep(5)
            break
    return process
      

def restart_kafka():
    """
    Navigates to the kubequery folder (where the docker-compose.yml is located),
    then runs 'docker-compose down' followed by 'docker-compose up -d' to restart Kafka.
    """
    # Set the directory where your docker-compose.yml file is located.
    kubeInsights_dir = KUBEINSIGHTS_DIR
    
    try:
        # Bring down the containers.
        print("Kubeinsights 'docker-compose down'...")
        yield "data: Restarting kafka...\n\n"

        subprocess.run(["docker-compose", "down"], cwd=kubeInsights_dir, check=True)
        
        # Bring up the containers in detached mode.
        print("Kubeinsights 'docker-compose up -d'...")
        yield "data: Starting up new database...\n\n"
        subprocess.run(["docker-compose", "up", "-d"], cwd=kubeInsights_dir, check=True)

        print("Database and kafka restarted successfully.")
    except subprocess.CalledProcessError as e:
        print("Error during Docker Compose operations:", e)


def restart_database(db_name="neo4j"):
    print("Test")
    """
    Remove any existing database container and start the specified one for benchmarking.

    For neo4j:
      - Container name: kubegrapher-neo4j-1
      - Ports: 7474 (HTTP) and 7687 (Bolt)
      - Volume: ./neo4j mounted at /data

    For memgraph:
      - Container name: kubegrapher-memgraph-1
      - Ports: 7687 (Bolt) and 3000 (Web UI)
      - Volume: ./memgraph mounted at /var/lib/memgraph
    """

    # Step 1: Remove any existing containers
    yield "data: Removing existing containers.\n\n"
    subprocess.run("docker rm -f kubegrapher-neo4j-1", shell=True, check=False)
    subprocess.run("docker rm -f kubegrapher-memgraph-1", shell=True, check=False)

    # Step 2: Prepare container configuration
    yield f"data: Starting up container {db_name}.\n\n"

    if db_name == "neo4j":
        container_name = "kubegrapher-neo4j-1"
        image = "neo4j:5.26.0"
        volume_path = os.path.join(os.getcwd(), "neo4j")
        ports = "-p 7474:7474 -p 7687:7687"
        volume_mount = f"-v {volume_path}:/data"

    elif db_name == "memgraph":
        container_name = "kubegrapher-memgraph-1"
        image = "memgraph/memgraph:latest"
        volume_path = os.path.join(os.getcwd(), "memgraph")
        ports = "-p 7474:7474 -p 7687:7687"
        volume_mount = f"-v {volume_path}:/var/lib/memgraph"

    else:
        raise ValueError(f"Database '{db_name}' is not supported.")

    # Ensure the volume directory exists
    os.makedirs(volume_path, exist_ok=True)

    # Step 3: Run the container
    cmd = f"docker run -d --name {container_name} {ports} {volume_mount} {image}"
    print("Running command:", cmd)
    subprocess.run(cmd, shell=True, check=True)

    # Step 4: Wait for the DB to start up
    yield f"data: Restarted {db_name} successfully...\n\n"
    time.sleep(10)
    print("Database restarted successfully.")



def run_sequence(test_id, total_num_pods, num_nodes, num_pods, description, auth, db_name):
    """
    Runs the sequence:
      1. Launch kubeInsights and wait until its target log line is detected.
      2. KubeInsights process is terminated. (kubeInsights could theoretically be adjusted to continue run in the background.)
      3. Launch kubeGrapher and wait until its target log line is detected.
         If kubeGrapher fails (i.e. an exception is raised), retry up to 3 total attempts.
         A result of "No messages after 1 minute" is considered a success.
      4. Once kubeGrapher completes its execution, the process is terminated.
    """
    yield "data: Waiting....\n\n"
    kafka_topic = generate_kafka_topic()

    time.sleep(60)
    # 1. Launch kubeInsights in background and wait for its target log line.
    start_ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    start_perf = time.perf_counter()
    print("\nStarting kubeInsights...")
    yield "data: Pushing to kafka...\n\n"
    # Launch kubeInsights and wait for its target output.
    kubeinsights_proc = wait_for_kubeinsights(kafka_topic)  # Returns the process once target is detected.


    # 2. Launch kubeInsights in background and wait for its target log line.

    kubeinsights_proc.terminate()

    duration = time.perf_counter() - start_perf
    end_ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    export_query(test_id, "Push to kafka", duration, start_ts, end_ts,
                 1, num_nodes, num_pods, description, 0, db_name)

    yield f"data: {json.dumps({'type': 'toggleDiv', 'div': 'dash'})}\n\n"

    # 3/4. Launch kubeGrapher and wait for its target output with retry attempts.
    max_retries = 3
    attempt = 0
    success = False
    while attempt < max_retries and not success:
        attempt += 1
        print(f"\nStarting kubeGrapher attempt {attempt}...")
        attempt_start_ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        attempt_start_perf = time.perf_counter()
        yield "data: Creating graph...\n\n"
        try:
            result = wait_for_kubegrapher(kafka_topic, auth, db_name)
            # Even if result == "No messages after 1 minute", it's considered a success.
            success = True
        except Exception as e:
            error_message = f"{datetime.now().isoformat()} - Attempt {attempt} failed with error: {e}\n"
            with open("static/data/error.txt", "a") as f:
                f.write(error_message)
            print(f"Attempt {attempt} failed with error: {e}. Retrying in 2 seconds...")
            time.sleep(2)

    if not success:
        print("kubeGrapher failed after maximum retry attempts.")
        kubeinsights_proc.terminate()
        raise RuntimeError("kubeGrapher failed after maximum retry attempts.")

    # Use timestamps from the successful attempt.
    attempt_duration = time.perf_counter() - attempt_start_perf
    end_ts_dt = datetime.now(timezone.utc)
    if result == "No messages after 1 minute":
        # Adjust the duration and end timestamp if needed.
        attempt_duration -= 60
        end_ts_dt -= timedelta(seconds=60)
    attempt_end_ts = end_ts_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    export_query(test_id, "Create graph", attempt_duration, attempt_start_ts, attempt_end_ts,
                 1, num_nodes, num_pods, description, 0, db_name)

    # # 3. Terminate the kubeInsights process.
    # print("\nKubeGrapher finished. Terminating kubeInsights process.")
    # kubeinsights_proc.terminate()
    print("\nAll processes have completed their initial output signals. Sequence complete.")
    yield "data: Sequence complete.\n\n"



# restart_database("memgraph")