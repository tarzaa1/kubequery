import datetime
from kubequery.queries import *
from kubequery.conf import MEMGRAPH_PID, NEO4J_PID
import csv, time, docker, os
from datetime import datetime  # Import the datetime class

TEST_DOCKER_CSV="kubequery/static/data/test.csv"
REAL_DOCKER_CSV = "kubequery/static/data/real_docker_stats.csv"
CSV_PATH = "kubequery/static/data/benchmark.csv"
THROUGHPUT_CSV = "kubequery/static/data/throughput.csv"
CONTINUOUS_DOCKER_STATS_CSV = "kubequery/static/data/continuous_docker_stats.csv"

IMAGE_DIR = "kubequery/static/images/"

REPETITIONS = 100 # How many times you repeatedly run a query for sequential testing, finding out latency
THROUGHPUT_TIME = 60 # How many times can you execute a query x amount of seconds?


################################################################################
# Query Timing & Logging -> Benchmarking
################################################################################

def time_query(neo4j, query, *args):
    """High-precision timing of a query's start/end by executing it 10 times consecutively."""
    start_ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    start_perf = time.perf_counter()
    neo4j.execute_read(query, *args)
    total_duration = time.perf_counter() - start_perf
    end_ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return total_duration, start_ts, end_ts

def export_query(test_id, query_name, duration, start_ts, end_ts, num_clusters, num_nodes, num_pods, desc, num_hops, db_name):
    """
    Writes a single row to the benchmark CSV, including hop count.
    """
    with open(CSV_PATH, "a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            test_id,
            start_ts,
            end_ts,
            duration,
            num_clusters,
            num_nodes,
            num_pods,
            query_name,
            desc,
            num_hops,
            db_name 
        ])

def export_throughput(test_id, query_name, num_clusters, num_nodes, num_pods, desc, total_queries, elapsed, throughput, num_hops, db_name):
    """
    Writes a single row to the throughput CSV file, including hop count.
    """
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with open(THROUGHPUT_CSV, "a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            test_id,
            timestamp,
            num_clusters,
            num_nodes,
            num_pods,
            elapsed,
            total_queries,
            throughput,
            query_name,
            desc,
            num_hops,
            db_name  
        ])

def sequential_benchmark_query(neo4j, test_id, query_name, num_clusters, num_nodes, num_pods, desc, query, num_hops, db_name, *args):
    """Sequential timing of a query by executing it multiple times consecutively."""
    print(f"Starting sequential benchmark for {query_name}...")

    # Warm-up iteration (ignored in timing)
    neo4j.execute_read(query, *args)

    total_start_ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    total_start_perf = time.perf_counter()

    # We'll adjust the number of repetitions based on query duration.
    adjusted_reps = REPETITIONS
    THRESHOLD = 180  # seconds (3 minutes)

    for i in range(1, REPETITIONS + 1):
        start_ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        start_perf = time.perf_counter()
        
        neo4j.execute_read(query, *args)
        
        end_ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        duration = time.perf_counter() - start_perf
        
        # On the first iteration, check if the query duration exceeds the threshold.
        if i == 1 and duration > THRESHOLD:
            adjusted_reps = 5
            print(f"Query duration of {duration:.2f} seconds exceeds {THRESHOLD} seconds. Reducing total repetitions to {adjusted_reps}.")
        
        # Export individual query run details.
        export_query(test_id, f"{query_name}-{i}", duration, start_ts, end_ts, num_clusters, num_nodes, num_pods, desc, num_hops, db_name)
        
        time.sleep(0.1)
        # If we have reached the adjusted repetition count, break out of the loop.
        if i >= adjusted_reps:
            break

    total_end_ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    total_duration = time.perf_counter() - total_start_perf

    # Export the overall benchmark run
    export_query(test_id, f"sequential-run-{query_name}", total_duration, total_start_ts, total_end_ts, num_clusters, num_nodes, num_pods, desc, num_hops, db_name)

    print(f"Total time for {query_name}: {adjusted_reps} repetitions, {total_duration:.2f} seconds")
    
def throughput_benchmark_query(neo4j, test_id, query_name, num_clusters, num_nodes, num_pods, desc, query, num_hops, db_name, *args):

    """
    Benchmark how many times a query can be executed in a fixed time window.
    """
    print(f"Running throughput benchmark for {query_name} for {THROUGHPUT_TIME} seconds")

    # Warm-up iteration (ignored)
    neo4j.execute_read(query, *args)

    counter = 0
    start_time = time.perf_counter()

    # Run queries repeatedly for x seconds
    while time.perf_counter() - start_time < THROUGHPUT_TIME:
        neo4j.execute_read(query, *args)
        counter += 1

    elapsed = time.perf_counter() - start_time
    throughput = counter / elapsed  # Queries per second

    # Export throughput results to CSV
    export_throughput(test_id, query_name, num_clusters, num_nodes, num_pods, desc, counter, elapsed, throughput, num_hops, db_name)

    print(f"Query {query_name} executed {counter} times in {THROUGHPUT_TIME} seconds")


def find_db_pid(container, db_name, default_pid):
    """
    Try to find the PID of the database process inside the container.
    First, try using 'pidof'. If that returns nothing, use a fallback 'ps' command.
    Returns an integer PID if found, otherwise returns default_pid.
    """
    if db_name == "neo4j":
        search_cmd = "pidof neo4j"
    elif db_name == "memgraph":
        search_cmd = "pidof memgraph"
    else:
        return default_pid

    try:
        result = container.exec_run(search_cmd)
        pid_str = result.output.decode().strip()
    except Exception as e:
        print(f"Error executing '{search_cmd}': {e}")
        pid_str = ""
    
    if pid_str:
        try:
            return int(pid_str.split()[0])
        except ValueError as e:
            print("Error parsing PID from pidof output:", pid_str)
    
    # Fallback using ps (using bash)
    if db_name == "neo4j":
        ps_cmd = "ps -eo pid,user,comm | grep neo4j | grep -v grep | awk '{print $1}'"
    elif db_name == "memgraph":
        ps_cmd = "ps -eo pid,comm | grep memgraph | grep -v grep | awk '{print $1}'"
    
    try:
        result = container.exec_run(["bash", "-c", ps_cmd])
        pid_str = result.output.decode().strip()
        if pid_str:
            return int(pid_str.split()[0])
    except Exception as e:
        print(f"Error executing fallback command '{ps_cmd}': {e}")
    
    print(f"Using default PID: {default_pid}")
    return default_pid


def monitor_neo4j_process(test_id, stop_event, db_name="neo4j", interval=0.3, batch_size=5):
    """
    Monitors CPU and memory usage of a process (with PID determined by searching based on db_name)
    inside a container. CPU usage is computed from /proc/<pid>/stat and /proc/stat.
    Memory usage is computed from /proc/<pid>/statm and /proc/meminfo.
    Writes test_id, timestamp, normalized CPU usage (0-100) and memory usage (%) to a CSV file in batches.
    """
    import csv
    import os
    import time
    from datetime import datetime
    import docker

    REAL_DOCKER_CSV = "kubequery/static/data/real_docker_stats.csv"

    client = docker.from_env()
    container_name = f"kubegrapher-{db_name}-1"
    try:
        container = client.containers.get(container_name)
    except Exception as e:
        print("Error: Unable to find container", container_name, e)
        return

    # Get number of CPU cores from inside the container.
    try:
        nproc_result = container.exec_run("nproc")
        n_cores = int(nproc_result.output.decode().strip())
    except Exception as e:
        n_cores = 1

    # Ensure the output directory exists.
    out_dir = os.path.dirname(REAL_DOCKER_CSV)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    metrics_buffer = []


    if db_name == "neo4j":
        default_pid = NEO4J_PID
    elif db_name == "memgraph":
        default_pid = MEMGRAPH_PID
    
    else:
        print(f"Unsupported db_name: {db_name}")
        return

    pid = find_db_pid(container, db_name, default_pid)
    print(f"Monitoring process PID {pid} for database {db_name} in container {container_name}.")

    # Initialize previous CPU stats.
    prev_proc_ticks = None
    prev_total_ticks = None

    def read_proc_cpu():
        """
        Reads and returns the cumulative CPU ticks for the process (sum of utime and stime)
        from /proc/<pid>/stat.
        """
        result = container.exec_run(f"cat /proc/{pid}/stat")
        stat_line = result.output.decode().strip()
        # print(stat_line)
        if not stat_line:
            return None
        # The process name can contain spaces enclosed in parentheses. To parse safely,
        # find the position of the closing parenthesis and split the rest.
        try:
            end_paren = stat_line.rfind(")")
            before = stat_line[:end_paren+1].split()
            after = stat_line[end_paren+1:].split()
            fields = before + after
            # Fields: 14 (index 13) is utime and 15 (index 14) is stime.
            utime = float(fields[13])
            stime = float(fields[14])
            return utime + stime
        except Exception as e:
            print("Error parsing /proc/{}/stat:".format(pid), e)
            return None

    def read_total_cpu():
        """
        Reads and returns the total CPU ticks from /proc/stat (summing the numbers on the first line).
        """
        result = container.exec_run("cat /proc/stat")
        stat_output = result.output.decode().splitlines()
        if not stat_output:
            return None
        cpu_line = stat_output[0]  # e.g. "cpu  2255 34 2290 22625563 ..."
        parts = cpu_line.split()
        # print(cpu_line)
        if parts[0] != "cpu":
            return None
        try:
            total = sum(float(x) for x in parts[1:])
            return total
        except Exception as e:
            print("Error parsing /proc/stat:", e)
            return None

    # Use monotonic clock for scheduling
    next_time = time.monotonic()

    # -------------------
    # MAIN MONITORING LOOP
    # -------------------
    while not stop_event.is_set():
        start_loop = time.monotonic()
        # Update the next scheduled time.
        next_time += interval

        try:
            # Read process CPU ticks and total CPU ticks from proc stats.
            curr_proc_ticks = read_proc_cpu()
            curr_total_ticks = read_total_cpu()

            # If readings failed, wait until the next interval.
            if curr_proc_ticks is None or curr_total_ticks is None:
                while time.monotonic() < next_time:
                    time.sleep(0.001)
                continue

            # Compute CPU usage only if we have a previous reading.
            if prev_proc_ticks is not None and prev_total_ticks is not None:
                delta_proc = curr_proc_ticks - prev_proc_ticks
                delta_total = curr_total_ticks - prev_total_ticks
                # Avoid division by zero.
                if delta_total > 0:
                    # Compute normalized CPU usage.
                    normalized_cpu = (delta_proc / delta_total) * n_cores * 100
                    normalized_cpu = min(normalized_cpu, 100)
                else:
                    normalized_cpu = 0
            else:
                # For the first measurement, we cannot compute CPU usage.
                normalized_cpu = 0

            # Update previous readings.
            prev_proc_ticks = curr_proc_ticks
            prev_total_ticks = curr_total_ticks

            # Get memory usage from statm.
            statm_result = container.exec_run(f"cat /proc/{pid}/statm")
            statm_output = statm_result.output.decode().splitlines()
            if not statm_output:
                while time.monotonic() < next_time:
                    time.sleep(0.001)
                continue
            statm_line = statm_output[0]
            statm_parts = statm_line.split()
            rss_pages = float(statm_parts[1])
            actual_mem = (rss_pages * 4) / 1024  # Convert pages (4 KB each) to MB

            # Get total memory from meminfo.
            meminfo_result = container.exec_run("cat /proc/meminfo")
            meminfo_output = meminfo_result.output.decode().splitlines()
            if not meminfo_output:
                while time.monotonic() < next_time:
                    time.sleep(0.001)
                continue
            meminfo_line = meminfo_output[0]
            meminfo_parts = meminfo_line.split()
            total_mem_kb = float(meminfo_parts[1])
            total_mem = total_mem_kb / 1024  # Convert kB to MB

            mem_usage = (actual_mem / total_mem) * 100 if total_mem > 0 else 0

            # Generate timestamp with millisecond precision
            timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            metrics_buffer.append([test_id, timestamp, normalized_cpu, mem_usage, n_cores])

            if len(metrics_buffer) >= batch_size:
                with open(REAL_DOCKER_CSV, "a", newline="") as f:
                    csv.writer(f).writerows(metrics_buffer)
                metrics_buffer.clear()

        except Exception as e:
            print("Error processing snapshot:", e)

        # Wait until the scheduled next iteration time is reached.
        while time.monotonic() < next_time:
            time.sleep(0.001)

