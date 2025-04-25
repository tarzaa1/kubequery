import os
import csv
import time
import requests
import re
import psutil 

PROCESS_EXPORTER_URL = "http://localhost:9256/metrics"
PROCESS_NAME = "neo4j"  # Match groupname in process-exporter
CSV_FILE = "kubequery/static/data/real_docker_stats.csv"

# Ensure CSV file exists with headers
if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["test_id", "timestamp", "cpu_usage_percent", "memory_usage_mb"])


def get_metrics(interval, prev_cpu_seconds):
    """Fetch metrics from process-exporter and calculate CPU & memory usage."""
    try:
        response = requests.get(PROCESS_EXPORTER_URL, timeout=5)
        response.raise_for_status()
        metrics = response.text
    except requests.RequestException as e:
        print(f"Error fetching metrics: {e}")
        return None, None, prev_cpu_seconds

    # Extract CPU usage (sum of user & system CPU time)
    cpu_user, cpu_system, memory_usage = None, None, None
    for line in metrics.split("\n"):
        if re.match(f'namedprocess_namegroup_cpu_seconds_total{{groupname="{PROCESS_NAME}",mode="user"}}', line):
            cpu_user = float(line.split(" ")[1])
        elif re.match(f'namedprocess_namegroup_cpu_seconds_total{{groupname="{PROCESS_NAME}",mode="system"}}', line):
            cpu_system = float(line.split(" ")[1])
        elif re.match(f'namedprocess_namegroup_memory_bytes{{groupname="{PROCESS_NAME}",memtype="resident"}}', line):
            memory_usage = float(line.split(" ")[1]) / (1024 * 1024)  # Convert bytes to MB

    # Compute CPU usage over time
    if cpu_user is not None and cpu_system is not None:
        total_cpu_seconds = cpu_user + cpu_system

        if prev_cpu_seconds is None:
            prev_cpu_seconds = total_cpu_seconds
            return None, memory_usage, prev_cpu_seconds  # Skip first iteration

        # Calculate CPU usage difference over the interval
        cpu_usage_delta = total_cpu_seconds - prev_cpu_seconds
        prev_cpu_seconds = total_cpu_seconds  # Update for next iteration

        # Normalize to percentage
        cpu_cores = psutil.cpu_count(logical=True) or 1
        cpu_usage_percent = (cpu_usage_delta / interval) * 100 / cpu_cores
    else:
        cpu_usage_percent = None

    return cpu_usage_percent, memory_usage, prev_cpu_seconds


def monitor_stats(test_id, stop_event, interval=.1):
    """Continuously monitor CPU and memory usage and write to CSV."""
    prev_cpu_seconds = None
    print("Monitoring Neo4j process...")

    while not stop_event.is_set():  # <-- Stop condition added
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        cpu_usage, mem_usage, prev_cpu_seconds = get_metrics(interval, prev_cpu_seconds)

        if cpu_usage is not None and mem_usage is not None:
            print(f"[{timestamp}] CPU: {cpu_usage:.2f}%, Memory: {mem_usage:.2f} MB")

            # Write metrics to CSV
            with open(CSV_FILE, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([test_id, timestamp, cpu_usage, mem_usage])

        time.sleep(interval)  # Scrape every x seconds

    print("Monitoring stopped.")  # Confirmation message when thread stops


