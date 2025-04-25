import os
import numpy as np
import pandas as pd
from kubequery.utils.benchmark.helpers import get_desc_from_test_id, get_percentile_values, improved_plot_bar_chart, load_csvs, plot_boxplot, plot_multiple_line_chart

IMAGE_DIR = "kubequery/static/images/"


def plot_create_graph(test_ids, metric):
    """
    Compare CPU/memory usage during create graph between 1->* tests.

    Loads the data, filters for each test, extracts start and end times from the
    create graph queries, and then plots CPU/memory usage (using the same helper)
    for each test, along with a bar chart comparing durations.

    test_ids -> list of test id's
    metric -> cpu_percent or mem_percent
    """
    csvs = load_csvs(real_stats=True, queries=True, throughput=False)
    df_real_stats = csvs[0]
    df_queries = csvs[1]

    if metric not in ["cpu", "mem"]:
        print("Invalid metric", metric)
        return False
    

    x_values = []
    y_values = []
    descriptions = []
    for test_id in test_ids:
        # Filter for the current test_id
        df_queries_test = df_queries[df_queries["test_id"].str.strip() == test_id].copy()

        # Locate the create graph query.
        create_graph_query = df_queries_test.loc[
            df_queries_test["query_name"].str.contains("create graph", case=False, na=False)
        ].head(1)
        if create_graph_query.empty:
            print("No 'create graph' query found, cannot graph.")
            return False

        # Extract start and end timestamps.
        create_graph_query = create_graph_query.squeeze()
        start_ts = create_graph_query["start_ts"]
        end_ts = create_graph_query["end_ts"]
        db = create_graph_query["db_name"]

        desc = create_graph_query["desc"]
        descriptions.append(f"{desc}: {db}")

        # Filter real stats between start_ts and end_ts and compute elapsed time.
        df_filtered = df_real_stats[(df_real_stats["timestamp"] >= start_ts) &
                                    (df_real_stats["timestamp"] <= end_ts)].copy()
        df_filtered["elapsed_sec"] = (df_filtered["timestamp"] - start_ts).dt.total_seconds()
        # Ensure the metric is numeric and apply smoothing (rolling average).
        df_filtered[f"{metric}_percent"] = pd.to_numeric(df_filtered[f"{metric}_percent"], errors="coerce")
        df_filtered[f"{metric}_percent_smooth"] = df_filtered[f"{metric}_percent"].rolling(window=5, center=True, min_periods=1).mean()

        # Append the x and y values.
        x_values.append(df_filtered["elapsed_sec"])
        y_values.append(df_filtered[f"{metric}_percent_smooth"])

    # Define plot parameters.
    title = f"Stats over time when creating graph for tests: {' | '.join(test_ids)}"
    ylabel = f"{metric.upper()} Usage (%)"
    legend_labels = descriptions
    filename = os.path.join(
        IMAGE_DIR,
        f"Compare-{'_'.join(test_ids)}-{metric}.png"
    )

    # Create the plot.
    plot_multiple_line_chart(
        filepath=filename,
        title=title,
        xlabel="Elapsed Time (seconds)",
        ylabel=ylabel,
        legend_labels=legend_labels,
        x_values=x_values,
        y_values=y_values
    )
    return True


def analyse_queries(test_ids, queries, hop=3, aggregate_hops=False):
    """
    For each test_id in test_ids, analyze sequential queries and compute latency statistics.
    Returns a list of DataFrames one for each test_id with per-query statistics (or aggregated by hops).
    
    - Gets a table of statistics for each query prefix in queries.
    - If aggregate_hops is True, aggregates the stats by the num_hops value.
    """

    
    # Load your CSV data once for all test IDs
    print("\n HOPS", hop)
    csvs = load_csvs(real_stats=False, queries=True, throughput=True)
    df_queries_all = csvs[0]
    df_throughput_all = csvs[1]

    dfs = []  # List to store a DataFrame per test_id

    for test_id in test_ids:
        # Filter data for the current test_id
        df_queries = df_queries_all[df_queries_all["test_id"] == test_id].copy()
        df_throughput = df_throughput_all[df_throughput_all["test_id"] == test_id].copy()

        stats = []
        for query_name in queries:
            df_query_subset = df_queries[df_queries["query_name"].str.startswith(query_name, na=False)].copy()

            if df_query_subset.empty:
                print(f"No queries found for test_id={test_id} and query_name prefix '{query_name}'")
                query_stats = {
                "Test ID": test_id,
                "Query Name": query_name,
                "Database Name": "db_name",
                "Num Hops": 10, # Just sets a high value for missing query
                "Total Executions": 0,
                "Total Queries Executed (Throughput)": 0,
                "Min Latency (s)": 0,
                "Max Latency (s)": 0,
                "Mean Latency (s)": 0,
                "P50 Latency (s)": 0,
                "P75 Latency (s)": 0,
                "P95 Latency (s)": 0,
                "P99 Latency (s)": 0,
                "Stadard Deviation": 0,

                }
                stats.append(query_stats)
                continue

            # Convert duration to numeric
            df_query_subset["duration"] = pd.to_numeric(df_query_subset["duration"], errors="coerce")

            # Get a representative database name
            db_name_arr = df_query_subset['db_name'].unique()
            db_name = db_name_arr[0] if len(db_name_arr) > 0 else None

            # Get a scalar for num_hops (assume it is consistent across the subset)
            num_hops = df_query_subset["num_hops"].iloc[0] if not df_query_subset["num_hops"].empty else None

            # Compute total queries executed from throughput
            total_queries_executed = df_throughput[df_throughput["query_name"] == query_name]["total_queries"] \
                                        .astype(float).sum()
            total_queries_executed = total_queries_executed if not np.isnan(total_queries_executed) else 0

            query_stats = {
                "Test ID": test_id,
                "Query Name": query_name,
                "Database Name": db_name,
                "Num Hops": num_hops,
                "Total Executions": len(df_query_subset),
                "Total Queries Executed (Throughput)": int(total_queries_executed),
                "Min Latency (s)": df_query_subset["duration"].min(),
                "Max Latency (s)": df_query_subset["duration"].max(),
                "Mean Latency (s)": df_query_subset["duration"].mean(),
                "P50 Latency (s)": np.percentile(df_query_subset["duration"], 50),
                "P75 Latency (s)": np.percentile(df_query_subset["duration"], 75),
                "P95 Latency (s)": np.percentile(df_query_subset["duration"], 95),
                "P99 Latency (s)": np.percentile(df_query_subset["duration"], 99),
                "Stadard Deviation": np.std(df_query_subset["duration"]),
            }
            if hop !="" and hop != "All":
                if query_stats["Num Hops"] != hop:
                    continue
            stats.append(query_stats)

        # If aggregation by hops is required
        if aggregate_hops:
            aggregated_by_hops = {}  # Keyed by the Num Hops value

            for stat in stats:
                num_hops_val = stat.get("Num Hops")
                if pd.isna(num_hops_val):
                    continue

                if num_hops_val not in aggregated_by_hops:
                    aggregated_by_hops[num_hops_val] = {
                        "Test ID": test_id,
                        "Num Hops": num_hops_val,
                        "Query Name": [],
                        "Database Name": stat["Database Name"],
                        "Sum of Total Executions": 0,
                        "Sum of Throughput": 0,
                        "Sum of Min Latency (s)": 0.0,
                        "Sum of Max Latency (s)": 0.0,
                        "Sum of Mean Latency (s)": 0.0,
                        "Sum of P50 Latency (s)": 0.0,
                        "Sum of P75 Latency (s)": 0.0,
                        "Sum of P95 Latency (s)": 0.0,
                        "Sum of P99 Latency (s)": 0.0,
                    }

                aggregator = aggregated_by_hops[num_hops_val]
                aggregator["Query Name"].append(stat["Query Name"])
                aggregator["Sum of Total Executions"] += stat["Total Executions"]
                aggregator["Sum of Throughput"] += stat["Total Queries Executed (Throughput)"]
                aggregator["Sum of Min Latency (s)"] += stat["Min Latency (s)"]
                aggregator["Sum of Max Latency (s)"] += stat["Max Latency (s)"]
                aggregator["Sum of Mean Latency (s)"] += stat["Mean Latency (s)"]
                aggregator["Sum of P50 Latency (s)"] += stat["P50 Latency (s)"]
                aggregator["Sum of P75 Latency (s)"] += stat["P75 Latency (s)"]
                aggregator["Sum of P95 Latency (s)"] += stat["P95 Latency (s)"]
                aggregator["Sum of P99 Latency (s)"] += stat["P99 Latency (s)"]

            final_aggregated_stats = list(aggregated_by_hops.values())
            df_result = pd.DataFrame(final_aggregated_stats)
        else:
            df_result = pd.DataFrame(stats)

        dfs.append(df_result)
    
    return dfs


def plot_query_latency(analysis_data, percentile, aggregate=False):
    data_values_list = []
    legend_values = []
    test_ids = []
    for data in analysis_data:
        df_stats = data
        values, ylabel = get_percentile_values(percentile, df_stats, aggregate)
        data_values_list.append(values)
        print(data_values_list)

        db_name = df_stats["Database Name"].iloc[0]
        test_id = df_stats["Test ID"].iloc[0]
        
        test_ids.append(test_id)

        desc = get_desc_from_test_id(test_id)
        legend_values.append(f"{desc} -> {db_name}")

    if aggregate:
        categories = df_stats["Num Hops"]
    else:
        categories = df_stats["Query Name"]


    
    title=f"{percentile} Latency Comparison"
    if aggregate:
        filepath = os.path.join(IMAGE_DIR, f"barplot_k_hop_by_{percentile}_latency.png")
    else:
        filepath = os.path.join(IMAGE_DIR, f"barplot_queries_by_{percentile}_latency_{'_'.join(test_ids)}.png")
    improved_plot_bar_chart(
    filepath, title, ylabel,
    data_values_list,       # List of lists: each inner list is one bar group (e.g. test)
    legend_values,      # Labels for bar in `values` (test id, db names)
    categories,   # Labels for each group on x-axis (query names)
    rotation=True,
    figure_size=(14, 6)
)


def plot_query_throughput(analysis_data):
    """
    Compare throughput (Total Queries Executed) for multiple tests.

    For each test in analysis_data, it extracts throughput values and query names,
    then produces a grouped bar chart (using improved_plot_bar_chart) similar to the
    latency comparison.

    analysis_data: List of data objects (one per test) that can be converted to a DataFrame.
    """
    data_values_list = []
    legend_values = []
    test_ids = []
    print(analysis_data)
    for data in analysis_data:
        df_stats = data
        # Extract throughput values as a list.
        values = df_stats["Total Queries Executed (Throughput)"].tolist()
        data_values_list.append(values)

        db_name = df_stats["Database Name"].iloc[0]
        test_id = df_stats["Test ID"].iloc[0]
        test_ids.append(test_id)
        legend_values.append(f"{test_id} -> {db_name}")
    
    # Use the Query Name column as categories; assume all tests share the same queries.
    categories = df_stats["Query Name"].tolist()

    title = "Throughput Comparison"

    filepath = os.path.join(
        IMAGE_DIR,
        f"barplot_queries_by_throughput_{'_'.join(test_ids)}.png"
    )
    
    improved_plot_bar_chart(
        filepath, 
        title, 
        "Total Queries Executed (Throughput)",  # y-axis label
        data_values_list,       # List of lists: each inner list is one bar group (one test)
        legend_values,          # Legend labels (e.g. "TestID -> DB Name")
        categories,             # x-axis labels (query names)
        rotation=True,
        figure_size=(14, 6)
    )


def plot_kafka_cg_duration(test_ids):
    """
    Plot the "Push to kafka" duration for one or more test IDs in a single chart.
    Plot the "Create graph" duration for one or more test IDs in a single chart.

    If multiple test IDs are provided, each bar will be placed side by side
    for comparison.
    """
    csvs = load_csvs(real_stats=False, queries=True, throughput=False)
    df_queries = csvs[0]
    kafka_durations = []
    cg_durations = []

    legend_labels = []

    # For each test ID, extract the first "Push to kafka" query row
    for tid in test_ids:
        df_queries_test = df_queries[df_queries["test_id"].str.strip() == tid].copy()
        kafka_query = (
            df_queries_test[df_queries_test["query_name"].str.contains("Push to kafka", case=False, na=False)]
            .head(1)
            .squeeze()
        )

        if kafka_query.empty:
            print(f"No 'Push to kafka' query found for test_id={tid}. Skipping.")
            continue

        cg_query = (
            df_queries_test[df_queries_test["query_name"].str.contains("Create graph", case=False, na=False)]
            .head(1)
            .squeeze()
        )
        if cg_query.empty:
            print(f"No 'Create graph' query found for test_id={tid}. Skipping.")
            continue

        db_name = kafka_query["db_name"]
        kafka_duration = float(kafka_query["duration"])
        cg_duration = float(cg_query["duration"])
        desc = kafka_query["desc"]
        kafka_durations.append([kafka_duration])  # Put in a list so each test is one group
        cg_durations.append([cg_duration])  # Put in a list so each test is one group
        print(db_name, desc, cg_duration)

        legend_labels.append(f"{desc} -> {db_name}")

    # If no data was found, bail
    if not kafka_durations:
        print("No valid Kafka durations found for the provided test IDs.")
        return False
    if not cg_durations:
        print("No valid Kafka durations found for the provided test IDs.")
        return False

    # Create the chart
    # Only one category on the x-axis: "Push to kafka"

    # Build the filename using all test IDs
    kafka_filename = os.path.join(
        IMAGE_DIR,
        f"kafka_duration_{'_'.join(test_ids)}.png"
    )
    cg_filename = os.path.join(
        IMAGE_DIR,
        f"cg_duration_{'_'.join(test_ids)}.png"
    )
    # Use the improved bar chart that can handle multiple groups
    improved_plot_bar_chart(
        filepath=kafka_filename,
        title="Kafka Push Duration Comparison",
        ylabel="Duration (seconds)",
        values=kafka_durations,          # List of lists, each sub-list is [duration] for that test
        xlabels=legend_labels,     # e.g. "3mCk8 -> memgraph", "7LP2z -> neo4j"
        categories=["Push to kafka"],     # single x-axis label
        rotation=True,
        figure_size=(8, 6)
    )
    improved_plot_bar_chart(
        filepath=cg_filename,
        title="Create Graph Duration Comparison",
        ylabel="Duration (seconds)",
        values=cg_durations,          # List of lists, each sub-list is [duration] for that test
        xlabels=legend_labels,     # e.g. "3mCk8 -> memgraph", "7LP2z -> neo4j"
        categories=["Create graph"],     # single x-axis label
        rotation=True,
        figure_size=(8, 6)
    )
    return True

def queries_boxplot(test_ids, queries):
    # Load your CSV data
    csvs = load_csvs(real_stats=False, queries=True, throughput=False)
    df_all = csvs[0]  # original dataframe with all queries

    for test_id in test_ids:
        print(f"Processing test_id: {test_id}")
        # Filter the original dataframe for the current test_id
        df_queries = df_all[df_all["test_id"] == test_id].copy()
        
        if df_queries.empty:
            print(f"No queries found for test_id {test_id}")
            continue

        db_name = df_queries['db_name'].iloc[0]

        # Collect data and labels for all queries that match
        boxplot_data = []
        boxplot_labels = []
        
        for query_name in queries:
            # Filter rows whose query_name starts with the given prefix, making a copy so later modifications don't affect the original
            df_query_subset = df_queries[
                df_queries["query_name"].str.startswith(query_name, na=False)
            ].copy()

            if df_query_subset.empty:
                print(f"No queries found for test_id={test_id} and query_name prefix '{query_name}'")
                continue

            # Convert duration to numeric (this is done on a copy, so it's safe)
            df_query_subset["duration"] = pd.to_numeric(df_query_subset["duration"], errors="coerce")
            
            # Drop NaNs and collect durations
            durations = df_query_subset["duration"].dropna().tolist()
            if not durations:
                print(f"All durations are NaN or invalid for '{query_name}'")
                continue

            # Append data and label for the current query
            boxplot_data.append(durations)
            boxplot_labels.append(query_name)

        # Only plot if we have some valid data for at least one query
        if boxplot_data:
            output_path = os.path.join(IMAGE_DIR, f"queries_boxplot_{test_id}.png")
            plot_boxplot(
                data=boxplot_data,
                labels=boxplot_labels,
                title=f"Query Durations for Test {test_id} - '{db_name}'",
                output_path=output_path
            )
        else:
            print(f"No valid query durations found for test_id {test_id}")


def plot_throughput_query_resource_usage_windowed(test_ids, target_query_name, metric="cpu"):
    """
    Plot CPU or memory usage for a throughput query using the time window between its surrounding queries.

    Parameters:
    - test_ids: list of test IDs
    - target_query_name: exact name of the throughput query to isolate
    - metric: 'cpu' or 'mem'
    """
    if metric not in ["cpu", "mem"]:
        print("Invalid metric:", metric)
        return False

    csvs = load_csvs(real_stats=True, queries=False, throughput=True)
    df_real_stats = csvs[0]
    df_throughput = csvs[1]

    x_values = []
    y_values = []
    legend_labels = []

    for test_id in test_ids:
        df_throughput_test = df_throughput[df_throughput["test_id"].str.strip() == test_id].copy()
        df_throughput_test["timestamp"] = pd.to_datetime(df_throughput_test["timestamp"])
        df_throughput_test = df_throughput_test.sort_values("timestamp").reset_index(drop=True)

        # Find the target query index
        target_row = df_throughput_test[df_throughput_test["query_name"].str.strip() == target_query_name]

        if len(target_row) != 1:
            print(f"Query '{target_query_name}' not uniquely found for test_id={test_id}")
            continue

        idx = target_row.index[0]
        if idx == 0 or idx + 1 >= len(df_throughput_test):
            print(f"Cannot infer window for test_id={test_id}; target query is at the start or end of the list.")
            continue

        start_ts = df_throughput_test.loc[idx - 1, "timestamp"]
        end_ts = df_throughput_test.loc[idx + 1, "timestamp"]

        db = df_throughput_test.loc[idx, "db_name"]
        desc = df_throughput_test.loc[idx, "desc"]
        legend_labels.append(f"{desc} -> {db}")

        df_filtered = df_real_stats[
            (df_real_stats["timestamp"] >= start_ts) &
            (df_real_stats["timestamp"] <= end_ts)
        ].copy()

        df_filtered["elapsed_sec"] = (df_filtered["timestamp"] - start_ts).dt.total_seconds()
        df_filtered[f"{metric}_percent"] = pd.to_numeric(df_filtered[f"{metric}_percent"], errors="coerce")
        df_filtered[f"{metric}_percent_smooth"] = df_filtered[f"{metric}_percent"].rolling(
            window=5, center=True, min_periods=1).mean()

        x_values.append(df_filtered["elapsed_sec"])
        y_values.append(df_filtered[f"{metric}_percent_smooth"])

    if not x_values or not y_values:
        print("No data to plot.")
        return False

    title = f"{metric.upper()} Usage During: {target_query_name}"
    ylabel = f"{metric.upper()} Usage (%)"
    filename = os.path.join(IMAGE_DIR, f"{target_query_name}_throughput_windowed_{metric}_usage.png")

    plot_multiple_line_chart(
        filepath=filename,
        title=title,
        xlabel="Elapsed Time (seconds)",
        ylabel=ylabel,
        legend_labels=legend_labels,
        x_values=x_values,
        y_values=y_values
    )
    return True


def get_max_memory_usage_for_tests(test_ids, verbose=True):
    """
    For each test_id, returns the maximum memory usage recorded in the real_stats.csv.
    Used in report, not hooked up to front end.
    """
    csvs = load_csvs(real_stats=True, queries=True, throughput=False)
    df_real_stats = csvs[0]
    df_queries = csvs[1]  # For metadata like db and desc

    summary = []

    for tid in test_ids:
        df_stats = df_real_stats[df_real_stats["test_id"].str.strip() == tid].copy()
        if df_stats.empty:
            print(f"[!] No stats found for test_id: {tid}")
            continue

        # Get matching row in queries file to extract metadata
        meta = df_queries[df_queries["test_id"].str.strip() == tid].head(1)
        db_name = meta["db_name"].values[0] if not meta.empty else "Unknown"
        desc = meta["desc"].values[0] if not meta.empty else "N/A"

        df_stats["mem_percent"] = pd.to_numeric(df_stats["mem_percent"], errors="coerce")
        max_mem = df_stats["mem_percent"].max()

        result = {
            "test_id": tid,
            "db": db_name,
            "desc": desc,
            "max_mem_percent": max_mem
        }
        summary.append(result)

        if verbose:
            print(f"{desc} -> {db_name} | Max Memory Usage: {max_mem:.2f}%")

    return summary

