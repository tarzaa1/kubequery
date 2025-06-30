import os
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
from kubequery.utils.benchmark.helpers import abbreviate_legend, get_desc_from_test_id, get_percentile_values, improved_plot_bar_chart, improved_plot_boxplot, load_csvs, plot_multiple_line_chart

IMAGE_DIR = "kubequery/static/images/"

def plot_create_graph(test_ids, metric="both"):
    """
    Compare CPU and memory usage during graph generation for given test IDs.
    Plots CPU and memory usage on the same plot for each test ID.

    test_ids -> list of test IDs
    metric -> CPU, MEM or BOTH

    Plot MEM AND CPU, or Both on one plot.
    """
    
    csvs = load_csvs(real_stats=True, queries=True, throughput=False)
    df_real_stats = csvs[0]
    df_queries = csvs[1]

    if metric not in ["cpu", "mem", "both"]:
        print(f"Invalid metric: {metric}")
        return False

    x_values = []
    y_values = []
    legend_labels = []
    colors = []
    styles = []

    for test_id in test_ids:
        df_queries_test = df_queries[df_queries["test_id"].str.strip() == test_id].copy()

        create_graph_query = df_queries_test.loc[
            df_queries_test["query_name"].str.contains("create graph", case=False, na=False)
        ].head(1)

        if create_graph_query.empty:
            print(f"No 'create graph' query found for {test_id}, skipping.")
            continue

        create_graph_query = create_graph_query.squeeze()
        start_ts = create_graph_query["start_ts"]
        end_ts = create_graph_query["end_ts"]
        db = create_graph_query["db_name"]
        desc = create_graph_query["desc"]

        base_label = abbreviate_legend(desc, db)
        color_cycle = plt.rcParams['axes.prop_cycle'].by_key()['color']
        db_color = color_cycle[0] if "memgraph" in db.lower() else color_cycle[1]

        df_filtered = df_real_stats[
            (df_real_stats["timestamp"] >= start_ts) &
            (df_real_stats["timestamp"] <= end_ts)
        ].copy()

        df_filtered["elapsed_sec"] = (df_filtered["timestamp"] - start_ts).dt.total_seconds()
        df_filtered["elapsed_min"] = df_filtered["elapsed_sec"] / 60

        # Process CPU
        df_filtered["cpu_percent"] = pd.to_numeric(df_filtered["cpu_percent"], errors="coerce")
        df_filtered["cpu_percent_smooth"] = df_filtered["cpu_percent"].rolling(window=5, center=True, min_periods=1).mean()

        # Process MEM
        df_filtered["mem_percent"] = pd.to_numeric(df_filtered["mem_percent"], errors="coerce")
        df_filtered["mem_percent_smooth"] = df_filtered["mem_percent"].rolling(window=5, center=True, min_periods=1).mean()

        if metric in ["cpu", "both"]:
            x_values.append(df_filtered["elapsed_min"])
            y_values.append(df_filtered["cpu_percent_smooth"])
            legend_labels.append(f"{base_label} CPU")
            colors.append(db_color)
            styles.append('-')

        if metric in ["mem", "both"]:
            x_values.append(df_filtered["elapsed_min"])
            y_values.append(df_filtered["mem_percent_smooth"])
            legend_labels.append(f"{base_label} MEM")
            colors.append(db_color)
            styles.append('-')

    # Set plot title and file name
    if metric == "both":
        title = "CPU and Memory Usage During Graph Generation"
        file_suffix = "cpu-mem"
    else:
        title = f"{metric.upper()} Usage During Graph Generation"
        file_suffix = metric

    filename = os.path.join(
        IMAGE_DIR,
        f"Compare-{'_'.join(test_ids)}-{file_suffix}.png"
    )

    # Plot
    plot_multiple_line_chart(
        filepath=filename,
        title=title,
        xlabel="Elapsed Time (minutes)",
        ylabel="Usage (%)",
        legend_labels=legend_labels,
        x_values=x_values,
        y_values=y_values,
        colors=colors,
        styles=styles,
        downsample_step=100
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
                        "Num Queries": 0
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
                aggregator["Num Queries"] += 1

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
        legend_name = abbreviate_legend(desc, db_name)
        legend_values.append(legend_name)

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
        desc = get_desc_from_test_id(test_id)

        legend_name = abbreviate_legend(desc, db_name)
        legend_values.append(legend_name)
    
    # Use the Query Name column as categories; assume all tests share the same queries.
    categories = df_stats["Query Name"].tolist()

    title = "Throughput Performance of K-Hop Queries"

    filepath = os.path.join(
        IMAGE_DIR,
        f"barplot_queries_by_throughput_{'_'.join(test_ids)}.png"
    )
    
    improved_plot_bar_chart(
        filepath, 
        title, 
        "Total Queries Executed",  # y-axis label
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
        cg_durations.append([int(cg_duration) / 60])  # Put in a list so each test is one group
        print(db_name, desc, cg_duration)

        legend_name = abbreviate_legend(desc, db_name)
        legend_labels.append(legend_name)

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
        rotation=False,
        logscale=False,
        figure_size=(8, 6)
    )
    improved_plot_bar_chart(
        filepath=cg_filename,
        title="Create Graph Duration Comparison",
        ylabel="Duration (minutes)",
        values=cg_durations,          # List of lists, each sub-list is [duration] for that test
        xlabels=legend_labels,     # e.g. "3mCk8 -> memgraph", "7LP2z -> neo4j"
        categories=["Create graph"],     # single x-axis label
        rotation=False,
        logscale=False,
        figure_size=(8, 6)
    )
    return True

def queries_boxplot(test_ids, queries):
    """
    Generates a side-by-side boxplot comparing query durations across databases/test IDs on the same plot.
    Dynamically handles the queries input for X-axis labels.
    """

    csvs = load_csvs(real_stats=False, queries=True, throughput=False)
    df_all = csvs[0]

    data_groups = []  # outer: DB/test_id, inner: queries, inner-most: durations
    db_labels = []    # labels for legend

    for test_id in test_ids:
        df_test = df_all[df_all["test_id"] == test_id].copy()
        if df_test.empty:
            print(f"No data for test_id {test_id}")
            continue

        db_name = df_test["db_name"].iloc[0]
        desc = get_desc_from_test_id(test_id=test_id)
        legend_name = abbreviate_legend(desc, db_name)
        db_labels.append(legend_name)

        query_data = []
        for query_name in queries:
            df_query = df_test[
                df_test["query_name"].str.startswith(query_name, na=False)
            ].copy()

            if df_query.empty:
                print(f"No queries for '{query_name}' in test_id {test_id}")
                query_data.append([])
                continue

            df_query["duration"] = pd.to_numeric(df_query["duration"], errors="coerce")
            durations = df_query["duration"].dropna().tolist()
            query_data.append(durations)

        data_groups.append(query_data)

    if not data_groups:
        print("No valid data to plot.")
        return

    # Build query_labels dynamically from queries (or map to short names if you'd like)
    query_labels = queries  

    test_ids_combined = "_".join(test_ids)
    output_path = os.path.join(IMAGE_DIR, f"queries_boxplot_{test_ids_combined}.png")
    improved_plot_boxplot(
        filepath=output_path,
        title="Latency Boxplot for Different Queries",
        ylabel="Duration (ms)",
        data_groups=data_groups,
        db_labels=db_labels,
        query_labels=query_labels,
        figure_size=(14, 6)
    )





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

        db_name = df_throughput_test.loc[idx, "db_name"]
        desc = df_throughput_test.loc[idx, "desc"]
        legend_name = abbreviate_legend(desc, db_name)
        legend_labels.append(legend_name)
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

