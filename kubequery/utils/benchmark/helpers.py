import os
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # non-GUI backend for multi-threading
import matplotlib.pyplot as plt

plt.style.use("seaborn-v0_8-notebook")

# File paths
REAL_DOCKER_CSV = "kubequery/static/data/real_docker_stats.csv"
CSV_PATH = "kubequery/static/data/benchmark.csv"
THROUGHPUT_CSV = "kubequery/static/data/throughput.csv"
STATS_CSV = "kubequery/static/data/real_docker_stats.csv"


def load_csvs(real_stats=True, queries=True, throughput=True):
    """Load CSV files and format the data."""
    CSVs = []
    if real_stats:
        # 1) Load REAL DOCKER STATS CSV
        df_real_stats = pd.read_csv(STATS_CSV, header=0, dtype=str)
        df_real_stats = df_real_stats[df_real_stats["test_id"] != "test_id"]
        df_real_stats["cpu_percent"] = pd.to_numeric(df_real_stats["cpu_percent"], errors="coerce")
        df_real_stats = df_real_stats.dropna(subset=["cpu_percent"])
        df_real_stats["timestamp"] = pd.to_datetime(
            df_real_stats["timestamp"],
            format="%Y-%m-%dT%H:%M:%S.%fZ",
            errors="coerce"
        )
        CSVs.append(df_real_stats)
        
        # 2) Load benchmark CSV
    if queries:
        df_queries = pd.read_csv(CSV_PATH, header=0, dtype=str)
        df_queries["start_ts"] = pd.to_datetime(df_queries["start_ts"], format="%Y-%m-%dT%H:%M:%SZ", errors="coerce")
        df_queries["end_ts"] = pd.to_datetime(df_queries["end_ts"], format="%Y-%m-%dT%H:%M:%SZ", errors="coerce")
        CSVs.append(df_queries)

    if throughput:
    # 3) Load throughput CSV
        df_throughput = pd.read_csv(THROUGHPUT_CSV, header=0, dtype=str)
        df_throughput = df_throughput[df_throughput["test_id"] != "test_id"]
        df_throughput["timestamp"] = pd.to_datetime(df_throughput["timestamp"], format="%Y-%m-%dT%H:%M:%SZ", errors="coerce")
        CSVs.append(df_throughput)

    return CSVs

def delete_from_csvs(test_ids):
    # df_real_stats = pd.read_csv(STATS_CSV, header=0, dtype=str)

    # df_queries = pd.read_csv(CSV_PATH, header=0, dtype=str)
    df_throughput = pd.read_csv(THROUGHPUT_CSV, header=0, dtype=str)
    df_throughput = df_throughput[~df_throughput["test_id"].isin(test_ids)]
    df_throughput.to_csv(THROUGHPUT_CSV, index=False)

    df_queries = pd.read_csv(CSV_PATH, header=0, dtype=str)
    df_queries = df_queries[~df_queries["test_id"].isin(test_ids)]
    df_queries.to_csv(CSV_PATH, index=False)

    df_real_stats = pd.read_csv(STATS_CSV, header=0, dtype=str)
    df_real_stats = df_real_stats[~df_real_stats["test_id"].isin(test_ids)]
    df_real_stats.to_csv(STATS_CSV, index=False)

def ensure_dir(filepath):
    """Ensure that the directory for the given filepath exists."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

def plot_line_chart(x, y, xlabel, ylabel, title, legend_label, filepath,
                    figure_size=(12, 6), grid=True, line_style='-', color='blue'):
    """General helper function to create a line chart and save it."""
    plt.figure(figsize=figure_size)
    plt.plot(x, y, linestyle=line_style, label=legend_label, color=color)
    if grid:
        plt.grid(True)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.legend()
    plt.tight_layout()
    plt.savefig(filepath)
    plt.close()


def plot_multiple_line_chart(
        filepath, title, xlabel, ylabel,
        legend_labels, 
        x_values,  # List of x-axis arrays/Series, one per line.
        y_values,  # List of y-axis arrays/Series, one per line.
        figure_size=(12, 6), grid=True, line_style='-', colors=None      
):  
    """
    Plot multiple line charts on the same plot.

    Parameters:
      filepath      : String - where to save the resulting image.
      title         : String - title of the chart.
      xlabel        : String - label for the x-axis.
      ylabel        : String - label for the y-axis.
      legend_labels : List of strings - one label for each line plotted.
      x_values      : List/array - each element is the x-axis values for a test.
      y_values      : List of lists - each inner list contains y-values for a test.
      figure_size   : Tuple - figure dimensions (default is (12, 6)).
      grid          : Boolean - whether to show grid lines.
      line_style    : String - line style to use (default '-').
      colors        : Optional - either a single color string or a list of color strings.
    """

    plt.figure(figsize=figure_size)

    # Handle colors: if a single color string is passed, convert it to a list.
    if colors is not None and isinstance(colors, str):
        colors = [colors] * len(y_values)

    # Iterate over each test's x and y data.
    for i, (x, y) in enumerate(zip(x_values, y_values)):
        label = legend_labels[i] if i < len(legend_labels) else None
        # If colors provided, use the i-th color; otherwise, let matplotlib choose.
        col = colors[i] if colors is not None and i < len(colors) else None
        plt.plot(x, y, linestyle=line_style, label=label, color=col)

    if grid:
        plt.grid(True)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.legend()
    plt.tight_layout()
    plt.savefig(filepath)
    plt.close()


def plot_bar_chart(categories, values, ylabel, title, filepath, second_values=None, second_label=None, rotation=True, figure_size=(14, 6)):
    """
    Plots a single bar chart if only `values` are given.
    If `second_values` is provided, plots grouped (side-by-side) bars.
    """

    ensure_dir(filepath)
    plt.figure(figsize=figure_size)
    x = range(len(categories))
    if second_values is None:
        # === Single bar chart ===
        plt.bar(categories, values)
    else:
        # === Grouped bar chart ===
        bar_width = 0.4
        plt.bar(
            [xi - bar_width/2 for xi in x],
            values,
            width=bar_width,
            label="Test" if not second_label else second_label[0]
        )
        plt.bar(
            [xi + bar_width/2 for xi in x],
            second_values,
            width=bar_width,
            label="Test" if not second_label else second_label[1]
        )
        plt.legend()
    plt.ylabel(ylabel)
    plt.title(title)
    if rotation:
        plt.xticks(x, categories, rotation=45, ha='right')
    else:
        plt.xticks(x, categories)

    plt.tight_layout()
    plt.savefig(filepath)
    plt.close()


def improved_plot_bar_chart(
    filepath, title, ylabel,
    values,       # List of lists: each inner list is one bar group (e.g. test)
    xlabels,      # Labels for each group in `values` (e.g. test IDs)
    categories,   # Labels for each bar on x-axis (e.g. query names)
    rotation=True,
    figure_size=(14, 6)
):
    ensure_dir(filepath)
    plt.figure(figsize=figure_size)

    num_categories = len(categories)
    num_groups = len(values)
    x = range(num_categories)

    # Calculate a bar width so all groups fit in the same category "slot"
    bar_width = 0.8 / num_groups if num_groups > 0 else 0.8

    for i, value_list in enumerate(values):
        # Shift bars so that the middle bar ends up at x.
        # i - (num_groups - 1)/2 -> negative shift for left bars, positive shift for right bars
        offsets = [xi + (i - (num_groups - 1)/2) * bar_width for xi in x]

        plt.bar(
            offsets,
            value_list,
            width=bar_width,
            label=xlabels[i]
        )

    plt.legend(loc="upper left")
    plt.ylabel(ylabel)
    plt.title(title)

    # Place the x-axis ticks at the original x positions
    # so the labels appear centered between the leftmost and rightmost bars.
    if rotation:
        plt.xticks(x, categories, rotation=45, ha='right')
    else:
        plt.xticks(x, categories)

    plt.tight_layout()
    plt.savefig(filepath)
    plt.close()



def plot_boxplot(data, labels, title, output_path):
    """
    Plots a single figure with multiple boxplots side by side,
    one for each list of durations in `data`.
    """
    ensure_dir(output_path)
    plt.figure(figsize=(12, 6))

    # Create the boxplot; each list in `data` becomes one box
    plt.boxplot(data, labels=labels)

    plt.title(title)
    plt.xlabel("Query Name")
    plt.ylabel("Duration")
    # Tilt labels if they are long
    # plt.xticks(rotation=45, ha="right")

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    
def get_percentile_values(percentile, df_stats, aggregate=False):

    print(percentile, df_stats)
    # Base mapping for non-aggregated values
    base_mapping = {
        "Min": "Min Latency (s)",
        "Max": "Max Latency (s)",
        "Mean": "Mean Latency (s)",
        "P50": "P50 Latency (s)",
        "P75": "P75 Latency (s)",
        "P95": "P95 Latency (s)",
        "P99": "P99 Latency (s)",
    }
    # If aggregate is True, prepend "Sum of " to each column name.
    mapping = {k: f"Sum of {v}" for k, v in base_mapping.items()} if aggregate else base_mapping

    try:
        print(mapping)
        print("percentile", percentile)
        col_name = mapping[percentile]
        print(col_name)
    except KeyError:
        raise ValueError("Error, percentile type is invalid")

    return df_stats[col_name], col_name



def get_all_tests():
    """
    Returns a list of dictionaries with keys 'test_id' and 'desc'
    extracted from df_queries.
    """
    csvs = load_csvs(real_stats=False, queries=True, throughput=False)
    df_queries = csvs[0]

    if "test_id" not in df_queries.columns or "desc" not in df_queries.columns or "db_name" not in df_queries.columns:
        raise ValueError("df_queries must contain 'test_id', 'desc', and 'db_name' columns.")
    df_tests = df_queries[["test_id", "desc", "db_name"]].dropna().drop_duplicates()
    tests_list = df_tests.to_dict(orient='records')
    return tests_list


def get_desc_from_test_id(test_id):
    print(test_id)
    csvs = load_csvs(real_stats=False, queries=False, throughput=True)
    df_throughput = csvs[0]

    df_throughput_test = df_throughput[df_throughput["test_id"].str.strip() == test_id].copy()
    descs = df_throughput_test["desc"].unique()
    print("Descs", descs)
    if len(descs) == 1:
        return descs[0]
    elif len(descs) == 0:
        return None  # or raise an error
    else:
        raise ValueError(f"Multiple descriptions found for test_id '{test_id}': {descs}")

# delete_from_csvs(["7OCRH"])
