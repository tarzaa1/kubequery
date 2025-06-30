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


# Consistent fontsize across plots
matplotlib.rcParams.update({
    'axes.titlesize': 22,
    'axes.labelsize': 20,
    'xtick.labelsize': 18,
    'ytick.labelsize': 18,
    'legend.fontsize': 15,
    'legend.title_fontsize': 15
})

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
        figure_size=(12, 6), grid=True, 
        colors=None, styles=None,
        downsample_step=100
):  
    """
    Plot multiple line charts on the same plot, with optional downsampling, custom colors, and styles.

    Parameters:
      filepath        : String - where to save the resulting image.
      title           : String - title of the chart.
      xlabel          : String - label for the x-axis.
      ylabel          : String - label for the y-axis.
      legend_labels   : List of strings - one label for each line plotted.
      x_values        : List/array - each element is the x-axis values for a test.
      y_values        : List of lists - each inner list contains y-values for a test.
      figure_size     : Tuple - figure dimensions (default is (12, 6)).
      grid            : Boolean - whether to show grid lines.
      colors          : List of colors for each line.
      styles          : List of line styles for each line (e.g. '-', '--')
      downsample_step : Integer - downsampling step (default = 100)
    """

    plt.figure(figsize=figure_size)

    for i, (x, y) in enumerate(zip(x_values, y_values)):
        label = legend_labels[i] if i < len(legend_labels) else None
        col = colors[i] if colors is not None and i < len(colors) else None
        style = styles[i] if styles is not None and i < len(styles) else '-'

        # Downsample
        x_ds = x[::downsample_step]
        y_ds = y[::downsample_step]

        plt.plot(x_ds, y_ds, linestyle=style, color=col, label=label)

    if grid:
        plt.grid(True)

    plt.xlabel(xlabel,)
    plt.ylabel(ylabel,)
    plt.title(title,)


    plt.legend(loc="center left", bbox_to_anchor=(1, 0.9), title="Legend")

    plt.tight_layout(rect=[0, 0, 1, 1])

    # Remove unnecessary spines
    ax = plt.gca()
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.grid(False)
    plt.savefig(filepath)
    plt.close()


def improved_plot_bar_chart(
    filepath, title, ylabel,
    values,
    xlabels,
    categories,
    rotation=True,
    figure_size=(14, 6),
    logscale=False,
):
    ensure_dir(filepath)
    plt.figure(figsize=figure_size)

    num_categories = len(categories)
    num_groups = len(values)
    x = range(num_categories)

    bar_width = 0.8 / num_groups if num_groups > 0 else 0.8

    for i, value_list in enumerate(values):
        offsets = [xi + (i - (num_groups - 1) / 2) * bar_width for xi in x]
        plt.bar(
            offsets,
            value_list,
            width=bar_width,
            label=xlabels[i]
        )

    plt.ylabel(ylabel)


    if logscale:
        plt.yscale('log')

    if rotation:
        plt.xticks(x, categories, rotation=45, ha='right', )
    else:
        plt.xticks(x, categories)

    # Place the legend outside the plot area
    plt.legend(loc="center left", bbox_to_anchor=(1, .5), title="Legend")
    plt.tight_layout(rect=[0, 0, 1, 1])  # Leave space on right for legend

    plt.savefig(filepath)
    plt.close()



def improved_plot_boxplot(
    filepath, title, ylabel,
    data_groups,   # outer: DBs, inner: queries, inner-most: durations
    db_labels,     # labels for legend
    query_labels,  # x-axis labels
    figure_size=(14, 6)
):
    ensure_dir(filepath)
    plt.figure(figsize=figure_size)

    num_queries = len(query_labels)
    num_dbs = len(data_groups)
    color_cycle = plt.rcParams['axes.prop_cycle'].by_key()['color']

    positions = []
    data = []
    widths = 0.8 / num_dbs
    box_colors = []

    for db_idx, db_data in enumerate(data_groups):
        for query_idx, durations in enumerate(db_data):
            pos = query_idx + (db_idx - (num_dbs - 1) / 2) * widths
            positions.append(pos)

            durations_ms = [d * 1000 for d in durations]


            data.append(durations_ms)
            box_colors.append(color_cycle[db_idx % len(color_cycle)])

    box = plt.boxplot(
        data,
        positions=positions,
        widths=widths,
        patch_artist=True,
        medianprops=dict(color='none'),
        showfliers=False
    )

    for patch, color in zip(box["boxes"], box_colors):
        patch.set_facecolor(color)

    plt.xticks(range(num_queries), query_labels, rotation=45, ha="right",)
    plt.ylabel(ylabel)
    plt.title(title)

    handles = [plt.Rectangle((0, 0), 1, 1, color=color_cycle[i % len(color_cycle)]) for i in range(num_dbs)]
    plt.legend(handles, db_labels, title="Legend", bbox_to_anchor=(1, 1))

    plt.tight_layout(rect=[0, 0, 1, 1])
    print(filepath)
    plt.savefig(filepath)
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
    

def abbreviate_legend(desc, db_name):
    """
    Work around for the legends names being too long. change from 10-nodes-with-50-pods -> Memgraph, 10-nodes-with-50-pods -> Neo4j
    To something like 10N or 10M.

    Take the first few characters before the first -. i.e 10, 100, 250, 500, 750, 1000.
    Then search for neo4j and memgraph in the string.
    """
    desc_prefix = desc.split('-')[0]
    db_name_short = db_name[0].upper()

    legend_name = f"{desc_prefix}{db_name_short}"
    return legend_name

# delete_from_csvs(["7OCRH"])
