import os
import pandas as pd
import plotly.graph_objs as go
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from plotly.subplots import make_subplots

# Path to your continuously updated CSV file
REAL_DOCKER_CSV = "kubequery/static/data/real_docker_stats.csv"

def create_dash_app(flask_app):
    dash_app = Dash(
        __name__,
        server=flask_app,
        url_base_pathname='/dash/'
    )
    
    dash_app.layout = html.Div([
        html.H1("Live Metrics Dashboard"),
        dcc.Graph(id="live-graph"),
        dcc.Interval(
            id="interval-component",
            interval=1500,  # Update every 1 second
            n_intervals=0
        )
    ])

    @dash_app.callback(
        Output("live-graph", "figure"),
        [Input("interval-component", "n_intervals")]
    )
    def update_graph_live(n):
        if not os.path.exists(REAL_DOCKER_CSV):
            return go.Figure()

        try:
            # Read CSV data (adjust usecols as needed)
            df_stats = pd.read_csv(REAL_DOCKER_CSV, header=0, usecols=range(5), dtype=str)
            df_stats.columns = [
                "test_id", "timestamp", "cpu_percent", "mem_percent", "n_cores"
            ]
            
            # Convert the timestamp column to datetime and sort
            df_stats["timestamp"] = pd.to_datetime(
                df_stats["timestamp"], format="%Y-%m-%dT%H:%M:%S.%fZ", errors="coerce"
            )
            df_stats.sort_values("timestamp", inplace=True)

            if df_stats.empty:
                return go.Figure()

            # Get the latest test_id and filter for latest rows
            latest_test_id = df_stats["test_id"].iloc[-1]
            df_latest = df_stats[df_stats["test_id"] == latest_test_id].copy()
            if df_latest.empty:
                return go.Figure()

            # Convert values to numeric
            df_latest["cpu_percent"] = pd.to_numeric(df_latest["cpu_percent"], errors="coerce")
            df_latest["mem_percent"] = pd.to_numeric(df_latest["mem_percent"], errors="coerce")
            df_latest["n_cores"] = pd.to_numeric(df_latest["n_cores"], errors="coerce")

            # Create line plots for CPU and Memory over time (using lines only for a smooth real-time look)
            trace_cpu = go.Scatter(
                x=df_latest["timestamp"],
                y=df_latest["cpu_percent"],
                mode="lines",
                name="CPU Usage (%)",
                line=dict(color="blue")
            )
            trace_mem = go.Scatter(
                x=df_latest["timestamp"],
                y=df_latest["mem_percent"],
                mode="lines",
                name="Memory Usage (%)",
                line=dict(color="red")
            )

            # Create a gauge for CPU cores usage (cores at 100%)
            cpu_gauge = go.Indicator(
                mode="gauge+number",
                value=df_latest["cpu_percent"].iloc[-1],
                title={"text": "CPU usage (%)"},
                gauge={
                     "axis": {"range": [0, 100]},
                    "bar": {"color": "blue"},
                    "steps": [
                        {"range": [0, 50], "color": "lightgray"},
                        {"range": [50, 100], "color": "gray"}
                    ]
                }
            )

            # Create a gauge for Memory usage
            mem_gauge = go.Indicator(
                mode="gauge+number",
                value=df_latest["mem_percent"].iloc[-1],
                title={"text": "Memory Usage (%)"},
                gauge={
                    "axis": {"range": [0, 100]},
                    "bar": {"color": "red"},
                    "steps": [
                        {"range": [0, 50], "color": "lightgray"},
                        {"range": [50, 100], "color": "gray"}
                    ]
                }
            )

            # Create a subplot:
            # - The xy graph spans 2 rows and 3 columns (cells 1-3)
            # - The two gauges are in the 4th column (one in each row)
            fig = make_subplots(
                rows=2, cols=4,
                specs=[
                    [{"type": "xy", "rowspan": 2, "colspan": 3}, None, None, {"type": "indicator"}],
                    [None, None, None, {"type": "indicator"}]
                ],
                subplot_titles=("Live CPU and Memory Usage", "", ""),
                vertical_spacing=0.3
            )

            # Add the line traces to the xy graph (cell at row 1, col 1)
            fig.add_trace(trace_cpu, row=1, col=1)
            fig.add_trace(trace_mem, row=1, col=1)

            # Update the x-axis and y-axis titles for the xy graph
            fig.update_xaxes(title_text="Timestamp", row=1, col=1)
            fig.update_yaxes(title_text="Usage (%)", row=1, col=1)

            # Add the gauge indicators to the right column (CPU in row 1, Memory in row 2)
            fig.add_trace(cpu_gauge, row=1, col=4)
            fig.add_trace(mem_gauge, row=2, col=4)

            # Use a rolling window for the x-axis (e.g., last 30 seconds) to compress the time steps
            latest_time = df_latest["timestamp"].max()
            window_start = latest_time - pd.Timedelta(seconds=30)
            print(f"Latest timestamp: {df_latest['timestamp'].max()}, Data points: {len(df_latest)}")

            fig.update_xaxes(range=[window_start, latest_time], row=1, col=1)

            # Update the overall layout
            fig.update_layout(
                title=f"",
                hovermode="closest"
            )

            return fig

        except Exception as e:
            print(f"Error updating live graph: {e}")
            return go.Figure()

    return dash_app
