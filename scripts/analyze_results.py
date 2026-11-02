"""
Analyzes all latency CSVs from the benchmark matrix (both brokers, all QoS
levels, client counts 1/10/50/100/200, plus the 500-client QoS-2 special
case), each run 3 times independently.

What this script does, step by step:
1. For each individual run (one broker + QoS + client count + repetition),
   it loads every client's CSV and pools all its messages into one sample,
   then computes descriptive statistics for that single run
   (stats_per_run.csv).
2. For each stage (broker + QoS + client count, across its 3 repetitions),
   it aggregates those per-run statistics into a mean and standard
   deviation across runs (stats_summary.csv). This is what captures
   run-to-run variance - a single run's internal percentiles say nothing
   about how reproducible that run was, only the spread across independent
   repetitions does.
3. It does the same for system throughput (throughput_summary.csv in,
   throughput_summary_aggregated.csv out).
4. It draws plots for the evaluation chapter, with error bars (standard
   deviation across the 3 repetitions) wherever a metric is aggregated
   across runs.

Run it with:
    python scripts\\analyze_results.py
(run from the repository root, so the relative paths below resolve correctly)
"""

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

RESULTS_DIR = Path("results")
PLOTS_DIR = RESULTS_DIR / "plots"

BROKERS = (("babroker", "BA-Broker"), ("mosquitto", "Mosquitto"))
QOS_LEVELS = (0, 1, 2)
CLIENT_COUNTS = (1, 10, 50, 100, 200)  # 1 = single-client baseline (former Test 2)
REPETITIONS = (1, 2, 3)
SPECIAL_CLIENTS = 500  # QoS 2 only, kept out of the scaling plots (see below)

BROKER_COLORS = {"BA-Broker": "tab:blue", "Mosquitto": "tab:red"}
QOS_COLORS = {0: "tab:blue", 1: "tab:green", 2: "tab:purple"}


def stage_folder(broker_key: str, qos: int, clients: int, rep: int) -> Path:
    return RESULTS_DIR / f"{broker_key}_qos{qos}_n{clients}_rep{rep}"


def all_runs():
    """Yields (broker_name, qos, clients, rep, folder, is_special) for every
    individual run in the matrix."""
    for broker_key, broker_name in BROKERS:
        for rep in REPETITIONS:
            for qos in QOS_LEVELS:
                for clients in CLIENT_COUNTS:
                    yield broker_name, qos, clients, rep, stage_folder(broker_key, qos, clients, rep), False
            yield (broker_name, 2, SPECIAL_CLIENTS, rep,
                   stage_folder(broker_key, 2, SPECIAL_CLIENTS, rep), True)


def load_run_latencies(folder: Path) -> pd.Series:
    """Reads every clientN.csv in a folder and returns one combined Series
    of all latency_ms values (across all clients of that single run)."""
    frames = [pd.read_csv(f) for f in folder.glob("client*.csv")]
    combined = pd.concat(frames, ignore_index=True)
    return combined["latency_ms"]


def run_stats(latencies: pd.Series, **extra) -> dict:
    """Descriptive statistics for one single run."""
    return {
        **extra,
        "n": len(latencies),
        "mean_ms": latencies.mean(),
        "median_ms": latencies.median(),
        "p95_ms": latencies.quantile(0.95),
        "p99_ms": latencies.quantile(0.99),
        "std_ms": latencies.std(),
        "min_ms": latencies.min(),
        "max_ms": latencies.max(),
    }


def pooled_stats_table(pooled: dict) -> pd.DataFrame:
    """For each (broker, qos, clients), computes percentiles directly on
    ALL messages from all 3 repetitions combined into one sample.

    This is deliberately NOT the same number as p95_ms_mean/p99_ms_mean in
    stats_summary.csv, which average the per-run percentiles across the 3
    repetitions. Averaging percentiles computed separately per run is a
    known statistical mistake when the goal is "the Nth percentile of all
    messages" - see e.g. the Redis engineering blog's posts on p95/p99,
    which both call out "averaging percentiles" as a common error. The
    mean-of-per-run-percentiles figure is still meaningful, but only as a
    measure of how reproducible that percentile was across independent
    runs - not as the percentile over the full, pooled population of
    messages. This function provides that second, distinct number.
    """
    rows = []
    for (broker, qos, clients), series_list in pooled.items():
        latencies = pd.concat(series_list, ignore_index=True)
        rows.append({
            "broker": broker, "qos": qos, "clients": clients,
            "pooled_n": len(latencies),
            "pooled_mean_ms": latencies.mean(),
            "pooled_median_ms": latencies.median(),
            "pooled_p95_ms": latencies.quantile(0.95),
            "pooled_p99_ms": latencies.quantile(0.99),
            "pooled_std_ms": latencies.std(),
            "pooled_min_ms": latencies.min(),
            "pooled_max_ms": latencies.max(),
        })
    return pd.DataFrame(rows)


def aggregate_across_reps(per_run: pd.DataFrame, group_cols: list, metric_cols: list) -> pd.DataFrame:
    """For each group (e.g. broker+qos+clients), computes mean and standard
    deviation ACROSS REPETITIONS of each metric column. This is the actual
    run-to-run variability, distinct from the within-run std_ms column."""
    grouped = per_run.groupby(group_cols)[metric_cols]
    agg = grouped.agg(["mean", "std"])
    agg.columns = [f"{col}_{stat}" for col, stat in agg.columns]
    agg["repetitions"] = grouped.size()
    return agg.reset_index()


def pooled_centered_spread(per_run: pd.DataFrame, pooled_df: pd.DataFrame,
                            group_cols: list, metric_cols: list) -> pd.DataFrame:
    """For each metric, measures how far the 3 individual repetitions
    deviate from the POOLED value (not from their own per-run mean).

    This exists only for the plots: plot_latency_vs_clients and
    plot_broker_comparison draw the line through pooled_{metric} (see
    pooled_stats_table), but the band's half-width used to be {metric}_std
    - the deviation of the 3 repetitions from their own mean_ms-style
    average, not from the pooled value the line is actually drawn through.
    Since the pooled value and the per-run mean are generally different
    numbers, a band of pooled_value +/- {metric}_std was not actually
    centered on, or representative of the scatter around, the line it was
    drawn on. This function instead computes
    sqrt(mean((per_run_value - pooled_value)^2)) for each group, so the
    resulting band is centered on the pooled line by construction and
    still reflects real repetition-to-repetition scatter."""
    merged = per_run.merge(pooled_df, on=group_cols, how="left")
    rows = []
    for keys, group in merged.groupby(group_cols):
        row = dict(zip(group_cols, keys))
        for col in metric_cols:
            deviation = group[col] - group[f"pooled_{col}"]
            row[f"{col}_pooled_spread"] = (deviation ** 2).mean() ** 0.5
        rows.append(row)
    return pd.DataFrame(rows)


def ecdf(latencies: pd.Series):
    """Returns (x, y) arrays for plotting an empirical CDF."""
    sorted_latencies = latencies.sort_values().to_numpy()
    fraction = (pd.Series(range(1, len(sorted_latencies) + 1)) / len(sorted_latencies)).to_numpy()
    return sorted_latencies, fraction


def band_plot(ax, x, mean, std, color, label, linestyle="-"):
    """Plots a line through `mean` with a shaded band from mean-std to
    mean+std - the standard way to show mean +/- standard deviation across
    repetitions along a continuous axis, used instead of per-point error
    bars so that overlapping lines in the same subplot stay readable."""
    ax.plot(x, mean, marker="o", color=color, linestyle=linestyle, label=label)
    ax.fill_between(x, mean - std, mean + std, color=color, alpha=0.15, linewidth=0)


def plot_latency_vs_clients(agg: pd.DataFrame, broker: str, filename: str, title: str):
    """One figure with 3 subplots (median/p95/p99 vs. client count), one
    line per QoS level, for a single broker. The line uses the pooled value
    (the true percentile over all messages of all 3 repetitions combined -
    the same number reported in stats_summary.csv's pooled_* columns and
    cited in the text), while the shaded band shows how far the 3
    individual repetitions deviate from that pooled value (see
    pooled_centered_spread) - so the band is actually centered on the
    plotted line. Using the per-run mean for the line instead would make
    the plot disagree with the table (the two are not the same number -
    see the pooled_stats_table docstring)."""
    fig, axes = plt.subplots(1, 3, figsize=(13, 4.5), sharex=True, sharey=True)
    metrics = [("Median", "median_ms"), ("p95", "p95_ms"), ("p99", "p99_ms")]
    for ax, (metric_name, col) in zip(axes, metrics):
        for qos in QOS_LEVELS:
            sub = agg[(agg["broker"] == broker) & (agg["qos"] == qos)].sort_values("clients")
            band_plot(ax, sub["clients"], sub[f"pooled_{col}"], sub[f"{col}_pooled_spread"],
                       QOS_COLORS[qos], f"QoS {qos}")
        ax.set_xlabel("Number of concurrent clients")
        ax.set_title(metric_name)
    axes[0].set_ylabel("End-to-End Latency (ms)")
    axes[0].legend()
    fig.suptitle(title)
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / filename, dpi=150)
    print(f"wrote {PLOTS_DIR / filename}")


def plot_broker_comparison(agg: pd.DataFrame, filename: str):
    """Broker comparison, one subplot per QoS level, median + p95. The line
    uses the pooled value (matches stats_summary.csv's pooled_* columns),
    the shaded band shows how far the 3 individual repetitions deviate from
    that pooled value (see pooled_centered_spread) - see
    plot_latency_vs_clients for why the line is not simply
    median_ms_mean/p95_ms_mean, and why the band must be centered on the
    pooled value rather than on that mean. Independent y-axes per subplot:
    QoS 2 reaches far higher latencies than QoS 0/1, so a shared scale
    would flatten the lower QoS levels to near zero."""
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    for ax, qos in zip(axes, QOS_LEVELS):
        for broker in ("BA-Broker", "Mosquitto"):
            sub = agg[(agg["broker"] == broker) & (agg["qos"] == qos)].sort_values("clients")
            band_plot(ax, sub["clients"], sub["pooled_median_ms"], sub["median_ms_pooled_spread"],
                       BROKER_COLORS[broker], f"{broker} - median")
            band_plot(ax, sub["clients"], sub["pooled_p95_ms"], sub["p95_ms_pooled_spread"],
                       BROKER_COLORS[broker], f"{broker} - p95", linestyle="--")
        ax.set_xlabel("Number of concurrent clients")
        ax.set_ylabel("End-to-End Latency (ms)")
        ax.set_title(f"QoS {qos}")
        ax.legend()
    fig.suptitle("End-to-End Latency vs. Number of Concurrent Clients, by Server")
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / filename, dpi=150)
    print(f"wrote {PLOTS_DIR / filename}")


def plot_throughput_vs_clients(throughput_agg: pd.DataFrame, filename: str):
    """System throughput vs. number of clients, one subplot per QoS level,
    one line per broker, shaded band showing the standard deviation across
    repetitions. Only covers the scaling stages (1/10/50/100/200 clients) -
    the 500-client special case is reported in the table only, not plotted
    here, for the same reason it is excluded from the latency scaling
    plots."""
    scaling = throughput_agg[throughput_agg["clients"] <= 200]
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    for ax, qos in zip(axes, QOS_LEVELS):
        for broker in ("BA-Broker", "Mosquitto"):
            sub = scaling[(scaling["broker"] == broker) & (scaling["qos"] == qos)].sort_values("clients")
            band_plot(ax, sub["clients"], sub["throughput_msgs_per_s_mean"], sub["throughput_msgs_per_s_std"],
                       BROKER_COLORS[broker], broker)
        ax.set_xlabel("Number of concurrent clients")
        ax.set_ylabel("Throughput (messages/s)")
        ax.set_title(f"QoS {qos}")
        ax.legend()
    fig.suptitle("System Throughput vs. Number of Concurrent Clients, by Server")
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / filename, dpi=150)
    print(f"wrote {PLOTS_DIR / filename}")


def main():
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    # --- Load every individual run, compute per-run stats, and also keep
    # the pooled (all 3 repetitions combined) raw latencies per stage for
    # the ECDF plots, where a bigger combined sample is more representative
    # of the distribution shape than any single repetition. ---
    per_run_rows = []
    pooled = {}  # (broker, qos, clients) -> list of Series, one per repetition
    for broker, qos, clients, rep, folder, is_special in all_runs():
        latencies = load_run_latencies(folder)
        per_run_rows.append(run_stats(latencies, broker=broker, qos=qos, clients=clients,
                                       repetition=rep, messages_per_client=1000, special_case=is_special))
        pooled.setdefault((broker, qos, clients), []).append(latencies)

    per_run = pd.DataFrame(per_run_rows)
    per_run.to_csv(RESULTS_DIR / "stats_per_run.csv", index=False)
    print(f"wrote {RESULTS_DIR / 'stats_per_run.csv'} ({len(per_run)} rows)")

    # --- Aggregate across the 3 repetitions: mean + std per stage. ---
    metric_cols = ["mean_ms", "median_ms", "p95_ms", "p99_ms", "min_ms", "max_ms"]
    agg = aggregate_across_reps(per_run, ["broker", "qos", "clients", "special_case"], metric_cols)

    # Add the pooled (true, not averaged) percentiles as separate columns -
    # see the docstring of pooled_stats_table for why these are not the
    # same thing as p95_ms_mean/p99_ms_mean above.
    pooled_df = pooled_stats_table(pooled)
    agg = agg.merge(pooled_df, on=["broker", "qos", "clients"], how="left")

    # Add, per metric, how far the 3 repetitions deviate from the pooled
    # value itself (not from their own mean) - this is what the plots use
    # for the band half-width, since the plotted line is the pooled value
    # (see pooled_centered_spread docstring).
    agg = agg.merge(
        pooled_centered_spread(per_run, pooled_df, ["broker", "qos", "clients"], metric_cols),
        on=["broker", "qos", "clients"], how="left",
    )

    agg.to_csv(RESULTS_DIR / "stats_summary.csv", index=False)
    print(f"wrote {RESULTS_DIR / 'stats_summary.csv'} ({len(agg)} rows)")
    print(agg.to_string(index=False))

    # --- Throughput: same aggregation, from the data run-load.ps1 logged. ---
    throughput = pd.read_csv(RESULTS_DIR / "throughput_summary.csv")
    throughput_agg = aggregate_across_reps(
        throughput.assign(special_case=throughput["clients"] == SPECIAL_CLIENTS),
        ["broker", "qos", "clients", "special_case"], ["throughput_msgs_per_s", "elapsed_s"])
    throughput_agg.to_csv(RESULTS_DIR / "throughput_summary_aggregated.csv", index=False)
    print(f"wrote {RESULTS_DIR / 'throughput_summary_aggregated.csv'} ({len(throughput_agg)} rows)")

    scaling_agg = agg[~agg["special_case"]]

    # --- Plot 1+2: per-broker QoS comparison, once per broker ---
    plot_latency_vs_clients(scaling_agg, "BA-Broker", "versuch3_latency_vs_clients_babroker.png",
                             "BA-Broker: End-to-End Latency vs. Number of Concurrent Clients")
    plot_latency_vs_clients(scaling_agg, "Mosquitto", "versuch3_latency_vs_clients_mosquitto.png",
                             "Mosquitto: End-to-End Latency vs. Number of Concurrent Clients")

    # --- Plot 3: broker comparison, one subplot per QoS level ---
    plot_broker_comparison(scaling_agg, "versuch3_broker_vergleich.png")

    # --- Plot: system throughput vs. number of clients ---
    plot_throughput_vs_clients(throughput_agg, "versuch3_throughput_vs_clients.png")

    # --- Plot 4: ECDF, BA-Broker vs. Mosquitto, one subplot per QoS level,
    # single client (clients == 1), pooled across the 3 repetitions. X-axis
    # cut a bit past the 99th percentile: a few extreme outliers would
    # otherwise squash the bulk of the distribution into a sliver at the
    # left edge. Those outliers are still fully captured in
    # stats_per_run.csv (max_ms column), just not in this plot. ---
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    for ax, qos in zip(axes, QOS_LEVELS):
        p99s = []
        for broker in ("BA-Broker", "Mosquitto"):
            latencies = pd.concat(pooled[(broker, qos, 1)], ignore_index=True)
            x, y = ecdf(latencies)
            ax.plot(x, y, label=broker, color=BROKER_COLORS[broker])
            p99s.append(latencies.quantile(0.99))
        ax.set_xlim(0, max(p99s) * 1.15)
        ax.set_xlabel("End-to-End Latency (ms)")
        ax.set_title(f"QoS {qos}")
    axes[0].set_ylabel("Fraction of messages ≤ latency")
    axes[0].legend()
    fig.suptitle("End-to-End Latency Distribution, Single Client, by Server")
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "versuch2_broker_vergleich.png", dpi=150)
    print(f"wrote {PLOTS_DIR / 'versuch2_broker_vergleich.png'}")

    # --- Plot 5: ECDF comparing QoS levels within BA-Broker, single client,
    # pooled across repetitions. ---
    fig, ax = plt.subplots(figsize=(7, 5))
    for qos in QOS_LEVELS:
        latencies = pd.concat(pooled[("BA-Broker", qos, 1)], ignore_index=True)
        x, y = ecdf(latencies)
        ax.plot(x, y, label=f"QoS {qos}", color=QOS_COLORS[qos])
    ax.set_xlabel("End-to-End Latency (ms)")
    ax.set_ylabel("Fraction of messages ≤ latency")
    ax.set_title("BA-Broker: End-to-End Latency Distribution by QoS Level, Single Client")
    ax.legend()
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "versuch2_qos_ecdf.png", dpi=150)
    print(f"wrote {PLOTS_DIR / 'versuch2_qos_ecdf.png'}")

    # --- Plot 6: special case, 500 clients, QoS 2 (WLAN saturation), ECDF,
    # pooled across the 3 repetitions. Kept as its own plot rather than part
    # of the scaling curves, since this data point is not comparable to the
    # other client counts (only QoS 2 has one). ---
    fig, ax = plt.subplots(figsize=(7, 5))
    for broker in ("BA-Broker", "Mosquitto"):
        latencies = pd.concat(pooled[(broker, 2, SPECIAL_CLIENTS)], ignore_index=True)
        x, y = ecdf(latencies)
        ax.plot(x, y, label=broker, color=BROKER_COLORS[broker])
    ax.set_xlabel("End-to-End Latency (ms)")
    ax.set_ylabel("Fraction of messages ≤ latency")
    ax.set_title("End-to-End Latency Distribution: 500 Clients, QoS 2, by Server")
    ax.legend()
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "sonderfall_500_qos2.png", dpi=150)
    print(f"wrote {PLOTS_DIR / 'sonderfall_500_qos2.png'}")


if __name__ == "__main__":
    main()
