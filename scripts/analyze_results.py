"""
Run it with:
    python scripts\\analyze_results.py
(run from the repository root, so the relative paths below resolve correctly)
"""

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

RESULTS_DIR = Path("results")
PLOTS_DIR = RESULTS_DIR / "plots"

# Which folder/file belongs to which test condition. Each entry:
# (label, path, broker, qos, clients, messages_per_client)

# Test 3 (scaling): 10/50/100/200 clients, 1000 messages/client each,
# both brokers, all three QoS levels. Folder: results/<broker>_qos<Q>_n<N>/.
SCALING_STAGES = [
    (f"{broker_key}_qos{qos}_n{n}", RESULTS_DIR / f"{broker_key}_qos{qos}_n{n}",
     broker_name, qos, n, 1000)
    for broker_key, broker_name in (("babroker", "BA-Broker"), ("mosquitto", "Mosquitto"))
    for qos in (0, 1, 2)
    for n in (10, 50, 100, 200)
]

# Test 2 (single client, all QoS levels).
SINGLE_CLIENT_STAGES = [
    (f"single_{broker_key}_qos{qos}", RESULTS_DIR / "versuch2" / f"{broker_key}_qos{qos}.csv",
     broker_name, qos)
    for broker_key, broker_name in (("babroker", "BA-Broker"), ("mosquitto", "Mosquitto"))
    for qos in (0, 1, 2)
]

# Special case: 500 clients, 1000 messages/client, QoS 2 only (the one
# configuration that saturated the WLAN). Kept separate from SCALING_STAGES
# on purpose: it should appear in the statistics table, but NOT feed into
# the scaling plots, since only QoS 2 has a 500-client data point there -
# mixing it in would make the scaling curves misleading.
SPECIAL_CASE_STAGES = [
    (f"{broker_key}_qos2_n500", RESULTS_DIR / f"{broker_key}_qos2_n500", broker_name, 2, 500, 1000)
    for broker_key, broker_name in (("babroker", "BA-Broker"), ("mosquitto", "Mosquitto"))
]

BROKER_COLORS = {"BA-Broker": "tab:blue", "Mosquitto": "tab:red"}
QOS_COLORS = {0: "tab:blue", 1: "tab:green", 2: "tab:purple"}


def load_stage_folder(folder: Path) -> pd.Series:
    """Reads every clientN.csv in a folder and returns one combined Series
    of all latency_ms values (across all clients of that stage)."""
    frames = [pd.read_csv(f) for f in folder.glob("client*.csv")]
    combined = pd.concat(frames, ignore_index=True)
    return combined["latency_ms"]


def stats_row(label: str, latencies: pd.Series, **extra) -> dict:
    """Computes the descriptive statistics we want for one stage."""
    return {
        "label": label,
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


def ecdf(latencies: pd.Series):
    """Returns (x, y) arrays for plotting an empirical CDF."""
    sorted_latencies = latencies.sort_values().to_numpy()
    fraction = (pd.Series(range(1, len(sorted_latencies) + 1)) / len(sorted_latencies)).to_numpy()
    return sorted_latencies, fraction


def plot_latency_vs_clients(scaling_data: dict, broker: str, filename: str, title: str):
    """One figure with 3 subplots (median/p95/p99), one line per QoS level,
    for a single broker."""
    fig, axes = plt.subplots(1, 3, figsize=(13, 4.5), sharex=True, sharey=True)
    metrics = [("Median", 0.5), ("p95", 0.95), ("p99", 0.99)]
    for ax, (metric_name, quantile) in zip(axes, metrics):
        for qos in (0, 1, 2):
            client_counts = sorted({c for (b, q, c) in scaling_data if b == broker and q == qos})
            values = [scaling_data[(broker, qos, c)].quantile(quantile) for c in client_counts]
            ax.plot(client_counts, values, marker="o", color=QOS_COLORS[qos], label=f"QoS {qos}")
        ax.set_xlabel("Number of concurrent clients")
        ax.set_title(metric_name)
    axes[0].set_ylabel("Latency (ms)")
    axes[0].legend()
    fig.suptitle(title)
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / filename, dpi=150)
    print(f"wrote {PLOTS_DIR / filename}")


def main():
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    rows = []

    # --- Test 3 ---
    scaling_data = {}  # (broker, qos, clients) -> Series
    for label, folder, broker, qos, clients, messages in SCALING_STAGES:
        latencies = load_stage_folder(folder)
        rows.append(stats_row(label, latencies, broker=broker, qos=qos, clients=clients, messages_per_client=messages))
        scaling_data[(broker, qos, clients)] = latencies

    # --- Test 2 ---
    single_client_data = {}  # (broker, qos) -> Series
    for label, path, broker, qos in SINGLE_CLIENT_STAGES:
        latencies = pd.read_csv(path)["latency_ms"]
        rows.append(stats_row(label, latencies, broker=broker, qos=qos, clients=1, messages_per_client=len(latencies)))
        single_client_data[(broker, qos)] = latencies

    # --- Special case: 500 clients, QoS 2 ---
    special_data = {}  # broker -> Series
    for label, folder, broker, qos, clients, messages in SPECIAL_CASE_STAGES:
        latencies = load_stage_folder(folder)
        rows.append(stats_row(label, latencies, broker=broker, qos=qos, clients=clients, messages_per_client=messages))
        special_data[broker] = latencies

    stats = pd.DataFrame(rows)
    stats.to_csv(RESULTS_DIR / "stats_summary.csv", index=False)
    print(f"wrote {RESULTS_DIR / 'stats_summary.csv'} ({len(stats)} rows)")
    print(stats.to_string(index=False))

    # --- Plot 1+2: per-broker QoS comparison, once per broker ---
    plot_latency_vs_clients(scaling_data, "BA-Broker", "versuch3_latency_vs_clients_babroker.png",
                             "BA-Broker: Latency vs. Number of Clients")
    plot_latency_vs_clients(scaling_data, "Mosquitto", "versuch3_latency_vs_clients_mosquitto.png",
                             "Mosquitto: Latency vs. Number of Clients")

    # --- Plot 3: broker comparison, one subplot per QoS level, median + p95.
    # Independent y-axes per subplot: QoS 2 reaches far higher latencies than
    # QoS 0/1, so a shared scale would flatten the lower QoS levels to near
    # zero. Stated here as a comment, not crammed into the plot title. ---
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    for ax, qos in zip(axes, (0, 1, 2)):
        for broker in ("BA-Broker", "Mosquitto"):
            client_counts = sorted({c for (b, q, c) in scaling_data if b == broker and q == qos})
            medians = [scaling_data[(broker, qos, c)].median() for c in client_counts]
            p95s = [scaling_data[(broker, qos, c)].quantile(0.95) for c in client_counts]
            ax.plot(client_counts, medians, marker="o", color=BROKER_COLORS[broker], label=f"{broker} - median")
            ax.plot(client_counts, p95s, marker="o", linestyle="--", color=BROKER_COLORS[broker], label=f"{broker} - p95")
        ax.set_xlabel("Number of concurrent clients")
        ax.set_ylabel("Latency (ms)")
        ax.set_title(f"QoS {qos}")
        ax.legend()
    fig.suptitle("BA-Broker vs. Mosquitto: Latency vs. Number of Clients")
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "versuch3_broker_vergleich.png", dpi=150)
    print(f"wrote {PLOTS_DIR / 'versuch3_broker_vergleich.png'}")

    # --- Plot 4: ECDF, BA-Broker vs. Mosquitto, one subplot per QoS level,
    # single client.
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    for ax, qos in zip(axes, (0, 1, 2)):
        p99s = []
        for broker in ("BA-Broker", "Mosquitto"):
            latencies = single_client_data[(broker, qos)]
            x, y = ecdf(latencies)
            ax.plot(x, y, label=broker, color=BROKER_COLORS[broker])
            p99s.append(latencies.quantile(0.99))
        ax.set_xlim(0, max(p99s) * 1.15)
        ax.set_xlabel("Latency (ms)")
        ax.set_title(f"QoS {qos}")
    axes[0].set_ylabel("Fraction of messages ≤ latency")
    axes[0].legend()
    fig.suptitle("BA-Broker vs. Mosquitto: Latency Distribution, Single Client")
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "versuch2_broker_vergleich.png", dpi=150)
    print(f"wrote {PLOTS_DIR / 'versuch2_broker_vergleich.png'}")

    # --- Plot 5: ECDF comparing QoS levels within BA-Broker ---
    fig, ax = plt.subplots(figsize=(7, 5))
    for qos in (0, 1, 2):
        x, y = ecdf(single_client_data[("BA-Broker", qos)])
        ax.plot(x, y, label=f"QoS {qos}", color=QOS_COLORS[qos])
    ax.set_xlabel("Latency (ms)")
    ax.set_ylabel("Fraction of messages ≤ latency")
    ax.set_title("BA-Broker: Latency Distribution per QoS Level, Single Client")
    ax.legend()
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "versuch2_qos_ecdf.png", dpi=150)
    print(f"wrote {PLOTS_DIR / 'versuch2_qos_ecdf.png'}")

    # --- Plot 6: special case, 500 clients, QoS 2 (WLAN saturation), ECDF.
    # Kept as its own plot rather than part of the scaling curves, since
    # this data point is not comparable to the other client counts - see
    # the comment above SPECIAL_CASE_STAGES. ---
    fig, ax = plt.subplots(figsize=(7, 5))
    for broker in ("BA-Broker", "Mosquitto"):
        x, y = ecdf(special_data[broker])
        ax.plot(x, y, label=broker, color=BROKER_COLORS[broker])
    ax.set_xlabel("Latency (ms)")
    ax.set_ylabel("Fraction of messages ≤ latency")
    ax.set_title("Special Case: 500 Clients, QoS 2, 1000 Messages per Client")
    ax.legend()
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "sonderfall_500_qos2.png", dpi=150)
    print(f"wrote {PLOTS_DIR / 'sonderfall_500_qos2.png'}")


if __name__ == "__main__":
    main()
