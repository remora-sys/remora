#!/usr/bin/env python3

import argparse
import re
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


RSDS_LINE_RE = re.compile(r"^\[rsds-bench\]\s+(?P<body>.+)$")

INT_FIELDS = {
    "object_space",
    "batch_size",
    "worker_count",
    "shared_objects_per_tx",
    "proxy_count",
    "warmup_batches",
    "measured_batches",
}
FLOAT_FIELDS = {
    "avg_ms",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "max_ms",
    "total_wall_ms",
    "throughput_batches_per_s",
    "throughput_tx_per_s",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse RSDS benchmark logs and render summary plots."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the benchmark log containing [rsds-bench] lines.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory to write the generated figures and CSV summary.",
    )
    return parser.parse_args()


def parse_log(input_path: Path) -> pd.DataFrame:
    rows = []
    for raw_line in input_path.read_text().splitlines():
        match = RSDS_LINE_RE.match(raw_line.strip())
        if not match:
            continue

        row = {}
        for token in match.group("body").split():
            key, value = token.split("=", 1)
            if key in INT_FIELDS:
                row[key] = int(value)
            elif key in FLOAT_FIELDS:
                row[key] = float(value)
            else:
                row[key] = value
        rows.append(row)

    if not rows:
        raise ValueError(f"No [rsds-bench] lines found in {input_path}")

    frame = pd.DataFrame(rows).sort_values(
        [
            "batch_size",
            "proxy_count",
            "shared_objects_per_tx",
            "object_space",
            "worker_count",
        ]
    )
    return frame


def benchmark_context(frame: pd.DataFrame) -> str:
    object_spaces = sorted(frame["object_space"].unique())
    worker_counts = sorted(frame["worker_count"].unique())
    object_space_text = (
        f"{object_spaces[0]:,}"
        if len(object_spaces) == 1
        else "[" + ", ".join(f"{value:,}" for value in object_spaces) + "]"
    )
    worker_count_text = (
        str(worker_counts[0])
        if len(worker_counts) == 1
        else "[" + ", ".join(str(value) for value in worker_counts) + "]"
    )
    return f"object_space={object_space_text}, worker_count={worker_count_text}"


def compute_object_sweep(frame: pd.DataFrame) -> pd.DataFrame:
    min_objects_per_tx = frame["shared_objects_per_tx"].min()
    baseline = (
        frame[frame["shared_objects_per_tx"] == min_objects_per_tx][
            ["batch_size", "proxy_count", "throughput_tx_per_s", "avg_ms"]
        ]
        .rename(
            columns={
                "throughput_tx_per_s": "baseline_throughput_tx_per_s",
                "avg_ms": "baseline_avg_ms",
            }
        )
        .copy()
    )
    merged = frame.merge(baseline, on=["batch_size", "proxy_count"], how="left")
    merged["throughput_ratio"] = (
        merged["throughput_tx_per_s"] / merged["baseline_throughput_tx_per_s"]
    )
    merged["latency_ratio"] = merged["avg_ms"] / merged["baseline_avg_ms"]
    return merged


def save_dashboard(frame: pd.DataFrame, output_path: Path) -> None:
    proxy_counts = sorted(frame["proxy_count"].unique())
    objects_per_txs = sorted(frame["shared_objects_per_tx"].unique())
    cmap = plt.get_cmap("viridis", len(objects_per_txs))
    colors = {objects_per_tx: cmap(i) for i, objects_per_tx in enumerate(objects_per_txs)}

    fig, axes = plt.subplots(
        2,
        len(proxy_counts),
        figsize=(6 * len(proxy_counts), 9),
        constrained_layout=True,
    )
    if len(proxy_counts) == 1:
        axes = [[axes[0]], [axes[1]]]

    for col, proxy_count in enumerate(proxy_counts):
        subset = frame[frame["proxy_count"] == proxy_count]
        throughput_ax = axes[0][col]
        latency_ax = axes[1][col]

        for objects_per_tx in objects_per_txs:
            object_slice = subset[
                subset["shared_objects_per_tx"] == objects_per_tx
            ].sort_values(
                "batch_size"
            )
            throughput_ax.plot(
                object_slice["batch_size"],
                object_slice["throughput_tx_per_s"],
                marker="o",
                linewidth=2,
                color=colors[objects_per_tx],
                label=f"{objects_per_tx} objs/tx",
            )
            latency_ax.plot(
                object_slice["batch_size"],
                object_slice["avg_ms"],
                marker="o",
                linewidth=2,
                color=colors[objects_per_tx],
            )
            latency_ax.fill_between(
                object_slice["batch_size"],
                object_slice["avg_ms"],
                object_slice["p95_ms"],
                color=colors[objects_per_tx],
                alpha=0.15,
            )

        throughput_ax.set_title(f"Proxy Count = {proxy_count}")
        throughput_ax.set_xscale("log", base=2)
        throughput_ax.set_xlabel("Batch Size")
        throughput_ax.set_ylabel("Throughput (tx/s)")
        throughput_ax.grid(alpha=0.25)

        latency_ax.set_xscale("log", base=2)
        latency_ax.set_xlabel("Batch Size")
        latency_ax.set_ylabel("Latency (ms)")
        latency_ax.grid(alpha=0.25)

    axes[0][0].legend(frameon=False, loc="upper left")
    fig.suptitle(
        "RSDS Performance by Batch Size and Objects per Txn\n"
        f"Top: throughput, Bottom: avg latency with p95 band ({benchmark_context(frame)})",
        fontsize=16,
    )
    fig.savefig(output_path, dpi=220)
    plt.close(fig)


def save_object_sweep(frame: pd.DataFrame, output_path: Path) -> None:
    batch_sizes = sorted(frame["batch_size"].unique())
    proxy_counts = sorted(frame["proxy_count"].unique())
    cmap = plt.get_cmap("plasma", len(proxy_counts))
    colors = {proxy_count: cmap(i) for i, proxy_count in enumerate(proxy_counts)}

    fig, axes = plt.subplots(
        2,
        len(batch_sizes),
        figsize=(6 * len(batch_sizes), 8.5),
        constrained_layout=True,
    )
    if len(batch_sizes) == 1:
        axes = [[axes[0]], [axes[1]]]

    for idx, batch_size in enumerate(batch_sizes):
        throughput_ax = axes[0][idx]
        latency_ax = axes[1][idx]
        subset = frame[frame["batch_size"] == batch_size]
        for proxy_count in proxy_counts:
            proxy_slice = subset[subset["proxy_count"] == proxy_count].sort_values(
                "shared_objects_per_tx"
            )
            throughput_ax.plot(
                proxy_slice["shared_objects_per_tx"],
                proxy_slice["throughput_tx_per_s"],
                marker="o",
                linewidth=2,
                color=colors[proxy_count],
                label=f"{proxy_count} proxies",
            )
            latency_ax.plot(
                proxy_slice["shared_objects_per_tx"],
                proxy_slice["avg_ms"],
                marker="o",
                linewidth=2,
                color=colors[proxy_count],
            )
            latency_ax.fill_between(
                proxy_slice["shared_objects_per_tx"],
                proxy_slice["avg_ms"],
                proxy_slice["p95_ms"],
                color=colors[proxy_count],
                alpha=0.15,
            )

        objects_per_txs = sorted(subset["shared_objects_per_tx"].unique())
        throughput_ax.set_title(f"Batch Size = {batch_size:,}")
        throughput_ax.set_xlabel("Shared Objects per Txn")
        throughput_ax.set_ylabel("Throughput (tx/s)")
        throughput_ax.set_xticks(objects_per_txs)
        throughput_ax.grid(alpha=0.25)

        latency_ax.set_xlabel("Shared Objects per Txn")
        latency_ax.set_ylabel("Latency (ms)")
        latency_ax.set_xticks(objects_per_txs)
        latency_ax.grid(alpha=0.25)

    axes[0][0].legend(frameon=False, loc="upper left")
    fig.suptitle(
        "RSDS Sensitivity to Shared Objects per Txn\n"
        f"Top: throughput, Bottom: avg latency with p95 band ({benchmark_context(frame)})",
        fontsize=16,
    )
    fig.savefig(output_path, dpi=220)
    plt.close(fig)


def save_heatmaps(frame: pd.DataFrame, output_path: Path) -> None:
    proxy_counts = sorted(frame["proxy_count"].unique())
    fig, axes = plt.subplots(
        2,
        len(proxy_counts),
        figsize=(5.5 * len(proxy_counts), 9),
        constrained_layout=True,
    )
    if len(proxy_counts) == 1:
        axes = [[axes[0]], [axes[1]]]

    for idx, proxy_count in enumerate(proxy_counts):
        subset = frame[frame["proxy_count"] == proxy_count]
        throughput_pivot = subset.pivot(
            index="shared_objects_per_tx",
            columns="batch_size",
            values="throughput_tx_per_s",
        ).sort_index()
        latency_pivot = subset.pivot(
            index="shared_objects_per_tx", columns="batch_size", values="avg_ms"
        ).sort_index()

        throughput_ax = axes[0][idx]
        latency_ax = axes[1][idx]

        throughput_im = throughput_ax.imshow(
            throughput_pivot.values, aspect="auto", cmap="YlGnBu"
        )
        latency_im = latency_ax.imshow(latency_pivot.values, aspect="auto", cmap="YlOrRd")

        for ax, pivot in ((throughput_ax, throughput_pivot), (latency_ax, latency_pivot)):
            ax.set_xticks(range(len(pivot.columns)))
            ax.set_xticklabels([f"{c:,}" for c in pivot.columns])
            ax.set_yticks(range(len(pivot.index)))
            ax.set_yticklabels([str(r) for r in pivot.index])
            ax.set_xlabel("Batch Size")
            ax.set_ylabel("Shared Objects per Txn")

        throughput_ax.set_title(f"Throughput (tx/s), proxies={proxy_count}")
        latency_ax.set_title(f"Avg Latency (ms), proxies={proxy_count}")

        annotate_heatmap(throughput_ax, throughput_pivot.values, fmt="{:.0f}")
        annotate_heatmap(latency_ax, latency_pivot.values, fmt="{:.2f}")

        fig.colorbar(throughput_im, ax=throughput_ax, shrink=0.85)
        fig.colorbar(latency_im, ax=latency_ax, shrink=0.85)

    fig.suptitle("RSDS Factor Heatmaps", fontsize=16)
    fig.savefig(output_path, dpi=220)
    plt.close(fig)


def annotate_heatmap(ax, values, fmt: str) -> None:
    flat = values.flatten()
    threshold = (flat.min() + flat.max()) / 2 if len(flat) else 0
    for row in range(values.shape[0]):
        for col in range(values.shape[1]):
            value = values[row, col]
            text_color = "white" if value > threshold else "black"
            ax.text(
                col,
                row,
                fmt.format(value),
                ha="center",
                va="center",
                color=text_color,
                fontsize=9,
            )


def save_summary(frame: pd.DataFrame, output_path: Path) -> None:
    object_sweep = compute_object_sweep(frame)
    best_throughput = frame.loc[frame["throughput_tx_per_s"].idxmax()]
    lowest_latency = frame.loc[frame["avg_ms"].idxmin()]
    min_objects_per_tx = frame["shared_objects_per_tx"].min()
    max_objects_per_tx = frame["shared_objects_per_tx"].max()

    if min_objects_per_tx == max_objects_per_tx:
        object_pressure = (
            f"Only shared_objects_per_tx={min_objects_per_tx} is present in this log, "
            "so there is no object-pressure sweep comparison yet."
        )
    else:
        max_objects = object_sweep[
            object_sweep["shared_objects_per_tx"] == max_objects_per_tx
        ]
        strongest_object_pressure = max_objects.loc[max_objects["latency_ratio"].idxmax()]
        object_pressure = (
            f"batch_size={int(strongest_object_pressure['batch_size'])}, "
            f"proxy_count={int(strongest_object_pressure['proxy_count'])}, "
            f"shared_objects_per_tx={min_objects_per_tx}->{max_objects_per_tx}, "
            f"throughput_ratio={strongest_object_pressure['throughput_ratio']:.3f}x, "
            f"latency_ratio={strongest_object_pressure['latency_ratio']:.3f}x"
        )

    lines = [
        "# RSDS Benchmark Summary",
        "",
        f"Context: {benchmark_context(frame)}",
        "",
        "## Best Throughput",
        format_point(best_throughput, "throughput_tx_per_s", "tx/s"),
        "",
        "## Lowest Average Latency",
        format_point(lowest_latency, "avg_ms", "ms"),
        "",
        "## Strongest Objects-per-Txn Pressure",
        object_pressure,
        "",
        "## Key Patterns",
        summarize_patterns(frame),
        "",
    ]
    output_path.write_text("\n".join(lines))


def format_point(row: pd.Series, metric: str, unit: str) -> str:
    return (
        f"batch_size={int(row['batch_size'])}, "
        f"proxy_count={int(row['proxy_count'])}, "
        f"shared_objects_per_tx={int(row['shared_objects_per_tx'])}, "
        f"object_space={int(row['object_space'])}, "
        f"worker_count={int(row['worker_count'])}, "
        f"{metric}={row[metric]:.3f} {unit}"
    )


def summarize_patterns(frame: pd.DataFrame) -> str:
    lines = []
    grouped = frame.sort_values(["proxy_count", "batch_size", "shared_objects_per_tx"])

    min_objects_per_tx = grouped["shared_objects_per_tx"].min()
    max_objects_per_tx = grouped["shared_objects_per_tx"].max()
    if min_objects_per_tx != max_objects_per_tx:
        for proxy_count in sorted(grouped["proxy_count"].unique()):
            proxy_slice = grouped[grouped["proxy_count"] == proxy_count]
            min_latency = proxy_slice[
                proxy_slice["shared_objects_per_tx"] == min_objects_per_tx
            ]["avg_ms"].mean()
            max_latency = proxy_slice[
                proxy_slice["shared_objects_per_tx"] == max_objects_per_tx
            ]["avg_ms"].mean()
            lines.append(
                f"- At proxy_count={proxy_count}, moving shared_objects_per_tx from "
                f"{min_objects_per_tx} to {max_objects_per_tx} changes avg latency from "
                f"{min_latency:.2f} ms to {max_latency:.2f} ms."
            )

    min_proxy_count = grouped["proxy_count"].min()
    max_proxy_count = grouped["proxy_count"].max()
    if min_proxy_count != max_proxy_count:
        for objects_per_tx in sorted(grouped["shared_objects_per_tx"].unique()):
            object_slice = grouped[
                grouped["shared_objects_per_tx"] == objects_per_tx
            ]
            min_proxy_throughput = object_slice[
                object_slice["proxy_count"] == min_proxy_count
            ]["throughput_tx_per_s"].mean()
            max_proxy_throughput = object_slice[
                object_slice["proxy_count"] == max_proxy_count
            ]["throughput_tx_per_s"].mean()
            lines.append(
                f"- At shared_objects_per_tx={objects_per_tx}, moving proxy_count from "
                f"{min_proxy_count} to {max_proxy_count} changes mean throughput from "
                f"{min_proxy_throughput:,.0f} tx/s to {max_proxy_throughput:,.0f} tx/s."
            )

    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    frame = parse_log(input_path)
    frame.to_csv(output_dir / "rsds_bench_metrics.csv", index=False)

    save_dashboard(frame, output_dir / "rsds_dashboard.png")
    save_object_sweep(frame, output_dir / "rsds_object_sweep.png")
    save_heatmaps(frame, output_dir / "rsds_heatmaps.png")
    save_summary(frame, output_dir / "rsds_summary.md")

    print(f"Wrote parsed metrics to {output_dir / 'rsds_bench_metrics.csv'}")
    print(f"Wrote dashboard to {output_dir / 'rsds_dashboard.png'}")
    print(f"Wrote object sweep plot to {output_dir / 'rsds_object_sweep.png'}")
    print(f"Wrote heatmaps to {output_dir / 'rsds_heatmaps.png'}")
    print(f"Wrote summary to {output_dir / 'rsds_summary.md'}")


if __name__ == "__main__":
    main()
