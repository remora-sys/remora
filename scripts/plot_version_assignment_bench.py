#!/usr/bin/env python3

import argparse
import re
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


VERSION_ASSIGNMENT_LINE_RE = re.compile(
    r"^\[version-assignment-bench\]\s+(?P<body>.+)$"
)

INT_FIELDS = {
    "object_space",
    "batch_size",
    "shared_objects_per_tx",
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
        description="Parse version-assignment benchmark logs and render summary plots."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the benchmark log containing [version-assignment-bench] lines.",
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
        match = VERSION_ASSIGNMENT_LINE_RE.match(raw_line.strip())
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
        raise ValueError(f"No [version-assignment-bench] lines found in {input_path}")

    return pd.DataFrame(rows).sort_values(["batch_size", "shared_objects_per_tx"])


def benchmark_context(frame: pd.DataFrame) -> str:
    object_spaces = sorted(frame["object_space"].unique())
    warmup_batches = sorted(frame["warmup_batches"].unique())
    measured_batches = sorted(frame["measured_batches"].unique())

    return (
        f"object_space={format_list_or_scalar(object_spaces, comma_sep=True)}, "
        f"warmup_batches={format_list_or_scalar(warmup_batches)}, "
        f"measured_batches={format_list_or_scalar(measured_batches)}"
    )


def compute_object_sweep(frame: pd.DataFrame) -> pd.DataFrame:
    min_objects_per_tx = frame["shared_objects_per_tx"].min()
    baseline = (
        frame[frame["shared_objects_per_tx"] == min_objects_per_tx][
            ["batch_size", "throughput_tx_per_s", "avg_ms"]
        ]
        .rename(
            columns={
                "throughput_tx_per_s": "baseline_throughput_tx_per_s",
                "avg_ms": "baseline_avg_ms",
            }
        )
        .copy()
    )
    merged = frame.merge(baseline, on=["batch_size"], how="left")
    merged["throughput_ratio"] = (
        merged["throughput_tx_per_s"] / merged["baseline_throughput_tx_per_s"]
    )
    merged["latency_ratio"] = merged["avg_ms"] / merged["baseline_avg_ms"]
    return merged


def save_dashboard(frame: pd.DataFrame, output_path: Path) -> None:
    objects_per_txs = sorted(frame["shared_objects_per_tx"].unique())
    cmap = plt.get_cmap("viridis", len(objects_per_txs))
    colors = {objects_per_tx: cmap(i) for i, objects_per_tx in enumerate(objects_per_txs)}

    fig, axes = plt.subplots(2, 1, figsize=(8, 8.5), constrained_layout=True)
    throughput_ax, latency_ax = axes

    for objects_per_tx in objects_per_txs:
        object_slice = frame[frame["shared_objects_per_tx"] == objects_per_tx].sort_values(
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

    throughput_ax.set_xscale("log", base=2)
    throughput_ax.set_xlabel("Batch Size")
    throughput_ax.set_ylabel("Throughput (tx/s)")
    throughput_ax.grid(alpha=0.25)

    latency_ax.set_xscale("log", base=2)
    latency_ax.set_xlabel("Batch Size")
    latency_ax.set_ylabel("Latency (ms)")
    latency_ax.grid(alpha=0.25)

    throughput_ax.legend(frameon=False, loc="upper left")
    fig.suptitle(
        "Version Assignment Performance by Batch Size and Objects per Txn\n"
        f"Top: throughput, Bottom: avg latency with p95 band ({benchmark_context(frame)})",
        fontsize=15,
    )
    fig.savefig(output_path, dpi=220)
    plt.close(fig)


def save_object_sweep(frame: pd.DataFrame, output_path: Path) -> None:
    batch_sizes = sorted(frame["batch_size"].unique())
    cmap = plt.get_cmap("plasma", len(batch_sizes))
    colors = {batch_size: cmap(i) for i, batch_size in enumerate(batch_sizes)}

    fig, axes = plt.subplots(2, 1, figsize=(8, 8.5), constrained_layout=True)
    throughput_ax, latency_ax = axes

    for batch_size in batch_sizes:
        batch_slice = frame[frame["batch_size"] == batch_size].sort_values(
            "shared_objects_per_tx"
        )
        throughput_ax.plot(
            batch_slice["shared_objects_per_tx"],
            batch_slice["throughput_tx_per_s"],
            marker="o",
            linewidth=2,
            color=colors[batch_size],
            label=f"batch_size={batch_size:,}",
        )
        latency_ax.plot(
            batch_slice["shared_objects_per_tx"],
            batch_slice["avg_ms"],
            marker="o",
            linewidth=2,
            color=colors[batch_size],
        )
        latency_ax.fill_between(
            batch_slice["shared_objects_per_tx"],
            batch_slice["avg_ms"],
            batch_slice["p95_ms"],
            color=colors[batch_size],
            alpha=0.15,
        )

    objects_per_txs = sorted(frame["shared_objects_per_tx"].unique())
    throughput_ax.set_xlabel("Shared Objects per Txn")
    throughput_ax.set_ylabel("Throughput (tx/s)")
    throughput_ax.set_xticks(objects_per_txs)
    throughput_ax.grid(alpha=0.25)

    latency_ax.set_xlabel("Shared Objects per Txn")
    latency_ax.set_ylabel("Latency (ms)")
    latency_ax.set_xticks(objects_per_txs)
    latency_ax.grid(alpha=0.25)

    throughput_ax.legend(frameon=False, loc="upper right")
    fig.suptitle(
        "Version Assignment Sensitivity to Shared Objects per Txn\n"
        f"Top: throughput, Bottom: avg latency with p95 band ({benchmark_context(frame)})",
        fontsize=15,
    )
    fig.savefig(output_path, dpi=220)
    plt.close(fig)


def save_heatmaps(frame: pd.DataFrame, output_path: Path) -> None:
    throughput_pivot = frame.pivot(
        index="shared_objects_per_tx", columns="batch_size", values="throughput_tx_per_s"
    ).sort_index()
    latency_pivot = frame.pivot(
        index="shared_objects_per_tx", columns="batch_size", values="avg_ms"
    ).sort_index()

    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.8), constrained_layout=True)
    throughput_ax, latency_ax = axes

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

    throughput_ax.set_title("Throughput (tx/s)")
    latency_ax.set_title("Avg Latency (ms)")

    annotate_heatmap(throughput_ax, throughput_pivot.values, fmt="{:.0f}")
    annotate_heatmap(latency_ax, latency_pivot.values, fmt="{:.2f}")

    fig.colorbar(throughput_im, ax=throughput_ax, shrink=0.85)
    fig.colorbar(latency_im, ax=latency_ax, shrink=0.85)

    fig.suptitle(
        f"Version Assignment Heatmaps ({benchmark_context(frame)})",
        fontsize=15,
    )
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
            f"shared_objects_per_tx={min_objects_per_tx}->{max_objects_per_tx}, "
            f"throughput_ratio={strongest_object_pressure['throughput_ratio']:.3f}x, "
            f"latency_ratio={strongest_object_pressure['latency_ratio']:.3f}x"
        )

    lines = [
        "# Version Assignment Benchmark Summary",
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
        f"shared_objects_per_tx={int(row['shared_objects_per_tx'])}, "
        f"object_space={int(row['object_space'])}, "
        f"{metric}={row[metric]:.3f} {unit}"
    )


def summarize_patterns(frame: pd.DataFrame) -> str:
    lines = []
    grouped = frame.sort_values(["batch_size", "shared_objects_per_tx"])

    min_objects_per_tx = grouped["shared_objects_per_tx"].min()
    max_objects_per_tx = grouped["shared_objects_per_tx"].max()
    if min_objects_per_tx != max_objects_per_tx:
        for batch_size in sorted(grouped["batch_size"].unique()):
            batch_slice = grouped[grouped["batch_size"] == batch_size]
            min_latency = batch_slice[
                batch_slice["shared_objects_per_tx"] == min_objects_per_tx
            ]["avg_ms"].mean()
            max_latency = batch_slice[
                batch_slice["shared_objects_per_tx"] == max_objects_per_tx
            ]["avg_ms"].mean()
            min_throughput = batch_slice[
                batch_slice["shared_objects_per_tx"] == min_objects_per_tx
            ]["throughput_tx_per_s"].mean()
            max_throughput = batch_slice[
                batch_slice["shared_objects_per_tx"] == max_objects_per_tx
            ]["throughput_tx_per_s"].mean()
            lines.append(
                f"- At batch_size={batch_size:,}, moving shared_objects_per_tx from "
                f"{min_objects_per_tx} to {max_objects_per_tx} changes avg latency from "
                f"{min_latency:.2f} ms to {max_latency:.2f} ms and throughput from "
                f"{min_throughput:,.0f} tx/s to {max_throughput:,.0f} tx/s."
            )

    min_batch_size = grouped["batch_size"].min()
    max_batch_size = grouped["batch_size"].max()
    if min_batch_size != max_batch_size:
        for objects_per_tx in sorted(grouped["shared_objects_per_tx"].unique()):
            object_slice = grouped[grouped["shared_objects_per_tx"] == objects_per_tx]
            min_batch_throughput = object_slice[
                object_slice["batch_size"] == min_batch_size
            ]["throughput_tx_per_s"].mean()
            max_batch_throughput = object_slice[
                object_slice["batch_size"] == max_batch_size
            ]["throughput_tx_per_s"].mean()
            min_batch_latency = object_slice[
                object_slice["batch_size"] == min_batch_size
            ]["avg_ms"].mean()
            max_batch_latency = object_slice[
                object_slice["batch_size"] == max_batch_size
            ]["avg_ms"].mean()
            lines.append(
                f"- At shared_objects_per_tx={objects_per_tx}, moving batch_size from "
                f"{min_batch_size:,} to {max_batch_size:,} changes throughput from "
                f"{min_batch_throughput:,.0f} tx/s to {max_batch_throughput:,.0f} tx/s "
                f"and avg latency from {min_batch_latency:.2f} ms to {max_batch_latency:.2f} ms."
            )

    return "\n".join(lines)


def format_list_or_scalar(values: list[int], comma_sep: bool = False) -> str:
    if len(values) == 1:
        return f"{values[0]:,}" if comma_sep else str(values[0])
    if comma_sep:
        return "[" + ", ".join(f"{value:,}" for value in values) + "]"
    return "[" + ", ".join(str(value) for value in values) + "]"


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    frame = parse_log(input_path)
    frame.to_csv(output_dir / "version_assignment_bench_metrics.csv", index=False)

    save_dashboard(frame, output_dir / "version_assignment_dashboard.png")
    save_object_sweep(frame, output_dir / "version_assignment_object_sweep.png")
    save_heatmaps(frame, output_dir / "version_assignment_heatmaps.png")
    save_summary(frame, output_dir / "version_assignment_summary.md")

    print(
        f"Wrote parsed metrics to {output_dir / 'version_assignment_bench_metrics.csv'}"
    )
    print(
        f"Wrote dashboard to {output_dir / 'version_assignment_dashboard.png'}"
    )
    print(
        f"Wrote object sweep plot to {output_dir / 'version_assignment_object_sweep.png'}"
    )
    print(
        f"Wrote heatmaps to {output_dir / 'version_assignment_heatmaps.png'}"
    )
    print(f"Wrote summary to {output_dir / 'version_assignment_summary.md'}")


if __name__ == "__main__":
    main()
