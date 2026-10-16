"""Recompute report comparisons and plots from the supplied cluster artifacts."""

from __future__ import annotations

import csv
import json
import math
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FuncFormatter

HERE = Path(__file__).resolve().parent
PHASES = ("initial", "l1_warm", "process_restart")
SCENARIOS = ("no-index-cache", "memory", "distributed")
MIB = 1024**2
GIB = 1024**3


def main() -> None:
    config = json.loads((HERE / "run.json").read_text())
    events = [
        json.loads(line)
        for line in (HERE / "events_excerpt.jsonl").read_text().splitlines()
    ]
    with (HERE / "summary.csv").open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    data = {
        (row["scenario"], row["phase"]): {
            key: float(value)
            for key, value in row.items()
            if key not in {"scenario", "phase"}
        }
        for row in rows
    }
    if set(data) != {(scenario, phase) for scenario in SCENARIOS for phase in PHASES}:
        raise ValueError("Expected all nine scenario/phase combinations")
    for row in data.values():
        if row["n"] != config["arguments"]["measure_queries"]:
            raise ValueError("Summary query count differs from the configuration")
        if not math.isclose(
            row["throughput_qps"], row["n"] / row["wall_time_s"], rel_tol=1e-12
        ):
            raise ValueError("Throughput differs from query count / phase wall time")

    comparisons = {}
    for phase in PHASES:
        disabled, memory, distributed = [
            data[scenario, phase] for scenario in SCENARIOS
        ]
        comparisons[phase] = {
            "throughput_ratio_vs_memory": distributed["throughput_qps"]
            / memory["throughput_qps"],
            "throughput_ratio_vs_no_index_cache": distributed["throughput_qps"]
            / disabled["throughput_qps"],
            "p50_latency_ratio_vs_memory": memory["p50_s"] / distributed["p50_s"],
            "p95_latency_ratio_vs_memory": memory["p95_s"] / distributed["p95_s"],
            "source_read_reduction_vs_memory_pct": 100
            * (1 - distributed["source_read_bytes"] / memory["source_read_bytes"]),
            "source_read_GiB_memory": memory["source_read_bytes"] / GIB,
            "source_read_GiB_no_index_cache": disabled["source_read_bytes"] / GIB,
            "source_read_MiB_distributed": distributed["source_read_bytes"] / MIB,
        }

    measurements = [event for event in events if event["event"] == "measure"]
    actor_sample = measurements[0]
    samples = actor_sample["latencies_by_k"]["10"]
    if len(samples) * 10 != actor_sample["returned_rows_by_k"]["10"]:
        raise ValueError("Excerpt latency count and returned-row count disagree")
    prewarm = [event for event in events if event["event"] == "prewarm"]
    derived = {
        "comparisons": comparisons,
        "checks": {
            "summary_rows": len(rows),
            "query_executions_in_summary": int(sum(row["n"] for row in data.values())),
            "throughput_matches_query_count_over_wall_time": True,
            "physical_disk_read_bytes_zero_in_all_summary_rows": all(
                row["process_disk_read_bytes"] == 0 for row in data.values()
            ),
        },
        "event_excerpt": {
            "complete_event_stream": False,
            "events": len(events),
            "measurement_events": len(measurements),
            "actor_sample_queries": len(samples),
            "actor_sample_returned_rows": actor_sample["returned_rows_by_k"]["10"],
            "no_index_cache_prewarm_source_read_bytes": sum(
                event["source_read_bytes"] for event in prewarm
            ),
            "no_index_cache_prewarm_source_read_iops": sum(
                event["source_read_iops"] for event in prewarm
            ),
        },
        "recorded_lance_build_commit": config["lance_build_commit"] or None,
    }
    (HERE / "derived_metrics.json").write_text(
        json.dumps(derived, indent=2, sort_keys=True) + "\n"
    )

    plt.switch_backend("Agg")
    plt.rcParams.update({"font.size": 10, "svg.hashsalt": config["run_id"]})
    figure, axes = plt.subplots(1, 2, figsize=(13, 5.4))
    labels = ("After strict\nprewarm", "Repeated\nqueries", "After actor\nrestart")
    names = ("Index cache disabled", "Memory only", "Two level cache")
    colors = ("#69737b", "#c87521", "#187fa1")
    positions = np.arange(len(PHASES))
    for axis, field, multiplier, title, ylabel in (
        (
            axes[0],
            "throughput_qps",
            1,
            "Aggregate throughput",
            "Queries per second (log scale)",
        ),
        (axes[1], "p50_s", 1000, "Median query latency", "Milliseconds (log scale)"),
    ):
        for offset, (scenario, name, color) in enumerate(zip(SCENARIOS, names, colors)):
            values = [data[scenario, phase][field] * multiplier for phase in PHASES]
            bars = axis.bar(
                positions + (offset - 1) * 0.25,
                values,
                width=0.23,
                color=color,
                label=name,
            )
            axis.bar_label(
                bars,
                labels=[
                    f"{value:.2f}"
                    if value < 10
                    else f"{value:,.1f}"
                    if value < 1000
                    else f"{value:,.0f}"
                    for value in values
                ],
                padding=4,
                fontsize=8,
            )
        axis.set_yscale("log")
        axis.set_ylim((0.2, 1600) if field == "throughput_qps" else (2, 18000))
        axis.set_xticks(positions, labels)
        axis.set_ylabel(ylabel)
        axis.set_title(title)
        axis.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:g}"))
        axis.grid(axis="y", alpha=0.2)
        axis.set_axisbelow(True)
        axis.spines[["top", "right"]].set_visible(False)
    figure.suptitle("Lance two level cache on two Ray workers", fontsize=16, y=0.98)
    figure.text(
        0.5,
        0.91,
        "Single trial · 1 million vectors · 256 dimensions · k=10 · 256 queries per phase",
        ha="center",
    )
    figure.legend(
        *axes[0].get_legend_handles_labels(),
        loc="upper center",
        bbox_to_anchor=(0.5, 0.88),
        ncol=3,
        frameon=False,
    )
    figure.subplots_adjust(top=0.73, bottom=0.16, left=0.07, right=0.99, wspace=0.24)
    figure.text(
        0.5,
        0.025,
        "Query timers exclude setup and prewarm. File-page eviction was disabled; physical disk-read counters were zero.",
        ha="center",
        fontsize=9,
    )
    figure.savefig(HERE / "performance.png", dpi=180)
    svg_path = HERE / "performance.svg"
    figure.savefig(svg_path, metadata={"Date": None})
    svg_path.write_text(
        "\n".join(line.rstrip() for line in svg_path.read_text().splitlines()) + "\n"
    )
    plt.close(figure)
    print(json.dumps(derived, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
