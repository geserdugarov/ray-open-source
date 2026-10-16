#!/usr/bin/env python3
"""Validate repeated runs and summarize their latency, throughput, and source IO."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from statistics import median

import matplotlib.pyplot as plt
import pandas as pd
from benchmark_lib import (
    CACHE_TIERS,
    L2_COUNTERS,
    aggregate_cache_metrics,
    percentiles,
    validate_result_digests,
)
from cache_inspect import invalidation_errors


def analyze(input_dir: Path, output_dir: Path) -> None:
    configs = sorted(input_dir.glob("*/run.json"))
    if (input_dir / "run.json").is_file():
        configs = [input_dir / "run.json"]
    if not configs:
        raise ValueError(f"no benchmark run.json files found in {input_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)
    trial_frames = []
    samples = defaultdict(list)
    first_queries = defaultdict(list)
    all_measurements = []
    validation = {"runs": [], "restarted_actors": 0, "invalidated_actors": 0}
    dataset_identity = None
    for config_path in configs:
        config = json.loads(config_path.read_text())
        args = config["arguments"]
        identity = (
            config["dataset_uri"],
            args["seed"],
            args["measure_queries"],
            args["num_actors"],
            args["nprobes"],
            args["k_list"],
            config["lance_build_commit"],
            args.get("drop_l2_page_cache_on_restart", False),
        )
        if dataset_identity is not None and dataset_identity != identity:
            raise ValueError(
                "runs differ in dataset, query workload, Lance build, or page-cache mode"
            )
        dataset_identity = identity
        partition_mb = args["partition_l1_mb"]
        profile = (
            "l2-only"
            if partition_mb == 0
            else "fit"
            if partition_mb >= 1024
            else "small"
        )
        events = [
            json.loads(line)
            for line in (config_path.parent / "events.jsonl").read_text().splitlines()
        ]
        initial = {
            (item["scenario"], item["actor_id"]): item
            for item in events
            if item["event"] == "actor_info"
        }
        restarts = [item for item in events if item["event"] == "restart_actor_info"]
        if args["measure_restart"] and len(restarts) != len(initial):
            raise ValueError(f"missing restarted actors in {config_path.parent}")
        for item in restarts:
            before = initial[(item["scenario"], item["actor_id"])]
            if item["node_id"] != before["node_id"] or item["pid"] == before["pid"]:
                raise ValueError(
                    "restart did not create a new process on the same node"
                )
            if item["scenario"] == "distributed":
                prewarm = next(
                    event
                    for event in events
                    if event["event"] == "prewarm"
                    and event["scenario"] == "distributed"
                    and event["actor_id"] == item["actor_id"]
                )
                if (
                    item["l2_snapshot"]["cache_entry_count"]
                    < prewarm["l2_after"]["cache_entry_count"]
                ):
                    raise ValueError("persisted entries were lost on process restart")
            validation["restarted_actors"] += 1
        for event in events:
            if event["event"] == "invalidate":
                errors = invalidation_errors(event["l2_after"])
                if errors or event["l2_after"]["cache_entry_count"]:
                    raise ValueError(f"invalidation failed: {errors}")
                validation["invalidated_actors"] += 1
            if event["event"] != "measure":
                continue
            all_measurements.append(event)
            for k, latencies in event["latencies_by_k"].items():
                key = (profile, event["scenario"], event["phase"], int(k))
                samples[key].extend(latencies)
                if event["phase"] == "process_restart":
                    first_queries[key].extend(latencies[:1])
        frame = pd.read_csv(config_path.parent / "summary.csv")
        frame.insert(0, "profile", profile)
        frame.insert(1, "run_id", config["run_id"])
        trial_frames.append(frame)
        validation["runs"].append(config["run_id"])

    validate_result_digests(all_measurements)
    validation["matching_query_results"] = True
    validation["measured_queries"] = sum(len(values) for values in samples.values())
    trials = pd.concat(trial_frames, ignore_index=True)
    trials.to_csv(output_dir / "trial_summary.csv", index=False)
    rows = []
    for key, frame in trials.groupby(["profile", "scenario", "phase", "k"], sort=True):
        profile, scenario, phase, k = key
        row = {
            "profile": profile,
            "scenario": scenario,
            "phase": phase,
            "k": int(k),
            "trials": len(frame),
            **percentiles(samples[key]),
            "throughput_qps_median": median(frame.throughput_qps),
            "throughput_qps_min": min(frame.throughput_qps),
            "throughput_qps_max": max(frame.throughput_qps),
            "source_read_bytes_median": median(frame.source_read_bytes),
            "source_read_iops_median": median(frame.source_read_iops),
            "session_size_bytes_median": median(frame.session_size_bytes),
            "index_cache_weight_bytes_median": (
                median(frame.index_cache_weight_bytes)
                if "index_cache_weight_bytes" in frame
                else None
            ),
            "l2_apparent_bytes_median": median(frame.l2_apparent_bytes),
            "l2_cache_entry_count_median": median(frame.l2_cache_entry_count),
            "process_disk_read_bytes_median": (
                median(frame.process_disk_read_bytes.dropna())
                if "process_disk_read_bytes" in frame
                and not frame.process_disk_read_bytes.dropna().empty
                else None
            ),
        }
        counter_names = [
            f"{tier}_{outcome}"
            for tier in CACHE_TIERS
            for outcome in ("hits", "misses")
        ] + list(L2_COUNTERS)
        counters = [
            {
                name: int(value) if pd.notna(value) else None
                for name, value in record.items()
                if name in counter_names
            }
            for record in frame.to_dict("records")
        ]
        row.update(aggregate_cache_metrics(counters))
        if first_queries[key]:
            row["restart_first_query_p50_s"] = percentiles(first_queries[key])["p50_s"]
            row["restart_first_query_n"] = len(first_queries[key])
        rows.append(row)
    comparison = pd.DataFrame(rows)
    comparison.to_csv(output_dir / "comparison.csv", index=False)
    (output_dir / "validation.json").write_text(json.dumps(validation, indent=2) + "\n")

    k = int(comparison.k.min())
    profiles = [
        name
        for name in ("small", "fit", "l2-only")
        if name in comparison.profile.values
    ]
    scenarios = ["no-index-cache", "memory", "distributed"]
    colors = ["#a3a3a3", "#3474b8", "#da852b"]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4), sharey=True)
    for axis, phase in zip(axes, ("l1_warm", "process_restart")):
        for scenario_index, (scenario, color) in enumerate(zip(scenarios, colors)):
            for profile_index, profile in enumerate(profiles):
                frame = comparison[
                    (comparison.profile == profile)
                    & (comparison.scenario == scenario)
                    & (comparison.phase == phase)
                    & (comparison.k == k)
                ]
                if frame.empty:
                    continue
                row = frame.iloc[0]
                x = profile_index + (scenario_index - 1) * 0.24
                axis.bar(x, row.throughput_qps_median, width=0.22, color=color)
                axis.errorbar(
                    x,
                    row.throughput_qps_median,
                    yerr=[
                        [row.throughput_qps_median - row.throughput_qps_min],
                        [row.throughput_qps_max - row.throughput_qps_median],
                    ],
                    fmt="none",
                    ecolor="black",
                    capsize=3,
                )
        axis.set_xticks(range(len(profiles)), profiles)
        axis.set_title(
            "Same process, repeated queries"
            if phase == "l1_warm"
            else "After actor process restart"
        )
        axis.set_xlabel("Cache capacity profile")
        axis.grid(axis="y", alpha=0.25)
        axis.set_axisbelow(True)
    axes[0].set_ylabel("Aggregate queries / second")
    handles = [plt.Rectangle((0, 0), 1, 1, color=color) for color in colors]
    fig.legend(handles, scenarios, loc="upper center", ncol=3, frameon=False)
    fig.tight_layout(rect=(0, 0, 1, 0.90))
    fig.savefig(output_dir / "throughput.png", dpi=180)
    fig.savefig(output_dir / "throughput.svg")
    plt.close(fig)
    print(json.dumps(validation, indent=2))
    print(f"Wrote comparison to {output_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_dir", type=Path)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    analyze(args.input_dir, args.out_dir)


if __name__ == "__main__":
    main()
