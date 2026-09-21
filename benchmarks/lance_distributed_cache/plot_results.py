#!/usr/bin/env python3
"""Plot latency, throughput, and cache footprint from benchmark summary.csv."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summary_csv", type=Path)
    parser.add_argument("--out-dir", type=Path, default=None)
    return parser.parse_args()


def _labels(frame: pd.DataFrame) -> list[str]:
    return [
        f"{scenario}\n{phase}" for scenario, phase in zip(frame.scenario, frame.phase)
    ]


def plot_summary(summary_csv: Path, out_dir: Path) -> None:
    frame = pd.read_csv(summary_csv)
    required = {
        "scenario",
        "phase",
        "k",
        "p50_s",
        "p95_s",
        "p99_s",
        "throughput_qps",
        "session_size_bytes",
        "l2_apparent_bytes",
    }
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"summary is missing columns: {sorted(missing)}")
    out_dir.mkdir(parents=True, exist_ok=True)

    for k, group in frame.groupby("k", sort=True):
        group = group.reset_index(drop=True)
        labels = _labels(group)
        axis = (
            group[["p50_s", "p95_s", "p99_s"]]
            .mul(1000)
            .plot.bar(figsize=(max(9, len(group) * 1.2), 5))
        )
        axis.set_xticklabels(labels, rotation=30, ha="right")
        axis.set_ylabel("Latency (ms)")
        axis.set_title(f"Lance cache query latency (k={k})")
        axis.grid(axis="y", alpha=0.3)
        axis.figure.tight_layout()
        axis.figure.savefig(out_dir / f"latency_k{k}.png", dpi=160)
        plt.close(axis.figure)

        fig, axes = plt.subplots(1, 2, figsize=(max(10, len(group) * 1.3), 5))
        axes[0].bar(labels, group["throughput_qps"])
        axes[0].set_ylabel("Queries / second")
        axes[0].set_title(f"Aggregate throughput (k={k})")
        axes[1].bar(labels, group["session_size_bytes"] / (1024**2), label="L1")
        axes[1].bar(
            labels,
            group["l2_apparent_bytes"] / (1024**2),
            bottom=group["session_size_bytes"] / (1024**2),
            label="L2",
        )
        axes[1].set_ylabel("MiB (stacked for display)")
        axes[1].set_title("Observed cache footprint")
        axes[1].legend()
        for plot_axis in axes:
            plot_axis.tick_params(axis="x", rotation=30)
            plot_axis.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        fig.savefig(out_dir / f"throughput_cache_k{k}.png", dpi=160)
        plt.close(fig)


def main() -> int:
    args = parse_args()
    out_dir = args.out_dir or args.summary_csv.parent / "plots"
    plot_summary(args.summary_csv, out_dir)
    print(f"wrote plots to {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
