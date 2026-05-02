"""Render CDFs and summary bar charts from run_bench.py output."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

SCENARIO_COLOR = {
    "no-cache": "#c44e52",
    "moka": "#dd8452",
    "hybrid": "#4c72b0",
}


def load_raw_latencies(results_jsonl: Path) -> Dict[Tuple[str, int], List[float]]:
    """Return {(scenario, k): [latency_s, ...]} pooled across repeats."""
    pooled: Dict[Tuple[str, int], List[float]] = defaultdict(list)
    with results_jsonl.open() as f:
        for line in f:
            rec = json.loads(line)
            name = rec["name"]
            for k_str, lats in rec["latencies_by_k"].items():
                pooled[(name, int(k_str))].extend(lats)
    return pooled


def plot_cdfs(
    pooled: Dict[Tuple[str, int], List[float]],
    k_values: List[int],
    out_png: Path,
) -> None:
    fig, axes = plt.subplots(1, len(k_values), figsize=(5 * len(k_values), 4), sharey=True)
    if len(k_values) == 1:
        axes = [axes]

    for ax, k in zip(axes, k_values):
        for scenario, _k in sorted(pooled.keys()):
            if _k != k:
                continue
            lats_ms = np.sort(np.asarray(pooled[(scenario, k)]) * 1000.0)
            if lats_ms.size == 0:
                continue
            y = np.linspace(0, 1, lats_ms.size, endpoint=True)
            ax.plot(
                lats_ms, y, label=scenario, color=SCENARIO_COLOR.get(scenario), linewidth=2
            )
        ax.set_title(f"k = {k}")
        ax.set_xlabel("latency (ms)")
        ax.set_xscale("log")
        ax.grid(True, alpha=0.3)
        ax.legend(loc="lower right")
    axes[0].set_ylabel("CDF")
    fig.suptitle("Lance IVF_RQ query latency — cache scenario comparison")
    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=120)
    plt.close(fig)


def plot_p99_bars(summary_csv: Path, out_png: Path) -> None:
    df = pd.read_csv(summary_csv)
    agg = (
        df.groupby(["scenario", "k"])["p99_s"]
        .agg(median="median", lo="min", hi="max")
        .reset_index()
    )
    agg["p99_ms"] = agg["median"] * 1000.0
    agg["err_lo_ms"] = (agg["median"] - agg["lo"]) * 1000.0
    agg["err_hi_ms"] = (agg["hi"] - agg["median"]) * 1000.0

    k_values = sorted(df["k"].unique().tolist())
    scenarios = [s for s in ("no-cache", "moka", "hybrid") if s in agg["scenario"].unique()]
    x = np.arange(len(k_values))
    width = 0.8 / max(1, len(scenarios))

    fig, ax = plt.subplots(figsize=(8, 4.5))
    for i, scenario in enumerate(scenarios):
        rows = agg[agg["scenario"] == scenario].set_index("k").reindex(k_values)
        ax.bar(
            x + i * width - (len(scenarios) - 1) * width / 2,
            rows["p99_ms"].values,
            width,
            label=scenario,
            color=SCENARIO_COLOR.get(scenario),
            yerr=[rows["err_lo_ms"].values, rows["err_hi_ms"].values],
            capsize=3,
        )
    ax.set_xticks(x)
    ax.set_xticklabels([f"k={k}" for k in k_values])
    ax.set_ylabel("p99 latency (ms)")
    ax.set_title("Lance IVF_RQ p99 latency (median across repeats, min/max bars)")
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=120)
    plt.close(fig)


def plot_hit_ratio(summary_csv: Path, out_png: Path) -> None:
    df = pd.read_csv(summary_csv)
    # hit_ratio is a per-scenario session metric, duplicated across k rows; take first.
    agg = df.groupby("scenario")["hit_ratio"].mean().reset_index()
    fig, ax = plt.subplots(figsize=(6, 3.5))
    ax.bar(
        agg["scenario"],
        agg["hit_ratio"] * 100.0,
        color=[SCENARIO_COLOR.get(s, "#888") for s in agg["scenario"]],
    )
    ax.set_ylabel("index-cache hit ratio (%)")
    ax.set_ylim(0, 100)
    ax.set_title("Lance index_cache_stats — hit ratio by scenario")
    ax.grid(True, axis="y", alpha=0.3)
    for i, v in enumerate(agg["hit_ratio"] * 100.0):
        ax.text(i, v + 1.0, f"{v:.1f}%", ha="center", fontsize=9)
    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=120)
    plt.close(fig)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", default=str(Path(__file__).parent / "out"))
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    results_jsonl = out_dir / "results.jsonl"
    summary_csv = out_dir / "summary.csv"
    plots_dir = out_dir / "plots"

    if not results_jsonl.exists() or not summary_csv.exists():
        print(f"[plot] missing {results_jsonl} or {summary_csv}; run run_bench.py first")
        return 1

    pooled = load_raw_latencies(results_jsonl)
    k_values = sorted({k for _, k in pooled.keys()})

    plot_cdfs(pooled, k_values, plots_dir / "latency_cdf.png")
    plot_p99_bars(summary_csv, plots_dir / "p99_bars.png")
    plot_hit_ratio(summary_csv, plots_dir / "hit_ratio.png")

    print(f"[plot] wrote plots to {plots_dir}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
