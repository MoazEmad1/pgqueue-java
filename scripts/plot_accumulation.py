#!/usr/bin/env python3
"""
M3b DROP-accumulation loop — visualise the sweeper's DROP-attempt count
and per-sweep tick duration against workload throughput on a shared
x-axis. Answers whether the accumulation curve and the throughput decay
line up in time.

Reads:
    results/<runId>.locks.slow.csv[.gz] or a window extract
    results/<runId>.csv (throughput per second)

Writes:
    results/<runId>.locks.accumulation.png

    python plot_accumulation.py --run M3b \
        --locks results/M3b.locks.slow.window-400-1200.csv.gz
"""
from __future__ import annotations

import argparse
import csv
import gzip
import re
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt

DROP_PAT = re.compile(r"DROP TABLE pgqueue\.(jobs_p_\d+)")
SWEEP_BUCKET_S = 60


def load_slow(path: Path) -> list[dict]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as f:
        return list(csv.DictReader(f))


def load_throughput(csv_path: Path) -> dict[int, float]:
    out: dict[int, float] = {}
    with open(csv_path) as f:
        for r in csv.DictReader(f):
            out[int(r["t_seconds"])] = float(r["throughput"])
    return out


def per_bucket_drops(rows: list[dict]) -> tuple[dict[int, set[str]], dict[int, list[int]]]:
    per_bucket_names: dict[int, set[str]] = defaultdict(set)
    per_bucket_ts: dict[int, list[int]] = defaultdict(list)
    for r in rows:
        m = DROP_PAT.search(r["query_snippet"] or "")
        if not m:
            continue
        t = int(r["t_seconds"])
        b = t // SWEEP_BUCKET_S
        per_bucket_names[b].add(m.group(1))
        per_bucket_ts[b].append(t)
    return per_bucket_names, per_bucket_ts


def rolling_throughput(throughput: dict[int, float], window: int = 30) -> dict[int, float]:
    ts = sorted(throughput.keys())
    out: dict[int, float] = {}
    running: list[float] = []
    for t in ts:
        running.append(throughput[t])
        if len(running) > window:
            running.pop(0)
        out[t] = sum(running) / len(running)
    return out


def main(argv):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run", required=True)
    p.add_argument("--locks", type=Path, required=True,
                   help="results/<run>.locks.slow.csv or a window extract (.gz ok)")
    p.add_argument("--results-dir", type=Path, default=Path("results"))
    p.add_argument("--out", type=Path)
    args = p.parse_args(argv)

    if args.out is None:
        args.out = args.results_dir / f"{args.run}.locks.accumulation.png"

    rows = load_slow(args.locks)
    per_names, per_ts = per_bucket_drops(rows)
    tp = load_throughput(args.results_dir / f"{args.run}.csv")
    tp_smooth = rolling_throughput(tp)

    if not per_names:
        print(f"no DROP TABLE rows found in {args.locks}", file=sys.stderr)
        return 2

    buckets = sorted(per_names)
    attempts = [len(per_names[b]) for b in buckets]
    durations = [max(per_ts[b]) - min(per_ts[b]) for b in buckets]
    bucket_ts = [b * SWEEP_BUCKET_S for b in buckets]
    cum = []
    seen: set[str] = set()
    for b in buckets:
        seen.update(per_names[b])
        cum.append(len(seen))

    fig, (ax_top, ax_bot) = plt.subplots(
        2, 1, sharex=True, figsize=(13, 8),
        gridspec_kw={"height_ratios": [3, 2]}
    )

    # Top: throughput (left) + DROP tick duration + attempts (right)
    ts_tp = sorted(tp_smooth.keys())
    ax_top.plot(ts_tp, [tp_smooth[t] for t in ts_tp],
                color="tab:blue", linewidth=1.5,
                label="throughput (30s rolling)")
    ax_top.set_ylabel("throughput (jobs/sec)")
    ax_top.grid(True, alpha=0.3)

    ax_r = ax_top.twinx()
    ax_r.step(bucket_ts, durations, where="post",
              color="tab:red", linewidth=1.4,
              label="sweep tick duration (s)")
    ax_r.set_ylabel("sweep tick duration (s) / DROP attempts per tick",
                    color="tab:red")
    ax_r.tick_params(axis="y", labelcolor="tab:red")
    ax_r.axhline(SWEEP_BUCKET_S, color="tab:red", linestyle=":", alpha=0.5,
                 label=f"{SWEEP_BUCKET_S}s sweep interval (crossover)")
    ax_r.step(bucket_ts, attempts, where="post",
              color="tab:orange", linewidth=1.2, alpha=0.85,
              label="distinct DROPs attempted per tick")

    lines_l, labels_l = ax_top.get_legend_handles_labels()
    lines_r, labels_r = ax_r.get_legend_handles_labels()
    ax_top.legend(lines_l + lines_r, labels_l + labels_r,
                  loc="upper right", fontsize=8, framealpha=0.9)
    ax_top.set_title(
        f"{args.run}: sweep DROP-attempt accumulation vs throughput. "
        f"Longest tick observed = {max(durations)} s (interval = {SWEEP_BUCKET_S} s); "
        f"crossover {'reached' if max(durations) >= SWEEP_BUCKET_S else 'NOT reached in window'}.",
        fontsize=10)

    # Bottom: cumulative distinct DROP-target relations ever attempted
    ax_bot.step(bucket_ts, cum, where="post", color="tab:purple", linewidth=1.5)
    ax_bot.fill_between(bucket_ts, cum, step="post", alpha=0.15, color="tab:purple")
    ax_bot.set_xlabel("t_seconds")
    ax_bot.set_ylabel("cumulative partition names\nthe sweeper ever tried to DROP")
    ax_bot.grid(True, alpha=0.3)
    ax_bot.set_title(
        "Cumulative distinct partition relations the sweeper attempted "
        "to DROP over the run. Because each DROP times out under the "
        "antagonist's AccessShare on the parent, the set only grows.",
        fontsize=9)

    fig.tight_layout()
    fig.savefig(args.out, dpi=140)
    plt.close(fig)
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
