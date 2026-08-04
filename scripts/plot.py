#!/usr/bin/env python3
"""
Plot pgqueue-java experiment results.

Modes:
  single-run:   python plot.py results/R1.csv
  compare:      python plot.py --compare R1 M1 M2 M3 [--out results/x.png]
  summary:      python plot.py --summary C1 R1 R2 R3 M1 M2 M3
  calibrate:    python plot.py --calibrate-runs R1 R2 R3 M1 M2 M3 <csv>
                (single-run render with y-limits shared across the named runs;
                 errors out if any run's data would exceed a calibrated max)

All summary values (B, t_50, t_25, descent_duration_seconds, stale_windows)
are read from results/<run>.meta.json — never recomputed here. If a
meta.json is missing the script errors out.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROLLING_SECONDS = 30
MIB = 1 << 20

# Columns the analyzer freezes together — same set drives the stale-window
# gaps on the plot so the visual matches the recorded fact in meta.json.
STALE_METRIC_COLS = [
    "n_dead_tup", "n_live_tup", "table_bytes", "index_bytes",
    "queue_depth", "oldest_backend_xmin_age",
]


def load_run(csv_path: Path) -> tuple[pd.DataFrame, dict]:
    df = pd.read_csv(csv_path)
    meta_path = csv_path.with_suffix(".meta.json")
    if not meta_path.exists():
        raise FileNotFoundError(
            f"no meta.json alongside {csv_path.name}; run analyze first"
        )
    meta = json.loads(meta_path.read_text())
    _apply_stale_gaps(df, meta)
    return df, meta


def _apply_stale_gaps(df: pd.DataFrame, meta: dict) -> None:
    """In-place: for each stale window from meta.json, NaN the frozen columns.

    Line plots skip NaN, so the frozen span shows as a gap. The tick loop's
    own throughput column is left alone — its zeros in a stale window are
    a distinct signal from the metric-derived columns freezing.
    """
    for w in meta.get("stale_windows", []) or []:
        mask = (df["t_seconds"] >= w["start_t"]) & (df["t_seconds"] <= w["end_t"])
        for col in STALE_METRIC_COLS:
            if col in df.columns:
                df.loc[mask, col] = np.nan


def antagonist_start_t(df: pd.DataFrame) -> int | None:
    active = df[df["antagonist_active"] == True]  # noqa: E712
    if active.empty:
        return None
    return int(active["t_seconds"].iloc[0])


def rolling_throughput(df: pd.DataFrame, window: int = ROLLING_SECONDS) -> pd.Series:
    return df["throughput"].rolling(window=window, min_periods=1).mean()


def cumulative_completed(df: pd.DataFrame) -> pd.Series:
    """Running total of completed jobs, in jobs. Sum of the per-tick throughput
    column (which is completions-in-window). Handy denominator for
    bytes-per-completed-job."""
    return df["throughput"].fillna(0).cumsum()


def bytes_per_completed_job(df: pd.DataFrame) -> pd.Series:
    total = df["table_bytes"] + df["index_bytes"]
    denom = cumulative_completed(df).replace(0, np.nan)
    return total / denom


def _title(run_id: str, meta: dict, b: float) -> str:
    mitigation = meta.get("mitigation")
    if not mitigation:
        subtitle = "baseline"
    elif run_id == mitigation or run_id.startswith(mitigation):
        subtitle = None
    else:
        subtitle = mitigation
    collapse = "collapse" if meta.get("collapse_declared") else "no collapse"
    parts = [run_id]
    if subtitle:
        parts.append(subtitle)
    parts.append(f"B={b:.0f} jobs/sec")
    parts.append(collapse)
    return "  —  ".join(parts)


def calibrate_ylimits(run_ids: list[str], results_dir: Path) -> dict[str, float]:
    """Per-column ymax across the named runs. Used to fix a shared scale on
    single-run charts so cross-run visual comparison isn't hidden by matplotlib
    autoscaling."""
    limits: dict[str, float] = {}
    cols = [
        "throughput_rolling",
        "oldest_backend_xmin_age",
        "n_dead_tup", "n_live_tup",
        "table_bytes_mib", "index_bytes_mib",
        "bytes_per_completed_job",
    ]
    for c in cols:
        limits[c] = 0.0
    for run_id in run_ids:
        df, _ = load_run(results_dir / f"{run_id}.csv")
        limits["throughput_rolling"] = max(limits["throughput_rolling"], rolling_throughput(df).max())
        limits["oldest_backend_xmin_age"] = max(
            limits["oldest_backend_xmin_age"], df["oldest_backend_xmin_age"].max()
        )
        limits["n_dead_tup"] = max(limits["n_dead_tup"], df["n_dead_tup"].max())
        limits["n_live_tup"] = max(limits["n_live_tup"], df["n_live_tup"].max())
        limits["table_bytes_mib"] = max(limits["table_bytes_mib"], df["table_bytes"].max() / MIB)
        limits["index_bytes_mib"] = max(limits["index_bytes_mib"], df["index_bytes"].max() / MIB)
        limits["bytes_per_completed_job"] = max(
            limits["bytes_per_completed_job"], bytes_per_completed_job(df).max()
        )
    return limits


def _enforce_max(name: str, run_id: str, series: pd.Series, ymax: float) -> None:
    """Hard error rather than silent clipping — the user wants to know when a
    new run overshoots the calibrated envelope."""
    observed = series.max()
    if pd.notna(observed) and observed > ymax:
        raise ValueError(
            f"{run_id}: column {name!r} max {observed:g} exceeds "
            f"calibrated ymax {ymax:g}; recalibrate with --calibrate-runs "
            f"or drop --calibrate-runs to autoscale"
        )


def single_run_plot(csv_path: Path, out_path: Path,
                    ylimits: dict[str, float] | None = None) -> None:
    df, meta = load_run(csv_path)
    run_id = csv_path.stem
    b = meta["baseline_throughput_median"]
    t50 = meta.get("t_50")
    t25 = meta.get("t_25")
    ant_t = antagonist_start_t(df)

    roll = rolling_throughput(df)
    dead = df["n_dead_tup"]
    live = df["n_live_tup"]
    xmin = df["oldest_backend_xmin_age"]
    tbl_mib = df["table_bytes"] / MIB
    idx_mib = df["index_bytes"] / MIB
    bpj = bytes_per_completed_job(df)

    fig, axes = plt.subplots(
        5, 1, sharex=True, figsize=(13, 12),
        gridspec_kw={"height_ratios": [3, 2, 2, 2, 2]}
    )
    ax_t, ax_x, ax_tup, ax_b, ax_bpj = axes

    # Panel 1: throughput
    ax_t.plot(df["t_seconds"], roll, color="tab:blue",
              label=f"throughput ({ROLLING_SECONDS}s rolling avg)")
    ax_t.set_ylabel("throughput (jobs/sec)")
    ax_t.grid(True, alpha=0.3)
    ax_t.axhline(0.5 * b, color="gray", linestyle="--", alpha=0.6,
                 label=f"0.5·B = {0.5 * b:.0f}")
    ax_t.axhline(0.25 * b, color="lightgray", linestyle="--", alpha=0.6,
                 label=f"0.25·B = {0.25 * b:.0f}")
    if t50 is not None:
        y = roll.iloc[min(t50, len(roll) - 1)]
        ax_t.scatter([t50], [y], color="orange", zorder=5)
        ax_t.annotate(f"t_50 = {t50}s", xy=(t50, y),
                      xytext=(t50 + 40, 0.5 * b + 0.15 * b),
                      color="orange", fontsize=9,
                      arrowprops=dict(arrowstyle="-", color="orange", alpha=0.6))
    if t25 is not None:
        y = roll.iloc[min(t25, len(roll) - 1)]
        ax_t.scatter([t25], [y], color="red", zorder=5)
        ax_t.annotate(f"t_25 = {t25}s", xy=(t25, y),
                      xytext=(t25 + 40, 0.25 * b - 0.15 * b),
                      color="red", fontsize=9,
                      arrowprops=dict(arrowstyle="-", color="red", alpha=0.6))
    ax_t.legend(loc="upper left", bbox_to_anchor=(1.02, 1.0), fontsize=8)

    # Panel 2: xmin age (log). Shared scale reveals M3's ~1e5 plateau vs
    # R1's ~2e6 climb — the whole point of calibrating.
    ax_x.plot(df["t_seconds"], xmin.clip(lower=1), color="tab:purple",
              label="oldest_backend_xmin_age")
    ax_x.set_yscale("log")
    ax_x.set_ylabel("XID age (log)")
    ax_x.grid(True, which="both", alpha=0.3)
    ax_x.legend(loc="upper left", bbox_to_anchor=(1.02, 1.0), fontsize=8)

    # Panel 3: dead + live tuples (log)
    ax_tup.plot(df["t_seconds"], dead.clip(lower=1), color="tab:red",
                label="n_dead_tup")
    ax_tup.plot(df["t_seconds"], live.clip(lower=1), color="tab:green",
                label="n_live_tup")
    ax_tup.set_yscale("log")
    ax_tup.set_ylabel("tuples (log)")
    ax_tup.grid(True, which="both", alpha=0.3)
    ax_tup.legend(loc="upper left", bbox_to_anchor=(1.02, 1.0), fontsize=8)

    # Panel 4: bytes (log, MiB). Log preserves M3's pre-antagonist sawtooth
    # against C1/R1 which reach hundreds of MiB.
    ax_b.plot(df["t_seconds"], tbl_mib.clip(lower=0.001),
              color="tab:olive", label="table_bytes")
    ax_b.plot(df["t_seconds"], idx_mib.clip(lower=0.001),
              color="tab:brown", label="index_bytes")
    ax_b.set_yscale("log")
    ax_b.set_ylabel("MiB (log)")
    ax_b.grid(True, which="both", alpha=0.3)
    ax_b.legend(loc="upper left", bbox_to_anchor=(1.02, 1.0), fontsize=8)

    # Panel 5: derived bytes / completed job (log).
    ax_bpj.plot(df["t_seconds"], bpj, color="tab:cyan",
                label="(table + index) bytes / completed job")
    ax_bpj.set_yscale("log")
    ax_bpj.set_ylabel("bytes / job (log)")
    ax_bpj.set_xlabel("t_seconds")
    ax_bpj.grid(True, which="both", alpha=0.3)
    ax_bpj.legend(loc="upper left", bbox_to_anchor=(1.02, 1.0), fontsize=8)

    # Shared y-limits — enforce or autoscale.
    if ylimits:
        _enforce_max("throughput_rolling", run_id, roll, ylimits["throughput_rolling"])
        _enforce_max("oldest_backend_xmin_age", run_id, xmin, ylimits["oldest_backend_xmin_age"])
        _enforce_max("n_dead_tup", run_id, dead, ylimits["n_dead_tup"])
        _enforce_max("n_live_tup", run_id, live, ylimits["n_live_tup"])
        _enforce_max("table_bytes_mib", run_id, tbl_mib, ylimits["table_bytes_mib"])
        _enforce_max("index_bytes_mib", run_id, idx_mib, ylimits["index_bytes_mib"])
        _enforce_max("bytes_per_completed_job", run_id, bpj, ylimits["bytes_per_completed_job"])
        ax_t.set_ylim(0, ylimits["throughput_rolling"] * 1.05)
        ax_x.set_ylim(1, ylimits["oldest_backend_xmin_age"] * 1.5)
        ax_tup.set_ylim(1, max(ylimits["n_dead_tup"], ylimits["n_live_tup"]) * 1.5)
        ax_b.set_ylim(0.001, max(ylimits["table_bytes_mib"], ylimits["index_bytes_mib"]) * 1.5)
        ax_bpj.set_ylim(top=ylimits["bytes_per_completed_job"] * 1.5)

    # Antagonist marker on every panel.
    for ax in axes:
        if ant_t is not None:
            ax.axvline(ant_t, color="black", linestyle=":", alpha=0.7)
    if ant_t is not None:
        top_ylim = ax_t.get_ylim()[1]
        ax_t.annotate(
            "antagonist starts", xy=(ant_t, top_ylim * 0.95),
            xytext=(ant_t + 20, top_ylim * 0.95),
            fontsize=9, color="black"
        )

    # Stale-window shading on every panel — visible cue that these values are
    # frozen, matching the meta.json fact.
    for w in meta.get("stale_windows", []) or []:
        for ax in axes:
            ax.axvspan(w["start_t"], w["end_t"], color="gray", alpha=0.08)

    fig.suptitle(_title(run_id, meta, b), fontsize=12)
    fig.tight_layout(rect=[0, 0, 0.86, 0.97])
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    print(f"wrote {out_path}")


def compare_plot(run_ids: list[str], results_dir: Path, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(13, 5.5))
    ax.set_xlabel("t_seconds")
    ax.set_ylabel("throughput / B  (normalised)")
    ax.grid(True, alpha=0.3)
    ax.axhline(1.0, color="gray", linestyle=":", alpha=0.4)
    ax.axhline(0.5, color="gray", linestyle="--", alpha=0.4)
    ax.axhline(0.25, color="lightgray", linestyle="--", alpha=0.4)

    ant_starts: set[int] = set()
    for run_id in run_ids:
        csv_path = results_dir / f"{run_id}.csv"
        df, meta = load_run(csv_path)
        b = meta["baseline_throughput_median"]
        roll = rolling_throughput(df)
        ax.plot(df["t_seconds"], roll / b, label=run_id, alpha=0.9)
        ant = antagonist_start_t(df)
        if ant is not None:
            ant_starts.add(ant)

    if len(ant_starts) == 1:
        (only,) = ant_starts
        ax.axvline(only, color="black", linestyle=":", alpha=0.7)
        ymax = ax.get_ylim()[1]
        ax.annotate("antagonist starts", xy=(only, ymax * 0.95),
                    xytext=(only + 20, ymax * 0.95), fontsize=9)
    elif len(ant_starts) > 1:
        for a in ant_starts:
            ax.axvline(a, color="black", linestyle=":", alpha=0.4)

    ax.legend(loc="upper right", fontsize=9)
    ax.set_title("Normalised throughput comparison")
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    print(f"wrote {out_path}")


def summary_table(run_ids: list[str], results_dir: Path) -> None:
    """Print a markdown table of end-of-run derived values for each run.

    The bytes/job column uses the last non-stale sample, so runs with a stale
    window (M3) report values from the last live tick, not from the frozen
    tail. Each row shows the t_seconds at which the bytes/job value was
    actually observed so the reader can tell fossil values from end-of-run.

    The bytes/job vs C1 ratio only fires when C1 is in the requested set;
    otherwise the column is empty. C1's own ratio is 1.00 by definition.
    """
    # Reference for ratios (control C1 if present).
    c1_bpj: float | None = None
    if "C1" in run_ids:
        df_c, meta_c = load_run(results_dir / "C1.csv")
        c1_bpj = float(bytes_per_completed_job(df_c).dropna().iloc[-1])

    header = ("| run | B (jobs/s) | t_50 | t_25 | descent | final_ratio "
              "| table MiB | index MiB | bytes/job | bytes/job vs C1 "
              "| observed at t | stale (s) |")
    sep = ("|-----|-----------:|-----:|-----:|--------:|-----------:|----------:"
           "|----------:|----------:|----------------:|--------------:|----------:|")
    print(header)
    print(sep)
    for run_id in run_ids:
        df, meta = load_run(results_dir / f"{run_id}.csv")
        b = meta["baseline_throughput_median"]
        t50 = meta.get("t_50")
        t25 = meta.get("t_25")
        desc = meta.get("descent_duration_seconds")
        final_r = meta.get("final_ratio")

        tbl_series = df.dropna(subset=["table_bytes"])
        idx_series = df.dropna(subset=["index_bytes"])
        bpj_series = bytes_per_completed_job(df).dropna()

        tbl_end_mib = tbl_series["table_bytes"].iloc[-1] / MIB
        idx_end_mib = idx_series["index_bytes"].iloc[-1] / MIB
        bpj_end = bpj_series.iloc[-1]
        observed_t = int(df.loc[bpj_series.index[-1], "t_seconds"])
        stale = sum(w["duration_s"] for w in (meta.get("stale_windows") or []))

        ratio_str = "—" if c1_bpj is None else f"{bpj_end / c1_bpj:.2f}×"
        # Mark fossil bytes/job values whose observation predates end-of-run.
        end_t = int(df["t_seconds"].iloc[-1])
        observed_str = f"{observed_t}"
        if observed_t < end_t:
            observed_str = f"{observed_t} (fossil)"

        print(f"| {run_id} | {b:.0f} "
              f"| {t50 if t50 is not None else '—'} "
              f"| {t25 if t25 is not None else '—'} "
              f"| {desc if desc is not None else '—'} "
              f"| {final_r:.4f} | {tbl_end_mib:.1f} | {idx_end_mib:.1f} "
              f"| {bpj_end:.0f} | {ratio_str} | {observed_str} | {stale} |")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", nargs="?", type=Path,
                        help="results/<run-id>.csv for single-run mode")
    parser.add_argument("--compare", nargs="+", metavar="RUN_ID",
                        help="run ids to overlay (uses results/<id>.csv)")
    parser.add_argument("--summary", nargs="+", metavar="RUN_ID",
                        help="print an end-of-run markdown table for these runs")
    parser.add_argument("--calibrate-runs", nargs="+", metavar="RUN_ID",
                        help="compute shared y-limits from these runs and apply "
                             "them to the single-run render")
    parser.add_argument("--out", type=Path,
                        help="compare-mode output path override")
    parser.add_argument("--results-dir", type=Path, default=Path("results"),
                        help="directory containing <id>.csv and <id>.meta.json")
    args = parser.parse_args(argv)

    if args.summary:
        summary_table(args.summary, args.results_dir)
        return 0

    if args.compare:
        run_ids = args.compare
        out = args.out or args.results_dir / f"compare-{'-'.join(run_ids)}.png"
        compare_plot(run_ids, args.results_dir, out)
        return 0

    if not args.csv:
        parser.error("provide a csv path, or use --compare / --summary")

    ylimits = None
    if args.calibrate_runs:
        ylimits = calibrate_ylimits(args.calibrate_runs, args.results_dir)
    single_run_plot(args.csv, args.csv.with_suffix(".png"), ylimits)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
