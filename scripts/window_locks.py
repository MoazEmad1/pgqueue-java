#!/usr/bin/env python3
"""
Produce the committable gzipped window extracts from a run's raw lock
CSVs. Reads:
    results/<runId>.locks.slow.csv
    results/<runId>.locks.fast.csv     (optional; skipped if missing)

Writes:
    results/<runId>.locks.slow.window-<FROM>-<TO>.csv.gz
    results/<runId>.locks.fast.window-<FROM>-<TO>.csv.gz

Both --from and --to are REQUIRED (in t_seconds). Choosing them is the
whole point of the exercise — bake the wedge window into the artifact
so a reader knows exactly what they're getting.

Raw slow=42 MiB, fast=150 MiB per run and are .gitignored. Compressed
extracts (~150 KiB to ~1 MiB depending on window) are the committable
audit trail for the lock-graph claims in the README.

Typical use, after an M3 run:
    python scripts/window_locks.py --run M3 --from 600 --to 1200
"""
from __future__ import annotations

import argparse
import gzip
import shutil
import sys
from pathlib import Path


def extract(src: Path, dst: Path, t_from: int, t_to: int) -> int:
    """Copy rows where t_from <= t_seconds <= t_to. Returns row count.
    Gzip level 9 for the smallest artifact — this only runs once per
    run so compression time doesn't matter."""
    kept = 0
    with open(src) as fin, gzip.open(dst, "wt", compresslevel=9) as fout:
        header = fin.readline()
        fout.write(header)
        for line in fin:
            # t_seconds is the second column; first split on ',' is cheap.
            try:
                t = int(line.split(",", 1)[0])
            except (IndexError, ValueError):
                continue
            if t_from <= t <= t_to:
                fout.write(line)
                kept += 1
    return kept


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run", required=True,
                   help="run id, e.g. M3. Reads results/<run>.locks.{slow,fast}.csv")
    p.add_argument("--from", dest="t_from", type=int, required=True,
                   help="window start (t_seconds, inclusive)")
    p.add_argument("--to", dest="t_to", type=int, required=True,
                   help="window end (t_seconds, inclusive)")
    p.add_argument("--results-dir", type=Path, default=Path("results"))
    args = p.parse_args(argv)

    if args.t_from > args.t_to:
        print("--from must be <= --to", file=sys.stderr)
        return 2

    any_written = False
    for probe in ("slow", "fast"):
        src = args.results_dir / f"{args.run}.locks.{probe}.csv"
        dst = args.results_dir / (
            f"{args.run}.locks.{probe}.window-{args.t_from}-{args.t_to}.csv.gz"
        )
        if not src.exists():
            print(f"skip {probe}: {src} not found")
            continue
        rows = extract(src, dst, args.t_from, args.t_to)
        size = dst.stat().st_size
        print(f"{probe}: rows={rows} → {dst} ({size} bytes, "
              f"{size / 1024:.1f} KiB)")
        any_written = True

    if not any_written:
        print(f"no raw lock CSVs found for run {args.run!r} in "
              f"{args.results_dir}", file=sys.stderr)
        return 1

    # Sanity nudge for the reader: remind that these belong in a commit.
    print()
    print("Commit these .csv.gz files alongside the run's meta.json / "
          "env.json / probe.csv / errors.csv — the raw .locks.slow.csv "
          "and .locks.fast.csv stay gitignored.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
