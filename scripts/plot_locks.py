#!/usr/bin/env python3
"""
Render the blocking chain from results/<runId>.locks.slow.csv (or its
window extract). Two-panel figure:

  1. Top: chain depth over time — number of unique pgqueue-workload PIDs
     whose pg_blocking_pids() is non-empty (workers, load generator, probes).
     Vertical marker at the focus time.
  2. Bottom: directed blocking graph at the focus time. Nodes = PIDs
     labelled by inferred role. Arrows point from WAITER to BLOCKER
     (arrow head = who is holding the lock the waiter needs). One label
     per (blocker, mode, relation) fan, placed clear of the fan.

Nothing hand-drawn. Every line, node, and edge comes from a row in the
CSV. Roles are inferred from the query snippet + backend state.

    python plot_locks.py results/M3.locks.slow.window-600-1200.csv --at 1000
"""
from __future__ import annotations

import argparse
import csv
import gzip
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt


# Row layout — higher y = further "upstream" in the blocking chain.
ROLE_ROW = {
    "antagonist":         4,
    "sweeper":            3,
    "queue-depth-probe":  2,
    "metrics-probe":      2,
    "load":               2,
    "worker":             1,
    "lock-collector":     0,
    "other":              0,
}
ROLE_COLOUR = {
    "antagonist":        "#c62828",
    "sweeper":           "#ef6c00",
    "queue-depth-probe": "#00838f",
    "metrics-probe":     "#00695c",
    "load":              "#2e7d32",
    "worker":            "#1565c0",
    "lock-collector":    "#4527a0",
    "other":             "#616161",
}
ROLE_ROW_LABEL = {
    4: "antagonist",
    3: "sweeper",
    2: "workload probes\n(load / queue-depth / metrics)",
    1: "workers",
    0: "collector (self) / other",
}
# Roles that count toward "workload blocked" in the top timeline. Excludes
# the lock-collector (that's us) and 'other' (unclassified backends —
# checkpointer, autovacuum launcher, etc).
WORKLOAD_ROLES = {"worker", "load", "queue-depth-probe", "metrics-probe"}


def infer_role(query_snippet: str, state: str) -> str:
    """Best-effort mapping from query + state to observed role in the
    experiment harness. Only used for chart annotation; the graph edges
    themselves come from pg_blocking_pids and don't depend on this."""
    q = (query_snippet or "").lstrip()
    if state == "idle in transaction" and q.startswith("SELECT count(*) FROM pgqueue.jobs"):
        return "antagonist"
    if q.startswith(("CREATE TABLE", "DROP TABLE", "ALTER TABLE")):
        return "sweeper"
    if q.startswith("UPDATE pgqueue.jobs"):
        return "worker"
    if q.startswith("INSERT INTO pgqueue.jobs"):
        return "load"
    if q.startswith("SELECT count(*) FROM pgqueue.jobs WHERE"):
        return "queue-depth-probe"
    # The MetricsCollector query starts with WITH tree AS (SELECT c.oid ...)
    if q.startswith("WITH tree AS"):
        return "metrics-probe"
    # The LockCollector's own slow query — pg_locks + pg_stat_activity.
    if "pg_locks" in q or "pg_stat_activity" in q:
        return "lock-collector"
    return "other"


def load_rows(csv_path: Path) -> list[dict]:
    """Read CSV rows, transparently handling gzip. .gz extensions plus
    magic-byte sniff so a mis-named file still opens correctly."""
    opener = gzip.open if csv_path.suffix == ".gz" else open
    if csv_path.suffix != ".gz":
        # Sniff for gzip magic on the off chance the extension lies.
        with open(csv_path, "rb") as probe:
            if probe.read(2) == b"\x1f\x8b":
                opener = gzip.open
    with opener(csv_path, "rt") as f:
        return list(csv.DictReader(f))


def chain_depth_series(rows: list[dict]) -> dict[int, int]:
    return _depth_series(rows, WORKLOAD_ROLES)


def worker_depth_series(rows: list[dict]) -> dict[int, int]:
    return _depth_series(rows, {"worker"})


def _depth_series(rows: list[dict], roles: set[str]) -> dict[int, int]:
    per_t: dict[int, set[str]] = defaultdict(set)
    for r in rows:
        if not r["blocking_pids"]:
            continue
        if infer_role(r["query_snippet"], r["state"]) not in roles:
            continue
        per_t[int(r["t_seconds"])].add(r["pid"])
    return {t: len(pids) for t, pids in per_t.items()}


def snapshot(rows: list[dict], at_t: int) -> list[dict]:
    return [r for r in rows if int(r["t_seconds"]) == at_t]


def build_graph(snap: list[dict]) -> tuple[dict[str, dict], list[dict]]:
    """Nodes keyed by PID. Edges only from granted=false rows so the mode +
    relation on the edge is the actual pending lock, not any of the pid's
    held locks."""
    nodes: dict[str, dict] = {}
    edges: list[dict] = []
    for r in snap:
        pid = r["pid"]
        if pid not in nodes:
            nodes[pid] = {
                "role": infer_role(r["query_snippet"], r["state"]),
                "query": (r["query_snippet"] or "")[:80],
                "state": r["state"],
            }
        if r["granted"] == "false" and r["blocking_pids"]:
            for blocker in r["blocking_pids"].split(":"):
                edges.append({
                    "waiter": pid,
                    "blocker": blocker,
                    "mode": r["mode"],
                    "relation": r["relation"] or "(no relation)",
                })
                if blocker not in nodes:
                    nodes[blocker] = {"role": "other", "query": "", "state": ""}
    return nodes, edges


def layout(nodes: dict[str, dict]) -> dict[str, tuple[float, float]]:
    by_row: dict[int, list[str]] = defaultdict(list)
    for pid, meta in nodes.items():
        by_row[ROLE_ROW.get(meta["role"], 0)].append(pid)
    pos: dict[str, tuple[float, float]] = {}
    for y, pids in by_row.items():
        pids.sort(key=lambda p: int(p))
        n = len(pids)
        for i, pid in enumerate(pids):
            x = (i + 0.5) / max(n, 1)
            pos[pid] = (x, y)
    return pos


def draw_chain(ax, nodes, edges, at_t):
    pos = layout(nodes)
    for pid, (x, y) in pos.items():
        meta = nodes[pid]
        colour = ROLE_COLOUR.get(meta["role"], "#616161")
        ax.scatter([x], [y], s=340, color=colour, edgecolors="black",
                   linewidths=0.6, zorder=3)
        ax.text(x, y - 0.14, f"pid={pid}", ha="center", va="top",
                fontsize=7, zorder=4)

    # Dedup edges: one label per (blocker, mode, relation) fan. Arrows still
    # drawn per waiter so fan-out size is visible.
    groups: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    for e in edges:
        groups[(e["blocker"], e["mode"], e["relation"])].append(e["waiter"])

    # Draw fan arrows first (all edges).
    for (blocker, mode, relation), waiters in groups.items():
        if blocker not in pos:
            continue
        (xb, yb) = pos[blocker]
        for w in waiters:
            if w not in pos:
                continue
            (xw, yw) = pos[w]
            ax.annotate("", xy=(xb, yb - 0.06), xytext=(xw, yw + 0.06),
                        arrowprops=dict(arrowstyle="->", color="#666",
                                        lw=0.6, alpha=0.45),
                        zorder=2)

    # Then place labels — offset by group index so no two overlap. Anchor to
    # the blocker, one horizontal step per subsequent group.
    per_blocker: dict[str, int] = defaultdict(int)
    for (blocker, mode, relation), waiters in groups.items():
        if blocker not in pos:
            continue
        (xb, yb) = pos[blocker]
        idx = per_blocker[blocker]
        per_blocker[blocker] += 1
        # Fan-out size in the label text.
        n = len(waiters)
        lead = f"{n} waiter" + ("s" if n != 1 else "")
        lbl = f"{lead}\n{mode}\non {relation}"
        # Stack labels either side of the blocker, alternating.
        x_off = 0.11 * (1 + idx // 2) * (-1 if idx % 2 else 1)
        y_off = -0.45 - 0.10 * (idx // 2)
        ax.text(xb + x_off, yb + y_off, lbl,
                fontsize=7, color="#222",
                ha="center", va="top",
                bbox=dict(boxstyle="round,pad=0.25", fc="#fff8e1",
                          ec="#e0c060", lw=0.5),
                zorder=5)
        # Small connector from label to blocker.
        ax.plot([xb, xb + x_off], [yb - 0.15, yb + y_off + 0.04],
                color="#e0c060", lw=0.5, alpha=0.6, zorder=1)

    ax.set_xlim(-0.08, 1.08)
    ax.set_ylim(-0.7, 4.7)
    ax.set_yticks(sorted(ROLE_ROW_LABEL.keys()))
    ax.set_yticklabels([ROLE_ROW_LABEL[y] for y in sorted(ROLE_ROW_LABEL.keys())],
                       fontsize=8)
    ax.set_xticks([])
    ax.set_title(
        f"blocking graph at t = {at_t} s  ({len(nodes)} PIDs, "
        f"{sum(len(v) for v in groups.values())} blocking edges).  "
        "Arrow → points from WAITER to BLOCKER.",
        fontsize=10)

    handles = [mpatches.Patch(color=c, label=r)
               for r, c in ROLE_COLOUR.items()
               if r in {n["role"] for n in nodes.values()}]
    ax.legend(handles=handles, loc="lower right", fontsize=8, frameon=False,
              ncol=2)


def draw_timeline(ax, workload_depth: dict[int, int],
                  worker_depth: dict[int, int], focus_t: int):
    """Two step-lines: workload total (workers + load + probes) and
    workers-only. Distinction is important because the two counts read
    differently in the M3 record — 21-23 vs 18-20 — and the whole point
    of publishing this chart is that the workload probes are victims of
    the same wedge that hits the workers."""
    if not workload_depth:
        ax.text(0.5, 0.5, "no workload PIDs blocked in this file",
                ha="center", va="center", transform=ax.transAxes)
        return

    def _plot(depth, color, label):
        ts = sorted(depth.keys())
        ys = [depth[t] for t in ts]
        ax.step(ts, ys, where="post", color=color, linewidth=1.4, label=label)
        ax.fill_between(ts, ys, step="post", alpha=0.15, color=color)

    _plot(workload_depth, "#1565c0", "workload total (worker + load + probes)")
    _plot(worker_depth, "#ef6c00", "workers only")

    ax.axvline(focus_t, color="black", linestyle=":", alpha=0.7)
    ys_all = list(workload_depth.values())
    ymax = max(ys_all)
    ax.set_ylim(0, ymax * 1.15 if ymax else 1)
    ax.text(focus_t + 4, ymax * 0.9, f"t={focus_t}", fontsize=8, color="black")
    ax.set_xlabel("t_seconds")
    ax.set_ylabel("PIDs blocked")

    ys_workers = list(worker_depth.values()) or [0]
    ax.set_title(
        f"chain depth (pg_blocking_pids non-empty) — "
        f"n={len(workload_depth)} samples · "
        f"workload total min={min(ys_all)} max={ymax} · "
        f"workers-only min={min(ys_workers)} max={max(ys_workers)}",
        fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="lower right", fontsize=8, framealpha=0.9)


def main(argv):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("csv", type=Path)
    p.add_argument("--at", type=int, required=True)
    p.add_argument("--out", type=Path)
    args = p.parse_args(argv)

    if args.out is None:
        stem = args.csv.name.split(".locks.")[0]
        args.out = args.csv.parent / f"{stem}.locks.chain-at-{args.at}.png"

    rows = load_rows(args.csv)
    workload_depth = chain_depth_series(rows)
    worker_depth = worker_depth_series(rows)
    snap = snapshot(rows, args.at)
    if not snap:
        print(f"no rows at t={args.at} in {args.csv}", file=sys.stderr)
        return 2
    nodes, edges = build_graph(snap)

    fig, (ax_top, ax_bot) = plt.subplots(
        2, 1, figsize=(15, 9),
        gridspec_kw={"height_ratios": [1, 4]}
    )
    draw_timeline(ax_top, workload_depth, worker_depth, args.at)
    draw_chain(ax_bot, nodes, edges, args.at)
    fig.tight_layout()
    fig.savefig(args.out, dpi=140)
    plt.close(fig)
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
