#!/usr/bin/env python3
"""Collects the submitted load-test reports and draws every diagram in the comparison.

Reads the unmodified client JSON reports, comparison/data/code_metrics.json, and
comparison/data/fault_injection.json. Writes comparison/data/performance.json and
SVG diagrams under comparison/figures/.

Usage: python3 comparison/scripts/figures.py
"""

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap, ListedColormap  # noqa: E402
from matplotlib.patches import Ellipse, FancyArrowPatch, FancyBboxPatch, Rectangle  # noqa: E402
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "comparison" / "data"
FIG = ROOT / "comparison" / "figures"

REPORTS = {
    "monolith": ("reports/default/report-20260915-200422.json", "reports/stress/report-20260915-200624.json"),
    "layered": ("layered/reports/default/report-20260921-201120.json", "layered/reports/stress/report-20260921-201322.json"),
    "pipeline": ("pipeline/reports/default/report-20260929-200339.json", "pipeline/reports/stress/report-20260929-200540.json"),
    "service-based": ("service-based/reports/default/report-20261007-203911.json",
                      "service-based/reports/stress/report-20261007-204114.json"),
    "microservices": ("microservices/reports/default/report-20261019-204525.json",
                      "microservices/reports/stress/report-20261019-204729.json"),
    "event-based": ("event-based/reports/default/report-20261026-214357.json",
                    "event-based/reports/stress/report-20261026-214609.json"),
}
LABELS = {"monolith": "Monolith", "layered": "Layered", "pipeline": "Pipeline", "service-based": "Service-based",
          "microservices": "Microservices", "event-based": "Event-based"}
ORDER = list(REPORTS)
LIGHT, DARK, RED, ORANGE, GREEN = "#9DB4D3", "#2F5D8C", "#C44E52", "#DD8452", "#55A868"

plt.rcParams.update({"font.family": "Helvetica", "font.size": 8.5, "axes.spines.top": False,
                     "axes.spines.right": False, "svg.fonttype": "none", "axes.titlesize": 9.5,
                     "axes.titleweight": "bold"})


def summarize(path):
    r = json.loads((ROOT / path).read_text())
    ops = {o["operation"]: o for o in r["operations"]}
    return {
        "file": path,
        "stations": r["config"]["stations"],
        "wall_s": r["wallClockSeconds"],
        "tx_per_s": r["transactionsPerSecond"],
        "scans_per_s": r["itemsPerSecond"],
        "completed": r["totalTransactions"],
        "scans": r["totalItemsScanned"],
        "ops": {k: {f: o[f] for f in ("successCount", "errorCount", "errorRate", "meanMs", "p50Ms", "p95Ms", "p99Ms", "maxMs")}
                for k, o in ops.items()},
        "requests_per_s": sum(o["successCount"] + o["errorCount"] for o in ops.values()) / r["wallClockSeconds"],
        "mean_success_ms": sum(o["meanMs"] * o["successCount"] for o in ops.values())
        / sum(o["successCount"] for o in ops.values()),
        "top3": [p["sku"] for p in r["popularItems"][:3]],
    }


def collect():
    perf = {a: {"default": summarize(d), "stress": summarize(s)} for a, (d, s) in REPORTS.items()}
    (DATA / "performance.json").write_text(json.dumps(perf, indent=2))
    return perf


def save(fig, name):
    fig.savefig(FIG / f"{name}.svg", bbox_inches="tight")
    plt.close(fig)


def xlabels(ax, rotate=True):
    ax.set_xticks(range(len(ORDER)))
    ax.set_xticklabels([LABELS[a] for a in ORDER], rotation=32 if rotate else 0, ha="right" if rotate else "center")


# ---------------------------------------------------------------- architecture


def box(ax, x, y, w, h, text, fc="#EEF3F9", ec=DARK, size=7, bold=False):
    ax.add_patch(FancyBboxPatch((x - w / 2, y - h / 2), w, h, boxstyle="round,pad=0.02,rounding_size=0.15",
                                fc=fc, ec=ec, lw=0.9))
    ax.text(x, y, text, ha="center", va="center", fontsize=size, weight="bold" if bold else "normal", linespacing=1.1)


def db(ax, x, y, w, text, fc="#FFF4E0", size=6.5):
    h = 0.9
    ax.add_patch(Rectangle((x - w / 2, y - h / 2), w, h, fc=fc, ec="#9A6B1F", lw=0.8))
    ax.add_patch(Ellipse((x, y - h / 2), w, 0.35, fc=fc, ec="#9A6B1F", lw=0.8))
    ax.add_patch(Ellipse((x, y + h / 2), w, 0.35, fc=fc, ec="#9A6B1F", lw=0.8))
    ax.text(x, y - 0.05, text, ha="center", va="center", fontsize=size)


def arrow(ax, p, q, color="#777777", style="-|>", ls="-", rad=0.0, lw=0.9):
    ax.add_patch(FancyArrowPatch(p, q, arrowstyle=style, mutation_scale=7, color=color, lw=lw, ls=ls,
                                 connectionstyle=f"arc3,rad={rad}", shrinkA=1, shrinkB=1))


def architecture_chart():
    fig, axes = plt.subplots(2, 3, figsize=(7.2, 4.9))
    for ax in axes.flat:
        ax.set_xlim(0, 10)
        ax.set_ylim(0, 10)
        ax.axis("off")
    titles = ["A1 Monolith", "A2 Layered", "A3 Pipeline", "A4 Service-based", "A5 Microservices", "A6 Event-based"]
    for ax, t in zip(axes.flat, titles):
        ax.set_title(t, fontsize=9)

    ax = axes[0][0]
    box(ax, 5, 9.2, 3, 0.9, "Client", fc="white", ec="#888888")
    box(ax, 5, 5.8, 7, 2.6, "checkout-monolith\nroutes + rules + SQL\nin one module", bold=False)
    db(ax, 5, 2.0, 3.2, "SQLite")
    arrow(ax, (5, 8.7), (5, 7.15))
    arrow(ax, (5, 4.45), (5, 2.65), color="#9A6B1F")

    ax = axes[0][1]
    box(ax, 5, 9.2, 3, 0.9, "Client", fc="white", ec="#888888")
    box(ax, 5, 7.5, 8, 1.0, "API layer (Axum routes)")
    for x, t in ((2.2, "Transactions"), (5, "Inventory"), (7.8, "Analytics")):
        box(ax, x, 5.6, 2.5, 1.0, t)
    box(ax, 5, 3.7, 8, 1.0, "Database access layer (all SQL)")
    db(ax, 5, 1.6, 3.2, "SQLite")
    arrow(ax, (5, 8.7), (5, 8.05))
    for x in (2.2, 5, 7.8):
        arrow(ax, (x, 6.95), (x, 6.15))
        arrow(ax, (x, 5.05), (x, 4.25))
    arrow(ax, (5, 3.15), (5, 2.25), color="#9A6B1F")

    ax = axes[0][2]
    box(ax, 5, 9.2, 3, 0.9, "Client", fc="white", ec="#888888")
    box(ax, 5, 7.6, 8, 0.9, "API layer")
    box(ax, 2.6, 6.0, 3.4, 0.9, "Transactions")
    box(ax, 7.4, 6.0, 3.4, 0.9, "Inventory")
    stages = ["seq", "journal", "window", "rank", "publish"]
    for i, s in enumerate(stages):
        box(ax, 1.2 + i * 1.9, 4.3, 1.55, 0.8, s, fc="#F1ECF8", ec="#8172B2", size=6.3)
        if i:
            arrow(ax, (1.2 + (i - 1) * 1.9 + 0.78, 4.3), (1.2 + i * 1.9 - 0.78, 4.3), color="#8172B2")
    ax.text(5, 3.55, "analytics: 5 filters, 4 bounded pipes", ha="center", fontsize=6.3, color="#8172B2")
    box(ax, 5, 2.55, 8, 0.8, "Database access layer")
    db(ax, 5, 0.85, 3.2, "SQLite")
    arrow(ax, (5, 8.75), (5, 8.05))
    arrow(ax, (2.6, 5.55), (1.2, 4.7), color="#8172B2")
    arrow(ax, (5, 2.15), (5, 1.4), color="#9A6B1F")

    services = [(1.25, "inventory"), (3.75, "basket"), (6.25, "payment"), (8.75, "analytics")]

    ax = axes[1][0]
    box(ax, 5, 9.2, 3, 0.8, "Client", fc="white", ec="#888888")
    box(ax, 5, 7.8, 5, 0.8, "gateway (routing only)", fc="#F4F4F4", ec="#888888")
    for x, t in services:
        box(ax, x, 5.9, 2.2, 1.0, t)
        arrow(ax, (5, 7.4), (x, 6.45))
    box(ax, 5, 3.9, 9, 0.8, "checkout-data library (all SQL)", fc="#FFF4E0", ec="#9A6B1F")
    for x, _ in services:
        arrow(ax, (x, 5.4), (x, 4.3), color="#9A6B1F")
    db(ax, 5, 2.0, 3.2, "one shared SQLite")
    ax.text(5, 0.75, "5 of 7 tables used by more than one service", ha="center", fontsize=6.0, color="#9A6B1F")
    arrow(ax, (5, 3.5), (5, 2.45), color="#9A6B1F")

    ax = axes[1][1]
    box(ax, 5, 9.2, 3, 0.8, "Client", fc="white", ec="#888888")
    box(ax, 5, 7.8, 5, 0.8, "gateway (routing only)", fc="#F4F4F4", ec="#888888")
    for x, t in services:
        box(ax, x, 5.2, 2.2, 1.0, t)
        arrow(ax, (5, 7.4), (x, 5.75), color="#BBBBBB")
        db(ax, x, 2.2, 1.9, "own DB")
        arrow(ax, (x, 4.7), (x, 2.85), color="#9A6B1F")
    arrow(ax, (6.25 - 1.1, 5.2), (3.75 + 1.1, 5.2), color=RED, lw=1.2)
    arrow(ax, (3.75 - 1.1, 5.2), (1.25 + 1.1, 5.2), color=RED, lw=1.2)
    arrow(ax, (6.0, 4.7), (1.5, 4.7), color=RED, rad=-0.25, lw=1.2)
    arrow(ax, (8.75, 5.7), (3.9, 5.7), color=RED, rad=0.45, lw=1.0, ls="--")
    ax.text(5, 0.6, "4 sync HTTP dependencies (red; dashed = background poll)\n3 calls in a row for each checkout",
            ha="center", fontsize=6.0, color=RED)

    ax = axes[1][2]
    box(ax, 5, 9.2, 3, 0.8, "Client", fc="white", ec="#888888")
    box(ax, 5, 7.8, 5, 0.8, "gateway (routing only)", fc="#F4F4F4", ec="#888888")
    for x, t in services:
        box(ax, x, 5.9, 2.2, 1.0, t)
        arrow(ax, (5, 7.4), (x, 6.45), color="#BBBBBB")
        db(ax, x - 0.35, 3.95, 1.35, "DB", size=6)
        arrow(ax, (x - 0.35, 5.4), (x - 0.35, 4.6), color="#9A6B1F")
        arrow(ax, (x + 0.7, 5.4), (x + 0.7, 2.0), color=ORANGE, style="<|-|>", ls="--")
    box(ax, 5, 1.5, 9.4, 0.9, "Kafka broker: 6 topics, 16 event types", fc="#FDEBDD", ec=ORANGE)
    ax.text(5, 0.35, "no service knows another service's address", ha="center", fontsize=6.3, color=ORANGE)

    fig.tight_layout(h_pad=0.6, w_pad=0.4)
    save(fig, "architecture")


# ---------------------------------------------------------------- performance


def performance_chart(perf):
    fig, axes = plt.subplots(1, 3, figsize=(7.2, 2.55), gridspec_kw={"width_ratios": [1.15, 1.15, 0.9]})
    x = range(len(ORDER))
    w = 0.38
    for ax, metric, title in ((axes[0], "scans_per_s", "Scans per second"),
                              (axes[1], "tx_per_s", "Completed checkouts per second")):
        d = [perf[a]["default"][metric] for a in ORDER]
        s = [perf[a]["stress"][metric] for a in ORDER]
        ax.bar([i - w / 2 for i in x], d, w, label="10 stations", color=LIGHT)
        ax.bar([i + w / 2 for i in x], s, w, label="100 stations", color=DARK)
        for i, (dv, sv) in enumerate(zip(d, s)):
            ax.text(i + w / 2, sv, f"{sv / dv:.2f}×", ha="center", va="bottom", fontsize=6, color=DARK)
        xlabels(ax)
        ax.set_title(title)
        ax.set_ylim(0, max(d + s) * 1.3)
        ax.grid(axis="y", alpha=0.3)
    axes[0].legend(fontsize=6.3, frameon=False, loc="upper left", ncol=2)
    ax = axes[2]
    for mode, marker, color in (("default", "o", LIGHT), ("stress", "s", DARK)):
        xs = [1000 * perf[a][mode]["stations"] / perf[a][mode]["requests_per_s"] for a in ORDER]
        ys = [perf[a][mode]["mean_success_ms"] for a in ORDER]
        ax.scatter(xs, ys, marker=marker, color=color, s=18, zorder=3,
                   label="10 stations" if mode == "default" else "100 stations")
    ax.plot([1, 60], [1, 60], color="#999999", lw=0.8, ls="--", label="stations ÷ throughput")
    ax.set_xscale("log")
    ax.set_yscale("log")
    for a in (ax.xaxis, ax.yaxis):
        a.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
    ax.set_xlabel("stations ÷ requests per s (ms)")
    ax.set_ylabel("measured mean latency (ms)")
    ax.set_title("Little's law")
    ax.legend(fontsize=6.3, frameon=False, loc="upper left")
    fig.tight_layout(w_pad=0.8)
    save(fig, "performance")


def latency_chart(perf):
    fig, axes = plt.subplots(2, 2, figsize=(7.2, 2.9), sharex=True)
    pct = ("p50Ms", "p95Ms", "p99Ms")
    shades = ("#B8C9E1", "#6A8FBF", DARK)
    w = 0.26
    for row, (op, name) in enumerate((("SCAN_ITEM", "Scan"), ("COMPLETE_TRANSACTION", "Checkout"))):
        for col, mode in enumerate(("default", "stress")):
            ax = axes[row][col]
            for k, (p, c) in enumerate(zip(pct, shades)):
                vals = [perf[a][mode]["ops"][op][p] for a in ORDER]
                ax.bar([i + (k - 1) * w for i in range(len(ORDER))], vals, w, color=c, label=p.replace("Ms", ""))
            ax.set_yscale("log")
            ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1, 2, 5)))
            ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
            ax.yaxis.set_minor_formatter(NullFormatter())
            ax.set_ylabel("ms (log)")
            ax.set_title(f"{name}, {'10' if mode == 'default' else '100'} stations")
            ax.grid(axis="y", alpha=0.3)
            xlabels(ax)
    axes[0][0].legend(fontsize=6.5, frameon=False, ncol=3)
    fig.tight_layout(h_pad=0.4)
    save(fig, "latency")


# ---------------------------------------------------------------- code metrics


def metrics_heatmap(code, faults):
    """One row per metric; colour shows rank within the row (green = better)."""
    hops = [1, 1, 1, 2, 5, 6]
    commits = [1, 1, 1, 1, 7, 7]
    sync = [0, 0, 0, 0, 4, 0]
    contracts = [0, 0, 4, 0, 7, 16]
    rows = [
        ("Size", "Production lines of code", [c["size"]["prod_loc"] for c in code], "low", "{:,.0f}"),
        ("Size", "Test lines of code", [c["size"]["test_loc"] for c in code], None, "{:,.0f}"),
        ("Size", "Functions and closures", [c["size"]["functions"] for c in code], "low", "{:.0f}"),
        ("Size", "Deployable units", [c["size"]["deployables"] for c in code], None, "{:.0f}"),
        ("Complexity", "Cyclomatic, mean per function", [c["complexity"]["cyclomatic_mean"] for c in code], "low", "{:.2f}"),
        ("Complexity", "Functions with cyclomatic > 10", [c["complexity"]["functions_cc_over_10"] for c in code], "low", "{:.0f}"),
        ("Complexity", "Maintainability index (0–100)", [c["complexity"]["mi_vs_weighted"] for c in code], "high", "{:.1f}"),
        ("Coupling", "Propagation cost (file graph)",
         [None] + [c["coupling"]["propagation_cost"] for c in code[1:]], "low", "{:.2f}"),
        ("Coupling", "Tables used by > 1 service",
         [None, None, None] + [len(c["data"]["tables_shared_by_deployables"]) for c in code[3:]], "low", "{:.0f}"),
        ("Coupling", "Sync service-to-service calls", sync, "low", "{:.0f}"),
        ("Coupling", "Internal endpoints, pipes, or events", contracts, "low", "{:.0f}"),
        ("Coupling", "Hops before a receipt", hops, "low", "{:.0f}"),
        ("Coupling", "Disk commits per checkout", commits, "low", "{:.0f}"),
        ("Cohesion", "Business domains per SQL module",
         [c["data"]["mean_domains_per_sql_module"] for c in code], "low", "{:.2f}"),
        ("Cohesion", "Types with LCOM4 = 1", [c["cohesion"]["lcom4_cohesive_share"] for c in code], "high", "{:.0%}"),
    ]
    cmap = LinearSegmentedColormap.from_list("rg", ["#E8A3A3", "#F6E7B5", "#A8D5A2"])
    fig, ax = plt.subplots(figsize=(7.2, 3.6))
    for r, (group, name, vals, better, fmt) in enumerate(rows):
        known = [v for v in vals if v is not None]
        lo, hi = min(known), max(known)
        for c, v in enumerate(vals):
            if v is None:
                color, label = "#EEEEEE", "n/a"
            else:
                score = 0.5 if better is None or hi == lo else (v - lo) / (hi - lo)
                if better == "low":
                    score = 1 - score
                color, label = (cmap(score) if better else "#F4F4F4"), fmt.format(v)
            ax.add_patch(Rectangle((c, r), 1, 1, fc=color, ec="white", lw=1.5))
            ax.text(c + 0.5, r + 0.5, label, ha="center", va="center", fontsize=7.2)
    ax.set_xlim(0, 6)
    ax.set_ylim(len(rows), 0)
    ax.set_xticks([i + 0.5 for i in range(6)])
    ax.set_xticklabels([LABELS[a] for a in ORDER], fontsize=7.5)
    ax.xaxis.tick_top()
    ax.set_yticks([i + 0.5 for i in range(len(rows))])
    ax.set_yticklabels([f"{n}" for _, n, *_ in rows], fontsize=7.3)
    ax.tick_params(length=0)
    for s in ax.spines.values():
        s.set_visible(False)
    prev = None
    for r, (group, *_rest) in enumerate(rows):
        if group != prev:
            ax.text(-2.75, r + 0.5, group.upper(), fontsize=6.5, weight="bold", color="#666666", va="center")
            if prev:
                ax.axhline(r, color="#555555", lw=0.8, xmin=0, xmax=1)
            prev = group
    save(fig, "metrics")


# ---------------------------------------------------------------- availability


def availability_chart(faults):
    archs = ["service-based", "microservices", "event-based"]
    comps_all = ["kafka-broker", "inventory-service", "basket-service", "payment-service", "analytics-service",
                 "checkout-gateway"]
    eps = ["GET /items", "POST /transactions", "POST /transactions/{id}/items", "POST /transactions/{id}/complete",
           "GET /transactions/{id}", "GET /inventory/low-stock", "GET /analytics/popular-items"]
    short = ["items", "start", "scan", "checkout", "status", "low-stock", "popular"]
    fig = plt.figure(figsize=(7.2, 4.4))
    gs = fig.add_gridspec(2, 3, height_ratios=[1.05, 1], hspace=0.75, wspace=0.12)
    bottom = gs[1, :].subgridspec(1, 2, width_ratios=[1.7, 1], wspace=0.55)
    cmap = ListedColormap(["#E8EAED", "#D9534F", "#5CB85C"])
    for k, arch in enumerate(archs):
        ax = fig.add_subplot(gs[0, k])
        grid = []
        for c in comps_all:
            f = faults[arch]["failures"].get(c)
            grid.append([0] * len(eps) if f is None else [2 if f["probes"][e]["ok"] else 1 for e in eps])
        ax.imshow(grid, cmap=cmap, vmin=0, vmax=2, aspect="auto")
        ax.set_xticks(range(len(eps)))
        ax.set_xticklabels(short, rotation=45, ha="right", fontsize=6.8)
        ax.set_yticks(range(len(comps_all)))
        ax.set_yticklabels(["Kafka broker", "inventory", "basket", "payment", "analytics", "gateway"] if k == 0 else [])
        ax.set_title(LABELS[arch])
        ax.set_xticks([i - 0.5 for i in range(1, len(eps))], minor=True)
        ax.set_yticks([i - 0.5 for i in range(1, len(comps_all))], minor=True)
        ax.grid(which="minor", color="white", linewidth=1.5)
        ax.tick_params(which="both", length=0)
        for s in ax.spines.values():
            s.set_visible(False)
        if k == 0:
            ax.set_ylabel("Stopped component")

    up, checkout_ok, recovery = [], [], []
    for a in ORDER:
        fails = faults[a]["failures"].values()
        up.append(100 * sum(f["available"] for f in fails) / (7 * len(fails)))
        checkout_ok.append(100 * sum(f["probes"]["POST /transactions/{id}/complete"]["ok"] for f in fails) / len(fails))
        recovery.append(max(f["restart_to_checkout_ms"] for f in fails))
    ax = fig.add_subplot(bottom[0])
    xs = range(len(ORDER))
    ax.bar([i - 0.2 for i in xs], up, 0.38, color=GREEN, label="all 7 endpoints")
    ax.bar([i + 0.2 for i in xs], checkout_ok, 0.38, color=DARK, label="checkout endpoint")
    for i, (u, c) in enumerate(zip(up, checkout_ok)):
        ax.text(i - 0.2, u + 1.5, f"{u:.0f}", ha="center", fontsize=6.3)
        ax.text(i + 0.2, c + 1.5, f"{c:.0f}", ha="center", fontsize=6.3)
    ax.set_ylim(0, 85)
    ax.set_title("Still working when one component stops (%)")
    ax.legend(fontsize=6.5, frameon=False, loc="upper left")
    ax.set_xticks(range(len(ORDER)))
    ax.set_xticklabels(["Mono", "Layered", "Pipeline", "Service-\nbased", "Micro-\nservices", "Event-\nbased"],
                       fontsize=7)
    ax.grid(axis="y", alpha=0.3)
    ax = fig.add_subplot(bottom[1])
    ax.barh(range(len(ORDER)), recovery, color=[RED if r > 1000 else GREEN for r in recovery])
    ax.set_xscale("log")
    ax.set_xlim(10, 200000)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1000:g} s" if v >= 1000 else f"{v:g} ms"))
    ax.set_yticks(range(len(ORDER)))
    ax.set_yticklabels([LABELS[a] for a in ORDER], fontsize=7)
    ax.invert_yaxis()
    for i, r in enumerate(recovery):
        ax.text(r * 1.15, i, f"{r / 1000:.1f} s" if r > 1000 else f"{r:.0f} ms", va="center", fontsize=6.3)
    ax.set_title("Worst time from restart\nto first good checkout")
    save(fig, "availability")


def main():
    FIG.mkdir(parents=True, exist_ok=True)
    perf = collect()
    code = json.loads((DATA / "code_metrics.json").read_text())
    faults = json.loads((DATA / "fault_injection.json").read_text())
    architecture_chart()
    performance_chart(perf)
    latency_chart(perf)
    metrics_heatmap(code, faults)
    availability_chart(faults)
    for old in ("throughput", "code"):
        (FIG / f"{old}.svg").unlink(missing_ok=True)


if __name__ == "__main__":
    main()
