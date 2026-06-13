#!/usr/bin/env python3
"""
benchmark_regression_check.py

Compares two JMH JSON result files and produces a Markdown regression report.

Usage:
    python3 scripts/benchmark_regression_check.py \\
        --baseline data/runs/abc1234/results.json \\
        --current  data/runs/def5678/results.json \\
        --threshold 10

Exit codes:
    0 — all benchmarks within threshold (or improvements)
    1 — one or more statistically significant regressions detected
    2 — script error (bad arguments, missing file, malformed JSON)
"""

import argparse
import json
import sys
from pathlib import Path


# ── Argument parsing ──────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare two JMH JSON result files and report regressions."
    )
    parser.add_argument(
        "--baseline",
        required=True,
        metavar="FILE",
        help="Path to the baseline JMH results JSON (the older / reference run).",
    )
    parser.add_argument(
        "--current",
        required=True,
        metavar="FILE",
        help="Path to the current JMH results JSON (the newer / candidate run).",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=10.0,
        metavar="PCT",
        help="Regression threshold as a percentage (default: 10). A benchmark is "
             "flagged only if the score increased by more than this amount AND the "
             "confidence intervals do not overlap.",
    )
    parser.add_argument(
        "--baseline-label",
        default=None,
        metavar="LABEL",
        help="Human-readable label for the baseline run (default: basename of file).",
    )
    parser.add_argument(
        "--current-label",
        default=None,
        metavar="LABEL",
        help="Human-readable label for the current run (default: basename of file).",
    )
    parser.add_argument(
        "--output",
        default=None,
        metavar="FILE",
        help="Write the Markdown report to this file in addition to stdout.",
    )
    return parser.parse_args()


# ── Data loading ──────────────────────────────────────────────────────────────

def load_results(path: str) -> list[dict]:
    p = Path(path)
    if not p.exists():
        print(f"ERROR: file not found: {path}", file=sys.stderr)
        sys.exit(2)
    try:
        data = json.loads(p.read_text())
    except json.JSONDecodeError as e:
        print(f"ERROR: malformed JSON in {path}: {e}", file=sys.stderr)
        sys.exit(2)
    if not isinstance(data, list):
        print(f"ERROR: expected a JSON array in {path}", file=sys.stderr)
        sys.exit(2)
    return data


# ── Benchmark identity ────────────────────────────────────────────────────────

def bench_key(entry: dict) -> str:
    """
    Unique key for a benchmark entry combining the fully-qualified benchmark
    name and its parameters.  Two entries with the same class but different
    @Param values (e.g. ADDMOD_32_32_32 vs ADDMOD_32_32_256) get distinct keys.
    """
    name = entry["benchmark"]
    params = entry.get("params") or {}
    param_str = str(sorted(params.items()))
    return f"{name}|{param_str}"


def short_name(entry: dict) -> str:
    """
    Strip the package prefix from the fully-qualified benchmark name.
    org.hyperledger.besu.ethereum.vm.operations.AddModOperationBenchmark.executeOperation
      → AddModOperationBenchmark
    """
    parts = entry["benchmark"].split(".")
    # The method name is the last segment; the class name is second-to-last.
    return parts[-2] if len(parts) >= 2 else entry["benchmark"]


def params_display(entry: dict) -> str:
    """Return a compact display string for the benchmark parameters."""
    params = entry.get("params") or {}
    if not params:
        return "—"
    return ", ".join(params.values())


# ── Comparison logic ──────────────────────────────────────────────────────────

STATUS_OK          = "✅ OK"
STATUS_REGRESSION  = "❌ REGRESSION"
STATUS_IMPROVEMENT = "🚀 IMPROVEMENT"
STATUS_NOISY       = "⚠️  NOISY"
STATUS_NEW         = "🆕 NEW"
STATUS_MISSING     = "➖ MISSING"


def classify(baseline_entry: dict, current_entry: dict, threshold: float) -> tuple[float, str]:
    """
    Compare one matching pair of benchmark entries.

    Returns (change_pct, status_string).

    Rules (avgt mode — lower score is better):
      - change_pct > threshold AND intervals do not overlap → REGRESSION
      - change_pct < -threshold AND intervals do not overlap → IMPROVEMENT
      - |change_pct| <= threshold → OK
      - intervals overlap regardless of magnitude → NOISY (measurement noise,
        not actionable)
    """
    b_score = baseline_entry["primaryMetric"]["score"]
    b_error = baseline_entry["primaryMetric"]["scoreError"]
    c_score = current_entry["primaryMetric"]["score"]
    c_error = current_entry["primaryMetric"]["scoreError"]

    if b_score == 0:
        return 0.0, STATUS_OK

    change_pct = (c_score - b_score) / b_score * 100.0

    # Confidence interval overlap check.
    # Baseline CI:  [b_score - b_error, b_score + b_error]
    # Current CI:   [c_score - c_error, c_score + c_error]
    # Intervals overlap when the lower bound of the higher one is below the
    # upper bound of the lower one.
    baseline_hi = b_score + b_error
    current_lo  = c_score - c_error
    intervals_overlap = current_lo < baseline_hi

    if intervals_overlap:
        return change_pct, STATUS_NOISY

    if change_pct > threshold:
        return change_pct, STATUS_REGRESSION

    if change_pct < -threshold:
        return change_pct, STATUS_IMPROVEMENT

    return change_pct, STATUS_OK


# ── Report generation ─────────────────────────────────────────────────────────

def format_score(entry: dict) -> str:
    score = entry["primaryMetric"]["score"]
    error = entry["primaryMetric"]["scoreError"]
    return f"{score:>10.2f} ± {error:.2f}"


def build_report(
    baseline_data: list[dict],
    current_data: list[dict],
    threshold: float,
    baseline_label: str,
    current_label: str,
) -> tuple[str, int]:
    """
    Build the full Markdown report string.
    Returns (report_text, exit_code) where exit_code is 0 or 1.
    """

    # Index baseline by key for O(1) lookup.
    baseline_index: dict[str, dict] = {bench_key(e): e for e in baseline_data}
    current_index:  dict[str, dict] = {bench_key(e): e for e in current_data}

    rows = []
    regression_count = 0
    compared_count = 0

    # Benchmarks present in both runs.
    for key, current_entry in sorted(current_index.items()):
        if key in baseline_index:
            compared_count += 1
            baseline_entry = baseline_index[key]
            change_pct, status = classify(baseline_entry, current_entry, threshold)
            if status == STATUS_REGRESSION:
                regression_count += 1
            rows.append({
                "name":     short_name(current_entry),
                "params":   params_display(current_entry),
                "baseline": format_score(baseline_entry),
                "current":  format_score(current_entry),
                "change":   f"{change_pct:+.1f}%",
                "status":   status,
                "sort_key": abs(change_pct),
            })
        else:
            # Present in current but not in baseline.
            rows.append({
                "name":     short_name(current_entry),
                "params":   params_display(current_entry),
                "baseline": "—",
                "current":  format_score(current_entry),
                "change":   "N/A",
                "status":   STATUS_NEW,
                "sort_key": -1,
            })

    # Benchmarks present in baseline but removed from current.
    for key, baseline_entry in sorted(baseline_index.items()):
        if key not in current_index:
            rows.append({
                "name":     short_name(baseline_entry),
                "params":   params_display(baseline_entry),
                "baseline": format_score(baseline_entry),
                "current":  "—",
                "change":   "N/A",
                "status":   STATUS_MISSING,
                "sort_key": -1,
            })

    # Sort: regressions first (by magnitude), then improvements, then the rest.
    def row_sort(r):
        order = {
            STATUS_REGRESSION:  0,
            STATUS_IMPROVEMENT: 1,
            STATUS_NOISY:       2,
            STATUS_OK:          3,
            STATUS_NEW:         4,
            STATUS_MISSING:     5,
        }
        return (order.get(r["status"], 9), -r["sort_key"])

    rows.sort(key=row_sort)

    # ── Build Markdown ────────────────────────────────────────────────────────

    lines = []
    lines.append("## JMH Regression Report\n")

    lines.append("| | |")
    lines.append("|---|---|")
    lines.append(f"| **Baseline** | `{baseline_label}` |")
    lines.append(f"| **Current**  | `{current_label}` |")
    lines.append(f"| **Threshold** | {threshold:.0f}% |")
    lines.append(f"| **Benchmarks compared** | {compared_count} |")
    lines.append(f"| **Regressions detected** | {regression_count} |")
    lines.append("")

    lines.append("| Benchmark | Params | Baseline (ns/op) | Current (ns/op) | Change | Status |")
    lines.append("|---|---|---|---|---|---|")
    for r in rows:
        lines.append(
            f"| {r['name']} | {r['params']} | {r['baseline']} "
            f"| {r['current']} | {r['change']} | {r['status']} |"
        )

    lines.append("")
    if regression_count > 0:
        lines.append(
            f"> **{regression_count} regression(s) detected** above the {threshold:.0f}% "
            f"threshold with non-overlapping confidence intervals."
        )
    else:
        lines.append(
            f"> No regressions detected above the {threshold:.0f}% threshold."
        )

    report = "\n".join(lines) + "\n"
    exit_code = 1 if regression_count > 0 else 0
    return report, exit_code


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    baseline_label = args.baseline_label or Path(args.baseline).parent.name or Path(args.baseline).name
    current_label  = args.current_label  or Path(args.current).parent.name  or Path(args.current).name

    baseline_data = load_results(args.baseline)
    current_data  = load_results(args.current)

    report, exit_code = build_report(
        baseline_data,
        current_data,
        args.threshold,
        baseline_label,
        current_label,
    )

    print(report)

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(report)
        print(f"Report written to {args.output}", file=sys.stderr)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
