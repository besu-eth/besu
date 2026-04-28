#!/usr/bin/env python3
"""
update_benchmark_index.py

Appends a new benchmark run entry to data/runs/index.json
inside the benchmark-results branch worktree.

Environment variables required:
    SHA               — the commit SHA of the benchmark run
    BENCHMARK_RESULTS — path to the benchmark-results worktree root
"""

import json
import os
import sys

sha = os.environ.get("SHA")
root = os.environ.get("BENCHMARK_RESULTS", "/tmp/benchmark-results")

if not sha:
    print("ERROR: SHA environment variable is required", file=sys.stderr)
    sys.exit(2)

index_path = os.path.join(root, "data", "runs", "index.json")
meta_path = os.path.join(root, "data", "runs", sha, "metadata.json")
results_path = os.path.join(root, "data", "runs", sha, "results.json")

runs = json.load(open(index_path)) if os.path.exists(index_path) else []

if any(r.get("sha") == sha for r in runs):
    print(f"SHA {sha} already in index, skipping.")
    sys.exit(0)

meta = json.load(open(meta_path))
data = json.load(open(results_path))

runs.append({
    "sha": meta["sha"],
    "ref": meta["ref"],
    "date": meta["date"],
    "run_id": meta["run_id"],
    "benchmark_filter": meta["benchmark_filter"],
    "runner_os": meta["runner_os"],
    "runner_arch": meta["runner_arch"],
    "benchmark_count": len(data),
})

json.dump(runs, open(index_path, "w"), indent=2)
print(f"Added run {sha} to index. Total runs: {len(runs)}")
