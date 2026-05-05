#!/usr/bin/env python3
"""Compare PageRank memory usage for GraphFrames and icebug/NetworKit.

The parent process launches each benchmark in a fresh child process and samples
RSS for the full process tree. That matters for GraphFrames because most of the
memory is held by Spark's JVM, not the Python wrapper process.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any


DEFAULT_DB = "livejournal-csr.duckdb"
DEFAULT_SPARK_PACKAGE = "io.graphframes:graphframes-spark4_2.13:0.11.0"


def bytes_human(value: int | float) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    size = float(value)
    for unit in units:
        if abs(size) < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024
    raise AssertionError("unreachable")


def monitor_process_tree(proc: subprocess.Popen[str], interval: float) -> dict[str, Any]:
    import psutil

    root = psutil.Process(proc.pid)
    peak_rss = 0
    samples = 0
    stop = threading.Event()

    def sample_once() -> int:
        rss = 0
        processes = [root]
        try:
            processes.extend(root.children(recursive=True))
        except psutil.Error:
            pass

        for child in processes:
            try:
                rss += child.memory_info().rss
            except psutil.Error:
                continue
        return rss

    def loop() -> None:
        nonlocal peak_rss, samples
        while not stop.is_set():
            rss = sample_once()
            peak_rss = max(peak_rss, rss)
            samples += 1
            stop.wait(interval)

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    return {"stop": stop, "thread": thread, "peak": lambda: peak_rss, "samples": lambda: samples}


def run_child(mode: str, args: argparse.Namespace) -> dict[str, Any]:
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        mode,
        "--db",
        args.db,
        "--reset-probability",
        str(args.reset_probability),
        "--tol",
        str(args.tol),
    ]
    if args.max_iter is not None:
        cmd.extend(["--max-iter", str(args.max_iter)])
    if mode == "graphframes":
        cmd.extend(
            [
                "--spark-package",
                args.spark_package,
                "--spark-master",
                args.spark_master,
                "--spark-driver-memory",
                args.spark_driver_memory,
                "--spark-shuffle-partitions",
                str(args.spark_shuffle_partitions),
            ]
        )
        if args.edge_parquet:
            cmd.extend(["--edge-parquet", args.edge_parquet])
        if args.keep_edge_parquet:
            cmd.append("--keep-edge-parquet")

    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    proc = subprocess.Popen(
        cmd,
        cwd=Path(__file__).resolve().parent,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    monitor = monitor_process_tree(proc, args.sample_interval)

    output_lines: list[str] = []
    assert proc.stdout is not None
    for line in proc.stdout:
        print(f"[{mode}] {line}", end="")
        output_lines.append(line)

    returncode = proc.wait()
    monitor["stop"].set()
    monitor["thread"].join(timeout=2)
    peak_rss = monitor["peak"]()

    child_result: dict[str, Any] = {}
    for line in reversed(output_lines):
        if line.startswith("RESULT_JSON="):
            child_result = json.loads(line.removeprefix("RESULT_JSON=").strip())
            break

    result = {
        "engine": mode,
        "returncode": returncode,
        "peak_rss_bytes": peak_rss,
        "peak_rss_human": bytes_human(peak_rss),
        "samples": monitor["samples"](),
        **child_result,
    }
    if returncode != 0:
        result["error"] = "child process failed"
    return result


def load_csr(db_path: str) -> tuple[int, int, bool, Any, Any]:
    import duckdb
    import pyarrow as pa

    con = duckdb.connect(db_path, read_only=True)
    try:
        n_nodes, n_edges, directed = con.execute(
            "SELECT n_nodes, n_edges, directed FROM livejournal_metadata"
        ).fetchone()
        indptr = (
            con.execute("SELECT ptr FROM livejournal_indptr_edges ORDER BY rowid")
            .arrow()
            .read_all()["ptr"]
            .cast(pa.uint64())
        )
        indices = (
            con.execute("SELECT target FROM livejournal_indices_edges ORDER BY rowid")
            .arrow()
            .read_all()["target"]
            .cast(pa.uint64())
        )
    finally:
        con.close()
    return int(n_nodes), int(n_edges), bool(directed), indptr, indices


def run_icebug(args: argparse.Namespace) -> None:
    import networkit as nk

    t_start = time.time()
    n_nodes, n_edges, directed, indptr, indices = load_csr(args.db)
    t_loaded = time.time()

    # Keep Arrow buffers alive while the NetworKit graph references CSR memory.
    arrow_registry = {"indptr": indptr, "indices": indices}
    graph = nk.Graph.fromCSR(n_nodes, directed, indices, indptr)
    t_built = time.time()

    pr = nk.centrality.PageRank(
        graph,
        damp=1.0 - args.reset_probability,
        tol=args.tol,
    )
    if args.max_iter is not None:
        pr.maxIterations = args.max_iter
    pr.run()
    scores = pr.scores()
    t_done = time.time()

    result = {
        "n_nodes": n_nodes,
        "n_edges": n_edges,
        "directed": directed,
        "load_seconds": t_loaded - t_start,
        "build_seconds": t_built - t_loaded,
        "pagerank_seconds": t_done - t_built,
        "total_seconds": t_done - t_start,
        "iterations": pr.numberOfIterations(),
        "score_count": len(scores),
        "registry_items": len(arrow_registry),
    }
    print("RESULT_JSON=" + json.dumps(result, sort_keys=True))


def prepare_edges_parquet(db_path: str, parquet_dir: str) -> dict[str, Any]:
    import duckdb

    destination = Path(parquet_dir)
    destination.mkdir(parents=True, exist_ok=True)
    edge_path = destination / "edges.parquet"
    vertex_path = destination / "vertices.parquet"

    con = duckdb.connect(db_path, read_only=True)
    try:
        n_nodes, n_edges, directed = con.execute(
            "SELECT n_nodes, n_edges, directed FROM livejournal_metadata"
        ).fetchone()
        con.execute(
            f"""
            COPY (
                SELECT
                    row_number() OVER () - 1 AS id
                FROM range({int(n_nodes)})
            )
            TO '{vertex_path.as_posix()}'
            (FORMAT PARQUET)
            """
        )
        con.execute(
            f"""
            COPY (
                WITH ranges AS (
                    SELECT
                        rowid AS src,
                        ptr AS start_idx,
                        lead(ptr) OVER (ORDER BY rowid) AS stop_idx
                    FROM livejournal_indptr_edges
                )
                SELECT
                    ranges.src,
                    idx.target AS dst
                FROM ranges
                JOIN livejournal_indices_edges AS idx
                  ON idx.rowid >= ranges.start_idx
                 AND idx.rowid < ranges.stop_idx
                WHERE ranges.stop_idx IS NOT NULL
            )
            TO '{edge_path.as_posix()}'
            (FORMAT PARQUET)
            """
        )
    finally:
        con.close()

    return {
        "n_nodes": int(n_nodes),
        "n_edges": int(n_edges),
        "directed": bool(directed),
        "edge_path": edge_path.as_posix(),
        "vertex_path": vertex_path.as_posix(),
    }


def run_graphframes(args: argparse.Namespace) -> None:
    from graphframes import GraphFrame
    from pyspark.sql import SparkSession

    t_start = time.time()
    temp_dir: tempfile.TemporaryDirectory[str] | None = None
    if args.edge_parquet:
        parquet_dir = args.edge_parquet
    else:
        temp_dir = tempfile.TemporaryDirectory(prefix="livejournal-graphframes-")
        parquet_dir = temp_dir.name

    prep = prepare_edges_parquet(args.db, parquet_dir)
    t_prepared = time.time()

    spark_builder = (
        SparkSession.builder.appName("livejournal-graphframes-pagerank")
        .master(args.spark_master)
        .config("spark.driver.memory", args.spark_driver_memory)
        .config("spark.sql.shuffle.partitions", str(args.spark_shuffle_partitions))
        .config("spark.jars.packages", args.spark_package)
    )
    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        vertices = spark.read.parquet(prep["vertex_path"])
        edges = spark.read.parquet(prep["edge_path"])
        graph = GraphFrame(vertices, edges)
        t_built = time.time()

        pagerank_kwargs: dict[str, Any] = {
            "resetProbability": args.reset_probability,
            "tol": args.tol,
        }
        if args.max_iter is not None:
            pagerank_kwargs = {
                "resetProbability": args.reset_probability,
                "maxIter": args.max_iter,
            }
        results = graph.pageRank(**pagerank_kwargs)
        score_count = results.vertices.count()
        edge_count = results.edges.count()
        t_done = time.time()
    finally:
        spark.stop()
        if temp_dir is not None and not args.keep_edge_parquet:
            temp_dir.cleanup()

    result = {
        **prep,
        "prepare_seconds": t_prepared - t_start,
        "build_seconds": t_built - t_prepared,
        "pagerank_seconds": t_done - t_built,
        "total_seconds": t_done - t_start,
        "score_count": score_count,
        "result_edge_count": edge_count,
    }
    print("RESULT_JSON=" + json.dumps(result, sort_keys=True))


def write_csv(path: str, rows: list[dict[str, Any]]) -> None:
    keys: list[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def run_compare(args: argparse.Namespace) -> None:
    results = []
    for engine in args.engine:
        print("=" * 80)
        print(f"Running {engine}")
        results.append(run_child(engine, args))

    print("=" * 80)
    print("Summary")
    for result in results:
        status = "ok" if result["returncode"] == 0 else "failed"
        total = result.get("total_seconds")
        total_text = f"{total:.2f}s" if isinstance(total, (int, float)) else "n/a"
        print(
            f"{result['engine']:12} {status:6} "
            f"peak_rss={result['peak_rss_human']:>12} total={total_text}"
        )

    if args.output:
        write_csv(args.output, results)
        print(f"Wrote {args.output}")

    failures = [result for result in results if result["returncode"] != 0]
    if failures:
        raise SystemExit(1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--db", default=DEFAULT_DB)
        p.add_argument("--reset-probability", type=float, default=0.15)
        p.add_argument("--tol", type=float, default=0.01)
        p.add_argument("--max-iter", type=int)

    compare = subparsers.add_parser("compare")
    add_common(compare)
    compare.add_argument("--engine", action="append", choices=("icebug", "graphframes"))
    compare.add_argument("--sample-interval", type=float, default=0.1)
    compare.add_argument("--output", default="pagerank-memory-results.csv")
    compare.add_argument("--edge-parquet")
    compare.add_argument("--keep-edge-parquet", action="store_true")
    compare.add_argument("--spark-package", default=DEFAULT_SPARK_PACKAGE)
    compare.add_argument("--spark-master", default="local[*]")
    compare.add_argument("--spark-driver-memory", default="16g")
    compare.add_argument("--spark-shuffle-partitions", type=int, default=200)
    compare.set_defaults(func=run_compare)

    icebug = subparsers.add_parser("icebug")
    add_common(icebug)
    icebug.set_defaults(func=run_icebug)

    graphframes = subparsers.add_parser("graphframes")
    add_common(graphframes)
    graphframes.add_argument("--edge-parquet")
    graphframes.add_argument("--keep-edge-parquet", action="store_true")
    graphframes.add_argument("--spark-package", default=DEFAULT_SPARK_PACKAGE)
    graphframes.add_argument("--spark-master", default="local[*]")
    graphframes.add_argument("--spark-driver-memory", default="16g")
    graphframes.add_argument("--spark-shuffle-partitions", type=int, default=200)
    graphframes.set_defaults(func=run_graphframes)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "compare" and args.engine is None:
        args.engine = ["icebug", "graphframes"]
    args.func(args)


if __name__ == "__main__":
    main()
