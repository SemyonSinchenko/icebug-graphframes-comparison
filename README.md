# GraphFrames vs icebug PageRank Memory Benchmark

This workspace benchmarks PageRank memory usage on the LiveJournal graph stored
as CSR arrays in `livejournal-csr.duckdb`.

## Data

The benchmark expects this DuckDB file in the repository root:

```text
livejournal-csr.duckdb
```

You can download it from [huggingface](https://huggingface.co/datasets/ladybugdb/livejournal-4m-35m/tree/main). Use xz to uncompress.

It contains:

```text
nodes: 3,997,962
edges: 69,362,378
directed: false
```

CSR tables used by the benchmark:

```text
livejournal_metadata
livejournal_indptr_edges
livejournal_indices_edges
livejournal_mapping_user
livejournal_nodes_user
```

## Environment

Python dependencies are managed with `uv`.

```bash
uv sync
```

GraphFrames also needs Java and Spark. The runs below used:

```text
Java: /usr/lib/jvm/java-21-openjdk-amd64
Spark: /home/ubuntu/comparison/spark-4.1.1-bin-hadoop3
GraphFrames Python package: graphframes-py 0.11.0
GraphFrames JVM package: io.graphframes:graphframes-spark4_2.13:0.11.0
PySpark: 4.1.1
icebug: 12.6
```

Set these before running GraphFrames:

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export SPARK_HOME=/home/ubuntu/comparison/spark-4.1.1-bin-hadoop3
export PATH="$SPARK_HOME/bin:$PATH"
```

## Benchmark Driver

The benchmark script is:

```text
benchmark_pagerank_memory.py
```

It launches each engine in a fresh child process and samples RSS for the whole
process tree. This is important for GraphFrames because most memory is held by
Spark's JVM, not the Python process.

Default PageRank settings:

```text
resetProbability = 0.15
tol = 0.01
maxIter = unset
```

For icebug/NetworKit, the damping factor is set to `1 - resetProbability`.

## Reproduce

### icebug

```bash
uv run python benchmark_pagerank_memory.py compare \
  --engine icebug \
  --output icebug-memory-results.csv
```

### GraphFrames, 16g driver heap

```bash
uv run python benchmark_pagerank_memory.py compare \
  --engine graphframes \
  --spark-driver-memory 16g \
  --output graphframes-memory-results.csv
```

### GraphFrames, 4g driver heap

```bash
uv run python benchmark_pagerank_memory.py compare \
  --engine graphframes \
  --spark-driver-memory 4g \
  --output graphframes-memory-4g-results.csv
```

### GraphFrames, 8g driver heap

```bash
uv run python benchmark_pagerank_memory.py compare \
  --engine graphframes \
  --spark-driver-memory 8g \
  --output graphframes-memory-8g-results.csv
```

## Results

All runs used the same graph and PageRank settings:

```text
resetProbability = 0.15
tol = 0.01
maxIter = unset
```

| Engine | Driver heap | Status | Peak RSS | Total time | PageRank time | Notes |
|---|---:|---|---:|---:|---:|---|
| icebug/NetworKit | n/a | ok | 3.32 GiB | 28.95s | 0.23s | converged in 1 iteration |
| GraphFrames | 16g | ok | 17.50 GiB | 231.75s | 218.37s | Spark local mode |
| GraphFrames | 8g | failed | 12.04 GiB | n/a | n/a | Java heap OOM |
| GraphFrames | 4g | failed | 12.04 GiB | n/a | n/a | Java heap OOM |

Successful run details:

| Engine | Load/prepare time | Build time | Nodes | Edges/result edges |
|---|---:|---:|---:|---:|
| icebug/NetworKit | 1.87s load | 26.85s | 3,997,962 | 69,362,378 |
| GraphFrames 16g | 7.16s prepare | 6.22s | 3,997,962 | 69,362,378 |

The lower-memory GraphFrames attempts failed with:

```text
java.lang.OutOfMemoryError: Java heap space
```

The OOM occurred during GraphFrames/GraphX edge partition construction and
PageRank startup, before PageRank results were produced.

## Output Files

Computed result CSVs:

```text
icebug-memory-results.csv
graphframes-memory-results.csv
graphframes-memory-4g-results.csv
graphframes-memory-8g-results.csv
```

The 16g GraphFrames run created temporary Parquet files under `/tmp` for the
converted vertices and edges. The benchmark cleans up those temporary files at
the end of the run unless `--keep-edge-parquet` is passed.

## Notes

GraphFrames does not consume the CSR arrays directly. The benchmark first
exports vertices and edges from the CSR DuckDB tables to Parquet, then loads
those Parquet files into Spark DataFrames and builds a `GraphFrame`.

icebug/NetworKit constructs the graph directly from the Arrow CSR buffers.
The benchmark keeps those Arrow buffers alive for the lifetime of the
NetworKit graph.
