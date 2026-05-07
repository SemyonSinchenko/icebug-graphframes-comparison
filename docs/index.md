# Icebug vs GraphFrames

This site presents a focused benchmark comparison between **icebug** and
**GraphFrames** on the LiveJournal graph dataset.

The benchmark data is stored in **icebug-format** (CSR arrays inside DuckDB).
icebug consumes this format directly.

GraphFrames does not natively consume icebug-format, so each GraphFrames run
includes a long **prepare** step that converts data to Parquet before the
algorithm starts.

The benchmark runs on a standard GitHub-hosted runner in CI with fixed memory
constraints, captures execution and memory metrics, and publishes reproducible
results.

| Component | Version | Notes |
|---|---|---|
| Runner | GitHub Actions `ubuntu-latest` | Standard GitHub-hosted Linux runner |
| Python | `3.12` | Installed via `actions/setup-python` |
| GraphFrames | `0.11.0` | `graphframes-py` package |
| icebug | `12.6` | Installed from `uv.lock` |
| Java | JDK `21` | Installed via `actions/setup-java` (Temurin) |

Go to the [Results](results.md) page for the latest generated table and plots.
