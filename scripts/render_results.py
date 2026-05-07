#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
import matplotlib.pyplot as plt


def _as_float(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def load_row(path: Path, scenario_name: str) -> dict[str, str]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        row = next(reader, None)
        if row is None:
            raise ValueError(f"No rows in CSV: {path}")

    status = "ok" if row.get("returncode") == "0" else "failed"
    total_seconds = _as_float(row.get("total_seconds"))
    peak_rss_human = row.get("peak_rss_human", "n/a")
    peak_rss_bytes = _as_float(row.get("peak_rss_bytes")) or 0.0

    return {
        "scenario": scenario_name,
        "status": status,
        "peak_rss_human": peak_rss_human,
        "peak_rss_bytes": f"{peak_rss_bytes}",
        "peak_rss_gib": f"{peak_rss_bytes / (1024 ** 3)}",
        "total_seconds": f"{total_seconds:.2f}" if total_seconds is not None else "n/a",
        "total_seconds_raw": "" if total_seconds is None else f"{total_seconds}",
    }


def render_markdown(rows: list[dict[str, str]], template_dir: Path, template_name: str, output_path: Path) -> None:
    env = Environment(loader=FileSystemLoader(template_dir.as_posix()), autoescape=False)
    template = env.get_template(template_name)
    output = template.render(rows=rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(output, encoding="utf-8")


def plot_metric(rows: list[dict[str, str]], value_key: str, title: str, ylabel: str, output_path: Path) -> None:
    labels = [row["scenario"] for row in rows]
    values: list[float] = []
    for row in rows:
        raw = row.get(value_key, "")
        try:
            values.append(float(raw))
        except ValueError:
            values.append(0.0)

    fig, ax = plt.subplots(figsize=(8, 4.5))
    bars = ax.bar(labels, values, color=["#2f6f5f", "#b45a3c"])
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.set_axisbelow(True)

    for bar, value in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2.0, bar.get_height(), f"{value:.2f}", ha="center", va="bottom", fontsize=9)

    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--icebug-csv", required=True)
    parser.add_argument("--graphframes-csv", required=True)
    parser.add_argument("--template", default="templates/results.md.j2")
    parser.add_argument("--results-md", default="docs/results.md")
    parser.add_argument("--assets-dir", default="docs/assets")
    args = parser.parse_args()

    icebug = load_row(Path(args.icebug_csv), "icebug")
    graphframes = load_row(Path(args.graphframes_csv), "graphframes")
    rows = [icebug, graphframes]

    template_path = Path(args.template)
    render_markdown(
        rows,
        template_dir=template_path.parent,
        template_name=template_path.name,
        output_path=Path(args.results_md),
    )

    assets_dir = Path(args.assets_dir)
    plot_metric(
        rows,
        value_key="peak_rss_gib",
        title="Memory Usage Comparison",
        ylabel="Peak RSS (GiB)",
        output_path=assets_dir / "memory_usage_comparison.png",
    )
    plot_metric(
        rows,
        value_key="total_seconds_raw",
        title="Runtime Comparison",
        ylabel="Total time (seconds)",
        output_path=assets_dir / "runtime_comparison.png",
    )


if __name__ == "__main__":
    main()
