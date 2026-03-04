from __future__ import annotations

import sqlite3
from collections import Counter
from pathlib import Path


def _to_markdown_table(records: list[dict[str, object]], columns: list[str]) -> str:
    if not records:
        return "(no data)"
    header = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join(["---"] * len(columns)) + " |"
    rows = []
    for row in records:
        rows.append("| " + " | ".join(str(row.get(col, "")) for col in columns) + " |")
    return "\n".join([header, divider, *rows])


def generate_daily_digest(db_path: Path, out_path: Path) -> Path:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    incidents = [dict(row) for row in conn.execute("SELECT * FROM incidents").fetchall()]
    if not incidents:
        content = "# Daily Digest\n\nNo incidents found for the selected period.\n"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(content, encoding="utf-8")
        conn.close()
        return out_path

    try:
        import polars as pl

        frame = pl.DataFrame(incidents)
        top_rules = frame.group_by("rule").len().sort("len", descending=True).head(5).to_dicts()
        top_sources = frame.group_by("source").len().sort("len", descending=True).head(5).to_dicts()
        buckets = (
            frame.with_columns(pl.col("detected_at").str.slice(0, 13).alias("hour_bucket"))
            .group_by(["hour_bucket", "severity"])
            .len()
            .sort("len", descending=True)
            .head(5)
            .to_dicts()
        )
    except ModuleNotFoundError:
        # Fallback keeps CLI functional if polars is not installed in a local environment.
        top_rules_counter: Counter[str] = Counter()
        top_sources_counter: Counter[str] = Counter()
        buckets_counter: Counter[tuple[str, str]] = Counter()
        for row in incidents:
            top_rules_counter[str(row["rule"])] += 1
            top_sources_counter[str(row["source"])] += 1
            buckets_counter[(str(row["detected_at"])[:13], str(row["severity"]))] += 1
        top_rules = [{"rule": key, "len": count} for key, count in top_rules_counter.most_common(5)]
        top_sources = [{"source": key, "len": count} for key, count in top_sources_counter.most_common(5)]
        buckets = [
            {"hour_bucket": key[0], "severity": key[1], "len": count}
            for key, count in buckets_counter.most_common(5)
        ]

    content = "\n".join(
        [
            "# Daily Digest",
            "",
            "## Top Rules",
            _to_markdown_table(top_rules, ["rule", "len"]),
            "",
            "## Top Affected Sources",
            _to_markdown_table(top_sources, ["source", "len"]),
            "",
            "## Worst Time Buckets",
            _to_markdown_table(buckets, ["hour_bucket", "severity", "len"]),
            "",
        ]
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content, encoding="utf-8")
    conn.close()
    return out_path
