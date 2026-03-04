from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

if __package__ in (None, ""):
    # Allow running via: streamlit run src/realtime_monitor/dashboard.py
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from realtime_monitor.adapters.registry import ADAPTERS
from realtime_monitor.cli import run_microbatch
from realtime_monitor.config import MonitorConfig

DEMO_DATA_PATH = Path("examples/demo_events.jsonl")


def _make_config(db: Path, checkpoint: Path, digest: Path) -> MonitorConfig:
    cfg = MonitorConfig()
    cfg.sqlite_path = db
    cfg.checkpoint_file = checkpoint
    cfg.digest_path = digest
    cfg.data_dir = db.parent
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    return cfg


def _render_analytics(db: Path) -> None:
    import streamlit as st

    if not db.exists():
        st.info("No database found yet. Run ingestion first in the Ingest tab.")
        return

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row

    incident_count = conn.execute("SELECT COUNT(*) AS c FROM incidents").fetchone()["c"]
    open_count = conn.execute("SELECT COUNT(*) AS c FROM incidents WHERE status='open'").fetchone()["c"]
    metric_count = conn.execute("SELECT COUNT(*) AS c FROM rolling_metrics").fetchone()["c"]

    col1, col2, col3 = st.columns(3)
    col1.metric("Incidents", incident_count)
    col2.metric("Open Incidents", open_count)
    col3.metric("Metric Rows", metric_count)

    st.subheader("Incidents by Rule")
    rule_rows = conn.execute(
        "SELECT rule, COUNT(*) AS incident_count FROM incidents GROUP BY rule ORDER BY incident_count DESC"
    ).fetchall()
    if rule_rows:
        import pandas as pd

        df_rule = pd.DataFrame([dict(row) for row in rule_rows])
        st.bar_chart(df_rule.set_index("rule"))
        st.dataframe(df_rule, width="stretch")
    else:
        st.info("No incidents available.")

    st.subheader("Recent Incidents")
    recent = conn.execute(
        """
        SELECT incident_id, detected_at, rule, severity, source, status, evidence
        FROM incidents
        ORDER BY incident_id DESC
        LIMIT 100
        """
    ).fetchall()
    if recent:
        import pandas as pd

        st.dataframe(pd.DataFrame([dict(row) for row in recent]), width="stretch")

    st.subheader("Rolling Metrics (Latest 500)")
    metrics = conn.execute(
        """
        SELECT batch_id, volume, unique_users, error_rate, duplicate_ratio, null_ratio
        FROM rolling_metrics
        ORDER BY batch_id DESC
        LIMIT 500
        """
    ).fetchall()
    if metrics:
        import pandas as pd

        df_metrics = pd.DataFrame([dict(row) for row in reversed(metrics)])
        st.line_chart(df_metrics.set_index("batch_id")[["error_rate", "duplicate_ratio", "null_ratio"]])
        st.line_chart(df_metrics.set_index("batch_id")[["volume", "unique_users"]])

    conn.close()


def _ensure_jsonl_input(path: Path) -> Path:
    if path.suffix.lower() != ".json":
        return path
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        raise ValueError("Input JSON file is empty.")

    records: list[object]
    try:
        raw = json.loads(text)
        if isinstance(raw, dict):
            records = [raw]
        elif isinstance(raw, list):
            records = raw
        else:
            raise ValueError("JSON input must be an object or an array of objects.")
    except json.JSONDecodeError as err:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines:
            raise ValueError("Input JSON file has no valid JSON records.") from err
        records = [json.loads(line) for line in lines]

    out = path.with_suffix(".jsonl")
    lines = [json.dumps(record) for record in records]
    out.write_text("\n".join(lines), encoding="utf-8")
    return out


def _run_ingestion(
    *,
    db_path: Path,
    checkpoint_path: Path,
    digest_path: Path,
    input_path: Path,
    batch_size: int,
    adapter_name: str,
    max_batches: int,
    sleep_seconds: float,
    min_volume: int,
    dup_ratio: float,
    null_ratio: float,
    drift_ratio: float,
    reset_state: bool,
) -> None:
    cfg = _make_config(db_path, checkpoint_path, digest_path)
    cfg.batch_size = int(batch_size)
    cfg.adapter_name = adapter_name
    cfg.max_batches = int(max_batches) if int(max_batches) > 0 else None
    cfg.sleep_seconds_per_batch = float(sleep_seconds)
    cfg.min_volume_threshold = int(min_volume)
    cfg.duplicate_ratio_threshold = float(dup_ratio)
    cfg.null_ratio_threshold = float(null_ratio)
    cfg.schema_drift_threshold = float(drift_ratio)

    if reset_state:
        for path in [cfg.sqlite_path, cfg.checkpoint_file, cfg.digest_path]:
            if path.exists():
                path.unlink()

    run_microbatch(cfg, input_path)


def main() -> None:
    try:
        import streamlit as st
    except ModuleNotFoundError as exc:
        raise SystemExit("Streamlit is not installed. Run: python3 -m pip install -e .[ui]") from exc

    st.set_page_config(page_title="Realtime DQ Monitor", layout="wide")
    st.title("Realtime DQ Monitor Dashboard")

    db_path = Path(st.sidebar.text_input("SQLite DB Path", value="data/incidents.db"))
    checkpoint_path = Path(st.sidebar.text_input("Checkpoint Path", value="data/checkpoint.json"))
    digest_path = Path(st.sidebar.text_input("Digest Path", value="data/daily_digest.md"))

    ingest_tab, analytics_tab = st.tabs(["Ingest", "Analytics"])

    with ingest_tab:
        st.subheader("Ingest Dataset")
        uploaded = st.file_uploader("Upload events file", type=["jsonl", "json", "txt"])
        local_path = st.text_input(
            "Or use existing local events path (.jsonl or .json)",
            value=str(DEMO_DATA_PATH),
        )
        if DEMO_DATA_PATH.exists():
            st.caption(f"Demo dataset available: {DEMO_DATA_PATH}")

        st.markdown("### Runtime Settings")
        c1, c2, c3 = st.columns(3)
        batch_size = c1.number_input("Batch size", min_value=1, value=50)
        max_batches = c2.number_input("Max batches (0 = all)", min_value=0, value=0)
        sleep_seconds = c3.number_input("Sleep between batches", min_value=0.0, value=0.0, step=0.1)
        adapter_name = st.selectbox("Adapter", options=sorted(ADAPTERS.keys()), index=0)

        st.markdown("### Thresholds")
        t1, t2, t3, t4 = st.columns(4)
        min_volume = t1.number_input("Min volume", min_value=0, value=5)
        dup_ratio = t2.number_input("Duplicate ratio", min_value=0.0, value=0.15, step=0.001, format="%.3f")
        null_ratio = t3.number_input("Null ratio", min_value=0.0, value=0.20, step=0.001, format="%.3f")
        drift_ratio = t4.number_input("Schema drift ratio", min_value=0.0, value=0.05, step=0.001, format="%.3f")

        reset_state = st.checkbox("Reset DB/checkpoint before run", value=False)
        demo_col, run_col = st.columns(2)

        if demo_col.button("Load Demo Data"):
            if not DEMO_DATA_PATH.exists():
                st.error(f"Demo dataset not found at {DEMO_DATA_PATH}.")
            else:
                with st.spinner("Running demo ingestion..."):
                    try:
                        _run_ingestion(
                            db_path=db_path,
                            checkpoint_path=checkpoint_path,
                            digest_path=digest_path,
                            input_path=DEMO_DATA_PATH,
                            batch_size=int(batch_size),
                            adapter_name=adapter_name,
                            max_batches=int(max_batches),
                            sleep_seconds=float(sleep_seconds),
                            min_volume=int(min_volume),
                            dup_ratio=float(dup_ratio),
                            null_ratio=float(null_ratio),
                            drift_ratio=float(drift_ratio),
                            reset_state=reset_state,
                        )
                        st.success(f"Ingestion completed for: {DEMO_DATA_PATH}")
                    except Exception as exc:  # noqa: BLE001
                        st.exception(exc)

        if run_col.button("Run Ingestion", type="primary"):
            input_path: Path | None = None
            if uploaded is not None:
                uploads_dir = Path("data/uploads")
                uploads_dir.mkdir(parents=True, exist_ok=True)
                input_path = uploads_dir / uploaded.name
                input_path.write_bytes(uploaded.getvalue())
                input_path = _ensure_jsonl_input(input_path)
            else:
                candidate = Path(local_path)
                if candidate.exists():
                    input_path = _ensure_jsonl_input(candidate)

            if input_path is None:
                st.error("Provide a dataset by upload or valid local path.")
            else:
                with st.spinner("Running micro-batch ingestion..."):
                    try:
                        _run_ingestion(
                            db_path=db_path,
                            checkpoint_path=checkpoint_path,
                            digest_path=digest_path,
                            input_path=input_path,
                            batch_size=int(batch_size),
                            adapter_name=adapter_name,
                            max_batches=int(max_batches),
                            sleep_seconds=float(sleep_seconds),
                            min_volume=int(min_volume),
                            dup_ratio=float(dup_ratio),
                            null_ratio=float(null_ratio),
                            drift_ratio=float(drift_ratio),
                            reset_state=reset_state,
                        )
                        st.success(f"Ingestion completed for: {input_path}")
                    except Exception as exc:  # noqa: BLE001
                        st.exception(exc)

    with analytics_tab:
        _render_analytics(db_path)


if __name__ == "__main__":
    main()
