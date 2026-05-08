import json
from pathlib import Path
from typing import Any, AsyncGenerator

import polars as pl

from ...core.context import Context
from ...core.events import EventType, ProgressEvent
from ...storage.artifact_store import Artifact
from ..base import ExecutionPlan, Pipeline, ValidationResult
from ...utils.loader import load_dataframe
from .template import PROFILE_HTML_TEMPLATE

_VALID_PARENTS = {"ingest", "preprocess", "profile"}

_DATA_FORMATS = {"csv", "parquet", "json", "jsonl", "excel"}
_PROFILE_FORMATS = {"json", "html"}

_FORMAT_EXT = {
    "csv": "csv",
    "parquet": "parquet",
    "json": "json",
    "jsonl": "jsonl",
    "excel": "xlsx",
    "html": "html",
}


def _build_profile_html(profile_result: dict[str, Any], parent: Any) -> str:
    summary = profile_result.get("summary", {})
    columns = profile_result.get("columns", [])
    correlations = profile_result.get("correlations", [])
    warnings = profile_result.get("warnings", [])
    col_types = summary.get("column_types", {})

    rows_html = ""
    for cp in columns:
        null_pct = cp.get("null_pct", 0) or 0
        null_color = (
            "var(--red)" if null_pct > 0.3
            else ("var(--yellow)" if null_pct > 0.1 else "var(--text-muted)")
        )
        mean_or_top = ""
        if cp.get("mean") is not None:
            try:
                mean_or_top = f"{float(cp['mean']):.4g}"
            except (TypeError, ValueError):
                mean_or_top = str(cp["mean"])
        elif cp.get("top_values"):
            top = cp["top_values"]
            if top:
                mean_or_top = str(top[0].get("value", ""))[:40]

        col_warns = cp.get("warnings", [])
        warn_count = len(col_warns)
        if any(w.get("severity") == "error" for w in col_warns):
            warn_cell = f'<span class="badge badge-error">{warn_count}</span>'
        elif warn_count > 0:
            warn_cell = f'<span class="badge badge-warn">{warn_count}</span>'
        else:
            warn_cell = '<span class="badge badge-ok">·</span>'

        dtype_label = cp.get("dtype_inferred", cp.get("dtype_physical", ""))
        null_bar_w = round(null_pct * 100, 1)

        rows_html += f"""
                    <tr class="col-row">
                        <td class="col-name">{cp.get("name","")}</td>
                        <td><span class="dtype-chip">{dtype_label}</span></td>
                        <td>
                            <div class="null-bar-wrap">
                                <div class="null-bar-fill" style="width:{null_bar_w}%;background:{null_color}"></div>
                                <span style="color:{null_color}">{null_pct*100:.1f}%</span>
                            </div>
                        </td>
                        <td class="num">{cp.get("distinct_count","")}</td>
                        <td class="num">{mean_or_top}</td>
                        <td style="text-align:center">{warn_cell}</td>
                    </tr>"""

    corr_rows_html = ""
    corr_chart_labels = []
    corr_chart_values = []
    corr_chart_colors = []
    for pair in sorted(correlations, key=lambda x: abs(x.get("value", 0)), reverse=True):
        val = pair.get("value", 0)
        val_color = (
            "var(--red)" if abs(val) >= 0.8
            else ("var(--yellow)" if abs(val) >= 0.5 else "var(--text)")
        )
        corr_rows_html += f"""
                    <tr>
                        <td class="col-name">{pair.get("col_a","")}</td>
                        <td class="col-name">{pair.get("col_b","")}</td>
                        <td><span class="dtype-chip">{pair.get("method","")}</span></td>
                        <td class="num" style="color:{val_color};font-weight:600">{val:+.4f}</td>
                    </tr>"""
        corr_chart_labels.append(f"{pair.get('col_a','')} × {pair.get('col_b','')}")
        corr_chart_values.append(round(val, 4))
        alpha = min(abs(val) + 0.25, 1.0)
        if val >= 0:
            corr_chart_colors.append(f"rgba(220,38,38,{alpha:.2f})")
        else:
            corr_chart_colors.append(f"rgba(79,70,229,{alpha:.2f})")

    warn_rows_html = ""
    sev_class = {"error": "badge-error", "warn": "badge-warn", "info": "badge-info"}
    for sev in ("error", "warn", "info"):
        for w in [x for x in warnings if x.get("severity") == sev]:
            col = w.get("column", "")
            col_label = col if col and col != "__dataset__" else "(dataset)"
            warn_rows_html += f"""
                        <tr>
                            <td><span class="badge {sev_class.get(sev,'')}">{sev.upper()}</span></td>
                            <td class="col-name">{col_label}</td>
                            <td style="color:var(--text-muted)">{w.get("code","")}</td>
                        </tr>"""

    hist_entries = []
    for nc in columns:
        if nc.get("dtype_inferred") != "numeric":
            continue
        hist = nc.get("histogram")
        if not hist or not isinstance(hist, dict):
            continue
        bins = hist.get("bins", [])
        counts = hist.get("counts", [])
        if not bins or not counts:
            continue
        labels = [f"{b:.3g}" for b in bins[:-1]] if len(bins) > 1 else [str(b) for b in bins]
        hist_entries.append({"name": nc.get("name", ""), "labels": labels, "counts": counts})
        if len(hist_entries) >= 8:
            break

    corr_section_html = ""
    if correlations:
        corr_section_html = f"""
  <div id="correlations" class="section">
    <div class="section-title">Correlations ({len(correlations)} pairs)</div>
    <div class="chart-card anim anim-3" style="margin-bottom:20px">
      <div class="chart-card-title">Correlation strength (top pairs)</div>
      <div class="chart-wrap" style="height:max(160px, min({len(corr_chart_labels[:15])*28}px, 340px))">
        <canvas id="corrChart"></canvas>
      </div>
    </div>
    <div class="table-wrap anim">
      <table>
        <thead><tr><th>Col A</th><th>Col B</th><th>Method</th><th style="text-align:right">r</th></tr></thead>
        <tbody>{corr_rows_html}</tbody>
      </table>
    </div>
  </div>"""

    warn_section_html = ""
    if warnings:
        warn_section_html = f"""
  <div id="warnings" class="section">
    <div class="section-title">Warnings ({len(warnings)})</div>
    <div class="table-wrap anim">
      <table>
        <thead><tr><th>Severity</th><th>Column</th><th>Code</th></tr></thead>
        <tbody>{warn_rows_html}</tbody>
      </table>
    </div>
  </div>"""

    n_errors = sum(1 for w in warnings if w.get("severity") == "error")
    n_warns  = sum(1 for w in warnings if w.get("severity") == "warn")

    warn_value_color = (
        "var(--red)" if n_errors
        else ("var(--yellow)" if n_warns else "var(--green)")
    )
    null_total_color = (
        "var(--red)" if (summary.get("total_null_pct", 0) or 0) > 0.3
        else "var(--text)"
    )

    corr_nav = "<a class='nav-item' href='#correlations'><span class='nav-dot'></span>Correlations</a>" if correlations else ""
    warn_nav = "<a class='nav-item' href='#warnings'><span class='nav-dot'></span>Warnings</a>" if warnings else ""

    rows_html_final = rows_html if rows_html else '<tr><td colspan="6" class="empty">no columns</td></tr>'

    return PROFILE_HTML_TEMPLATE.format(
        run_id=parent.run_id,
        parent_run_id=parent.parent_run_id or "-",
        corr_nav=corr_nav,
        warn_nav=warn_nav,
        n_rows=f"{summary.get('n_rows', 0):,}",
        n_columns=summary.get("n_columns", 0),
        n_columns_label=len(columns),
        memory_mb=f"{summary.get('memory_mb', 0):.1f}",
        null_total_color=null_total_color,
        total_null_pct=f"{(summary.get('total_null_pct', 0) or 0)*100:.1f}%",
        duplicate_pct=f"{(summary.get('duplicate_pct', 0) or 0)*100:.1f}%",
        warn_value_color=warn_value_color,
        n_warnings=len(warnings),
        n_errors=n_errors,
        n_warns=n_warns,
        rows_html=rows_html_final,
        corr_section_html=corr_section_html,
        warn_section_html=warn_section_html,
        null_chart_labels=json.dumps([c.get("name", "") for c in columns]),
        null_chart_values=json.dumps([round((c.get("null_pct", 0) or 0) * 100, 2) for c in columns]),
        col_type_labels=json.dumps(["numeric", "categorical", "datetime", "other"]),
        col_type_values=json.dumps([
            col_types.get("numeric", 0),
            col_types.get("categorical", 0),
            col_types.get("datetime", 0),
            col_types.get("other", 0),
        ]),
        hist_data_js=json.dumps(hist_entries, default=str),
        corr_labels_js=json.dumps(corr_chart_labels[:15]),
        corr_values_js=json.dumps(corr_chart_values[:15]),
        corr_colors_js=json.dumps(corr_chart_colors[:15]),
    )


class ExportPipeline(Pipeline):
    def __init__(
        self,
        parent_run_id: str,
        format: str,
        output_path: str,
        options: dict[str, Any] | None = None,
    ) -> None:
        self.parent_run_id = parent_run_id
        self.format = format
        self.output_path = output_path
        self.options = options or {}

    def validate(self, context: Context) -> ValidationResult:
        errors: list[str] = []

        parent = context.artifact_store.get(self.parent_run_id)
        if parent is None:
            errors.append(f"artifact not found: {self.parent_run_id!r}")
            return ValidationResult(ok=False, errors=errors)

        if parent.pipeline not in _VALID_PARENTS:
            errors.append(
                f"export cannot accept input from {parent.pipeline!r}, "
                f"expected one of: {sorted(_VALID_PARENTS)}"
            )
            return ValidationResult(ok=False, errors=errors)

        if parent.pipeline == "profile":
            if self.format not in _PROFILE_FORMATS:
                errors.append(
                    f"profile artifact only supports formats: {sorted(_PROFILE_FORMATS)}, "
                    f"got {self.format!r}"
                )
        else:
            if self.format not in _DATA_FORMATS:
                errors.append(
                    f"format {self.format!r} not supported, "
                    f"expected one of: {sorted(_DATA_FORMATS)}"
                )

        p = Path(self.output_path)
        if p.suffix and p.suffix != f".{_FORMAT_EXT.get(self.format, self.format)}":
            errors.append(
                f"output_path extension {p.suffix!r} does not match format {self.format!r}"
            )

        if self.format == "csv":
            delimiter = self.options.get("delimiter", ",")
            if not isinstance(delimiter, str) or len(delimiter) != 1:
                errors.append("delimiter must be a single character string")

        if self.format == "parquet":
            compression = self.options.get("compression", "snappy")
            if compression not in ("snappy", "gzip", "zstd", "lz4", "uncompressed"):
                errors.append(f"unsupported parquet compression: {compression!r}")

        return ValidationResult(ok=not errors, errors=errors)

    def plan(self, context: Context) -> ExecutionPlan:
        parent = context.artifact_store.get(self.parent_run_id)

        if parent and parent.pipeline == "profile":
            load_mode = "profile_json" if (parent.materialized and parent.path) else "profile_options"
        else:
            load_mode = "parquet" if (parent and parent.materialized) else "connector"

        p = Path(self.output_path)
        if p.suffix:
            resolved_path = str(p)
        else:
            filename = self.options.get("filename") or f"{parent.pipeline}_{self.parent_run_id}.{_FORMAT_EXT[self.format]}"
            resolved_path = str(p / filename)

        steps: list[dict[str, Any]] = [
            {"action": "load_data", "mode": load_mode, "parent_run_id": self.parent_run_id},
            {"action": "write", "format": self.format, "output_path": resolved_path},
        ]

        return ExecutionPlan(
            steps=steps,
            metadata={
                "parent_run_id": self.parent_run_id,
                "format": self.format,
                "load_mode": load_mode,
                "output_path": resolved_path,
                "options": self.options,
            },
        )

    async def execute(
        self, plan: ExecutionPlan, context: Context
    ) -> AsyncGenerator[ProgressEvent, None]:
        parent = context.artifact_store.get(self.parent_run_id)
        artifact = Artifact.new(
            pipeline="export",
            parent=parent,
            options={
                "format": self.format,
                "output_path": plan.metadata["output_path"],
                **self.options,
            },
            is_sample=parent.is_sample if parent else False,
            sample_size=parent.sample_size if parent else None,
        )

        resolved_path = Path(plan.metadata["output_path"])

        yield ProgressEvent(event_type=EventType.STARTED, message="loading artifact data")

        if parent.pipeline == "profile":
            profile_result: dict[str, Any] | None = None

            if parent.materialized and parent.path:
                abs_path = context.artifact_store.path / parent.path
                if abs_path.exists():
                    profile_result = json.loads(abs_path.read_text())

            if profile_result is None and "profile_summary" in parent.options:
                profile_result = parent.options["profile_summary"]

            if profile_result is None:
                yield ProgressEvent(
                    event_type=EventType.ERROR,
                    message="profile artifact has no data - run with mode=full or ensure profile_summary exists",
                )
                return

            yield ProgressEvent(
                event_type=EventType.PROGRESS,
                message="loaded profile result",
                progress_pct=30.0,
            )

            resolved_path.parent.mkdir(parents=True, exist_ok=True)

            if self.format == "json":
                resolved_path.write_text(json.dumps(profile_result, indent=2, default=str))

            elif self.format == "html":
                html = _build_profile_html(profile_result, parent)
                resolved_path.write_text(html, encoding="utf-8")

        else:
            try:
                df = await load_dataframe(parent, context)
            except Exception as exc:
                yield ProgressEvent(event_type=EventType.ERROR, message=str(exc))
                return

            yield ProgressEvent(
                event_type=EventType.PROGRESS,
                message=f"loaded {len(df):,} rows, {len(df.columns)} columns",
                progress_pct=30.0,
                payload={"rows": len(df), "columns": len(df.columns)},
            )

            artifact.schema = {c: str(df[c].dtype) for c in df.columns}
            resolved_path.parent.mkdir(parents=True, exist_ok=True)

            if self.format == "csv":
                df.write_csv(resolved_path, separator=self.options.get("delimiter", ","))

            elif self.format == "parquet":
                compression = self.options.get("compression", "snappy")
                partition_by = self.options.get("partition_by")
                if partition_by:
                    df.write_parquet(
                        resolved_path,
                        compression=compression,
                        use_pyarrow=True,
                        pyarrow_options={"partition_cols": partition_by},
                    )
                else:
                    df.write_parquet(resolved_path, compression=compression)

            elif self.format == "json":
                df.write_json(resolved_path)

            elif self.format == "jsonl":
                df.write_ndjson(resolved_path)

            elif self.format == "excel":
                df.write_excel(
                    resolved_path,
                    worksheet=self.options.get("sheet_name", "Sheet1"),
                )

        size_bytes = resolved_path.stat().st_size if resolved_path.exists() else 0

        yield ProgressEvent(
            event_type=EventType.PROGRESS,
            message=f"written to {resolved_path}",
            progress_pct=90.0,
            payload={"path": str(resolved_path), "size_bytes": size_bytes},
        )

        artifact.materialized = True
        artifact.path = str(resolved_path)

        context.artifact_store.save(artifact)
        context.artifact = artifact

        yield ProgressEvent(
            event_type=EventType.DONE,
            message="export complete",
            progress_pct=100.0,
            payload={
                "run_id": artifact.run_id,
                "ref": artifact.ref,
                "format": self.format,
                "path": str(resolved_path),
                "size_bytes": size_bytes,
            },
        )

    def serialize(self) -> dict[str, Any]:
        return {
            "pipeline": "export",
            "version": "1.0",
            "parent_run_id": self.parent_run_id,
            "format": self.format,
            "output_path": self.output_path,
            "options": self.options,
            "schema": {"input": "data | profile", "output": "file"},
            "capabilities": ["csv", "parquet", "json", "jsonl", "excel", "html"],
        }