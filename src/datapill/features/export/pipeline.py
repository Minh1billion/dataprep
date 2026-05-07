from __future__ import annotations

import json
from pathlib import Path
from typing import Any, AsyncGenerator

import polars as pl

from ...core.context import Context
from ...core.events import EventType, ProgressEvent
from ...storage.artifact_store import Artifact
from ..base import ExecutionPlan, Pipeline, ValidationResult
from ...utils.loader import load_dataframe

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
                summary = profile_result.get("summary", {})
                columns = profile_result.get("columns", [])
                correlations = profile_result.get("correlations", [])
                warnings = profile_result.get("warnings", [])

                col_types = summary.get("column_types", {})

                rows_html = ""
                for cp in columns:
                    null_pct = cp.get("null_pct", 0) or 0
                    null_color = "#d13212" if null_pct > 0.3 else ("#f89c24" if null_pct > 0.1 else "#687078")
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
                        warn_cell = f'<span style="color:#d13212;font-weight:500">{warn_count}</span>'
                    elif warn_count > 0:
                        warn_cell = f'<span style="color:#f89c24;font-weight:500">{warn_count}</span>'
                    else:
                        warn_cell = '<span style="color:#aab7b8">-</span>'

                    rows_html += f"""
                    <tr>
                        <td style="font-weight:500;color:#16191f">{cp.get("name","")}</td>
                        <td style="color:#687078">{cp.get("dtype_inferred", cp.get("dtype_physical",""))}</td>
                        <td style="color:{null_color}">{null_pct*100:.1f}%</td>
                        <td style="color:#687078">{cp.get("distinct_count","")}</td>
                        <td style="color:#687078">{mean_or_top}</td>
                        <td style="text-align:center">{warn_cell}</td>
                    </tr>"""

                corr_rows_html = ""
                for pair in sorted(correlations, key=lambda x: abs(x.get("value", 0)), reverse=True):
                    val = pair.get("value", 0)
                    val_color = "#d13212" if abs(val) >= 0.8 else ("#f89c24" if abs(val) >= 0.5 else "#16191f")
                    corr_rows_html += f"""
                    <tr>
                        <td>{pair.get("col_a","")}</td>
                        <td>{pair.get("col_b","")}</td>
                        <td style="color:#687078">{pair.get("method","")}</td>
                        <td style="color:{val_color};font-weight:500">{val:+.4f}</td>
                    </tr>"""

                warn_rows_html = ""
                sev_color = {"error": "#d13212", "warn": "#f89c24", "info": "#0073bb"}
                for sev in ("error", "warn", "info"):
                    for w in [x for x in warnings if x.get("severity") == sev]:
                        col = w.get("column", "")
                        col_label = col if col and col != "__dataset__" else "(dataset)"
                        warn_rows_html += f"""
                        <tr>
                            <td style="color:{sev_color.get(sev,'#687078')};font-weight:500;text-transform:uppercase;font-size:11px">{sev}</td>
                            <td style="color:#0073bb">{col_label}</td>
                            <td style="color:#687078">{w.get("code","")}</td>
                        </tr>"""

                html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>datapill profile - {parent.run_id}</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, "Segoe UI", sans-serif; font-size: 14px; color: #16191f; background: #f2f3f3; }}
  .page {{ max-width: 1080px; margin: 0 auto; padding: 32px 24px; }}
  h1 {{ font-size: 20px; font-weight: 600; color: #16191f; margin-bottom: 4px; }}
  .run-id {{ font-size: 12px; color: #687078; font-family: monospace; margin-bottom: 28px; }}
  .section {{ background: #fff; border: 1px solid #eaeded; border-radius: 4px; margin-bottom: 20px; }}
  .section-header {{ padding: 14px 20px; border-bottom: 1px solid #eaeded; font-weight: 600; font-size: 13px; color: #16191f; letter-spacing: 0.02em; }}
  .section-body {{ padding: 20px; }}
  .stat-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 16px; }}
  .stat {{ background: #f8f8f8; border: 1px solid #eaeded; border-radius: 4px; padding: 14px 16px; }}
  .stat-label {{ font-size: 11px; color: #687078; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 6px; }}
  .stat-value {{ font-size: 20px; font-weight: 600; color: #16191f; }}
  .stat-sub {{ font-size: 11px; color: #687078; margin-top: 2px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th {{ text-align: left; padding: 8px 12px; color: #687078; font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.04em; border-bottom: 2px solid #eaeded; }}
  td {{ padding: 9px 12px; border-bottom: 1px solid #f2f3f3; }}
  tr:last-child td {{ border-bottom: none; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 2px; font-size: 11px; font-weight: 500; }}
  .badge-numeric {{ background: #e8f4fb; color: #0073bb; }}
  .badge-categorical {{ background: #f3ebff; color: #6b3fd4; }}
  .badge-datetime {{ background: #eafaf1; color: #1d8348; }}
  .empty {{ color: #aab7b8; font-size: 13px; padding: 16px 0; }}
</style>
</head>
<body>
<div class="page">
  <h1>Profile Report</h1>
  <div class="run-id">run {parent.run_id} &nbsp;·&nbsp; parent {parent.parent_run_id or "-"}</div>

  <div class="section">
    <div class="section-header">Dataset</div>
    <div class="section-body">
      <div class="stat-grid">
        <div class="stat">
          <div class="stat-label">Rows</div>
          <div class="stat-value">{summary.get("n_rows", 0):,}</div>
        </div>
        <div class="stat">
          <div class="stat-label">Columns</div>
          <div class="stat-value">{summary.get("n_columns", 0)}</div>
        </div>
        <div class="stat">
          <div class="stat-label">Memory</div>
          <div class="stat-value">{summary.get("memory_mb", 0):.2f}</div>
          <div class="stat-sub">MB</div>
        </div>
        <div class="stat">
          <div class="stat-label">Null %</div>
          <div class="stat-value">{summary.get("total_null_pct", 0)*100:.2f}%</div>
        </div>
        <div class="stat">
          <div class="stat-label">Duplicate %</div>
          <div class="stat-value">{summary.get("duplicate_pct", 0)*100:.2f}%</div>
        </div>
        <div class="stat">
          <div class="stat-label">Column types</div>
          <div class="stat-value" style="font-size:13px;margin-top:4px">
            <span class="badge badge-numeric">{col_types.get("numeric", 0)} numeric</span><br style="margin:3px 0">
            <span class="badge badge-categorical" style="margin-top:4px">{col_types.get("categorical", 0)} categorical</span><br style="margin:3px 0">
            <span class="badge badge-datetime" style="margin-top:4px">{col_types.get("datetime", 0)} datetime</span>
          </div>
        </div>
      </div>
    </div>
  </div>

  <div class="section">
    <div class="section-header">Columns</div>
    <div class="section-body" style="padding:0">
      <table>
        <thead>
          <tr>
            <th>Column</th><th>Type</th><th>Null %</th><th>Distinct</th><th>Mean / top value</th><th style="text-align:center">Warns</th>
          </tr>
        </thead>
        <tbody>{rows_html if rows_html else f'<tr><td colspan="6" class="empty">no columns</td></tr>'}</tbody>
      </table>
    </div>
  </div>

  {'<div class="section"><div class="section-header">Correlations</div><div class="section-body" style="padding:0"><table><thead><tr><th>Col A</th><th>Col B</th><th>Method</th><th>r</th></tr></thead><tbody>' + corr_rows_html + '</tbody></table></div></div>' if correlations else ''}

  {'<div class="section"><div class="section-header">Warnings</div><div class="section-body" style="padding:0"><table><thead><tr><th>Severity</th><th>Column</th><th>Code</th></tr></thead><tbody>' + warn_rows_html + '</tbody></table></div></div>' if warnings else ''}

</div>
</body>
</html>"""
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