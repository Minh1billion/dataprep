import asyncio
from typing import Optional

import typer
from rich.text import Text

from ..features.export.pipeline import ExportPipeline, _DATA_FORMATS, _PROFILE_FORMATS
from ..storage.artifact_store import ArtifactStore
from ..core.context import Context
from .shared import (
    BOLD_WHITE,
    ORANGE,
    GRAY,
    WHITE,
    GREEN,
    RED,
    YELLOW,
    CYAN,
    _rule,
    err,
    out,
    print_run_summary,
    run_pipeline,
    with_spinner,
)

app = typer.Typer(help="export a pipeline artifact to a file")


def _fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


@app.command("run")
def run_export(
    parent_run_id: str = typer.Argument(help="run id of the artifact to export (ingest, preprocess, or profile)"),
    format: str = typer.Option(..., "--format", "-f", help=f"output format: {sorted(_DATA_FORMATS | _PROFILE_FORMATS)}"),
    output: str = typer.Option(..., "--output", "-o", help="output path (file or directory)"),
    delimiter: Optional[str] = typer.Option(None, "--delimiter", "-d", help="CSV delimiter character (default: ',')"),
    compression: Optional[str] = typer.Option(None, "--compression", help="parquet compression: snappy | gzip | zstd | lz4 | uncompressed (default: snappy)"),
    partition_by: Optional[str] = typer.Option(None, "--partition-by", help="parquet partition columns, comma-separated"),
    sheet_name: Optional[str] = typer.Option(None, "--sheet", help="excel sheet name (default: Sheet1)"),
    filename: Optional[str] = typer.Option(None, "--filename", help="output filename when --output is a directory"),
    store_path: str = typer.Option(".datapill", "--store", help="artifact store directory"),
) -> None:
    options: dict = {}

    if delimiter is not None:
        options["delimiter"] = delimiter
    if compression is not None:
        options["compression"] = compression
    if partition_by is not None:
        options["partition_by"] = partition_by.split(",")
    if sheet_name is not None:
        options["sheet_name"] = sheet_name
    if filename is not None:
        options["filename"] = filename

    artifact_store = ArtifactStore(store_path)
    context = Context(artifact_store=artifact_store)
    pipeline = ExportPipeline(
        parent_run_id=parent_run_id,
        format=format,
        output_path=output,
        options=options,
    )

    validation = pipeline.validate(context)
    if not validation.ok:
        for e in validation.errors:
            err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(e, style=RED))
        raise typer.Exit(1)

    exported_path: str = ""
    exported_size: int = 0

    def _on_done(event) -> None:
        nonlocal exported_path, exported_size
        if event.payload:
            exported_path = event.payload.get("path", "")
            exported_size = event.payload.get("size_bytes", 0)

    async def _run() -> None:
        plan = pipeline.plan(context)
        await run_pipeline(pipeline.execute(plan, context), on_done=_on_done)

        if context.artifact:
            art = context.artifact
            print_run_summary({"run_id": art.run_id, "ref": art.ref})

        if exported_path:
            _rule("output")
            size_str = _fmt_size(exported_size) if exported_size else ""
            out.print(
                Text("  path:   ", style=GRAY)
                + Text(exported_path, style=f"bold {ORANGE}")
            )
            if size_str:
                out.print(Text("  size:   ", style=GRAY) + Text(size_str, style=WHITE))
            out.print()

    asyncio.run(_run())


@app.command("formats")
def list_formats() -> None:
    """List supported export formats and their options."""
    from rich.table import Table, box

    _rule("data formats")
    dt = Table(
        box=box.SIMPLE,
        show_edge=False,
        header_style=BOLD_WHITE,
        padding=(0, 2),
        pad_edge=False,
    )
    dt.add_column("format", style=f"bold {CYAN}", no_wrap=True)
    dt.add_column("extension", style=GRAY, no_wrap=True)
    dt.add_column("options", style=WHITE)

    _data_info = [
        ("csv",     ".csv",     "--delimiter (default ,)"),
        ("parquet", ".parquet", "--compression snappy|gzip|zstd|lz4|uncompressed  --partition-by col1,col2"),
        ("json",    ".json",    "—"),
        ("jsonl",   ".jsonl",   "—"),
        ("excel",   ".xlsx",    "--sheet (default Sheet1)"),
    ]
    for fmt, ext, opts in _data_info:
        dt.add_row(fmt, ext, opts)
    out.print(dt)

    _rule("profile formats")
    pt = Table(
        box=box.SIMPLE,
        show_edge=False,
        header_style=BOLD_WHITE,
        padding=(0, 2),
        pad_edge=False,
    )
    pt.add_column("format", style=f"bold {CYAN}", no_wrap=True)
    pt.add_column("extension", style=GRAY, no_wrap=True)
    pt.add_column("notes", style=WHITE)
    pt.add_row("json", ".json", "raw profile result as JSON")
    pt.add_row("html", ".html", "standalone HTML report")
    out.print(pt)
    _rule()