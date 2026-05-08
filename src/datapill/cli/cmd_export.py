import asyncio
from pathlib import Path
from typing import Optional

import typer
from rich.prompt import Prompt, Confirm
from rich.table import Table, box
from rich.text import Text

from ..features.export.pipeline import ExportPipeline, _DATA_FORMATS, _PROFILE_FORMATS, _FORMAT_EXT
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

_ALL_FORMATS = sorted(_DATA_FORMATS | _PROFILE_FORMATS)


_FORMAT_EXT_MAP = {
    "csv": ".csv", "parquet": ".parquet", "json": ".json",
    "jsonl": ".jsonl", "excel": ".xlsx", "html": ".html",
}


def _fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _pick_artifact(store: ArtifactStore) -> str:
    artifacts = store.list()
    if not artifacts:
        err.print(Text("[FAIL] ", style=f"bold {RED}") + Text("no artifacts found in store", style=RED))
        raise typer.Exit(1)

    _rule("available artifacts")
    tbl = Table(box=box.SIMPLE, show_edge=False, header_style=BOLD_WHITE, padding=(0, 2), pad_edge=False)
    tbl.add_column("#", style=GRAY, no_wrap=True)
    tbl.add_column("run_id", style=f"bold {CYAN}", no_wrap=True)
    tbl.add_column("pipeline", style=WHITE, no_wrap=True)
    tbl.add_column("ref", style=GRAY)
    tbl.add_column("created", style=GRAY)

    for i, art in enumerate(artifacts, 1):
        tbl.add_row(
            str(i),
            art.run_id,
            art.pipeline,
            art.ref or "-",
            str(art.created_at)[:19] if hasattr(art, "created_at") else "-",
        )
    out.print(tbl)

    raw = Prompt.ask(
        Text("  select artifact", style=GRAY)
        .append(" (#, run_id, or ref)", style=GRAY)
    )

    for i, art in enumerate(artifacts, 1):
        if raw == str(i) or raw == art.run_id or raw == art.ref:
            return art.run_id

    err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"no match for {raw!r}", style=RED))
    raise typer.Exit(1)


def _pick_format(is_profile: bool) -> str:
    available = sorted(_PROFILE_FORMATS if is_profile else _DATA_FORMATS)
    out.print()
    out.print(Text("  formats: ", style=GRAY) + Text("  ".join(available), style=f"bold {CYAN}"))
    fmt = Prompt.ask(Text("  format", style=GRAY))
    if fmt not in available:
        close = [f for f in available if f.startswith(fmt[:2])]
        hint = f"  did you mean: {close[0]!r}?" if close else f"  valid: {available}"
        err.print(Text(f"[FAIL] unknown format {fmt!r}", style=RED))
        err.print(Text(hint, style=GRAY))
        raise typer.Exit(1)
    return fmt


def _default_output(pipeline_name: str, run_id: str, fmt: str) -> str:
    ext = _FORMAT_EXT_MAP.get(fmt, fmt)
    return f"./{pipeline_name}_{run_id[:8]}{ext}"


def _ask_format_options(fmt: str) -> dict:
    options: dict = {}
    if fmt == "csv":
        delim = Prompt.ask(Text("  delimiter", style=GRAY), default=",")
        if delim != ",":
            options["delimiter"] = delim
    elif fmt == "parquet":
        comp = Prompt.ask(Text("  compression", style=GRAY), default="snappy",
                          choices=["snappy", "gzip", "zstd", "lz4", "uncompressed"])
        if comp != "snappy":
            options["compression"] = comp
        pb = Prompt.ask(Text("  partition-by columns (comma-separated, or Enter to skip)", style=GRAY), default="")
        if pb.strip():
            options["partition_by"] = [c.strip() for c in pb.split(",")]
    elif fmt == "excel":
        sheet = Prompt.ask(Text("  sheet name", style=GRAY), default="Sheet1")
        if sheet != "Sheet1":
            options["sheet_name"] = sheet
    return options


@app.command("run")
def run_export(
    parent_run_id: Optional[str] = typer.Argument(default=None, help="run id of the artifact to export"),
    format: Optional[str] = typer.Option(None, "--format", "-f", help=f"output format: {_ALL_FORMATS}"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="output path (file or directory)"),
    delimiter: Optional[str] = typer.Option(None, "--delimiter", "-d", help="CSV delimiter (default ',')"),
    compression: Optional[str] = typer.Option(None, "--compression", help="parquet compression (default: snappy)"),
    partition_by: Optional[str] = typer.Option(None, "--partition-by", help="parquet partition columns, comma-separated"),
    sheet_name: Optional[str] = typer.Option(None, "--sheet", help="excel sheet name (default: Sheet1)"),
    filename: Optional[str] = typer.Option(None, "--filename", help="output filename when --output is a directory"),
    store_path: str = typer.Option(".datapill", "--store", help="artifact store directory"),
    interactive: bool = typer.Option(True, "--interactive/--no-interactive", "-i/-I", help="prompt for missing values"),
) -> None:
    artifact_store = ArtifactStore(store_path)
    context = Context(artifact_store=artifact_store)

    if parent_run_id is None:
        if not interactive:
            err.print(Text("[FAIL] ", style=f"bold {RED}") + Text("parent_run_id is required (or use -i)", style=RED))
            raise typer.Exit(1)
        parent_run_id = _pick_artifact(artifact_store)

    parent = artifact_store.get(parent_run_id)
    if parent is None:
        err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"artifact not found: {parent_run_id!r}", style=RED))
        if interactive:
            if Confirm.ask(Text("  pick from available artifacts?", style=GRAY)):
                parent_run_id = _pick_artifact(artifact_store)
                parent = artifact_store.get(parent_run_id)
        if parent is None:
            raise typer.Exit(1)

    is_profile = parent.pipeline == "profile"

    if format is None:
        if not interactive:
            err.print(Text("[FAIL] ", style=f"bold {RED}") + Text("--format is required (or use -i)", style=RED))
            raise typer.Exit(1)
        format = _pick_format(is_profile)

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

    if interactive and not options and format in ("csv", "parquet", "excel"):
        if Confirm.ask(Text(f"  configure {format} options?", style=GRAY), default=False):
            options.update(_ask_format_options(format))

    if output is None:
        default_out = _default_output(parent.pipeline, parent_run_id, format)
        if interactive:
            output = Prompt.ask(Text("  output path", style=GRAY), default=default_out)
        else:
            output = default_out
            out.print(Text("  output:  ", style=GRAY) + Text(output, style=ORANGE))

    pipeline = ExportPipeline(
        parent_run_id=parent_run_id,
        format=format,
        output_path=output,
        options=options,
    )

    validation = pipeline.validate(context)
    if not validation.ok:
        for e in validation.errors:
            ext_hint = ""
            if "extension" in e and "does not match" in e:
                expected = _FORMAT_EXT_MAP.get(format, f".{format}")
                suggested = str(Path(output).with_suffix(expected))
                ext_hint = f" → try: --output {suggested}"
            err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(e + ext_hint, style=RED))
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
            out.print(Text("  path:    ", style=GRAY) + Text(exported_path, style=f"bold {ORANGE}"))
            if size_str:
                out.print(Text("  size:    ", style=GRAY) + Text(size_str, style=WHITE))
            out.print(Text("  format:  ", style=GRAY) + Text(format, style=CYAN))
            if options:
                opts_str = "  ".join(f"{k}={v}" for k, v in options.items())
                out.print(Text("  options: ", style=GRAY) + Text(opts_str, style=GRAY))
            out.print()

    asyncio.run(_run())


@app.command("formats")
def list_formats() -> None:
    """List supported export formats and their options."""
    _rule("data formats")
    dt = Table(box=box.SIMPLE, show_edge=False, header_style=BOLD_WHITE, padding=(0, 2), pad_edge=False)
    dt.add_column("format", style=f"bold {CYAN}", no_wrap=True)
    dt.add_column("extension", style=GRAY, no_wrap=True)
    dt.add_column("options", style=WHITE)

    for fmt, ext, opts in [
        ("csv",     ".csv",     "--delimiter (default ,)"),
        ("parquet", ".parquet", "--compression snappy|gzip|zstd|lz4|uncompressed  --partition-by col1,col2"),
        ("json",    ".json",    "-"),
        ("jsonl",   ".jsonl",   "-"),
        ("excel",   ".xlsx",    "--sheet (default Sheet1)"),
    ]:
        dt.add_row(fmt, ext, opts)
    out.print(dt)

    _rule("profile formats")
    pt = Table(box=box.SIMPLE, show_edge=False, header_style=BOLD_WHITE, padding=(0, 2), pad_edge=False)
    pt.add_column("format", style=f"bold {CYAN}", no_wrap=True)
    pt.add_column("extension", style=GRAY, no_wrap=True)
    pt.add_column("notes", style=WHITE)
    pt.add_row("json", ".json", "raw profile result as JSON")
    pt.add_row("html", ".html", "standalone HTML report")
    out.print(pt)
    _rule()