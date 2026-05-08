import asyncio
import json
from pathlib import Path
from typing import Optional

import typer
from rich.prompt import Prompt, Confirm
from rich.table import Table, box
from rich.text import Text

from ..connectors import registry
from ..core.context import Context
from ..features.ingest.pipeline import IngestPipeline
from ..storage.artifact_store import ArtifactStore
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
    print_artifact_path,
    print_connection_result,
    print_run_summary,
    print_schema,
    run_pipeline,
)

app = typer.Typer(help="ingest data from a source into datapill")

_CONFIG_REQUIRED_SOURCES = {"postgres", "mysql", "sqlite", "kafka", "rest", "s3"}

_SOURCE_FIELDS: dict[str, list[dict]] = {
    "postgres": [
        {"name": "host",             "required": True,  "default": None,      "hint": "e.g. localhost"},
        {"name": "port",             "required": False, "default": "5432",    "hint": "",          "type": int},
        {"name": "database",         "required": True,  "default": None,      "hint": ""},
        {"name": "user",             "required": True,  "default": None,      "hint": ""},
        {"name": "password",         "required": True,  "default": None,      "hint": "hidden",    "secret": True},
        {"name": "schema",           "required": False, "default": "public",  "hint": ""},
        {"name": "ssl",              "required": False, "default": "",        "hint": "disable | require | verify-full"},
        {"name": "connect_timeout",  "required": False, "default": "10.0",   "hint": "seconds",   "type": float},
    ],
    "mysql": [
        {"name": "host",             "required": True,  "default": None,      "hint": "e.g. localhost"},
        {"name": "port",             "required": False, "default": "3306",    "hint": "",          "type": int},
        {"name": "database",         "required": True,  "default": None,      "hint": ""},
        {"name": "user",             "required": True,  "default": None,      "hint": ""},
        {"name": "password",         "required": True,  "default": None,      "hint": "hidden",    "secret": True},
        {"name": "charset",          "required": False, "default": "utf8mb4", "hint": ""},
        {"name": "connect_timeout",  "required": False, "default": "10.0",   "hint": "seconds",   "type": float},
    ],
    "sqlite": [
        {"name": "path",             "required": True,  "default": None,      "hint": "path to .db file"},
        {"name": "read_only",        "required": False, "default": "false",   "hint": "true | false"},
        {"name": "timeout",          "required": False, "default": "30.0",    "hint": "seconds",   "type": float},
    ],
    "kafka": [
        {"name": "brokers",          "required": True,  "default": None,      "hint": "comma-separated, e.g. localhost:9092"},
        {"name": "group_id",         "required": False, "default": "datapill","hint": ""},
        {"name": "auto_offset_reset","required": False, "default": "earliest","hint": "earliest | latest"},
        {"name": "security_protocol","required": False, "default": "PLAINTEXT","hint": "PLAINTEXT | SASL_PLAINTEXT | SSL | SASL_SSL"},
        {"name": "sasl_mechanism",   "required": False, "default": "",        "hint": "PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512"},
        {"name": "sasl_username",    "required": False, "default": "",        "hint": ""},
        {"name": "sasl_password",    "required": False, "default": "",        "hint": "hidden",    "secret": True},
    ],
    "rest": [
        {"name": "base_url",         "required": True,  "default": None,      "hint": "e.g. https://api.example.com"},
        {"name": "auth_type",        "required": False, "default": "",        "hint": "bearer | basic"},
        {"name": "auth_token",       "required": False, "default": "",        "hint": "hidden",    "secret": True},
        {"name": "basic_user",       "required": False, "default": "",        "hint": ""},
        {"name": "basic_password",   "required": False, "default": "",        "hint": "hidden",    "secret": True},
        {"name": "pagination_type",  "required": False, "default": "",        "hint": "page | cursor"},
        {"name": "page_size",        "required": False, "default": "100",     "hint": "",          "type": int},
        {"name": "results_key",      "required": False, "default": "",        "hint": "JSON key containing results array"},
        {"name": "timeout_s",        "required": False, "default": "30.0",    "hint": "seconds",   "type": float},
    ],
    "s3": [
        {"name": "bucket",           "required": True,  "default": None,      "hint": ""},
        {"name": "region",           "required": False, "default": "us-east-1","hint": ""},
        {"name": "access_key",       "required": False, "default": "",        "hint": "leave blank for IAM role"},
        {"name": "secret_key",       "required": False, "default": "",        "hint": "hidden",    "secret": True},
        {"name": "endpoint_url",     "required": False, "default": "",        "hint": "for S3-compatible storage"},
        {"name": "prefix",           "required": False, "default": "",        "hint": "key prefix"},
    ],
    "local": [
        {"name": "base_path",        "required": False, "default": "",        "hint": "base directory (or pass full path via --path)"},
        {"name": "encoding",         "required": False, "default": "utf-8",   "hint": ""},
    ],
}

_SOURCE_OPTIONS: dict[str, list[dict]] = {
    "postgres": [
        {"name": "query_or_table", "label": "table or SQL query", "required": True,  "hint": "table name or full SELECT ..."},
    ],
    "mysql": [
        {"name": "query_or_table", "label": "table or SQL query", "required": True,  "hint": "table name or full SELECT ..."},
    ],
    "sqlite": [
        {"name": "query_or_table", "label": "table or SQL query", "required": True,  "hint": "table name or full SELECT ..."},
    ],
    "kafka": [
        {"name": "topic",          "label": "topic",              "required": True,  "hint": ""},
    ],
    "rest": [
        {"name": "endpoint",       "label": "endpoint path",      "required": True,  "hint": "e.g. /v1/users"},
        {"name": "params",         "label": "query params (JSON)","required": False, "hint": 'e.g. {"page_size": 50} or leave blank'},
    ],
    "s3": [
        {"name": "path",           "label": "object key / path",  "required": True,  "hint": "e.g. data/file.parquet"},
    ],
    "local": [
        {"name": "path",           "label": "file path",          "required": True,  "hint": "absolute or relative to base_path"},
    ],
}


def _load_config(value: str) -> dict:
    p = Path(value)
    if not p.exists():
        typer.echo(f"[FAIL] config file not found: {p}", err=True)
        raise typer.Exit(1)
    if p.suffix != ".json":
        typer.echo(f"[FAIL] config file must be a .json file: {p}", err=True)
        raise typer.Exit(1)
    try:
        return json.loads(p.read_text())
    except json.JSONDecodeError as exc:
        typer.echo(f"[FAIL] invalid config file {p}: {exc}", err=True)
        raise typer.Exit(1)


def _resolve_local_path(path: str, config: dict) -> tuple[str, str]:
    p = Path(path)

    if "base_path" in config:
        base = Path(config["base_path"])
        if p.is_absolute():
            try:
                rel = p.relative_to(base)
            except ValueError:
                typer.echo(
                    f"[FAIL] --path {path!r} is not inside base_path {str(base)!r} from config",
                    err=True,
                )
                raise typer.Exit(1)
            return str(base), str(rel)
        if not (base / p).exists():
            typer.echo(f"[FAIL] file not found: {base / p}", err=True)
            raise typer.Exit(1)
        return str(base), str(p)

    p = p.resolve()
    if not p.exists():
        typer.echo(f"[FAIL] file not found: {p}", err=True)
        raise typer.Exit(1)
    if p.is_dir():
        typer.echo(f"[FAIL] --path must point to a file, not a directory: {p}", err=True)
        raise typer.Exit(1)
    return str(p.parent), p.name


def _prompt_config(source: str) -> dict:
    fields = _SOURCE_FIELDS.get(source, [])
    if not fields:
        return {}

    _rule(f"{source} connection")

    tbl = Table(box=box.SIMPLE, show_edge=False, header_style=BOLD_WHITE, padding=(0, 2), pad_edge=False)
    tbl.add_column("field", style=CYAN, no_wrap=True)
    tbl.add_column("required", style=GRAY, no_wrap=True)
    tbl.add_column("default", style=GRAY)
    tbl.add_column("hint", style=GRAY)
    for f in fields:
        tbl.add_row(
            f["name"],
            Text("yes", style=f"bold {RED}") if f["required"] else Text("no", style=GRAY),
            str(f["default"]) if f["default"] else "-",
            f.get("hint", ""),
        )
    out.print(tbl)
    out.print()

    config: dict = {}
    for f in fields:
        label = Text(f"  {f['name']}", style=CYAN)
        if f["required"]:
            label.append(" *", style=f"bold {RED}")

        if f.get("secret"):
            import getpass
            while True:
                val = getpass.getpass(f"  {f['name']}{'  *' if f['required'] else ''}: ")
                if val or not f["required"]:
                    break
                err.print(Text(f"  {f['name']} is required", style=RED))
            if val:
                config[f["name"]] = val
        else:
            default = f["default"] or ""
            val = Prompt.ask(label, default=default)
            if not val and f["required"]:
                while not val:
                    err.print(Text(f"  {f['name']} is required", style=RED))
                    val = Prompt.ask(label, default=default)
            if val and val != default:
                config[f["name"]] = val
            elif val == default and default:
                config[f["name"]] = val

    for f in fields:
        name = f["name"]
        cast = f.get("type")
        if cast and name in config:
            try:
                config[name] = cast(config[name])
            except (ValueError, TypeError):
                err.print(Text(f"  invalid value for {name!r} — using default", style=YELLOW))
                config.pop(name, None)

    if source == "kafka" and "brokers" in config:
        raw = config["brokers"]
        config["brokers"] = [b.strip() for b in raw.split(",")]

    if source == "sqlite" and "read_only" in config:
        config["read_only"] = config["read_only"].lower() in ("true", "1", "yes")

    return config


def _prompt_options(source: str) -> dict:
    option_fields = _SOURCE_OPTIONS.get(source, [])
    if not option_fields:
        return {}

    _rule(f"{source} options")
    options: dict = {}

    for f in option_fields:
        label = Text(f"  {f['label']}", style=CYAN)
        if f["required"]:
            label.append(" *", style=f"bold {RED}")
        hint = f.get("hint", "")
        if hint:
            label.append(f"  ({hint})", style=GRAY)

        val = Prompt.ask(label, default="")
        if not val and f["required"]:
            while not val:
                err.print(Text(f"  {f['label']} is required", style=RED))
                val = Prompt.ask(label, default="")

        if f["name"] == "query_or_table" and val:
            stripped = val.strip().upper()
            if stripped.startswith("SELECT") or stripped.startswith("WITH"):
                options["query"] = val
            else:
                options["query"] = f"SELECT * FROM {val}"
        elif f["name"] == "params" and val:
            try:
                options["params"] = json.loads(val)
            except json.JSONDecodeError:
                err.print(Text("  invalid JSON for params — skipping", style=YELLOW))
        elif val:
            options[f["name"]] = val

    return options


@app.command()
def run(
    source: str = typer.Argument(help="source type: postgres, mysql, sqlite, s3, local, kafka, rest"),
    config: Optional[str] = typer.Option(None, "--config", "-c", help="path to config .json file (required for postgres, mysql, sqlite, kafka, rest, s3; optional for local)"),
    table: Optional[str] = typer.Option(None, "--table", "-t", help="table name (postgres, mysql, sqlite)"),
    query: Optional[str] = typer.Option(None, "--query", "-q", help="raw SQL query (postgres, mysql, sqlite)"),
    topic: Optional[str] = typer.Option(None, "--topic", help="kafka topic"),
    path: Optional[str] = typer.Option(None, "--path", "-p", help="file path (local: full path to file; s3: key within bucket)"),
    endpoint: Optional[str] = typer.Option(None, "--endpoint", "-e", help="REST endpoint path"),
    params: Optional[str] = typer.Option(None, "--params", help="path to params .json file"),
    sample: bool = typer.Option(False, "--sample", help="read a sample instead of full data"),
    sample_size: int = typer.Option(10_000, "--sample-size", help="number of rows to sample"),
    batch_size: Optional[int] = typer.Option(None, "--batch-size", help="rows per streaming batch"),
    materialized: bool = typer.Option(False, "--materialize", "-m", help="write output to parquet artifact"),
    store_path: str = typer.Option(".datapill", "--store", help="artifact store directory"),
    schema: bool = typer.Option(False, "--schema", help="print column schema after ingest"),
    mkdir: bool = typer.Option(False, "--mkdir", help="create base_path directory if it does not exist (local source only)"),
    interactive: bool = typer.Option(True, "--interactive/--no-interactive", "-i/-I", help="prompt for missing values"),
) -> None:
    if source not in registry.sources():
        err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"unknown source: {source!r}. available: {registry.sources()}", style=RED))
        raise typer.Exit(1)

    connector_config: dict = {}
    options: dict = {}

    needs_interactive_config = (
        interactive
        and config is None
        and source in _CONFIG_REQUIRED_SOURCES
    )
    needs_interactive_local = (
        interactive
        and config is None
        and source == "local"
    )

    if config:
        connector_config = _load_config(config)
    elif needs_interactive_config or needs_interactive_local:
        connector_config = _prompt_config(source)

    cli_has_options = any([table, query, topic, path, endpoint, params])

    if not cli_has_options and interactive:
        prompted = _prompt_options(source)
        options.update(prompted)
    else:
        if query and table:
            err.print(Text("[FAIL] ", style=f"bold {RED}") + Text("cannot use both --table and --query", style=RED))
            raise typer.Exit(1)
        if query:
            options["query"] = query
        elif table:
            options["query"] = f"SELECT * FROM {table}"
        if topic:
            options["topic"] = topic
        if endpoint:
            options["endpoint"] = endpoint
        if params:
            options["params"] = _load_config(params)

    if batch_size:
        options["batch_size"] = batch_size

    if source == "local":
        resolved_path = options.get("path") or path
        if not resolved_path:
            err.print(Text("[FAIL] ", style=f"bold {RED}") + Text("path is required for source 'local'", style=RED))
            raise typer.Exit(1)
        base_path, rel_path = _resolve_local_path(resolved_path, connector_config)
        connector_config = {**connector_config, "base_path": base_path, "mkdir": mkdir}
        options["path"] = rel_path
    elif path:
        options["path"] = path

    options["source"] = source
    options["sample"] = sample
    options["sample_size"] = sample_size
    options["materialized"] = materialized

    artifact_store = ArtifactStore(store_path)
    context = Context(artifact_store=artifact_store)
    pipeline = IngestPipeline(source=source, config=connector_config, options=options)

    validation = pipeline.validate(context)
    if not validation.ok:
        for e in validation.errors:
            err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(e, style=RED))
        raise typer.Exit(1)

    async def _run() -> None:
        plan = pipeline.plan(context)
        await run_pipeline(pipeline.execute(plan, context))

        if context.artifact:
            art = context.artifact
            print_run_summary({"run_id": art.run_id, "ref": art.ref})
            if schema and art.schema:
                print_schema(art.schema)
            if art.materialized and art.path:
                print_artifact_path(art.path)

    asyncio.run(_run())


@app.command("sources")
def list_sources() -> None:
    _rule("available sources")
    tbl = Table(box=box.SIMPLE, show_edge=False, header_style=BOLD_WHITE, padding=(0, 2), pad_edge=False)
    tbl.add_column("source", style=f"bold {CYAN}", no_wrap=True)
    tbl.add_column("config required", style=GRAY, no_wrap=True)
    tbl.add_column("key options", style=WHITE)
    rows = [
        ("postgres", "yes", "--table / --query"),
        ("mysql",    "yes", "--table / --query"),
        ("sqlite",   "yes", "--table / --query"),
        ("kafka",    "yes", "--topic"),
        ("rest",     "yes", "--endpoint  [--params]"),
        ("s3",       "yes", "--path"),
        ("local",    "no",  "--path"),
    ]
    for src, req, opts in rows:
        tbl.add_row(src, req, opts)
    out.print(tbl)
    _rule()


@app.command("check")
def check_connection(
    source: str = typer.Argument(help="source type"),
    config: Optional[str] = typer.Option(None, "--config", "-c", help="path to config .json file"),
    path: Optional[str] = typer.Option(None, "--path", "-p", help="file path (for local source)"),
    mkdir: bool = typer.Option(False, "--mkdir", help="create base_path directory if it does not exist (local source only)"),
    interactive: bool = typer.Option(True, "--interactive/--no-interactive", "-i/-I", help="prompt for missing values"),
) -> None:
    if source not in registry.sources():
        err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"unknown source: {source!r}", style=RED))
        raise typer.Exit(1)

    connector_config: dict = {}

    if config:
        connector_config = _load_config(config)
    elif interactive and source in _CONFIG_REQUIRED_SOURCES:
        connector_config = _prompt_config(source)
    elif source in _CONFIG_REQUIRED_SOURCES:
        err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"--config is required for source '{source}' (or use -i)", style=RED))
        raise typer.Exit(1)

    if source == "local":
        if not path and "base_path" not in connector_config:
            if interactive:
                raw = Prompt.ask(Text("  base_path", style=CYAN))
                connector_config = {**connector_config, "base_path": raw, "mkdir": mkdir}
            else:
                err.print(Text("[FAIL] ", style=f"bold {RED}") + Text("--path or 'base_path' in config required for source 'local'", style=RED))
                raise typer.Exit(1)
        elif path:
            base_path, _ = _resolve_local_path(path, connector_config)
            connector_config = {**connector_config, "base_path": base_path, "mkdir": mkdir}

    async def _check() -> None:
        connector = registry.build(source, connector_config)
        status = await connector.connect()
        if status.ok:
            print_connection_result(status.latency_ms)
        else:
            err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(status.error or "connection failed", style=RED))
            raise typer.Exit(1)
        await connector.cleanup()

    asyncio.run(_check())