import asyncio
import json
from pathlib import Path
from typing import Optional

import polars as pl
import typer
from rich.prompt import Prompt, Confirm
from rich.table import Table, box
from rich.text import Text

from ..core.context import Context
from ..features.preprocess.ops import validate_op, _DISPATCH, _REQUIRED
from ..features.preprocess.pipeline import PreprocessPipeline
from ..utils.loader import load_dataframe
from ..storage.artifact_store import ArtifactStore
from .shared import (
    BOLD_WHITE,
    CYAN,
    GRAY,
    GREEN,
    ORANGE,
    RED,
    WHITE,
    YELLOW,
    _rule,
    err,
    out,
    print_run_summary,
    print_schema,
    run_pipeline,
    with_spinner,
)

app = typer.Typer(help="preprocess a dataset artifact")


def _parse_op(raw: str) -> dict:
    parts = raw.split(":")
    head = parts[0]
    if "." not in head:
        err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"op must be group.type, got: {head!r}", style=RED))
        raise typer.Exit(1)
    group, type_ = head.split(".", 1)
    op: dict = {"group": group, "type": type_}
    for part in parts[1:]:
        if "=" not in part:
            err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"op param must be key=value, got: {part!r}", style=RED))
            raise typer.Exit(1)
        key, val = part.split("=", 1)
        list_keys = {"cols", "parts", "out_cols", "breaks", "by", "on"}
        if "," in val:
            op[key] = val.split(",")
        elif key in list_keys:
            op[key] = [val]
        elif val.lstrip("-").isdigit():
            op[key] = int(val)
        elif val.replace(".", "", 1).lstrip("-").isdigit():
            op[key] = float(val)
        elif val.lower() in ("true", "false"):
            op[key] = val.lower() == "true"
        else:
            op[key] = val
    return op


def _load_ops_file(path: str) -> list[dict]:
    p = Path(path)
    if not p.exists():
        err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"ops file not found: {p}", style=RED))
        raise typer.Exit(1)
    if p.suffix != ".json":
        err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"ops file must be .json: {p}", style=RED))
        raise typer.Exit(1)
    try:
        data = json.loads(p.read_text())
    except json.JSONDecodeError as exc:
        err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"invalid ops file: {exc}", style=RED))
        raise typer.Exit(1)
    if not isinstance(data, list):
        err.print(Text("[FAIL] ", style=f"bold {RED}") + Text("ops file must contain a JSON array", style=RED))
        raise typer.Exit(1)
    return data


def _print_dataframe(df: pl.DataFrame, n: int) -> None:
    sample = df.head(n)
    t = Table(
        box=box.SIMPLE,
        show_edge=False,
        header_style=BOLD_WHITE,
        padding=(0, 2),
        pad_edge=False,
        border_style=GRAY,
    )
    for col in sample.columns:
        t.add_column(col, style=CYAN, no_wrap=True)
    for row in sample.iter_rows():
        t.add_row(*[str(v) if v is not None else Text("null", style=GRAY) for v in row])
    out.print(t)
    out.print(Text(f"  {n} of {len(df):,} rows", style=GRAY))


def _pick_artifact(store: ArtifactStore, pipelines: Optional[list[str]] = None) -> str:
    artifacts = store.list()
    if pipelines:
        artifacts = [a for a in artifacts if a.pipeline in pipelines]
    if not artifacts:
        label = f"({', '.join(pipelines)})" if pipelines else ""
        err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"no artifacts found {label}".strip(), style=RED))
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
        Text("  select artifact", style=GRAY).append(" (#, run_id, or ref)", style=GRAY)
    )
    for i, art in enumerate(artifacts, 1):
        if raw == str(i) or raw == art.run_id or raw == art.ref:
            return art.run_id

    err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"no match for {raw!r}", style=RED))
    raise typer.Exit(1)


def _pick_ops_interactively() -> list[dict]:
    _rule("available op groups")
    groups = list(_DISPATCH.keys())
    tbl = Table(box=box.SIMPLE, show_edge=False, header_style=BOLD_WHITE, padding=(0, 2), pad_edge=False)
    tbl.add_column("#", style=GRAY, no_wrap=True)
    tbl.add_column("group", style=f"bold {CYAN}", no_wrap=True)
    tbl.add_column("ops", style=WHITE)
    for i, g in enumerate(groups, 1):
        tbl.add_row(str(i), g, "  ".join(_DISPATCH[g].keys()))
    out.print(tbl)

    ops_list: list[dict] = []

    while True:
        out.print()
        undo_hint = Text("  undo", style=GRAY).append(" to remove last", style=GRAY) if ops_list else Text("")
        if ops_list:
            out.print(
                Text("  ops so far: ", style=GRAY)
                + Text("  ".join(f"{o['group']}.{o['type']}" for o in ops_list), style=CYAN)
            )
        raw_group = Prompt.ask(
            Text("  group", style=GRAY).append(" (name or #, Enter to finish, 'undo' to remove last)", style=GRAY),
            default="",
        )
        if not raw_group:
            if not ops_list:
                err.print(Text("[FAIL] ", style=f"bold {RED}") + Text("at least one op is required", style=RED))
                continue
            break

        if raw_group.strip().lower() == "undo":
            if not ops_list:
                err.print(Text("[FAIL] ", style=f"bold {RED}") + Text("nothing to undo", style=RED))
            else:
                removed = ops_list.pop()
                out.print(Text(f"  removed {removed['group']}.{removed['type']}", style=YELLOW) + Text(f"  ({len(ops_list)} op(s) remaining)", style=GRAY))
            continue

        group = raw_group if raw_group in _DISPATCH else (
            groups[int(raw_group) - 1] if raw_group.isdigit() and 1 <= int(raw_group) <= len(groups) else None
        )
        if group is None:
            err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"unknown group {raw_group!r}", style=RED))
            continue

        type_names = list(_DISPATCH[group].keys())
        out.print()
        type_tbl = Table(box=box.SIMPLE, show_edge=False, header_style=BOLD_WHITE, padding=(0, 2), pad_edge=False)
        type_tbl.add_column("#", style=GRAY, no_wrap=True)
        type_tbl.add_column("op", style=f"bold {CYAN}", no_wrap=True)
        type_tbl.add_column("required params", style=GRAY)
        for j, t in enumerate(type_names, 1):
            required = _REQUIRED[group][t]
            type_tbl.add_row(str(j), t, ", ".join(required) if required else Text("", style=GRAY))
        out.print(type_tbl)

        while True:
            raw_type = Prompt.ask(Text(f"  {group} op", style=GRAY).append(" (name or #, 'back' to reselect group)", style=GRAY))
            if raw_type.strip().lower() == "back":
                break

            type_ = raw_type if raw_type in _DISPATCH[group] else (
                type_names[int(raw_type) - 1] if raw_type.isdigit() and 1 <= int(raw_type) <= len(type_names) else None
            )
            if type_ is None:
                err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"unknown op {raw_type!r} in group {group!r}", style=RED))
                continue

            op: dict = {"group": group, "type": type_}
            required_params = _REQUIRED[group][type_]

            if required_params:
                out.print(Text(f"  required: {', '.join(required_params)}", style=GRAY))

            while True:
                raw_params = Prompt.ask(
                    Text("  params", style=GRAY).append(" (key=value:key=value, Enter to skip optional, 'back' to reselect op)", style=GRAY),
                    default="",
                )
                if raw_params.strip().lower() == "back":
                    op = None
                    break
                if raw_params:
                    try:
                        parsed = _parse_op(f"{group}.{type_}:{raw_params}")
                        op.update({k: v for k, v in parsed.items() if k not in ("group", "type")})
                    except SystemExit:
                        err.print(Text("  try again with format key=value:key=value", style=GRAY))
                        continue

                op_errors = validate_op(op)
                if op_errors:
                    for e in op_errors:
                        err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(e, style=RED))
                    continue
                break

            if op is None:
                continue

            ops_list.append(op)
            out.print(Text(f"  added {group}.{type_}", style=GREEN) + Text(f"  ({len(ops_list)} op(s) so far)", style=GRAY))
            break

        continue

        ops_list.append(op)
        out.print(Text(f"  added {group}.{type_}", style=GREEN) + Text(f"  ({len(ops_list)} op(s) so far)", style=GRAY))

    return ops_list


@app.command("run")
def run_preprocess(
    parent_run_id: Optional[str] = typer.Argument(default=None, help="run id of the parent artifact (ingest or preprocess)"),
    op: Optional[list[str]] = typer.Option(None, "--op", help="op in format group.type:key=value:key=value"),
    ops: Optional[str] = typer.Option(None, "--ops", help="path to ops .json file"),
    materialized: bool = typer.Option(False, "--materialize", "-m", help="apply ops and write output to parquet"),
    store_path: str = typer.Option(".datapill", "--store", help="artifact store directory"),
    schema: bool = typer.Option(False, "--schema", help="print column schema after run"),
) -> None:
    artifact_store = ArtifactStore(store_path)
    context = Context(artifact_store=artifact_store)

    if parent_run_id is None:
        parent_run_id = _pick_artifact(artifact_store, pipelines=["ingest", "preprocess"])

    if op and ops:
        err.print(Text("[FAIL] ", style=f"bold {RED}") + Text("use --op or --ops, not both", style=RED))
        raise typer.Exit(1)

    if op:
        ops_list = [_parse_op(o) for o in op]
    elif ops:
        ops_list = _load_ops_file(ops)
    else:
        ops_list = _pick_ops_interactively()

    for i, o in enumerate(ops_list):
        op_errors = validate_op(o)
        for e in op_errors:
            err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"ops[{i}]: {e}", style=RED))
        if op_errors:
            raise typer.Exit(1)

    pipeline = PreprocessPipeline(
        parent_run_id=parent_run_id,
        ops=ops_list,
        options={"materialized": materialized},
    )

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
                _rule("output")
                out.print(Text("  parquet: ", style=GRAY) + Text(art.path, style=f"bold {ORANGE}"))
                out.print()

    asyncio.run(_run())


@app.command("materialize")
def materialize_preprocess(
    run_id: Optional[str] = typer.Argument(default=None, help="run id of an existing preprocess artifact"),
    store_path: str = typer.Option(".datapill", "--store", help="artifact store directory"),
    schema: bool = typer.Option(False, "--schema", help="print column schema after materialize"),
) -> None:
    store = ArtifactStore(store_path)

    if run_id is None:
        run_id = _pick_artifact(store, pipelines=["preprocess"])

    with with_spinner(f"loading {run_id}"):
        artifact = store.get(run_id)
        store.close()

    if not artifact:
        err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"artifact not found: {run_id}", style=RED))
        if Confirm.ask(Text("  pick from available artifacts?", style=GRAY)):
            store2 = ArtifactStore(store_path)
            run_id = _pick_artifact(store2, pipelines=["preprocess"])
            store2.close()
            with with_spinner(f"loading {run_id}"):
                store3 = ArtifactStore(store_path)
                artifact = store3.get(run_id)
                store3.close()
        if not artifact:
            raise typer.Exit(1)

    if artifact.pipeline != "preprocess":
        err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"artifact {run_id!r} is not a preprocess artifact (got {artifact.pipeline!r})", style=RED))
        raise typer.Exit(1)

    if artifact.materialized:
        out.print(Text("  already materialized: ", style=GRAY) + Text(artifact.path or "", style=f"bold {ORANGE}"))
        raise typer.Exit(0)

    ops_list: list[dict] = artifact.options.get("ops", [])
    if not ops_list:
        err.print(Text("[FAIL] ", style=f"bold {RED}") + Text("artifact has no ops config", style=RED))
        raise typer.Exit(1)

    artifact_store = ArtifactStore(store_path)
    context = Context(artifact_store=artifact_store)
    pipeline = PreprocessPipeline(
        parent_run_id=artifact.parent_run_id,
        ops=ops_list,
        options={"materialized": True},
    )

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
                _rule("output")
                out.print(Text("  parquet: ", style=GRAY) + Text(art.path, style=f"bold {ORANGE}"))
                out.print()

    asyncio.run(_run())


@app.command("preview")
def preview_preprocess(
    run_id: Optional[str] = typer.Argument(default=None, help="run id of the preprocess artifact"),
    rows: int = typer.Option(20, "--rows", "-n", help="number of rows to show"),
    store_path: str = typer.Option(".datapill", "--store", help="artifact store directory"),
) -> None:
    store = ArtifactStore(store_path)

    if run_id is None:
        run_id = _pick_artifact(store, pipelines=["preprocess"])

    with with_spinner(f"loading {run_id}"):
        artifact = store.get(run_id)
        store.close()

    if not artifact:
        err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"artifact not found: {run_id}", style=RED))
        if Confirm.ask(Text("  pick from available artifacts?", style=GRAY)):
            store2 = ArtifactStore(store_path)
            run_id = _pick_artifact(store2, pipelines=["preprocess"])
            store2.close()
            with with_spinner(f"loading {run_id}"):
                store3 = ArtifactStore(store_path)
                artifact = store3.get(run_id)
                store3.close()
        if not artifact:
            raise typer.Exit(1)

    if artifact.pipeline != "preprocess":
        err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"artifact {run_id!r} is not a preprocess artifact", style=RED))
        raise typer.Exit(1)

    if artifact.materialized and artifact.path:
        with with_spinner("reading parquet"):
            store = ArtifactStore(store_path)
            abs_path = store.path / artifact.path
            store.close()
            df = pl.read_parquet(abs_path)
        _rule("preview")
        _print_dataframe(df, rows)
        return

    ops_list: list[dict] = artifact.options.get("ops", [])
    if not ops_list:
        err.print(Text("[FAIL] ", style=f"bold {RED}") + Text("artifact has no ops config to apply", style=RED))
        raise typer.Exit(1)

    artifact_store = ArtifactStore(store_path)
    context = Context(artifact_store=artifact_store)

    async def _preview() -> None:
        parent = context.artifact_store.get(artifact.parent_run_id)
        if not parent:
            err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"parent artifact not found: {artifact.parent_run_id}", style=RED))
            raise typer.Exit(1)

        with with_spinner("loading sample data"):
            df = await load_dataframe(parent, context)

        df = df.head(rows * 10)

        from ..features.preprocess.ops import apply_op
        for op in ops_list:
            if op.get("type") == "join":
                out.print(Text("  [SKIP] ", style=YELLOW) + Text("join ops skipped in preview", style=GRAY))
                continue
            try:
                df, _ = apply_op(df, op)
            except Exception as exc:
                err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"{op.get('group')}.{op.get('type')}: {exc}", style=RED))
                raise typer.Exit(1)

        _rule("preview")
        _print_dataframe(df, rows)

    asyncio.run(_preview())


@app.command("ops")
def list_ops(
    group: Optional[str] = typer.Option(None, "--group", "-g", help="filter by group"),
) -> None:
    groups = [group] if group else list(_DISPATCH.keys())
    unknown = [g for g in groups if g not in _DISPATCH]
    if unknown:
        err.print(Text("[FAIL] ", style=f"bold {RED}") + Text(f"unknown group(s): {', '.join(unknown)}", style=RED))
        raise typer.Exit(1)

    examples = {
        ("schema", "cast"): "schema.cast:col=age:dtype=Int32",
        ("schema", "rename"): "schema.rename:old_name=new_name",
        ("schema", "drop_columns"): "schema.drop_columns:cols=a,b,c",
        ("schema", "select_columns"): "schema.select_columns:cols=id,name,age",
        ("schema", "reorder_columns"): "schema.reorder_columns:cols=id,name",
        ("clean", "fill_null"): "clean.fill_null:cols=salary:value=0",
        ("clean", "drop_null"): "clean.drop_null:cols=email,name",
        ("clean", "impute"): "clean.impute:cols=age,salary:strategy=median",
        ("clean", "clip"): "clean.clip:cols=score:min=0:max=100",
        ("clean", "winsorize"): "clean.winsorize:cols=revenue:lower=0.05:upper=0.95",
        ("clean", "drop_outlier"): "clean.drop_outlier:cols=price:method=iqr",
        ("clean", "flag_outlier"): "clean.flag_outlier:cols=price:method=zscore",
        ("clean", "replace_value"): "clean.replace_value:cols=status:mapping=...",
        ("transform", "normalize"): "transform.normalize:cols=age,salary",
        ("transform", "standardize"): "transform.standardize:cols=revenue",
        ("transform", "log_transform"): "transform.log_transform:cols=price:base=log10",
        ("transform", "bin"): "transform.bin:col=age:breaks=0,18,35,60,100",
        ("transform", "rank"): "transform.rank:cols=score:method=dense",
        ("transform", "power_transform"): "transform.power_transform:cols=income:method=sqrt",
        ("transform", "encode"): "transform.encode:col=city:method=onehot",
        ("transform", "math_expr"): "transform.math_expr:out_col=ratio:expr=...",
        ("parse", "trim"): "parse.trim:cols=name,email",
        ("parse", "lower"): "parse.lower:cols=email",
        ("parse", "upper"): "parse.upper:cols=country_code",
        ("parse", "regex_extract"): "parse.regex_extract:col=text:pattern=...:group=1",
        ("parse", "split_col"): "parse.split_col:col=full_name:sep= :out_cols=first,last",
        ("parse", "parse_datetime"): "parse.parse_datetime:cols=created_at:format=%Y-%m-%d",
        ("parse", "extract_datetime_part"): "parse.extract_datetime_part:col=ts:parts=year,month,dow",
        ("parse", "date_diff"): "parse.date_diff:col_a=end_date:col_b=start_date:unit=day",
        ("reshape", "filter_rows"): "reshape.filter_rows:expr=...",
        ("reshape", "dedup"): "reshape.dedup:cols=email:keep=first",
        ("reshape", "sort"): "reshape.sort:cols=created_at:descending=true",
        ("reshape", "add_column"): "reshape.add_column:col=source:value=web",
        ("reshape", "explode"): "reshape.explode:col=tags",
        ("reshape", "pivot"): "reshape.pivot:on=month:index=store_id:values=revenue",
        ("reshape", "unpivot"): "reshape.unpivot:on=jan,feb,mar:index=store_id",
        ("compose", "window_agg"): "compose.window_agg:col=revenue:fn=rolling_mean:window=7:partition_by=store_id",
        ("compose", "group_agg"): "compose.group_agg:by=region:aggs=...",
        ("compose", "feature_cross"): "compose.feature_cross:col_a=gender:col_b=age_group",
        ("compose", "resample"): "compose.resample:time_col=ts:every=1d:agg_col=revenue:fn=sum",
        ("compose", "sample"): "compose.sample:n=1000",
        ("compose", "custom_expr"): "compose.custom_expr:out_col=flag:expr=...",
    }

    for g in groups:
        _rule(g)
        t = Table(box=None, show_header=True, show_edge=False, header_style=BOLD_WHITE, padding=(0, 2), pad_edge=False)
        t.add_column("op", style=f"bold {CYAN}", no_wrap=True)
        t.add_column("required params", style=GRAY)
        t.add_column("example", style=WHITE)
        for type_ in _DISPATCH[g]:
            required = _REQUIRED[g][type_]
            req_str = ", ".join(required) if required else Text("", style=GRAY)
            t.add_row(type_, req_str, examples.get((g, type_), ""))
        out.print(t)