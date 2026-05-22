#!/usr/bin/env python3
"""Universal Tableau Prep flow → Visual Data Prep designer notebook converter.

Walks a `.tflx` (or `.tfl`) file, maps every node to a Lakeflow Designer
Visual Data Prep operator using the rules in
`tableau-to-lakeflow-designer/SKILL.md`, and emits a `.designer.ipynb`.

Output operator coverage (matches the docs at
https://docs.databricks.com/aws/en/designer/built-in-operators):

    Source · Output · AI Function · Aggregate · Combine · Filter · Join ·
    Limit · Pivot · Sort · SQL · Transform · Python · Note · Group

Usage:
    python convert_tflx.py PATH/TO/FLOW.tflx \\
        --out PATH/TO/OUTPUT.designer.ipynb \\
        --extracted-data /Workspace/Users/me/extracted_data \\
        --output-table main.default.my_table
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import uuid
import zipfile
from collections import defaultdict, deque
from pathlib import Path
from typing import Any


# ───────────────────────────────────────────────────────────────────────────
# CANONICAL RUN-BODY TEMPLATES (Designer validates these byte-for-byte)
# ───────────────────────────────────────────────────────────────────────────
RUN_SOURCE = '''
# generated from the system
from typing import Dict, Any

def _strip_sql_quotes(s):
    if isinstance(s, str) and len(s) >= 2:
        if (s[0] == '"' and s[-1] == '"') or (s[0] == "'" and s[-1] == "'"):
            return s[1:-1]
    return s

def run(
    config: Dict[str, Any], inputs: Dict[str, Any], spark
) -> Dict[str, Any]:
    file_source = config.get("file_source")
    table_source = config.get("table_source")

    if file_source:
        path = file_source.get("path")
        if not path:
            raise ValueError("Source: 'path' is required for file source")
        options = []
        for key, value in file_source.items():
            if key == "path":
                continue
            if key == "headerRows" and isinstance(value, bool):
                options.append(f"{key}=>{1 if value else 0}")
            elif isinstance(value, bool):
                options.append(f'{key}=>{"true" if value else "false"}')
            elif isinstance(value, (int, float)):
                options.append(f"{key}=>{value}")
            else:
                clean = _strip_sql_quotes(str(value))
                if key == "dataAddress" and clean.startswith("!"):
                    clean = clean[1:]
                options.append(f'{key}=>"{clean}"')
        opts = ", ".join(options)
        sql = f'SELECT * FROM read_files("{path}", {opts})' if opts else f'SELECT * FROM read_files("{path}")'
        out = spark.sql(sql)
    elif table_source:
        table_name = table_source.get("tableName")
        if not table_name:
            raise ValueError("Source: 'tableName' is required for table source")
        out = spark.table(table_name)
    else:
        raise ValueError("Source: either 'file_source' or 'table_source' must be configured")

    return {"data": out}
'''

RUN_FILTER = '''
# generated from the system
from typing import Dict, Any

def run(
    config: Dict[str, Any], inputs: Dict[str, Any], spark
) -> Dict[str, Any]:
    df = inputs["data"]
    condition = config.get("condition", "")
    if not condition:
        return {"filtered_data": df}
    return {"filtered_data": df.filter(condition)}
'''

RUN_TRANSFORM = '''
# generated from the system
from typing import Dict, Any, List

def run(
    config: Dict[str, Any], inputs: Dict[str, Any], spark
) -> Dict[str, Any]:
    df = inputs["data"]
    expressions: List[str] = config.get("expressions", [])
    if not expressions:
        return {"transformed_data": df}
    return {"transformed_data": df.selectExpr(*expressions)}
'''

RUN_COMBINE = '''
# generated from the system
from typing import Dict, Any

def run(
    config: Dict[str, Any], inputs: Dict[str, Any], spark
) -> Dict[str, Any]:
    df_0 = inputs["data_0"]
    df_1 = inputs["data_1"]
    operator = config.get("operator", "UNION")
    quantifier = config.get("quantifier", "DISTINCT")

    op_map = {
        "UNION": lambda a, b: a.union(b),
        "INTERSECT": lambda a, b: a.intersectAll(b),
        "EXCEPT": lambda a, b: a.exceptAll(b),
        "MINUS": lambda a, b: a.exceptAll(b),
    }
    op_distinct_map = {
        "UNION": lambda a, b: a.union(b).distinct(),
        "INTERSECT": lambda a, b: a.intersect(b),
        "EXCEPT": lambda a, b: a.subtract(b),
        "MINUS": lambda a, b: a.subtract(b),
    }
    if quantifier == "DISTINCT":
        combine_fn = op_distinct_map.get(operator)
    else:
        combine_fn = op_map.get(operator)
    if not combine_fn:
        raise ValueError(f"Unsupported combine operator: {operator}")

    return {"combined_data": combine_fn(df_0, df_1)}
'''

RUN_AGGREGATE = '''
# generated from the system
from typing import Dict, Any
import pyspark.sql.functions as F

def run(
    config: Dict[str, Any], inputs: Dict[str, Any], spark
) -> Dict[str, Any]:
    df = inputs.get("data")
    group_bys = config.get("group_bys", [])
    aggregations = config.get("aggregations", [])

    group_by_set = set(e for gb in group_bys if (e := gb.get("expr", "")))

    agg_exprs = []
    for agg_def in aggregations:
        col_expr = agg_def.get("columnExpr", {})
        raw_expr = col_expr.get("expr", "")
        fn = agg_def.get("fn", "-")
        alias = agg_def.get("alias")

        if (fn == "-" or fn == "_") and not alias and raw_expr in group_by_set:
            continue

        fn_map = {
            "SUM": F.sum, "AVG": F.avg, "COUNT": F.count, "MIN": F.min, "MAX": F.max,
            "MEAN": F.mean, "MEDIAN": F.median, "STDDEV": F.stddev, "VARIANCE": F.variance,
        }
        agg_fn = fn_map.get(fn)
        if agg_fn:
            col = agg_fn(raw_expr)
        elif fn == "-" or fn == "_":
            col = F.col(raw_expr)
        else:
            col = F.expr(f"{fn}({raw_expr})")
        if alias:
            col = col.alias(alias)
        agg_exprs.append(col)

    group_cols = [gb.get("expr", "") for gb in group_bys if gb.get("expr", "")]
    if not agg_exprs:
        if group_cols:
            return {"aggregated_data": df.select(*group_cols).distinct()}
        return {"aggregated_data": df}
    if group_cols:
        return {"aggregated_data": df.groupBy(*group_cols).agg(*agg_exprs)}
    return {"aggregated_data": df.agg(*agg_exprs)}
'''

RUN_JOIN = '''
# generated from the system
from typing import Dict, Any, List
import pyspark.sql.functions as F

def run(
    config: Dict[str, Any], inputs: Dict[str, Any], spark
) -> Dict[str, Any]:
    join_type = config.get("join_type", "inner").replace(" ", "_")
    join_condition = config.get("join_conditions", "")
    expressions: List[str] = config.get("expressions", [])

    df_left = inputs.get("left")
    df_right = inputs.get("right")
    if df_left is None or df_right is None:
        raise ValueError("Both left and right inputs must be connected")
    df_left = df_left.alias("left")
    df_right = df_right.alias("right")

    if not join_condition:
        result = df_left.join(df_right, how=join_type)
    else:
        result = df_left.join(df_right, F.expr(join_condition), how=join_type)
    if expressions:
        result = result.selectExpr(*expressions)
    return {"joined_data": result}
'''

RUN_PIVOT = '''
# generated from the system
from typing import Dict, Any, List
import pyspark.sql.functions as F

def run(
    config: Dict[str, Any], inputs: Dict[str, Any], spark
) -> Dict[str, Any]:
    df = inputs["data"]
    mode = config.get("mode", "pivot")
    if mode == "pivot":
        group_by = config.get("group_by", [])
        pivot_col = config.get("pivot_column")
        value_col = config.get("value_column")
        agg_fn = config.get("agg", "count").lower()
        result = df.groupBy(*group_by).pivot(pivot_col).agg(getattr(F, agg_fn)(value_col))
        return {"pivoted_data": result}
    else:
        id_cols: List[str] = config.get("id_columns", [])
        value_cols: List[str] = config.get("value_columns", [])
        key_name = config.get("key_name", "key")
        value_name = config.get("value_name", "value")
        stack_expr = "stack({n}, {pairs}) AS ({k}, {v})".format(
            n=len(value_cols),
            pairs=", ".join(f"'{c}', `{c}`" for c in value_cols),
            k=key_name,
            v=value_name,
        )
        result = df.selectExpr(*id_cols, stack_expr)
        return {"pivoted_data": result}
'''

RUN_SORT = '''
# generated from the system
from typing import Dict, Any
import pyspark.sql.functions as F

def run(
    config: Dict[str, Any], inputs: Dict[str, Any], spark
) -> Dict[str, Any]:
    df = inputs.get("data")
    sort_expressions = config.get("sort_expressions", [])
    if not sort_expressions:
        return {"sorted_data": df}
    order_cols = []
    for sort_def in sort_expressions:
        col_expr = sort_def.get("columnExpr", {})
        raw_expr = col_expr.get("expr", "")
        direction = sort_def.get("sortBy", "UNSET")
        col = F.col(raw_expr)
        if direction == "DESC":
            col = col.desc()
        elif direction == "ASC":
            col = col.asc()
        order_cols.append(col)
    return {"sorted_data": df.orderBy(*order_cols)}
'''

RUN_LIMIT = '''
# generated from the system
from typing import Dict, Any

def run(
    config: Dict[str, Any], inputs: Dict[str, Any], spark
) -> Dict[str, Any]:
    df = inputs["data"]
    n = int(config.get("n", 100))
    return {"limited_data": df.limit(n)}
'''

RUN_SQL = '''
# generated from the system
import re
from typing import Any, Dict, List

def run(
    config: Dict[str, Any], inputs: Dict[str, Any], spark
) -> Dict[str, Any]:
    sources: List[Dict[str, str]] = inputs.get("data__sources") or []
    for i, df in enumerate(inputs.get("data") or []):
        if df is not None and i < len(sources):
            df.createOrReplaceTempView(sources[i]["df_name"])

    query = config.get("query", "")
    param_names = set(re.findall(r"(?<!:):(\\w+)", query))
    if param_names:
        all_widgets = dbutils.widgets.getAll()
        args = {name: all_widgets[name] for name in param_names if name in all_widgets}
        result = spark.sql(query, args=args) if args else spark.sql(query)
    else:
        result = spark.sql(query)
    return {"result": result}
'''

RUN_PYTHON = '''
# generated from the system
from typing import Dict, Any

def run(
    config: Dict[str, Any], inputs: Dict[str, Any], spark
) -> Dict[str, Any]:
    code = config.get("code", "")
    local_vars = {"inputs": inputs, "spark": spark, "result": None}
    exec(code, {}, local_vars)
    return {"result": local_vars.get("result")}
'''

RUN_AI_FUNCTION = '''
# generated from the system
from typing import Dict, Any, List

def run(
    config: Dict[str, Any], inputs: Dict[str, Any], spark
) -> Dict[str, Any]:
    df = inputs["data"]
    expressions: List[str] = config.get("expressions", [])
    if not expressions:
        return {"ai_data": df}
    return {"ai_data": df.selectExpr(*expressions, "*")}
'''

RUN_OUTPUT = '''
# generated from the system
from typing import Dict, Any

def run(
    config: Dict[str, Any], inputs: Dict[str, Any], spark
) -> Dict[str, Any]:
    df = inputs["data"]
    catalog = config.get("catalog", "")
    schema = config.get("schema", "")
    table_name = config.get("table_name", "")
    if not table_name:
        raise ValueError("Output: 'table_name' is required")
    parts = [p for p in [catalog, schema, table_name] if p]
    full_name = ".".join(parts)
    df.write.mode("overwrite").saveAsTable(full_name)
    return {}
'''

RUN_BODIES = {
    "source":      RUN_SOURCE,
    "filter":      RUN_FILTER,
    "transform":   RUN_TRANSFORM,
    "combine":     RUN_COMBINE,
    "aggregate":   RUN_AGGREGATE,
    "join":        RUN_JOIN,
    "pivot":       RUN_PIVOT,
    "sort":        RUN_SORT,
    "limit":       RUN_LIMIT,
    "sql":         RUN_SQL,
    "python":      RUN_PYTHON,
    "ai_function": RUN_AI_FUNCTION,
    "output":      RUN_OUTPUT,
}

# Output port name per template (per the skill's Appendix B)
OUT_PORT = {
    "source": "data",
    "filter": "filtered_data",
    "transform": "transformed_data",
    "combine": "combined_data",
    "aggregate": "aggregated_data",
    "join": "joined_data",
    "pivot": "pivoted_data",
    "sort": "sorted_data",
    "limit": "limited_data",
    "sql": "result",
    "python": "result",
    "ai_function": "ai_data",
    "output": None,
}

# Input port name(s) per template
IN_PORTS = {
    "source": [],
    "filter": ["data"],
    "transform": ["data"],
    "combine": ["data_0", "data_1"],
    "aggregate": ["data"],
    "join": ["left", "right"],
    "pivot": ["data"],
    "sort": ["data"],
    "limit": ["data"],
    "sql": ["data"],
    "python": ["data"],
    "ai_function": ["data"],
    "output": ["data"],
}


# ───────────────────────────────────────────────────────────────────────────
# TFLX → CELL GRAPH
# ───────────────────────────────────────────────────────────────────────────
def parse_tflx(path: Path) -> tuple[dict, list[tuple[str, bytes]]]:
    """Open a .tflx archive (or .tfl bare JSON). Returns (flow_json, embedded_files)."""
    if path.suffix == ".tfl":
        return json.loads(path.read_text()), []
    embedded = []
    with zipfile.ZipFile(path, "r") as z:
        flow = json.loads(z.read("flow").decode("utf-8"))
        for info in z.infolist():
            if info.filename.startswith("Data/"):
                embedded.append((info.filename, z.read(info.filename)))
    return flow, embedded


def slugify(name: str) -> str:
    """Tableau allows duplicates — we'll de-dup downstream."""
    s = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").lower()
    return s or "node"


def build_dag(flow: dict) -> tuple[dict, dict, dict, list[str]]:
    """Return (nodes_by_id, fwd, rev, topological_order)."""
    nodes = flow["nodes"]
    fwd, rev = defaultdict(list), defaultdict(list)
    for nid, n in nodes.items():
        for nxt in n.get("nextNodes") or []:
            nid2 = nxt["nextNodeId"]
            fwd[nid].append(nid2)
            rev[nid2].append(nid)

    # Kahn topological sort, deterministic by initialNodes order
    indeg = {nid: len(rev[nid]) for nid in nodes}
    q = deque(flow["initialNodes"])
    order = []
    visited = set()
    while q:
        nid = q.popleft()
        if nid in visited:
            continue
        visited.add(nid)
        order.append(nid)
        for nxt in fwd[nid]:
            indeg[nxt] -= 1
            if indeg[nxt] == 0 and nxt not in visited:
                q.append(nxt)
    # Catch any disconnected nodes (rare)
    for nid in nodes:
        if nid not in visited:
            order.append(nid)
    return nodes, dict(fwd), dict(rev), order


# ─── Node-type → VDP template mapping (per SKILL.md) ─────────────────────
def node_to_template(n: dict) -> str:
    nt = n.get("nodeType", "")
    if nt in (".v1.LoadCsv", ".v1.LoadExcel", ".v1.LoadSql", ".v1.LoadHyper"):
        return "source"
    if nt == ".v2018_2_3.SuperJoin":
        return "join"
    if nt == ".v2018_2_3.SuperAggregate":
        return "aggregate"
    if nt == ".v2018_2_3.SuperUnion":
        return "combine"
    if nt == ".v2018_3_3.SuperPivot":
        return "pivot"
    if nt == ".v2018_3_4.SuperUnpivotExtended":
        return "sql"  # multi-column unpivot → SQL UNPIVOT
    if nt in (".v1.PublishExtract", ".v1.WriteToHyper", ".v1.WriteToCsv"):
        return "output"
    if nt == ".v1.Container":
        # Decide based on inner action types
        return _classify_container(n)
    return "transform"  # safe default


def _container_actions(n: dict) -> list[dict]:
    lc = n.get("loomContainer") or {}
    sub = lc.get("nodes", {})
    init = lc.get("initialNodes") or []
    seen, order = set(), []
    def dfs(x):
        if x in seen or x not in sub:
            return
        seen.add(x)
        for nx in sub[x].get("nextNodes") or []:
            dfs(nx["nextNodeId"])
        order.append(x)
    for s in init:
        dfs(s)
    order.reverse()
    return [sub[a] for a in order]


def _classify_container(n: dict) -> str:
    """Container → 'transform' (renames/types/computes), 'filter' (pure filter)
    or 'sql' (mixed). Empty containers become 'transform' pass-throughs."""
    actions = _container_actions(n)
    if not actions:
        return "transform"
    has_filter   = any(a.get("nodeType") in (".v1.FilterOperation", ".v1.ValueFilter",
                                              ".v1.CalculatedFilter", ".v1.RangeFilter") for a in actions)
    has_compute  = any(a.get("nodeType") in (".v1.AddColumn", ".v1.RenameColumn",
                                              ".v1.RemoveColumns", ".v1.ChangeColumnType",
                                              ".v2018_3_3.Remap") for a in actions)
    if has_filter and not has_compute:
        return "filter"
    if not has_filter and has_compute:
        return "transform"
    return "sql"  # mixed → safer to emit one SQL operator


# ─── Builders for each operator's `config` block ─────────────────────────
def _strip_brackets(expr: str) -> str:
    """Tableau column refs are `[col]` — strip brackets and backtick-quote."""
    if not isinstance(expr, str):
        return str(expr)
    out = re.sub(r"\[([^\[\]]+)\]", lambda m: f"`{m.group(1)}`", expr)
    return out


def _tableau_expr_to_spark(expr: str) -> str:
    """Convert the most common Tableau formula functions to Spark SQL."""
    if not isinstance(expr, str):
        return str(expr)
    s = _strip_brackets(expr)
    # IFNULL(x, y) → COALESCE(x, y)
    s = re.sub(r"\bIFNULL\b", "COALESCE", s, flags=re.IGNORECASE)
    # ISNULL(x) → x IS NULL — but only in standalone calls; we let Spark resolve it as-is.
    # IIF(c, t, f) → IF(c, t, f) (Spark supports IF)
    s = re.sub(r"\bIIF\b", "IF", s, flags=re.IGNORECASE)
    # STR(x) → CAST(x AS STRING)
    s = re.sub(r"\bSTR\s*\(\s*([^()]*)\s*\)", r"CAST(\1 AS STRING)", s, flags=re.IGNORECASE)
    # INT(x) → CAST(x AS INT)
    s = re.sub(r"\bINT\s*\(\s*([^()]*)\s*\)", r"CAST(\1 AS INT)", s, flags=re.IGNORECASE)
    # `IF cond THEN a ELSEIF b THEN c ELSE d END` is already valid Spark CASE-ish via IF,
    # but Tableau uses ELSEIF / END. Convert "ELSEIF" → "ELSE IF" and unwrap.
    if re.search(r"\bIF\b.*\bTHEN\b", s, flags=re.IGNORECASE):
        s = re.sub(r"\bELSEIF\b", "ELSE IF", s, flags=re.IGNORECASE)
    return s


def _join_type(tableau_type: str) -> str:
    return {
        "left": "left", "right": "right", "inner": "inner", "full": "full",
        "leftOnly": "left_anti", "rightOnly": "right_anti",
    }.get(tableau_type, "inner")


def _join_conds(conds: list[dict]) -> str:
    parts = []
    for c in conds:
        l = _strip_brackets(c.get("leftExpression", ""))
        r = _strip_brackets(c.get("rightExpression", ""))
        cmp = "=" if c.get("comparator") in ("==", "=") else c.get("comparator", "=")
        parts.append(f"left.{l} {cmp} right.{r}")
    return " AND ".join(parts)


def _build_source_config(n: dict, extracted_root: str) -> dict:
    nt = n.get("nodeType", "")
    if nt == ".v1.LoadSql":
        # No reachable connection — assume synthetic CSV named after the node.
        slug = slugify(n.get("name", "source"))
        return {"file_source": {
            "path": f"{extracted_root}/synthetic/{slug}.csv",
            "format": "csv", "header": True, "inferSchema": True,
        }}
    if nt == ".v1.LoadExcel":
        # Excel files need to be exported to CSV first; reference the .csv.
        slug = slugify(n.get("name", "source"))
        return {"file_source": {
            "path": f"{extracted_root}/synthetic/{slug}.csv",
            "format": "csv", "header": True, "inferSchema": True,
        }}
    if nt == ".v1.LoadCsv":
        # Real embedded CSV — preserve original filename.
        ca = n.get("connectionAttributes", {}) or {}
        fname = ca.get("filename") or f"{slugify(n.get('name','source'))}.csv"
        return {"file_source": {
            "path": f"{extracted_root}/{fname}",
            "format": "csv", "header": True, "inferSchema": True,
        }}
    return {"file_source": {
        "path": f"{extracted_root}/synthetic/{slugify(n.get('name','source'))}.csv",
        "format": "csv", "header": True, "inferSchema": True,
    }}


def _build_join_config(n: dict) -> dict:
    an = n.get("actionNode") or {}
    jt = _join_type(an.get("joinType", "inner"))
    cond = _join_conds(an.get("conditions", []))
    cfg = {"join_type": jt, "join_conditions": cond, "expressions": []}
    if jt == "right_anti":
        # right anti: anti from the right side — swap inputs at wiring time
        cfg["_swap_inputs"] = True
    return cfg


def _build_aggregate_config(n: dict) -> dict:
    an = n.get("actionNode") or {}
    group_bys = []
    for gb in an.get("groupByFields") or []:
        col = _strip_brackets(gb.get("columnName", ""))
        if col:
            group_bys.append({"expr": col, "type": "expr"})
    aggregations = []
    fn_map = {"GroupBy": None, "SUM": "SUM", "AVG": "AVG", "COUNT": "COUNT",
              "CountDistinct": "COUNT", "MIN": "MIN", "MAX": "MAX",
              "MEDIAN": "MEDIAN", "STDEV": "STDDEV"}
    for ag in an.get("aggregateFields") or []:
        col = _strip_brackets(ag.get("columnName", ""))
        fn  = fn_map.get(ag.get("function"), ag.get("function"))
        if not col or fn is None:
            continue
        alias = _strip_brackets(ag.get("newColumnName") or col).strip("`")
        aggregations.append({
            "columnExpr": {"expr": col, "type": "expr"},
            "fn": fn,
            "alias": alias,
        })
    return {"group_bys": group_bys, "aggregations": aggregations}


def _build_combine_config(_n: dict) -> dict:
    return {"operator": "UNION", "quantifier": "ALL"}


def _build_pivot_config(n: dict) -> dict:
    an = n.get("actionNode") or {}
    pivot_col = _strip_brackets(an.get("pivotColumnName", "")).strip("`")
    value_col = _strip_brackets(an.get("aggregateColumnName", "")).strip("`")
    grouping  = [_strip_brackets(g.get("columnName", "")).strip("`")
                 for g in an.get("pivotGroupingColumns") or []]
    fn = (an.get("defaultAggregation") or "count").lower()
    if fn == "countd":
        fn = "countDistinct"
    return {
        "mode": "pivot",
        "group_by":     grouping or [],
        "pivot_column": pivot_col,
        "value_column": value_col,
        "agg":          fn,
    }


def _build_unpivot_sql(n: dict, upstream_name: str) -> dict:
    """SuperUnpivotExtended → SQL UNPIVOT statement (multi-column-aware)."""
    an = n.get("actionNode") or {}
    grp = an.get("unpivotGroup") or {}
    if not grp:
        # Smart-defaults unpivot — emit a passthrough; user must edit
        return {"query": f"SELECT * FROM {upstream_name}"}
    lit_col = grp.get("literalColumn", {})
    key_name = lit_col.get("literalColumnName", "Pivot_Key")
    aliases = lit_col.get("literals") or lit_col.get("names") or []
    unpiv_cols = grp.get("unpivotColumns") or []
    val_names = [u["unpivotColumnName"] for u in unpiv_cols]
    bindings = [u["columnInformation"]["manualBindings"] for u in unpiv_cols]
    if not val_names or not bindings:
        return {"query": f"SELECT * FROM {upstream_name}"}
    # Build IN-list rows: each alias maps to one tuple of source columns
    rows = []
    n_rows = len(aliases) if aliases else len(bindings[0])
    for i in range(n_rows):
        cols = [bindings[k][i] for k in range(len(val_names))]
        alias = aliases[i] if i < len(aliases) else f"col_{i}"
        if len(cols) == 1:
            rows.append(f"`{cols[0]}` AS '{alias}'")
        else:
            rows.append("(" + ", ".join(f"`{c}`" for c in cols) + f") AS '{alias}'")
    if len(val_names) == 1:
        unpivot_clause = (
            f"UNPIVOT (\n  `{val_names[0]}` FOR `{key_name}` IN (\n    "
            + ",\n    ".join(rows) + "\n  )\n)"
        )
    else:
        unpivot_clause = (
            "UNPIVOT (\n  ("
            + ", ".join(f"`{v}`" for v in val_names) + ")"
            + f"\n  FOR `{key_name}` IN (\n    "
            + ",\n    ".join(rows) + "\n  )\n)"
        )
    return {"query": f"SELECT * FROM {upstream_name}\n{unpivot_clause}"}


def _build_container_config(n: dict, template: str, upstream_name: str) -> dict:
    """Flatten container actions into a transform / filter / sql config."""
    actions = _container_actions(n)
    if not actions:
        # Empty container → pass-through transform
        return {"expressions": ["*"]}

    if template == "filter":
        # Concat all filter conditions with AND.
        clauses = []
        for a in actions:
            nt = a.get("nodeType", "")
            if nt == ".v1.FilterOperation":
                clauses.append(_tableau_expr_to_spark(a.get("filterExpression", "")))
            elif nt == ".v1.ValueFilter":
                values = a.get("values") or {}
                exclude = bool(a.get("exclude"))
                for col, vals in values.items():
                    quoted = ", ".join(v if v is not None else "NULL" for v in vals)
                    op = "NOT IN" if exclude else "IN"
                    clauses.append(f"`{col}` {op} ({quoted})")
            elif nt == ".v1.CalculatedFilter":
                clauses.append(_tableau_expr_to_spark(a.get("expression", "")))
            elif nt == ".v1.RangeFilter":
                col, mn, mx = a.get("columnName"), a.get("min"), a.get("max")
                if col is not None:
                    clauses.append(f"`{col}` BETWEEN {mn} AND {mx}")
        return {"condition": " AND ".join(c for c in clauses if c) or "true"}

    if template == "transform":
        exprs = ["*"]
        rename_map: dict[str, str] = {}
        type_casts: list[str] = []
        adds: list[str] = []
        removes: list[str] = []
        for a in actions:
            nt = a.get("nodeType", "")
            if nt == ".v1.AddColumn":
                col  = a["columnName"]
                expr = _tableau_expr_to_spark(a["expression"])
                adds.append(f"{expr} AS `{col}`")
            elif nt == ".v1.RenameColumn":
                rename_map[a["columnName"]] = a["rename"]
            elif nt == ".v1.RemoveColumns":
                removes.extend(a.get("columnNames") or [])
            elif nt == ".v1.ChangeColumnType":
                fields = a.get("fields") or {}
                for col, spec in fields.items():
                    t = spec.get("type", "string").upper()
                    spark_t = {"INTEGER": "INT", "REAL": "DOUBLE",
                               "STRING": "STRING", "DATE": "DATE",
                               "DATETIME": "TIMESTAMP", "BOOLEAN": "BOOLEAN"}.get(t, t)
                    type_casts.append(f"CAST(`{col}` AS {spark_t}) AS `{col}`")
            elif nt == ".v2018_3_3.Remap":
                col = a["columnName"]
                values = a.get("values") or {}
                cases = []
                for new_v, old_vs in values.items():
                    nv = new_v.strip('"')
                    for o in old_vs:
                        if o is None or o == "null":
                            cases.append(f"WHEN `{col}` IS NULL THEN '{nv}'")
                        else:
                            ov = str(o).strip('"')
                            cases.append(f"WHEN `{col}` = '{ov}' THEN '{nv}'")
                if cases:
                    case_expr = "CASE " + " ".join(cases) + f" ELSE `{col}` END AS `{col}`"
                    adds.append(case_expr)
        # Compose the expression list
        exprs = []
        if removes:
            removed = ", ".join(f"`{r}`" for r in removes)
            exprs.append(f"* EXCEPT ({removed})")
        else:
            exprs.append("*")
        for old, new in rename_map.items():
            exprs.append(f"`{old}` AS `{new}`")
        exprs.extend(type_casts)
        exprs.extend(adds)
        return {"expressions": exprs}

    # template == "sql": emit a single SQL with everything
    # (Heuristic — keeps the operator count low for mixed containers.)
    where_clauses, exprs = [], []
    for a in actions:
        nt = a.get("nodeType", "")
        if nt == ".v1.FilterOperation":
            where_clauses.append(_tableau_expr_to_spark(a.get("filterExpression", "")))
        elif nt == ".v1.ValueFilter":
            for col, vals in (a.get("values") or {}).items():
                quoted = ", ".join(v if v is not None else "NULL" for v in vals)
                op = "NOT IN" if a.get("exclude") else "IN"
                where_clauses.append(f"`{col}` {op} ({quoted})")
        elif nt == ".v1.AddColumn":
            exprs.append(f"{_tableau_expr_to_spark(a['expression'])} AS `{a['columnName']}`")
        elif nt == ".v1.RenameColumn":
            exprs.append(f"`{a['columnName']}` AS `{a['rename']}`")
        elif nt == ".v1.ChangeColumnType":
            for col, spec in (a.get("fields") or {}).items():
                t = spec.get("type", "string").upper()
                spark_t = {"INTEGER": "INT", "REAL": "DOUBLE", "STRING": "STRING",
                           "DATE": "DATE", "DATETIME": "TIMESTAMP", "BOOLEAN": "BOOLEAN"}.get(t, t)
                exprs.append(f"CAST(`{col}` AS {spark_t}) AS `{col}`")
    select = ", ".join(["*"] + exprs) if exprs else "*"
    where  = " AND ".join(where_clauses) if where_clauses else "1=1"
    return {"query": f"SELECT {select} FROM {upstream_name} WHERE {where}"}


def _build_output_config(n: dict, fallback_table: str) -> dict:
    nt = n.get("nodeType", "")
    parts = fallback_table.split(".")
    catalog = parts[0] if len(parts) >= 3 else ""
    schema  = parts[1] if len(parts) >= 2 else "default"
    table   = parts[-1]
    if nt == ".v1.PublishExtract":
        # Use datasourceName to differentiate multiple publish sinks
        ds = n.get("datasourceName")
        if ds:
            table = re.sub(r"[^A-Za-z0-9_]+", "_", ds).strip("_").lower()
    elif nt == ".v1.WriteToCsv":
        table = f"{table}_csv"
    elif nt == ".v1.WriteToHyper":
        table = f"{table}_hyper"
    return {"catalog": catalog, "schema": schema, "table_name": table}


# ───────────────────────────────────────────────────────────────────────────
# CELL RENDERERS (unchanged from build_designer_ipynb_reference.py)
# ───────────────────────────────────────────────────────────────────────────
def yaml_indent(s, prefix):
    return "\n".join(prefix + line for line in s.splitlines())


def render_yaml_config(config: dict, indent: str = "  ") -> str:
    def _scalar(v):
        if isinstance(v, bool):  return "true" if v else "false"
        if v is None:            return "null"
        if isinstance(v, (int, float)): return str(v)
        s = str(v)
        if "\n" in s:
            return "|\n" + yaml_indent(s, indent + "  ")
        if any(c in s for c in [":", "#", "{", "}", "[", "]", ","]) or s.strip() != s:
            return json.dumps(s)
        return s

    def _render(obj, ind):
        if isinstance(obj, dict):
            out = []
            for k, v in obj.items():
                if k.startswith("_"):
                    continue   # private wiring hints
                if isinstance(v, dict):
                    out.append(f"{ind}{k}:")
                    out.append(_render(v, ind + "  "))
                elif isinstance(v, list):
                    if not v:
                        out.append(f"{ind}{k}: []")
                    else:
                        out.append(f"{ind}{k}:")
                        for item in v:
                            if isinstance(item, dict):
                                first = True
                                for kk, vv in item.items():
                                    pre = f"{ind}- " if first else f"{ind}  "
                                    if isinstance(vv, dict):
                                        out.append(f"{pre}{kk}:")
                                        out.append(_render(vv, ind + "      "))
                                    else:
                                        out.append(f"{pre}{kk}: {_scalar(vv)}")
                                    first = False
                            else:
                                out.append(f"{ind}- {_scalar(item)}")
                else:
                    out.append(f"{ind}{k}: {_scalar(v)}")
            return "\n".join(out)
        return _scalar(obj)
    return _render(config, indent)


def render_yaml_inputs(inputs: list[tuple[str, str, str]]) -> str:
    if not inputs:
        return "input: []"
    lines = ["input:"]
    for port, uid, uout in inputs:
        lines.append(f"  - node: {uid}")
        lines.append(f"    input_port: {port}")
        lines.append(f"    output_port: {uout}")
    return "\n".join(lines)


def render_inputs_wiring(spec: dict) -> str:
    if not spec["inputs"]:
        return "inputs = {}"
    if spec.get("input_is_sql_list"):
        lines = ["inputs = {", "    \"data\": ["]
        for _, uid, uout in spec["inputs"]:
            lines.append(f"        ctx[\"{uid}.{uout}\"],")
        lines.append("    ],")
        lines.append("    \"data__sources\": [")
        for _, uid, uout in spec["inputs"]:
            lines.append(
                f"        {{\"node\": \"{uid}\", \"output_port\": \"{uout}\", "
                f"\"name\": \"{uid}\", \"df_name\": \"{uid}\"}},"
            )
        lines.append("    ],")
        lines.append("}")
        return "\n".join(lines)
    lines = ["inputs = {"]
    for port, uid, uout in spec["inputs"]:
        lines.append(f"    \"{port}\": ctx[\"{uid}.{uout}\"],")
    lines.append("}")
    return "\n".join(lines)


def render_ctx_assign(spec: dict) -> str:
    if not spec["output_ports"]:
        return "out = run(config, inputs, spark)"
    lines = ["out = run(config, inputs, spark)"]
    for port in spec["output_ports"]:
        lines.append(f"ctx[\"{spec['id']}.{port}\"] = out[\"{port}\"]")
    return "\n".join(lines)


def make_code_cell(spec: dict) -> dict:
    cfg_for_yaml = {k: v for k, v in spec["config"].items() if not k.startswith("_")}
    cfg_yaml = render_yaml_config(cfg_for_yaml)
    inputs_yaml = render_yaml_inputs(spec["inputs"])

    desc = json.dumps(spec.get("description", ""))
    name_yaml = json.dumps(spec["name"]) if any(c in spec["name"] for c in [":", "#"]) else spec["name"]
    docstring = f'''"""
id: {spec["id"]}
template: {spec["template"]}
name: {name_yaml}
position:
  x: {spec["position"][0]}
  y: {spec["position"][1]}
description:
  text: {desc}
previewMode: "1000"
config:
{cfg_yaml}
{inputs_yaml}
"""'''
    body = RUN_BODIES[spec["template"]]
    source_text = (
        docstring + "\n\n"
        + body.strip("\n") + "\n\n"
        "# generated from the system\n"
        "ctx = globals().setdefault(\"ctx\", {})\n"
        + "config = " + json.dumps(cfg_for_yaml, indent=4) + "\n"
        + render_inputs_wiring(spec) + "\n"
        + render_ctx_assign(spec)
    )
    return {
        "cell_type": "code",
        "execution_count": 0,
        "metadata": {"application/vnd.databricks.v1+cell": {
            "cellMetadata": {"byteLimit": 2048000, "rowLimit": 10000},
            "inputWidgets": {}, "nuid": str(uuid.uuid4()),
            "showTitle": False, "tableResultSettingsMap": {}, "title": "",
        }},
        "outputs": [],
        "source": [line + "\n" for line in source_text.splitlines()],
    }


def make_markdown_cell(spec: dict) -> dict:
    src = (
        "---\n"
        f"id: {spec['id']}\n"
        f"template: markdown\n"
        f"name: {spec['name']}\n"
        f"position:\n  x: {spec['position'][0]}\n  y: {spec['position'][1]}\n"
        f"dimensions:\n  width: {spec['dimensions'][0]}\n  height: {spec['dimensions'][1]}\n"
        "config:\n"
        "  md: |\n"
        + "\n".join("    " + line for line in spec["md"].splitlines())
        + "\n---"
    )
    return {
        "cell_type": "markdown",
        "metadata": {"application/vnd.databricks.v1+cell": {
            "cellMetadata": {}, "inputWidgets": {}, "nuid": str(uuid.uuid4()),
            "showTitle": False, "tableResultSettingsMap": {}, "title": "",
        }},
        "source": [line + "\n" for line in src.splitlines()],
    }


# ───────────────────────────────────────────────────────────────────────────
# CONVERTER ENTRY POINT
# ───────────────────────────────────────────────────────────────────────────
def convert(tflx_path: Path, out_path: Path,
            extracted_data: str = "/Workspace/extracted_data",
            output_table:   str = "main.default.flow_output") -> dict:
    flow, _embedded = parse_tflx(tflx_path)
    nodes, fwd, rev, order = build_dag(flow)

    # Assign unique cell ids (Tableau allows duplicate names → suffix)
    name_counts: dict[str, int] = defaultdict(int)
    nid_to_id: dict[str, str] = {}
    for nid in order:
        base = slugify(nodes[nid].get("name", "node"))
        name_counts[base] += 1
        nid_to_id[nid] = base if name_counts[base] == 1 else f"{base}_{name_counts[base]}"

    # Layout: simple lane-based grid
    LANE_H, STAGE_W = 145, 260
    depth: dict[str, int] = {}
    for nid in order:
        depth[nid] = max((depth[p] + 1 for p in rev.get(nid, [])), default=0)
    lane_at_depth: dict[int, int] = defaultdict(int)
    pos: dict[str, tuple[int, int]] = {}
    for nid in order:
        d = depth[nid]
        lane = lane_at_depth[d]
        pos[nid] = (d * STAGE_W, lane * LANE_H)
        lane_at_depth[d] += 1

    # Build cell specs
    cells: list[dict] = []
    cells.append({
        "kind": "markdown", "id": "pipeline_header",
        "name": tflx_path.stem,
        "position": (0, -260), "dimensions": (700, 220),
        "md": (
            f"# {tflx_path.stem}\n\n"
            f"**Converted from `{tflx_path.name}`** by `convert_tflx.py` "
            f"(tableau-to-lakeflow-designer skill).\n\n"
            f"- {len(nodes)} nodes in the original Tableau Prep flow\n"
            f"- Output table: `{output_table}`\n"
        ),
    })

    for nid in order:
        n = nodes[nid]
        tpl = node_to_template(n)
        cell_id = nid_to_id[nid]
        in_specs: list[tuple[str, str, str]] = []
        in_ports = IN_PORTS[tpl]
        parents = rev.get(nid, [])

        # config + wiring per template
        if tpl == "source":
            cfg = _build_source_config(n, extracted_data)
        elif tpl == "join":
            cfg = _build_join_config(n)
        elif tpl == "aggregate":
            cfg = _build_aggregate_config(n)
        elif tpl == "combine":
            cfg = _build_combine_config(n)
        elif tpl == "pivot":
            cfg = _build_pivot_config(n)
        elif tpl == "sql":
            # Only one upstream → use its name as the table identifier
            up = parents[0] if parents else ""
            up_name = nid_to_id.get(up, up)
            if n.get("nodeType") == ".v2018_3_4.SuperUnpivotExtended":
                cfg = _build_unpivot_sql(n, up_name)
            else:
                cfg = _build_container_config(n, "sql", up_name)
        elif tpl in ("transform", "filter"):
            up = parents[0] if parents else ""
            up_name = nid_to_id.get(up, up)
            cfg = _build_container_config(n, tpl, up_name)
        elif tpl == "output":
            cfg = _build_output_config(n, output_table)
        else:
            cfg = {}

        # Wire inputs
        if tpl == "join":
            # Tableau's parents come in DAG order; first parent = left, second = right.
            # `_swap_inputs` flag set for right-only joins.
            if len(parents) >= 2:
                left, right = parents[0], parents[1]
                if cfg.pop("_swap_inputs", False):
                    left, right = right, left
                in_specs.append(("left",  nid_to_id[left],  OUT_PORT[node_to_template(nodes[left])]  or "data"))
                in_specs.append(("right", nid_to_id[right], OUT_PORT[node_to_template(nodes[right])] or "data"))
            elif len(parents) == 1:
                in_specs.append(("left", nid_to_id[parents[0]],
                                 OUT_PORT[node_to_template(nodes[parents[0]])] or "data"))
        elif tpl == "combine":
            for i, p in enumerate(parents[:2]):
                in_specs.append((f"data_{i}", nid_to_id[p],
                                 OUT_PORT[node_to_template(nodes[p])] or "data"))
        elif tpl == "sql":
            # SQL takes a list of inputs
            for p in parents:
                in_specs.append(("data", nid_to_id[p],
                                 OUT_PORT[node_to_template(nodes[p])] or "data"))
        elif in_ports:
            for port in in_ports[: len(parents) or 1]:
                if not parents:
                    break
                p = parents[0]
                in_specs.append((port, nid_to_id[p],
                                 OUT_PORT[node_to_template(nodes[p])] or "data"))

        spec = {
            "kind": "code",
            "template": tpl,
            "id": cell_id,
            "name": cell_id,
            "position": pos[nid],
            "config": cfg,
            "inputs": in_specs,
            "output_ports": [OUT_PORT[tpl]] if OUT_PORT[tpl] else [],
            "description": n.get("name", ""),
        }
        if tpl == "sql":
            spec["input_is_sql_list"] = True
        cells.append(spec)

    # Render
    rendered = []
    for spec in cells:
        if spec["kind"] == "markdown":
            rendered.append(make_markdown_cell(spec))
        else:
            rendered.append(make_code_cell(spec))

    notebook = {
        "cells": rendered,
        "metadata": {
            "application/vnd.databricks.v1+notebook": {
                "computePreferences": {
                    "hardware":  {"accelerator": None, "gpuPoolId": None, "memory": None},
                    "software":  {"pinSparkToX86": None},
                },
                "dashboards": [],
                "environmentMetadata": {"base_environment": "", "environment_version": "5"},
                "inputWidgetPreferences": None,
                "language": "python",
                "notebookMetadata": {},
                "notebookName": out_path.name,
                "widgets": {},
            },
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 0,
    }
    out_path.write_text(json.dumps(notebook, indent=1))

    # Stats per template
    stats: dict[str, int] = defaultdict(int)
    for spec in cells:
        if spec["kind"] == "code":
            stats[spec["template"]] += 1
    return {"cells": len(rendered), "operators": dict(stats)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("tflx", type=Path, help="Path to the .tflx (or .tfl) flow file")
    ap.add_argument("--out", type=Path, default=None,
                    help="Output .designer.ipynb path (default: alongside the .tflx)")
    ap.add_argument("--extracted-data", default="/Workspace/extracted_data",
                    help="Workspace/Volume root that holds the extracted CSVs")
    ap.add_argument("--output-table", default="main.default.flow_output",
                    help="catalog.schema.table for the final output sink")
    args = ap.parse_args()

    out = args.out or args.tflx.with_suffix(".designer.ipynb")
    res = convert(args.tflx, out, args.extracted_data, args.output_table)
    print(f"Wrote {out}")
    print(f"  cells:     {res['cells']}")
    print(f"  operators: {dict(sorted(res['operators'].items()))}")


if __name__ == "__main__":
    sys.exit(main() or 0)
