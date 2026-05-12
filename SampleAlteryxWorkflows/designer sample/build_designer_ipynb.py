"""Build the customer_feedback.designer.ipynb from a list of cell specs.

Each operator cell follows the verified Designer format:
- A triple-quoted YAML docstring describing the operator (id/template/name/position/config/input)
- A `def run(config, inputs, spark)` body (the canonical implementation per template)
- A wiring block at the bottom that builds config + inputs from `ctx` and stores the output.

This script is the canonical output of applying the alteryx-to-vdp skill to
customer_feedback.yxmd.
"""
import json
import uuid
from pathlib import Path

OUT = Path(__file__).parent / "customer_feedback.designer.ipynb"

# ---------- canonical run-body templates per operator ----------

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
    "source": RUN_SOURCE,
    "filter": RUN_FILTER,
    "transform": RUN_TRANSFORM,
    "ai_function": RUN_AI_FUNCTION,
    "combine": RUN_COMBINE,
    "aggregate": RUN_AGGREGATE,
    "join": RUN_JOIN,
    "pivot": RUN_PIVOT,
    "sort": RUN_SORT,
    "limit": RUN_LIMIT,
    "sql": RUN_SQL,
    "python": RUN_PYTHON,
    "output": RUN_OUTPUT,
}

# ---------- per-operator cell specifications ----------
# Each spec: (template, name, position, config, inputs_wiring, output_port_used_by_downstream)
# inputs_wiring: list of (port_on_this, upstream_id, upstream_port_out)

CELLS = [
    # group: AI Enrichment branch (visual container)
    # Note: group YAML schema is best-guess — no group cell appeared in the verified
    # exports. If Designer rejects this on import, delete this cell; the rest of the
    # pipeline still works.
    {
        "kind": "code",
        "is_group": True,
        "template": "group",
        "id": "ai_branch_group",
        "name": "AI Enrichment Branch",
        "position": (240, -40),
        "dimensions": (1620, 240),
        "description": "Visual container around the AI enrichment branch.",
    },

    # markdown header (cell_type=markdown)
    {
        "kind": "markdown",
        "id": "pipeline_header",
        "name": "Customer Feedback Pipeline",
        "position": (0, -260),
        "dimensions": (560, 220),
        "md": (
            "# Customer Feedback Pipeline\n\n"
            "**Converted from `customer_feedback.yxmd`** using the `alteryx-to-vdp` skill.\n\n"
            "Combines current + historical feedback CSVs, enriches with AI (sentiment, extraction, masking),\n"
            "joins to the products catalog, aggregates per category, ranks, and writes the gold table.\n\n"
            "**Inputs**\n"
            "- `/Volumes/aldi_aus/aldi_us/test/current_feedback.csv`\n"
            "- `/Volumes/aldi_aus/aldi_us/test/historical_feedback.csv`\n"
            "- `aldi_aus.aldi_us.products`\n\n"
            "**Output**\n"
            "- `aldi_aus.aldi_us.gold_feedback_summary`\n"
        ),
    },

    # --- Sources ---
    {
        "kind": "code", "template": "source", "id": "src_current", "name": "src_current",
        "position": (0, 0),
        "config": {
            "file_source": {
                "path": "/Volumes/aldi_aus/aldi_us/test/current_feedback.csv",
                "format": "csv", "header": True, "inferSchema": True,
            }
        },
        "inputs": [],
        "output_ports": ["data"],
        "description": "Read current feedback CSV from the UC Volume.",
    },
    {
        "kind": "code", "template": "source", "id": "src_historical", "name": "src_historical",
        "position": (0, 145),
        "config": {
            "file_source": {
                "path": "/Volumes/aldi_aus/aldi_us/test/historical_feedback.csv",
                "format": "csv", "header": True, "inferSchema": True,
            }
        },
        "inputs": [],
        "output_ports": ["data"],
        "description": "Read historical feedback CSV from the UC Volume.",
    },
    {
        "kind": "code", "template": "source", "id": "src_products", "name": "src_products",
        "position": (0, 720),
        "config": {"table_source": {"tableName": "aldi_aus.aldi_us.products"}},
        "inputs": [],
        "output_ports": ["data"],
        "description": "Read the products catalog from Unity Catalog.",
    },

    # --- Combine current + historical ---
    {
        "kind": "code", "template": "combine", "id": "combine_all", "name": "combine_all",
        "position": (260, 70),
        "config": {"operator": "UNION", "quantifier": "ALL"},
        "inputs": [
            ("data_0", "src_current", "data"),
            ("data_1", "src_historical", "data"),
        ],
        "output_ports": ["combined_data"],
        "description": "Union all feedback (current + historical), keeping duplicates.",
    },

    # --- Filter valid ---
    {
        "kind": "code", "template": "filter", "id": "filter_valid", "name": "filter_valid",
        "position": (520, 70),
        "config": {"condition": "rating BETWEEN 1 AND 5 AND comment IS NOT NULL"},
        "inputs": [("data", "combine_all", "combined_data")],
        "output_ports": ["filtered_data"],
        "description": "Keep only valid feedback rows.",
    },

    # --- Transform: cast types, add active flag ---
    {
        "kind": "code", "template": "transform", "id": "transform_clean", "name": "transform_clean",
        "position": (780, 70),
        "config": {
            "expressions": [
                "CAST(customer_id AS INT) AS `customer_id`",
                "CAST(product_id AS INT) AS `product_id`",
                "CAST(rating AS INT) AS `rating`",
                "comment",
                "TO_DATE(feedback_date, 'yyyy-MM-dd') AS `feedback_date`",
                "CASE WHEN rating >= 4 THEN 'positive' WHEN rating = 3 THEN 'neutral' ELSE 'negative' END AS `rating_band`",
            ]
        },
        "inputs": [("data", "filter_valid", "filtered_data")],
        "output_ports": ["transformed_data"],
        "description": "Cast types, derive a rating_band column.",
    },

    # --- AI enrichment: sentiment + extraction + masking ---
    {
        "kind": "code", "template": "ai_function", "id": "ai_enrich", "name": "ai_enrich",
        "position": (1040, 70),
        "config": {
            "expressions": [
                "ai_analyze_sentiment(comment) AS `sentiment`",
                "ai_extract(comment, ARRAY('issue_type','severity')) AS `extracted_issue`",
                "ai_mask(comment, ARRAY('EMAIL','PHONE','PERSON')) AS `comment_masked`",
            ]
        },
        "inputs": [("data", "transform_clean", "transformed_data")],
        "output_ports": ["ai_data"],
        "description": "Run sentiment, structured extraction, and PII masking on the comment column.",
    },

    # --- Python: weighted score ---
    {
        "kind": "code", "template": "python", "id": "py_score", "name": "py_score",
        "position": (1300, 70),
        "config": {
            "code": (
                "from pyspark.sql.functions import col, when, lit\n"
                "df = inputs[\"data\"][0]\n"
                "sentiment_weight = when(col(\"sentiment\") == \"positive\", lit(1.0)) \\\n"
                "                  .when(col(\"sentiment\") == \"negative\", lit(-1.0)) \\\n"
                "                  .otherwise(lit(0.0))\n"
                "result = df.withColumn(\"weighted_score\", col(\"rating\") * (lit(1.0) + sentiment_weight))\n"
            )
        },
        "inputs": [("data", "ai_enrich", "ai_data")],
        "output_ports": ["result"],
        "description": "Compute a weighted feedback score from rating + sentiment.",
        "input_is_list": True,
    },

    # --- SQL: dedupe by (customer_id, product_id), keep latest by feedback_date ---
    {
        "kind": "code", "template": "sql", "id": "sql_dedupe", "name": "sql_dedupe",
        "position": (1560, 70),
        "config": {
            "query": (
                "SELECT * EXCEPT (_dedup_rn) FROM (\n"
                "  SELECT *,\n"
                "         ROW_NUMBER() OVER (PARTITION BY customer_id, product_id ORDER BY feedback_date DESC) AS _dedup_rn\n"
                "  FROM py_score\n"
                ") WHERE _dedup_rn = 1\n"
            )
        },
        "inputs": [("data", "py_score", "result")],
        "output_ports": ["result"],
        "description": "Deduplicate to the latest feedback per (customer, product).",
        "input_is_sql_list": True,
    },

    # --- Join feedback x products ---
    {
        "kind": "code", "template": "join", "id": "join_products", "name": "join_products",
        "position": (1820, 300),
        "config": {
            "join_type": "left",
            "join_conditions": "left.product_id = right.product_id",
            "expressions": [],
        },
        "inputs": [
            ("left", "sql_dedupe", "result"),
            ("right", "src_products", "data"),
        ],
        "output_ports": ["joined_data"],
        "description": "Left join the deduped feedback with the products catalog.",
    },

    # --- Aggregate per category ---
    {
        "kind": "code", "template": "aggregate", "id": "aggregate_by_category", "name": "aggregate_by_category",
        "position": (2080, 300),
        "config": {
            "group_bys": [{"expr": "category", "type": "expr"}],
            "aggregations": [
                {"columnExpr": {"expr": "weighted_score", "type": "expr"}, "fn": "AVG", "alias": "avg_weighted_score"},
                {"columnExpr": {"expr": "rating",         "type": "expr"}, "fn": "AVG", "alias": "avg_rating"},
                {"columnExpr": {"expr": "rating",         "type": "expr"}, "fn": "COUNT", "alias": "feedback_count"},
                {"columnExpr": {"expr": "weighted_score", "type": "expr"}, "fn": "MEDIAN", "alias": "median_weighted_score"},
                {"columnExpr": {"expr": "weighted_score", "type": "expr"}, "fn": "SUM", "alias": "sum_weighted_score"},
            ],
        },
        "inputs": [("data", "join_products", "joined_data")],
        "output_ports": ["aggregated_data"],
        "description": "Per-category metrics: avg / median / sum / count.",
    },

    # --- Pivot: rating distribution per category (pulled from a parallel pre-aggregate stream) ---
    # For simplicity, pivot the joined dataset (before final aggregation collapses ratings).
    {
        "kind": "code", "template": "pivot", "id": "pivot_rating_dist", "name": "pivot_rating_dist",
        "position": (2080, 460),
        "config": {
            "mode": "pivot",
            "group_by": ["category"],
            "pivot_column": "rating",
            "value_column": "rating",
            "agg": "count",
        },
        "inputs": [("data", "join_products", "joined_data")],
        "output_ports": ["pivoted_data"],
        "description": "Distribution of ratings 1..5 per category.",
    },

    # --- Join the per-category aggregates with the pivot to produce final shape ---
    {
        "kind": "code", "template": "join", "id": "join_pivot", "name": "join_pivot",
        "position": (2340, 380),
        "config": {
            "join_type": "left",
            "join_conditions": "left.category = right.category",
            "expressions": [],
        },
        "inputs": [
            ("left", "aggregate_by_category", "aggregated_data"),
            ("right", "pivot_rating_dist", "pivoted_data"),
        ],
        "output_ports": ["joined_data"],
        "description": "Combine aggregates with the pivoted distribution.",
    },

    # --- Sort by avg_weighted_score DESC ---
    {
        "kind": "code", "template": "sort", "id": "sort_by_score", "name": "sort_by_score",
        "position": (2600, 380),
        "config": {
            "sort_expressions": [
                {"columnExpr": {"expr": "avg_weighted_score", "type": "expr"}, "sortBy": "DESC"}
            ]
        },
        "inputs": [("data", "join_pivot", "joined_data")],
        "output_ports": ["sorted_data"],
        "description": "Sort categories by average weighted score, descending.",
    },

    # --- Limit top 10 ---
    {
        "kind": "code", "template": "limit", "id": "limit_top_n", "name": "limit_top_n",
        "position": (2860, 380),
        "config": {"n": 10},
        "inputs": [("data", "sort_by_score", "sorted_data")],
        "output_ports": ["limited_data"],
        "description": "Keep the top 10 categories.",
    },

    # --- Output gold table ---
    {
        "kind": "code", "template": "output", "id": "output_gold", "name": "output_gold",
        "position": (3120, 380),
        "config": {
            "catalog": "aldi_aus", "schema": "aldi_us", "table_name": "gold_feedback_summary",
        },
        "inputs": [("data", "limit_top_n", "limited_data")],
        "output_ports": [],
        "description": "Materialize the result as a Unity Catalog Delta table.",
    },
]


# ---------- helpers ----------

def yaml_indent(s, prefix):
    return "\n".join(prefix + line for line in s.splitlines())

def render_yaml_config(config, indent="  "):
    """Render a Python dict as YAML, using simple rules for our scope."""
    lines = []
    def _render(obj, ind):
        if isinstance(obj, dict):
            out = []
            for k, v in obj.items():
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
    def _scalar(v):
        if isinstance(v, bool):
            return "true" if v else "false"
        if v is None:
            return "null"
        if isinstance(v, (int, float)):
            return str(v)
        s = str(v)
        # use block scalar for multi-line strings
        if "\n" in s:
            return "|\n" + yaml_indent(s, indent + "  ")
        # quote if has special yaml chars
        if any(c in s for c in [":", "#", "{", "}", "[", "]", ","]) or s.strip() != s:
            return json.dumps(s)
        return s
    return _render(config, indent)

def render_yaml_inputs(inputs, op_name_to_id):
    if not inputs:
        return "input: []"
    lines = ["input:"]
    for port, upstream_id, upstream_out in inputs:
        lines.append(f"  - node: {upstream_id}")
        lines.append(f"    input_port: {port}")
        lines.append(f"    output_port: {upstream_out}")
    return "\n".join(lines)

def render_inputs_wiring(spec):
    """Render the bottom-of-cell `inputs = {...}` block from spec."""
    if not spec["inputs"]:
        return "inputs = {}"
    if spec.get("input_is_sql_list"):
        # SQL operator: inputs["data"] is a list, plus inputs["data__sources"]
        lines = ["inputs = {"]
        lines.append("    \"data\": [")
        for port, upstream_id, upstream_out in spec["inputs"]:
            lines.append(f"        ctx[\"{upstream_id}.{upstream_out}\"],")
        lines.append("    ],")
        lines.append("    \"data__sources\": [")
        for port, upstream_id, upstream_out in spec["inputs"]:
            lines.append(
                f"        {{\"node\": \"{upstream_id}\", \"output_port\": \"{upstream_out}\", "
                f"\"name\": \"{upstream_id}\", \"df_name\": \"{upstream_id}\"}},"
            )
        lines.append("    ],")
        lines.append("}")
        return "\n".join(lines)
    if spec.get("input_is_list"):
        # Python operator: inputs["data"] is a list
        lines = ["inputs = {", "    \"data\": ["]
        for port, upstream_id, upstream_out in spec["inputs"]:
            lines.append(f"        ctx[\"{upstream_id}.{upstream_out}\"],")
        lines.append("    ],")
        lines.append("}")
        return "\n".join(lines)
    # Standard: dict per port
    lines = ["inputs = {"]
    for port, upstream_id, upstream_out in spec["inputs"]:
        lines.append(f"    \"{port}\": ctx[\"{upstream_id}.{upstream_out}\"],")
    lines.append("}")
    return "\n".join(lines)

def render_config_python(config):
    """Render the config dict as a python literal."""
    return "config = " + json.dumps(config, indent=4)

def render_ctx_assign(spec):
    if not spec["output_ports"]:
        return "out = run(config, inputs, spark)"
    lines = ["out = run(config, inputs, spark)"]
    for port in spec["output_ports"]:
        lines.append(f"ctx[\"{spec['id']}.{port}\"] = out[\"{port}\"]")
    return "\n".join(lines)

def make_code_cell(spec):
    cfg_yaml = render_yaml_config(spec["config"])
    inputs_yaml = render_yaml_inputs(spec["inputs"], None)

    desc = json.dumps(spec["description"])  # always quote — descriptions can contain colons
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
    wiring_config = render_config_python(spec["config"])
    wiring_inputs = render_inputs_wiring(spec)
    wiring_assign = render_ctx_assign(spec)

    source_text = (
        docstring + "\n\n"
        + body.strip("\n") + "\n\n"
        "# generated from the system\n"
        "ctx = globals().setdefault(\"ctx\", {})\n"
        + wiring_config + "\n"
        + wiring_inputs + "\n"
        + wiring_assign
    )
    return {
        "cell_type": "code",
        "execution_count": 0,
        "metadata": {
            "application/vnd.databricks.v1+cell": {
                "cellMetadata": {"byteLimit": 2048000, "rowLimit": 10000},
                "inputWidgets": {},
                "nuid": str(uuid.uuid4()),
                "showTitle": False,
                "tableResultSettingsMap": {},
                "title": "",
            }
        },
        "outputs": [],
        "source": [line + "\n" for line in source_text.splitlines()],
    }

def make_markdown_cell(spec):
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
        "metadata": {
            "application/vnd.databricks.v1+cell": {
                "cellMetadata": {},
                "inputWidgets": {},
                "nuid": str(uuid.uuid4()),
                "showTitle": False,
                "tableResultSettingsMap": {},
                "title": "",
            }
        },
        "source": [line + "\n" for line in src.splitlines()],
    }


# ---------- assembly ----------

def make_group_cell(spec):
    desc = json.dumps(spec["description"])
    name_yaml = json.dumps(spec["name"]) if any(c in spec["name"] for c in [":", "#"]) else spec["name"]
    src = (
        f'"""\n'
        f"id: {spec['id']}\n"
        f"template: group\n"
        f"name: {name_yaml}\n"
        f"position:\n  x: {spec['position'][0]}\n  y: {spec['position'][1]}\n"
        f"dimensions:\n  width: {spec['dimensions'][0]}\n  height: {spec['dimensions'][1]}\n"
        f"description:\n  text: {desc}\n"
        f"config: {{}}\n"
        f"input: []\n"
        f'"""\n'
        f"# group is a visual-only container; no run body needed.\n"
    )
    return {
        "cell_type": "code",
        "execution_count": 0,
        "metadata": {
            "application/vnd.databricks.v1+cell": {
                "cellMetadata": {"byteLimit": 2048000, "rowLimit": 10000},
                "inputWidgets": {},
                "nuid": str(uuid.uuid4()),
                "showTitle": False,
                "tableResultSettingsMap": {},
                "title": "",
            }
        },
        "outputs": [],
        "source": [line + "\n" for line in src.splitlines()],
    }

cells = []
for spec in CELLS:
    if spec["kind"] == "markdown":
        cells.append(make_markdown_cell(spec))
    elif spec.get("is_group"):
        cells.append(make_group_cell(spec))
    else:
        cells.append(make_code_cell(spec))

notebook = {
    "cells": cells,
    "metadata": {
        "application/vnd.databricks.v1+notebook": {
            "computePreferences": {
                "hardware": {"accelerator": None, "gpuPoolId": None, "memory": None},
                "software": {"pinSparkToX86": None},
            },
            "dashboards": [],
            "environmentMetadata": {"base_environment": "", "environment_version": "5"},
            "inputWidgetPreferences": None,
            "language": "python",
            "notebookMetadata": {},
            "notebookName": "customer_feedback.designer.ipynb",
            "widgets": {},
        },
        "language_info": {"name": "python"},
    },
    "nbformat": 4,
    "nbformat_minor": 0,
}

OUT.write_text(json.dumps(notebook, indent=1))
print(f"Wrote {OUT} ({OUT.stat().st_size} bytes, {len(cells)} cells)")
