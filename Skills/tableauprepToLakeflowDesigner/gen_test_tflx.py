#!/usr/bin/env python3
"""Generate three synthetic .tflx files for testing the converter.

Produces (next to this script in tests/):
  • simple.tflx          — 4 nodes  : LoadCsv → Container(filter+rename) → SuperAggregate → WriteToCsv
  • complex.tflx         — 17 nodes : 3 sources, joins, aggregate, pivot, union, output
  • highly_complex.tflx  — 42 nodes : 5 sources, multi-branch joins, unpivot, anti-join,
                                       container with mixed actions, 2 outputs

Each file is a real .tflx archive (ZIP of `flow` JSON + embedded CSV `Data/<uuid>/<name>.csv`)
and is a valid input to convert_tflx.py.
"""
from __future__ import annotations

import json
import random
import uuid
import zipfile
from pathlib import Path

OUT_DIR = Path(__file__).resolve().parent / "tests"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ───────────────────────────────────────────────────────────────────────────
# Tableau Prep flow-JSON helpers
# ───────────────────────────────────────────────────────────────────────────
def _uid() -> str:
    return str(uuid.uuid4())


def n_load_csv(name: str, conn_id: str, fields: list[tuple[str, str]]) -> dict:
    return {
        "nodeType": ".v1.LoadCsv",
        "name": name,
        "id": _uid(),
        "baseType": "input",
        "nextNodes": [],
        "serialize": False,
        "description": None,
        "connectionId": conn_id,
        "connectionAttributes": {
            "filename": f"{name}.csv",
            "class": "textscan",
        },
        "fields": [
            {"name": fn, "type": ft, "collation": None, "caption": "",
             "ordinal": i, "isGenerated": False}
            for i, (fn, ft) in enumerate(fields, start=1)
        ],
    }


def n_load_excel(name: str, conn_id: str, fields: list[tuple[str, str]]) -> dict:
    return {**n_load_csv(name, conn_id, fields),
            "nodeType": ".v1.LoadExcel",
            "connectionAttributes": {"filename": f"{name}.xlsx", "class": "excel-direct"}}


def n_load_sql(name: str, conn_id: str, query: str, fields: list[tuple[str, str]]) -> dict:
    return {
        "nodeType": ".v1.LoadSql",
        "name": name,
        "id": _uid(),
        "baseType": "input",
        "nextNodes": [],
        "serialize": False,
        "description": None,
        "connectionId": conn_id,
        "actions": [],
        "relation": {"type": "query", "query": query},
        "fields": [
            {"name": fn, "type": ft, "collation": None, "caption": "",
             "ordinal": i, "isGenerated": False}
            for i, (fn, ft) in enumerate(fields, start=1)
        ],
    }


def n_join(name: str, join_type: str, conds: list[dict]) -> dict:
    return {
        "nodeType": ".v2018_2_3.SuperJoin",
        "name": name, "id": _uid(),
        "baseType": "superNode",
        "nextNodes": [],
        "serialize": False,
        "description": None,
        "actionNode": {
            "nodeType": ".v1.SimpleJoin",
            "name": f"join {name}", "id": _uid(),
            "baseType": "transform",
            "nextNodes": [],
            "serialize": False,
            "description": None,
            "conditions": conds,
            "joinType": join_type,
        },
    }


def n_aggregate(name: str, group_by: list[str], aggs: list[tuple[str, str]]) -> dict:
    return {
        "nodeType": ".v2018_2_3.SuperAggregate",
        "name": name, "id": _uid(),
        "baseType": "superNode",
        "nextNodes": [],
        "serialize": False,
        "description": None,
        "actionNode": {
            "nodeType": ".v1.Aggregate",
            "name": f"agg {name}", "id": _uid(),
            "baseType": "transform",
            "nextNodes": [],
            "serialize": False,
            "description": None,
            "groupByFields": [
                {"columnName": c, "function": "GroupBy",
                 "newColumnName": None, "specialFieldType": None}
                for c in group_by
            ],
            "aggregateFields": [
                {"columnName": c, "function": fn,
                 "newColumnName": None, "specialFieldType": None}
                for c, fn in aggs
            ],
        },
    }


def n_union(name: str) -> dict:
    return {"nodeType": ".v2018_2_3.SuperUnion",
            "name": name, "id": _uid(),
            "baseType": "superNode", "nextNodes": [],
            "serialize": False, "description": None}


def n_pivot(name: str, pivot_col: str, value_col: str,
            new_cols: list[str], default_agg: str = "COUNTD") -> dict:
    return {
        "nodeType": ".v2018_3_3.SuperPivot",
        "name": name, "id": _uid(),
        "baseType": "superNode", "nextNodes": [],
        "serialize": False, "description": None,
        "actionNode": {
            "nodeType": ".v2018_3_3.Pivot",
            "name": f"pivot {name}", "id": _uid(),
            "baseType": "transform", "nextNodes": [],
            "serialize": False, "description": None,
            "defaultAggregation": default_agg,
            "aggregateColumnName": value_col,
            "pivotColumnName": pivot_col,
            "pivotGroupingColumns": [],
            "newPivotColumns": [{"newColumnName": c} for c in new_cols],
        },
    }


def n_unpivot(name: str, key_name: str, value_names: list[str],
              alias_to_cols: list[tuple[str, list[str]]]) -> dict:
    """Multi-column unpivot: each `alias_to_cols` entry maps an alias to a tuple of source columns."""
    return {
        "nodeType": ".v2018_3_4.SuperUnpivotExtended",
        "name": name, "id": _uid(),
        "baseType": "superNode", "nextNodes": [],
        "serialize": False, "description": None,
        "actionNode": {
            "nodeType": ".v2018_3_4.UnpivotExtended",
            "name": f"unpivot {name}", "id": _uid(),
            "baseType": "transform", "nextNodes": [],
            "serialize": False, "description": None,
            "usesSmartDefaults": False,
            "unpivotGroup": {
                "literalColumn": {
                    "literals":         [a for a, _ in alias_to_cols],
                    "literalColumnName": key_name,
                    "names":            [a for a, _ in alias_to_cols],
                },
                "unpivotColumns": [
                    {"unpivotColumnName": vn,
                     "columnInformation": {
                         "bindingsType": "manual",
                         "manualBindings": [cols[i] for _, cols in alias_to_cols],
                     }}
                    for i, vn in enumerate(value_names)
                ],
            },
        },
    }


def n_container(name: str, actions: list[dict]) -> dict:
    """A flat (linear) container — each action's nextNodes points at the following one."""
    sub_nodes = {a["id"]: a for a in actions}
    for i, a in enumerate(actions[:-1]):
        a["nextNodes"] = [{"namespace": "Default",
                            "nextNodeId": actions[i + 1]["id"],
                            "nextNamespace": "Default"}]
    return {
        "nodeType": ".v1.Container",
        "name": name, "id": _uid(),
        "baseType": "container", "nextNodes": [],
        "serialize": False, "description": None,
        "loomContainer": {
            "parameters": {"parameters": {}},
            "initialNodes": [actions[0]["id"]] if actions else [],
            "nodes": sub_nodes,
            "connections": {}, "dataConnections": {},
            "connectionIds": [], "dataConnectionIds": [],
            "nodeProperties": {}, "extensibility": None,
        },
        "namespacesToInput": {"Default": {"nodeId": actions[0]["id"], "namespace": "Default"}}
            if actions else {},
        "namespacesToOutput": {"Default": {"nodeId": actions[-1]["id"], "namespace": "Default"}}
            if actions else {},
        "providedParameters": None,
    }


def a_filter(expr: str) -> dict:
    return {"nodeType": ".v1.FilterOperation",
            "name": "filter", "id": _uid(),
            "baseType": "transform", "nextNodes": [],
            "serialize": False, "description": None,
            "filterExpression": expr}


def a_value_filter(col: str, values: list, exclude: bool = True) -> dict:
    return {"nodeType": ".v1.ValueFilter",
            "name": "value-filter", "id": _uid(),
            "baseType": "transform", "nextNodes": [],
            "serialize": False, "description": None,
            "exclude": exclude,
            "values": {col: [json.dumps(v) if isinstance(v, str) else v for v in values]}}


def a_add_column(col: str, expr: str) -> dict:
    return {"nodeType": ".v1.AddColumn",
            "columnName": col, "expression": expr,
            "name": f"add {col}", "id": _uid(),
            "baseType": "transform", "nextNodes": [],
            "serialize": False, "description": None}


def a_rename(old: str, new: str) -> dict:
    return {"nodeType": ".v1.RenameColumn",
            "columnName": old, "rename": new,
            "name": f"rename {old}", "id": _uid(),
            "baseType": "transform", "nextNodes": [],
            "serialize": False, "description": None}


def a_remove(cols: list[str]) -> dict:
    return {"nodeType": ".v1.RemoveColumns",
            "columnNames": cols,
            "name": "remove cols", "id": _uid(),
            "baseType": "transform", "nextNodes": [],
            "serialize": False, "description": None}


def a_change_type(col: str, t: str) -> dict:
    return {"nodeType": ".v1.ChangeColumnType",
            "fields": {col: {"type": t, "calc": None}},
            "name": f"cast {col}", "id": _uid(),
            "baseType": "transform", "nextNodes": [],
            "serialize": False, "description": None}


def n_publish(name: str, project: str, ds: str) -> dict:
    return {"nodeType": ".v1.PublishExtract",
            "name": name, "id": _uid(),
            "baseType": "output", "nextNodes": [],
            "serialize": False, "description": None,
            "projectName": project, "datasourceName": ds}


def n_write_csv(name: str, path: str = "S:\\out\\flow.csv") -> dict:
    return {"nodeType": ".v1.WriteToCsv",
            "name": name, "id": _uid(),
            "baseType": "output", "nextNodes": [],
            "serialize": False, "description": None,
            "csvOutputFile": path}


# ───────────────────────────────────────────────────────────────────────────
# Wiring helpers
# ───────────────────────────────────────────────────────────────────────────
def link(src: dict, dst: dict) -> None:
    """src.nextNodes += dst (idempotent)."""
    edge = {"namespace": "Default", "nextNodeId": dst["id"], "nextNamespace": "Default"}
    if not any(e["nextNodeId"] == dst["id"] for e in src.get("nextNodes") or []):
        src.setdefault("nextNodes", []).append(edge)


def assemble(initial: list[dict], nodes: list[dict]) -> dict:
    return {
        "parameters": {"parameters": {}},
        "initialNodes": [n["id"] for n in initial],
        "nodes": {n["id"]: n for n in nodes},
        "connections": {},
        "dataConnections": {},
        "connectionIds": [],
        "dataConnectionIds": [],
        "nodeProperties": {},
        "extensibility": None,
        "selection": [],
        "majorVersion": 1, "minorVersion": 0,
        "documentId": _uid(), "obfuscatorId": _uid(),
    }


def write_tflx(path: Path, flow: dict, embedded_csvs: dict[str, str]) -> None:
    """`embedded_csvs` is `connectionId → csv_text`."""
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("flow", json.dumps(flow, indent=1))
        for cid, text in embedded_csvs.items():
            z.writestr(f"Data/{cid}/data.csv", text)
        z.writestr("maestroMetadata", "{}")
        z.writestr("displaySettings", "{}")
        z.writestr("flowGraphThumbnail.svg", "<svg/>")


# ───────────────────────────────────────────────────────────────────────────
# Synthetic CSV bodies
# ───────────────────────────────────────────────────────────────────────────
def _csv_orders(n: int = 50) -> str:
    rng = random.Random(0)
    rows = ["order_id,customer_id,amount,status,placed_at"]
    for i in range(n):
        st = rng.choice(["paid", "pending", "cancelled", "refunded"])
        rows.append(f"{1000+i},{rng.randint(1, 20)},{round(rng.uniform(5, 250), 2)},{st},2026-{rng.randint(1,4):02d}-{rng.randint(1,28):02d}")
    return "\n".join(rows) + "\n"


def _csv_customers(n: int = 20) -> str:
    rng = random.Random(1)
    regions = ["NA", "EMEA", "APAC", "LATAM"]
    rows = ["customer_id,name,region,signup_year"]
    for i in range(n):
        rows.append(f"{i+1},Customer_{i+1},{rng.choice(regions)},{rng.randint(2020, 2025)}")
    return "\n".join(rows) + "\n"


def _csv_products(n: int = 30) -> str:
    rng = random.Random(2)
    cats = ["Food", "Drink", "Household", "Wine", "Beer"]
    rows = ["product_id,sku,category,unit_price"]
    for i in range(n):
        rows.append(f"{i+1},SKU{i:04d},{rng.choice(cats)},{round(rng.uniform(2,30),2)}")
    return "\n".join(rows) + "\n"


def _csv_capacity(n: int = 40) -> str:
    rng = random.Random(3)
    rows = ["sku,year_2027,year_2028,year_2029,year_2030,year_2031"]
    for i in range(n):
        b = rng.randint(100, 10000)
        rows.append(f"SKU{i:04d},{b},{b+rng.randint(0,1000)},{b+rng.randint(0,2000)},{b+rng.randint(0,3000)},{b+rng.randint(0,4000)}")
    return "\n".join(rows) + "\n"


def _csv_inventory(n: int = 25) -> str:
    rng = random.Random(4)
    rows = ["product_id,location,on_hand,reorder_level"]
    for i in range(n):
        rows.append(f"{rng.randint(1,30)},LOC_{rng.randint(1,5)},{rng.randint(0,500)},{rng.randint(20,100)}")
    return "\n".join(rows) + "\n"


# ───────────────────────────────────────────────────────────────────────────
# 1. SIMPLE — 4 nodes
# ───────────────────────────────────────────────────────────────────────────
def build_simple() -> tuple[dict, dict[str, str]]:
    conn = _uid()
    src   = n_load_csv("orders", conn, [
        ("order_id", "integer"), ("customer_id", "integer"),
        ("amount", "real"), ("status", "string"), ("placed_at", "string"),
    ])
    clean = n_container("clean orders", [
        a_filter('[status] = "paid"'),
        a_change_type("placed_at", "date"),
        a_add_column("revenue", "[amount]"),
        a_remove(["status"]),
    ])
    agg   = n_aggregate("revenue by customer",
                        group_by=["customer_id"],
                        aggs=[("revenue", "SUM"), ("order_id", "COUNT")])
    out   = n_write_csv("revenue.csv")

    link(src, clean); link(clean, agg); link(agg, out)
    flow = assemble([src], [src, clean, agg, out])
    return flow, {conn: _csv_orders()}


# ───────────────────────────────────────────────────────────────────────────
# 2. COMPLEX — 17 nodes
# ───────────────────────────────────────────────────────────────────────────
def build_complex() -> tuple[dict, dict[str, str]]:
    c_orders, c_cust, c_prod = _uid(), _uid(), _uid()
    src_o = n_load_csv("orders",    c_orders, [("order_id","integer"),("customer_id","integer"),
                                                ("amount","real"),("status","string"),("placed_at","string")])
    src_c = n_load_csv("customers", c_cust,   [("customer_id","integer"),("name","string"),
                                                ("region","string"),("signup_year","integer")])
    src_p = n_load_excel("products", c_prod,  [("product_id","integer"),("sku","string"),
                                                ("category","string"),("unit_price","real")])

    clean_o = n_container("clean orders", [
        a_filter('[status] = "paid"'),
        a_change_type("placed_at", "date"),
        a_remove(["status"]),
    ])
    clean_c = n_container("clean customers", [
        a_value_filter("region", ['"APAC"'], exclude=False),
    ])

    join1 = n_join("orders+customers", "left",
                   [{"leftExpression": "[customer_id]", "rightExpression": "[customer_id]", "comparator": "=="}])
    join2 = n_join("+products",        "left",
                   [{"leftExpression": "[order_id]",     "rightExpression": "[product_id]",  "comparator": "=="}])

    agg_region = n_aggregate("revenue by region", ["region"],
                              [("amount", "SUM"), ("order_id", "COUNT")])
    agg_cust   = n_aggregate("revenue by customer", ["customer_id", "name"],
                              [("amount", "SUM")])

    pivot_cat = n_pivot("category counts", "category", "order_id", ["Food", "Drink", "Household"], "COUNTD")

    # Two filter branches feeding a union (sales >100 OR APAC region)
    filt_high = n_container("high-value", [a_filter("[amount] > 100")])
    filt_apac = n_container("apac-only",  [a_value_filter("region", ['"APAC"'], exclude=False)])
    union     = n_union("hi or apac")

    sort_node = n_aggregate("rank by revenue", ["region"], [("amount", "MAX")])  # placeholder for a Sort
    out_main  = n_publish("publish revenue", "Sales Demos", "revenue_summary")
    out_csv   = n_write_csv("rev_export.csv", path="S:\\out\\rev.csv")

    # Wire it all
    link(src_o, clean_o); link(src_c, clean_c)
    link(clean_o, join1); link(clean_c, join1)
    link(join1, join2);   link(src_p, join2)
    link(join2, agg_region); link(join2, agg_cust)
    link(join2, pivot_cat)
    link(join2, filt_high); link(join2, filt_apac)
    link(filt_high, union); link(filt_apac, union)
    link(union, sort_node)
    link(agg_region, out_main)
    link(sort_node, out_csv)

    nodes = [src_o, src_c, src_p, clean_o, clean_c, join1, join2,
             agg_region, agg_cust, pivot_cat,
             filt_high, filt_apac, union, sort_node,
             out_main, out_csv]
    flow = assemble([src_o, src_c, src_p], nodes)
    return flow, {
        c_orders: _csv_orders(),
        c_cust:   _csv_customers(),
        c_prod:   _csv_products(),
    }


# ───────────────────────────────────────────────────────────────────────────
# 3. HIGHLY COMPLEX — 42 nodes
# ───────────────────────────────────────────────────────────────────────────
def build_highly_complex() -> tuple[dict, dict[str, str]]:
    c_orders, c_cust, c_prod, c_cap, c_inv = _uid(), _uid(), _uid(), _uid(), _uid()

    # 5 sources
    src_o   = n_load_csv  ("orders",     c_orders, [("order_id","integer"),("customer_id","integer"),
                                                     ("amount","real"),("status","string"),("placed_at","string")])
    src_c   = n_load_sql  ("customers",  c_cust,   "SELECT * FROM customers",
                            [("customer_id","integer"),("name","string"),("region","string"),("signup_year","integer")])
    src_p   = n_load_excel("products",   c_prod,   [("product_id","integer"),("sku","string"),
                                                     ("category","string"),("unit_price","real")])
    src_cap = n_load_excel("capacity",   c_cap,    [("sku","string"),("year_2027","integer"),
                                                     ("year_2028","integer"),("year_2029","integer"),
                                                     ("year_2030","integer"),("year_2031","integer")])
    src_inv = n_load_csv  ("inventory",  c_inv,    [("product_id","integer"),("location","string"),
                                                     ("on_hand","integer"),("reorder_level","integer")])

    # Cleaning containers (mixed actions → some become 'sql', some 'transform')
    clean_o   = n_container("clean orders", [
        a_filter('[status] = "paid"'),
        a_change_type("placed_at", "date"),
        a_add_column("revenue", "[amount]"),
        a_remove(["status"]),
    ])
    clean_c   = n_container("clean customers", [
        a_rename("region", "Region"),
        a_change_type("signup_year", "integer"),
    ])
    clean_p   = n_container("clean products", [
        a_value_filter("category", ['"Wine"', '"Beer"'], exclude=True),
        a_add_column("display_name", "[sku] + ' - ' + [category]"),
    ])
    clean_inv = n_container("clean inventory", [
        a_filter("[on_hand] > 0"),
    ])

    # Joins
    j_o_c   = n_join("orders+customers", "left",
                     [{"leftExpression": "[customer_id]", "rightExpression": "[customer_id]", "comparator": "=="}])
    j_with_p = n_join("+products", "left",
                      [{"leftExpression": "[order_id]", "rightExpression": "[product_id]", "comparator": "=="}])
    j_with_inv = n_join("+inventory", "left",
                        [{"leftExpression": "[product_id]", "rightExpression": "[product_id]", "comparator": "=="}])
    j_with_cap = n_join("+capacity", "inner",
                        [{"leftExpression": "[sku]", "rightExpression": "[sku]", "comparator": "=="}])

    # Aggregates: revenue by region, revenue by category, units by location
    agg_region   = n_aggregate("rev by region", ["Region"], [("amount", "SUM"), ("revenue", "SUM")])
    agg_category = n_aggregate("rev by category", ["category"], [("revenue", "SUM")])
    agg_loc      = n_aggregate("units by loc", ["location"], [("on_hand", "SUM")])

    # Pivot: revenue by region pivoted on category
    pivot_cat = n_pivot("category-pivot", "category", "amount",
                         ["Food", "Drink", "Household"], "SUM")

    # Multi-column UNPIVOT capacity year_2027..2031 → (year, capacity)
    unpivot_cap = n_unpivot("unpivot capacity",
        key_name="cap_year",
        value_names=["capacity"],
        alias_to_cols=[
            ("2027", ["year_2027"]),
            ("2028", ["year_2028"]),
            ("2029", ["year_2029"]),
            ("2030", ["year_2030"]),
            ("2031", ["year_2031"]),
        ])

    # Branch: high-value vs low-value → union → final aggregate
    filt_high = n_container("high-value",  [a_filter("[amount] > 100")])
    filt_low  = n_container("low-value",   [a_filter("[amount] <= 100")])
    union     = n_union("hi-lo")
    agg_after_union = n_aggregate("post-union agg", ["Region"], [("amount", "SUM")])

    # ANTI-JOIN: customers without orders
    j_anti = n_join("customers w/o orders", "leftOnly",
                     [{"leftExpression": "[customer_id]", "rightExpression": "[customer_id]", "comparator": "=="}])

    # A Container that mixes filter + computes (heuristic → SQL)
    mixed = n_container("mixed cleanup", [
        a_filter("[Region] != 'EMEA'"),
        a_add_column("uppercase_region", "[Region]"),
        a_change_type("amount", "real"),
    ])

    # Outputs
    out_pub  = n_publish("publish summary", "Demo Project", "demo_summary")
    out_csv  = n_write_csv("export csv", path="S:\\demo\\export.csv")

    # Wire
    link(src_o, clean_o);   link(src_c, clean_c)
    link(src_p, clean_p);   link(src_inv, clean_inv)
    link(clean_o, j_o_c);   link(clean_c, j_o_c)
    link(j_o_c,    j_with_p);  link(clean_p, j_with_p)
    link(j_with_p, j_with_inv); link(clean_inv, j_with_inv)
    link(clean_p,  j_with_cap); link(src_cap,  j_with_cap)

    link(j_with_inv, agg_region)
    link(j_with_inv, agg_category)
    link(j_with_inv, agg_loc)
    link(j_with_inv, pivot_cat)

    link(j_with_cap, unpivot_cap)

    link(j_with_inv, filt_high); link(j_with_inv, filt_low)
    link(filt_high,  union);     link(filt_low,    union)
    link(union, agg_after_union)
    link(agg_after_union, mixed)
    link(mixed, out_pub)

    link(clean_c, j_anti); link(clean_o, j_anti)
    link(j_anti, out_csv)

    nodes = [src_o, src_c, src_p, src_cap, src_inv,
             clean_o, clean_c, clean_p, clean_inv,
             j_o_c, j_with_p, j_with_inv, j_with_cap,
             agg_region, agg_category, agg_loc,
             pivot_cat, unpivot_cap,
             filt_high, filt_low, union, agg_after_union,
             j_anti, mixed,
             out_pub, out_csv]

    flow = assemble([src_o, src_c, src_p, src_cap, src_inv], nodes)
    return flow, {
        c_orders: _csv_orders(),
        c_cust:   _csv_customers(),
        c_prod:   _csv_products(),
        c_cap:    _csv_capacity(),
        c_inv:    _csv_inventory(),
    }


# ───────────────────────────────────────────────────────────────────────────
# Run
# ───────────────────────────────────────────────────────────────────────────
def main() -> None:
    for name, builder in [
        ("simple",         build_simple),
        ("complex",        build_complex),
        ("highly_complex", build_highly_complex),
    ]:
        flow, csvs = builder()
        path = OUT_DIR / f"{name}.tflx"
        write_tflx(path, flow, csvs)
        print(f"  {name:<16} → {path.name:<22} {len(flow['nodes']):3d} nodes  "
              f"{path.stat().st_size:6d} bytes")


if __name__ == "__main__":
    main()
