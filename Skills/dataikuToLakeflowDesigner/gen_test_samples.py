#!/usr/bin/env python3
"""
Generate 5 synthetic Dataiku DSS project exports for testing the dataiku-to-vdp skill.
Layout per project (same as an unzipped DSS project export):

  <PROJECT>/params.json                 project settings
  <PROJECT>/variables.json              project variables ({"standard": {...}, "local": {...}})
  <PROJECT>/datasets/<name>.json        dataset definitions (connection, table, schema)
  <PROJECT>/recipes/<name>.json         recipe definition (type, inputs, outputs)
  <PROJECT>/recipes/<name>.<payload>    recipe payload: .shaker .join .grouping .window .pivot .split .vstack .topn .sql .py
  <PROJECT>/scenarios/<id>.json         scenarios (triggers, steps, reporters)

  01_simple        CUSTOMER_PREP              Prepare (10 processors)
  02_medium        SALES_BY_COUNTRY_CHANNEL   3-way Join (pre-filter, computed column) -> Prepare -> Group
  03_medium        ORDER_CHANNEL_SPLIT        Prepare -> Split (by value, 3 outputs) -> Stack (origin column)
  04_complex       CUSTOMER_ORDER_BEHAVIOUR   Join -> Group -> Window -> Group -> Pivot -> Join -> Prepare -> Top N
  05_very_complex  CUSTOMER_360               SQL recipe + Python recipe + Group + Pivot + Prepare + 3x Join + Prepare + Window,
                                              project variables, scenario
Usage: python3 gen_test_samples.py <out_dir>
"""
import json
import os
import sys

OUT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "samples")
CONN = "postgres_retail_dwh"
SCHEMAS = {
    "customers": [("customer_id", "bigint"), ("first_name", "string"), ("last_name", "string"), ("email", "string"), ("country", "string"),
                  ("segment", "string"), ("signup_date", "string"), ("status", "string")],
    "orders": [("order_id", "bigint"), ("customer_id", "bigint"), ("order_date", "date"), ("channel", "string"), ("store_id", "bigint"), ("status", "string")],
    "order_lines": [("order_id", "bigint"), ("line_no", "int"), ("product_id", "bigint"), ("quantity", "int"), ("unit_price", "double"), ("discount_pct", "double")],
    "products": [("product_id", "bigint"), ("product_name", "string"), ("category", "string"), ("list_price", "double"), ("cost", "double"), ("active_flag", "string")],
    "web_events": [("event_id", "bigint"), ("customer_id", "bigint"), ("event_ts", "date"), ("event_type", "string"), ("page", "string")],
}


class Project:
    def __init__(self, folder, key, desc, variables=None):
        self.root = os.path.join(OUT, folder, key)
        self.key = key
        for sub in ("datasets", "recipes", "scenarios"):
            os.makedirs(os.path.join(self.root, sub), exist_ok=True)
        self._w("params.json", {"projectKey": key, "name": key.replace("_", " ").title(), "shortDesc": desc,
                                "projectStatus": "Production", "settings": {"flowZones": []}})
        self._w("variables.json", {"standard": variables or {}, "local": {}})

    def _w(self, rel, obj):
        p = os.path.join(self.root, rel)
        with open(p, "w") as f:
            if isinstance(obj, str):
                f.write(obj)
            else:
                json.dump(obj, f, indent=2)

    def dataset(self, name, table=None, schema=None, managed=False):
        cols = [{"name": c, "type": t} for c, t in (schema or SCHEMAS.get(table or name, []))]
        self._w(f"datasets/{name}.json", {
            "type": "PostgreSQL", "managed": managed, "formatType": None,
            "params": {"connection": CONN, "mode": "table", "schema": "dwh" if managed else "erp",
                       "table": "${projectKey}_" + name if managed else (table or name)},
            "schema": {"columns": cols, "userModified": False}, "tags": ["output"] if managed else ["source"]})

    def recipe(self, name, rtype, inputs, outputs, payload=None, ext=None, extra=None):
        def roles(x):
            if isinstance(x, dict):
                return {r: {"items": [{"ref": d, "deps": []} for d in v]} for r, v in x.items()}
            return {"main": {"items": [{"ref": d, "deps": []} for d in x]}}
        js = {"type": rtype, "neverRecomputeExistingPartitions": False, "inputs": roles(inputs), "outputs": roles(outputs),
              "params": extra or {}, "customMeta": {"kv": {}}, "versionTag": {"versionNumber": 3}}
        self._w(f"recipes/{name}.json", js)
        if payload is not None:
            self._w(f"recipes/{name}.{ext}", payload)

    def scenario(self, sid, obj):
        self._w(f"scenarios/{sid}.json", obj)


def step(t, **params):
    return {"preview": False, "metaType": "PROCESSOR", "disabled": False, "type": t, "params": params, "alwaysShowComment": False}


def shaker(steps):
    return {"columnsSelection": {"mode": "ALL"}, "explorationSampling": {"selection": {"maxRecords": 10000}},
            "origin": "PREPARE_RECIPE", "steps": steps, "maxProcessedMemTableBytes": -1}


def gval(col, **aggs):
    base = {"column": col, "type": "double", "sum": False, "avg": False, "min": False, "max": False, "count": False,
            "countDistinct": False, "first": False, "last": False, "concat": False, "stddev": False}
    base.update(aggs)
    return base


# ============================================================================= 01 simple
def p01():
    p = Project("01_simple_customer_prep", "CUSTOMER_PREP", "Clean the CRM customer extract.")
    p.dataset("customers"); p.dataset("customers_prepared", schema=[("customer_id", "bigint"), ("full_name", "string"), ("email", "string"),
                                                                   ("country", "string"), ("segment", "string"), ("customer_type", "string"), ("signup_date", "date")], managed=True)
    steps = [
        step("StringTransformer", appliesTo="COLUMNS", columns=["first_name", "last_name", "email", "country", "segment"], mode="TRIM"),
        step("StringTransformer", appliesTo="SINGLE_COLUMN", columns=["email"], mode="TO_LOWER"),
        step("StringTransformer", appliesTo="COLUMNS", columns=["country", "segment"], mode="TO_UPPER"),
        step("FillEmptyWithValue", appliesTo="SINGLE_COLUMN", columns=["segment"], value="UNKNOWN"),
        step("DateParser", appliesTo="SINGLE_COLUMN", columns=["signup_date"], formats=["dd.MM.yyyy", "yyyy-MM-dd"], lang="auto", timezone_id="UTC", outCol="signup_date_parsed"),
        step("CreateColumnWithGREL", column="full_name", expression='first_name + " " + last_name', errorColumn=""),
        step("CreateColumnWithGREL", column="customer_type", expression='if(segment == "CORP" || segment == "SMB", "B2B", "B2C")', errorColumn=""),
        step("FilterOnValue", appliesTo="SINGLE_COLUMN", columns=["status"], values=["A"], matchingMode="FULL_STRING", normalizationMode="EXACT", action="KEEP_ROW"),
        step("ColumnsSelector", appliesTo="COLUMNS", columns=["customer_id", "full_name", "email", "country", "segment", "customer_type", "signup_date_parsed"], keep=True),
        step("ColumnRenamer", renamings=[{"from": "signup_date_parsed", "to": "signup_date"}]),
    ]
    p.recipe("compute_customers_prepared", "shaker", ["customers"], ["customers_prepared"], shaker(steps), "shaker")


# ============================================================================= 02 medium
def p02():
    p = Project("02_medium_sales_by_country_channel", "SALES_BY_COUNTRY_CHANNEL", "Completed-order sales by customer country and channel.")
    for d in ("orders", "order_lines", "customers"):
        p.dataset(d)
    p.dataset("order_lines_joined", schema=[], managed=True); p.dataset("order_lines_clean", schema=[], managed=True)
    p.dataset("sales_by_country_channel", schema=[], managed=True)
    join = {
        "virtualInputs": [
            {"index": 0, "preFilter": {"enabled": True, "uiData": {"mode": "CUSTOM"}, "expression": 'status == "COMPLETED"', "language": "GREL"}, "computedColumns": []},
            {"index": 1, "preFilter": {"enabled": False}, "computedColumns": []},
            {"index": 2, "preFilter": {"enabled": False}, "computedColumns": []}],
        "joins": [
            {"table1": 0, "table2": 1, "type": "INNER", "conditionsMode": "AND",
             "on": [{"column1": {"name": "order_id", "table": 0}, "column2": {"name": "order_id", "table": 1}, "type": "EQ", "maxMatches": 0}]},
            {"table1": 0, "table2": 2, "type": "LEFT", "conditionsMode": "AND",
             "on": [{"column1": {"name": "customer_id", "table": 0}, "column2": {"name": "customer_id", "table": 2}, "type": "EQ", "maxMatches": 1}]}],
        "selectedColumns": [{"name": "order_id", "table": 0}, {"name": "order_date", "table": 0}, {"name": "channel", "table": 0},
                            {"name": "line_no", "table": 1}, {"name": "quantity", "table": 1}, {"name": "country", "table": 2}],
        "computedColumns": [{"name": "line_amount", "type": "double", "mode": "GREL", "expr": "quantity * coalesce(unit_price, 0) * (1 - discount_pct)"}],
        "postFilter": {"enabled": False}, "enableAutoCastInJoinConditions": False}
    p.recipe("compute_order_lines_joined", "join", ["orders", "order_lines", "customers"], ["order_lines_joined"], join, "join")
    steps = [step("StringTransformer", appliesTo="SINGLE_COLUMN", columns=["country"], mode="TRIM"),
             step("StringTransformer", appliesTo="SINGLE_COLUMN", columns=["country"], mode="TO_UPPER"),
             step("FillEmptyWithValue", appliesTo="SINGLE_COLUMN", columns=["country"], value="UNKNOWN")]
    p.recipe("compute_order_lines_clean", "shaker", ["order_lines_joined"], ["order_lines_clean"], shaker(steps), "shaker")
    grp = {"keys": [{"column": "country"}, {"column": "channel"}], "globalCount": True,
           "values": [gval("order_id", countDistinct=True), gval("line_amount", sum=True, avg=True), gval("quantity", sum=True), gval("order_date", max=True)],
           "preFilter": {"enabled": False}, "postFilter": {"enabled": False}, "computedColumns": []}
    p.recipe("compute_sales_by_country_channel", "grouping", ["order_lines_clean"], ["sales_by_country_channel"], grp, "grouping")


# ============================================================================= 03 medium
def p03():
    p = Project("03_medium_order_channel_split", "ORDER_CHANNEL_SPLIT", "Split orders by channel and re-stack web + store for the omnichannel report.")
    p.dataset("orders")
    for d in ("orders_prepared", "orders_web", "orders_store", "orders_other", "orders_web_store"):
        p.dataset(d, schema=[], managed=True)
    steps = [step("FilterOnValue", appliesTo="SINGLE_COLUMN", columns=["status"], values=["COMPLETED", "RETURNED"], matchingMode="FULL_STRING", normalizationMode="EXACT", action="KEEP_ROW"),
             step("CreateColumnWithGREL", column="order_quarter",
                  expression='if(datePart(order_date, "month") <= 3, "Q1", if(datePart(order_date, "month") <= 6, "Q2", if(datePart(order_date, "month") <= 9, "Q3", "Q4")))'),
             step("CreateColumnWithGREL", column="is_return", expression='if(status == "RETURNED", 1, 0)'),
             step("ColumnsSelector", appliesTo="COLUMNS", columns=["store_id"], keep=False)]
    p.recipe("compute_orders_prepared", "shaker", ["orders"], ["orders_prepared"], shaker(steps), "shaker")
    split = {"mode": "VALUES", "column": "channel", "values": [{"value": "WEB", "outputIndex": 0}, {"value": "STORE", "outputIndex": 1}],
             "defaultOutputIndex": 2, "dropOutputIndex": -1, "missingValueOutputIndex": 2}
    p.recipe("split_orders_by_channel", "split", ["orders_prepared"], ["orders_web", "orders_store", "orders_other"], split, "split")
    vstack = {"mode": "UNION", "selectedColumns": [], "addOriginColumn": True, "originColumnName": "original_dataset",
              "originColumnMapping": [{"originDatasetName": "orders_web", "originColumnValue": "orders_web"},
                                      {"originDatasetName": "orders_store", "originColumnValue": "orders_store"}],
              "virtualInputs": [{"index": 0, "preFilter": {"enabled": False}}, {"index": 1, "preFilter": {"enabled": False}}]}
    p.recipe("stack_web_store", "vstack", ["orders_web", "orders_store"], ["orders_web_store"], vstack, "vstack")


# ============================================================================= 04 complex
def p04():
    p = Project("04_complex_customer_order_behaviour", "CUSTOMER_ORDER_BEHAVIOUR", "Order cadence, lifetime value and channel mix per customer; top 10.")
    for d in ("orders", "order_lines", "products"):
        p.dataset(d)
    for d in ("lines_priced", "orders_amount", "orders_window", "customer_behaviour", "channel_pivot", "customer_summary_joined", "customer_summary", "top10_customers"):
        p.dataset(d, schema=[], managed=True)
    join = {"virtualInputs": [{"index": 0, "preFilter": {"enabled": True, "expression": 'status == "COMPLETED"', "language": "GREL"}},
                              {"index": 1, "preFilter": {"enabled": False}}, {"index": 2, "preFilter": {"enabled": False}}],
            "joins": [{"table1": 0, "table2": 1, "type": "INNER", "on": [{"column1": {"name": "order_id", "table": 0}, "column2": {"name": "order_id", "table": 1}, "type": "EQ"}]},
                      {"table1": 1, "table2": 2, "type": "LEFT", "on": [{"column1": {"name": "product_id", "table": 1}, "column2": {"name": "product_id", "table": 2}, "type": "EQ"}]}],
            "selectedColumns": [{"name": "order_id", "table": 0}, {"name": "customer_id", "table": 0}, {"name": "order_date", "table": 0}, {"name": "channel", "table": 0},
                                {"name": "quantity", "table": 1}],
            "computedColumns": [{"name": "net_amount", "type": "double", "mode": "SQL", "expr": "ROUND(quantity * COALESCE(unit_price, list_price) * (1 - discount_pct), 2)"}],
            "postFilter": {"enabled": False}}
    p.recipe("compute_lines_priced", "join", ["orders", "order_lines", "products"], ["lines_priced"], join, "join")
    g1 = {"keys": [{"column": "order_id"}, {"column": "customer_id"}, {"column": "order_date"}, {"column": "channel"}], "globalCount": False,
          "values": [gval("net_amount", sum=True)]}
    p.recipe("compute_orders_amount", "grouping", ["lines_priced"], ["orders_amount"], g1, "grouping")
    win = {"windows": [{"prefix": "", "enablePartitioning": True, "partitioningColumns": ["customer_id"], "enableOrdering": True,
                        "orders": [{"column": "order_date", "desc": False}, {"column": "order_id", "desc": False}],
                        "limitPreceding": False, "limitFollowing": True, "followingRows": 0, "rowNumber": True, "rank": False, "denseRank": False}],
           "values": [{"column": "net_amount_sum", "type": "double", "sum": True, "lag": False},
                      {"column": "order_date", "type": "date", "lagDiff": True, "lagValues": "1", "lagDiffUnit": "DAYS"}],
           "postFilter": {"enabled": False}}
    p.recipe("compute_orders_window", "window", ["orders_amount"], ["orders_window"], win, "window")
    g2 = {"keys": [{"column": "customer_id"}], "globalCount": False,
          "values": [gval("row_number", max=True), gval("net_amount_sum_sum", max=True), gval("order_date_lagdiff1", avg=True), gval("order_date", min=True, max=True)]}
    p.recipe("compute_customer_behaviour", "grouping", ["orders_window"], ["customer_behaviour"], g2, "grouping")
    piv = {"explicitIdentifiers": ["customer_id"], "keyColumns": ["channel"], "valueLimit": 20, "otherColumnsAggregation": "NONE",
           "valueColumns": [{"column": "net_amount_sum", "sum": True}], "explicitValues": ["APP", "STORE", "WEB"], "computeCount": False}
    p.recipe("compute_channel_pivot", "pivot", ["orders_amount"], ["channel_pivot"], piv, "pivot")
    j2 = {"virtualInputs": [{"index": 0}, {"index": 1}],
          "joins": [{"table1": 0, "table2": 1, "type": "LEFT", "on": [{"column1": {"name": "customer_id", "table": 0}, "column2": {"name": "customer_id", "table": 1}, "type": "EQ"}]}],
          "selectedColumns": [{"name": "customer_id", "table": 0}, {"name": "row_number_max", "table": 0}, {"name": "net_amount_sum_sum_max", "table": 0},
                              {"name": "order_date_lagdiff1_avg", "table": 0}, {"name": "order_date_min", "table": 0}, {"name": "order_date_max", "table": 0},
                              {"name": "APP_net_amount_sum_sum", "table": 1}, {"name": "STORE_net_amount_sum_sum", "table": 1}, {"name": "WEB_net_amount_sum_sum", "table": 1}],
          "computedColumns": []}
    p.recipe("compute_customer_summary_joined", "join", ["customer_behaviour", "channel_pivot"], ["customer_summary_joined"], j2, "join")
    steps = [step("ColumnRenamer", renamings=[{"from": "row_number_max", "to": "orders_count"}, {"from": "net_amount_sum_sum_max", "to": "lifetime_amount"},
                                              {"from": "order_date_lagdiff1_avg", "to": "avg_days_between_orders"}, {"from": "order_date_min", "to": "first_order_date"},
                                              {"from": "order_date_max", "to": "last_order_date"}, {"from": "APP_net_amount_sum_sum", "to": "app_amount"},
                                              {"from": "STORE_net_amount_sum_sum", "to": "store_amount"}, {"from": "WEB_net_amount_sum_sum", "to": "web_amount"}]),
             step("FillEmptyWithValue", appliesTo="COLUMNS", columns=["app_amount", "store_amount", "web_amount"], value="0"),
             step("CreateColumnWithGREL", column="avg_order_value", expression="round(lifetime_amount / orders_count * 100) / 100"),
             step("CreateColumnWithGREL", column="web_share", expression="round(web_amount / lifetime_amount * 1000) / 1000"),
             step("RoundProcessor", appliesTo="SINGLE_COLUMN", columns=["lifetime_amount"], mode="ROUND", precision=2, places=2)]
    p.recipe("compute_customer_summary", "shaker", ["customer_summary_joined"], ["customer_summary"], shaker(steps), "shaker")
    topn = {"firstRows": 10, "lastRows": 0, "keys": [], "orders": [{"column": "lifetime_amount", "desc": True}, {"column": "customer_id", "desc": False}],
            "computeRowNumber": True, "rowNumberColumn": "top_rank", "retrievedColumnsMode": "ALL", "preFilter": {"enabled": False}}
    p.recipe("compute_top10_customers", "topn", ["customer_summary"], ["top10_customers"], topn, "topn")


# ============================================================================= 05 very complex
def p05():
    vars_ = {"min_order_date": "2025-07-01", "analysis_end": "2025-11-15", "session_gap_minutes": 30}
    p = Project("05_very_complex_customer_360", "CUSTOMER_360", "Customer 360: recent revenue, web sessions, funnel events and engagement tier.", vars_)
    for d in ("orders", "order_lines", "products", "customers", "web_events"):
        p.dataset(d)
    for d in ("recent_orders", "sessions", "sessions_by_customer", "event_pivot", "customers_prepared", "customer_360_joined", "customer_360_scored", "customer_360"):
        p.dataset(d, schema=[], managed=True)
    sql = """-- SQL recipe (PostgreSQL). Project variables are expanded by DSS before execution.
SELECT o.customer_id,
       COUNT(DISTINCT o.order_id)                                                              AS orders_6m,
       ROUND(SUM(l.quantity * COALESCE(l.unit_price, p.list_price) * (1 - l.discount_pct))::numeric, 2) AS revenue_6m,
       MAX(o.order_date)                                                                       AS last_order_date
FROM "erp"."orders" o
JOIN "erp"."order_lines" l ON l.order_id = o.order_id
LEFT JOIN "erp"."products" p ON p.product_id = l.product_id
WHERE o.status = 'COMPLETED'
  AND o.order_date >= DATE '${min_order_date}'
GROUP BY o.customer_id
"""
    p.recipe("compute_recent_orders", "sql_query", ["orders", "order_lines", "products"], ["recent_orders"], sql, "sql")
    py = '''# -*- coding: utf-8 -*-
import dataiku
import pandas as pd

gap = int(dataiku.get_custom_variables()["session_gap_minutes"])

ev = dataiku.Dataset("web_events").get_dataframe()
ev["event_ts"] = pd.to_datetime(ev["event_ts"])
ev = ev.sort_values(["customer_id", "event_ts", "event_id"])

# a new session starts when the gap to the previous event of the same customer exceeds `gap` minutes
prev = ev.groupby("customer_id")["event_ts"].shift(1)
ev["new_session"] = prev.isna() | ((ev["event_ts"] - prev).dt.total_seconds() > gap * 60)
ev["session_no"] = ev.groupby("customer_id")["new_session"].cumsum()

sessions = (ev.groupby(["customer_id", "session_no"])
              .agg(session_start=("event_ts", "min"), session_end=("event_ts", "max"),
                   n_events=("event_id", "count"),
                   has_checkout=("event_type", lambda s: int((s == "checkout").any())))
              .reset_index())
sessions["duration_min"] = (sessions["session_end"] - sessions["session_start"]).dt.total_seconds() / 60.0

dataiku.Dataset("sessions").write_with_schema(sessions)
'''
    p.recipe("compute_sessions", "python", ["web_events"], ["sessions"], py, "py", extra={"envSelection": {"envMode": "EXPLICIT_ENV", "envName": "py39_pandas"}})
    g = {"keys": [{"column": "customer_id"}], "globalCount": True,
         "values": [gval("n_events", sum=True), gval("has_checkout", sum=True), gval("duration_min", avg=True)]}
    p.recipe("compute_sessions_by_customer", "grouping", ["sessions"], ["sessions_by_customer"], g, "grouping")
    piv = {"explicitIdentifiers": ["customer_id"], "keyColumns": ["event_type"], "valueColumns": [], "computeCount": True,
           "explicitValues": ["add_to_cart", "checkout", "view"], "otherColumnsAggregation": "NONE"}
    p.recipe("compute_event_pivot", "pivot", ["web_events"], ["event_pivot"], piv, "pivot")
    steps = [step("StringTransformer", appliesTo="SINGLE_COLUMN", columns=["country"], mode="TRIM"),
             step("StringTransformer", appliesTo="SINGLE_COLUMN", columns=["country"], mode="TO_UPPER"),
             step("FindReplace", appliesTo="SINGLE_COLUMN", columns=["country"], output="country_name", matching="FULL_STRING", normalization="EXACT",
                  mapping=[{"from": "DE", "to": "Germany"}, {"from": "AT", "to": "Austria"}, {"from": "CH", "to": "Switzerland"},
                           {"from": "NL", "to": "Netherlands"}, {"from": "FR", "to": "France"}], stopAfterFirstMatch=True),
             step("StringTransformer", appliesTo="SINGLE_COLUMN", columns=["segment"], mode="TRIM"),
             step("StringTransformer", appliesTo="SINGLE_COLUMN", columns=["segment"], mode="TO_UPPER"),
             step("FillEmptyWithValue", appliesTo="SINGLE_COLUMN", columns=["segment"], value="UNKNOWN"),
             step("FilterOnValue", appliesTo="SINGLE_COLUMN", columns=["status"], values=["X"], matchingMode="FULL_STRING", normalizationMode="EXACT", action="REMOVE_ROW"),
             step("ColumnsSelector", appliesTo="COLUMNS", columns=["customer_id", "country", "country_name", "segment", "status"], keep=True)]
    p.recipe("compute_customers_prepared", "shaker", ["customers"], ["customers_prepared"], shaker(steps), "shaker")
    j = {"virtualInputs": [{"index": i} for i in range(4)],
         "joins": [{"table1": 0, "table2": t, "type": "LEFT", "on": [{"column1": {"name": "customer_id", "table": 0}, "column2": {"name": "customer_id", "table": t}, "type": "EQ"}]} for t in (1, 2, 3)],
         "selectedColumns": [{"name": c, "table": 0} for c in ["customer_id", "country", "country_name", "segment", "status"]]
         + [{"name": c, "table": 1} for c in ["orders_6m", "revenue_6m", "last_order_date"]]
         + [{"name": "count", "table": 2, "alias": "sessions"}, {"name": "n_events_sum", "table": 2, "alias": "total_events"},
            {"name": "has_checkout_sum", "table": 2, "alias": "checkout_sessions"}, {"name": "duration_min_avg", "table": 2, "alias": "avg_session_minutes"}]
         + [{"name": c, "table": 3} for c in ["add_to_cart_count", "checkout_count", "view_count"]],
         "computedColumns": []}
    p.recipe("compute_customer_360_joined", "join", ["customers_prepared", "recent_orders", "sessions_by_customer", "event_pivot"], ["customer_360_joined"], j, "join")
    steps = [step("FillEmptyWithValue", appliesTo="COLUMNS", columns=["orders_6m", "revenue_6m", "sessions", "total_events", "checkout_sessions",
                                                                         "add_to_cart_count", "checkout_count", "view_count"], value="0"),
             step("CreateColumnWithGREL", column="days_since_last_order",
                  expression='if(isBlank(last_order_date), -1, diff(asDate("${analysis_end}"), last_order_date, "days"))'),
             step("CreateColumnWithGREL", column="conversion_rate", expression="if(view_count > 0, round(checkout_count / view_count * 10000) / 10000, 0)"),
             step("CreateColumnWithGREL", column="engagement_tier",
                  expression='if(revenue_6m >= 500 && sessions >= 3, "HIGH", if(revenue_6m >= 100 || sessions >= 1, "MEDIUM", "LOW"))'),
             step("RoundProcessor", appliesTo="SINGLE_COLUMN", columns=["avg_session_minutes"], mode="ROUND", places=1)]
    p.recipe("compute_customer_360_scored", "shaker", ["customer_360_joined"], ["customer_360_scored"], shaker(steps), "shaker")
    win = {"windows": [{"prefix": "", "enablePartitioning": True, "partitioningColumns": ["country"], "enableOrdering": True,
                        "orders": [{"column": "revenue_6m", "desc": True}], "rowNumber": False, "rank": True, "denseRank": False}],
           "values": [], "postFilter": {"enabled": False}}
    p.recipe("compute_customer_360", "window", ["customer_360_scored"], ["customer_360"], win, "window")
    p.scenario("DAILY_REFRESH", {"name": "Daily refresh", "type": "step_based", "active": True,
                                 "triggers": [{"type": "temporal", "params": {"frequency": "Daily", "hour": 6, "minute": 30, "timezone": "Europe/Berlin"}}],
                                 "params": {"steps": [{"type": "build_flowitem", "params": {"builds": [{"type": "DATASET", "itemId": "customer_360"}], "jobType": "RECURSIVE_FORCED_BUILD"}},
                                                      {"type": "check_dataset", "params": {"checks": [{"type": "recordCount", "minValue": 300}]}}]},
                                 "reporters": [{"messaging": {"type": "mail-scenario", "configuration": {"recipients": "crm-analytics@retail.example", "subject": "Customer 360 refreshed"}},
                                                "phase": "END", "runConditionEnabled": True, "runCondition": "outcome != 'SUCCESS'"}]})


if __name__ == "__main__":
    p01(); p02(); p03(); p04(); p05()
    print("wrote 5 DSS project exports to", OUT)
