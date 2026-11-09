#!/usr/bin/env python3
"""
designer_gen.py: build a Lakeflow Designer (.designer.ipynb) file from a pipeline spec.

Only emits operator templates whose generated run() bodies have been verified in real
Designer exports (see designer_templates.json): source, output, transform, filter, join,
aggregate, combine, limit, pivot, sql, python, ai_function, markdown.

Spec (JSON or Python dict):
{
  "name": "pipeline_name",
  "readme": "# Markdown note shown on the canvas",
  "nodes": [
    {"id": "src_orders", "template": "source", "desc": "...",
     "config": {"table_source": {"tableName": "cat.sch.orders"}}},
    {"id": "clean", "template": "transform", "desc": "...",
     "config": {"expressions": ["*", "UPPER(name) AS name_uc"]},
     "inputs": ["src_orders"]},
    {"id": "j", "template": "join", "config": {...},
     "inputs": [{"node": "clean", "input_port": "left"}, {"node": "src_cust", "input_port": "right"}]},
    {"id": "out", "template": "output",
     "config": {"catalog": "cat", "schema": "sch", "table_name": "t"}, "inputs": ["j"]}
  ]
}
An input can be a node id (default ports are used) or a dict with
"node", optional "output_port" (upstream port) and optional "input_port".

Usage: python3 designer_gen.py spec.json out.designer.ipynb
"""
import json
import os
import sys
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
TPL = json.load(open(os.path.join(HERE, "designer_templates.json")))

OUT_PORT = {
    "source": "data", "transform": "transformed_data", "filter": "filtered_data",
    "join": "joined_data", "aggregate": "aggregated_data", "combine": "combined_data",
    "limit": "limited_data", "pivot": "pivoted_data", "sql": "result", "python": "result",
    "ai_function": "ai_data", "output": None,
}
LIST_INPUT = {"sql", "python"}          # inputs["data"] is a list + data__sources
VERSION = {"combine": None}             # combine is only verified without templateVersion
SUPPORTED = set(OUT_PORT)


# ---------------------------------------------------------------- YAML emitter
def _y_scalar(v):
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return json.dumps(v)
    return json.dumps(str(v), ensure_ascii=False)   # JSON string == valid YAML double-quoted scalar


def _y(v, ind):
    pad = " " * ind
    out = []
    if isinstance(v, dict):
        for k, x in v.items():
            if isinstance(x, str) and "\n" in x:
                out.append(f"{pad}{k}: |-")
                out += [f"{pad}  {line}" if line else "" for line in x.split("\n")]
            elif isinstance(x, (dict, list)) and x:
                out.append(f"{pad}{k}:")
                out += _y(x, ind + 2)
            elif isinstance(x, (dict, list)):
                out.append(f"{pad}{k}: {'{}' if isinstance(x, dict) else '[]'}")
            else:
                out.append(f"{pad}{k}: {_y_scalar(x)}")
    elif isinstance(v, list):
        for x in v:
            if isinstance(x, dict) and x:
                sub = _y(x, ind + 2)
                sub[0] = f"{pad}- " + sub[0].lstrip()
                out += sub
            elif isinstance(x, str) and "\n" in x:
                out.append(f"{pad}- |-")
                out += [f"{pad}  {line}" for line in x.split("\n")]
            else:
                out.append(f"{pad}- {_y_scalar(x)}")
    return out


# ------------------------------------------------------------- Python literal
def _py(v, ind=0):
    pad, pad2 = " " * ind, " " * (ind + 4)
    if v is None:
        return "None"
    if isinstance(v, bool):
        return "True" if v else "False"
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, str):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, list):
        if not v:
            return "[]"
        return "[\n" + ",\n".join(pad2 + _py(x, ind + 4) for x in v) + "\n" + pad + "]"
    if isinstance(v, dict):
        if not v:
            return "{}"
        return "{\n" + ",\n".join(f"{pad2}{json.dumps(k)}: {_py(x, ind + 4)}" for k, x in v.items()) + "\n" + pad + "}"
    raise TypeError(type(v))


def _norm_inputs(node, nodes_by_id):
    t = node["template"]
    raw = node.get("inputs", [])
    res = []
    for i, inp in enumerate(raw):
        d = {"node": inp} if isinstance(inp, str) else dict(inp)
        up = nodes_by_id[d["node"]]
        d.setdefault("output_port", OUT_PORT[up["template"]])
        if "input_port" not in d:
            if t == "join":
                d["input_port"] = ["left", "right"][i]
            elif t == "combine":
                d["input_port"] = f"data_{i}"
            else:
                d["input_port"] = "data"
        res.append(d)
    return res


def _layout(nodes, nodes_by_id):
    depth = {}
    def dep(nid):
        if nid in depth:
            return depth[nid]
        ins = [i if isinstance(i, str) else i["node"] for i in nodes_by_id[nid].get("inputs", [])]
        depth[nid] = 0 if not ins else 1 + max(dep(x) for x in ins)
        return depth[nid]
    lanes = {}
    for n in nodes:
        d = dep(n["id"])
        lane = lanes.get(d, 0)
        lanes[d] = lane + 1
        n.setdefault("x", 300 * d)
        n.setdefault("y", 155 * lane + 360)


def _code_cell(node, inputs):
    t = node["template"]
    cfg = node.get("config", {})
    hdr = {"id": node["id"], "template": t}
    ver = VERSION.get(t, "1.0.0")
    if ver:
        hdr["templateVersion"] = ver
    hdr["name"] = node["id"]
    hdr["position"] = {"x": node["x"], "y": node["y"]}
    hdr["description"] = {"text": node.get("desc", ""), "hash": ""}
    hdr["previewMode"] = "full" if t == "output" else "1000"
    hdr["config"] = cfg
    hdr["input"] = [{"node": i["node"], "input_port": i["input_port"], "output_port": i["output_port"]} for i in inputs]
    yaml_txt = "\n".join(_y(hdr, 0))

    if t == "python":
        user = cfg.get("code", "")
        body = TPL["bodies"]["python_head"] + "\n" + "\n".join(("    " + l) if l else "" for l in user.split("\n")) + "\n\n    return {\"result\": result}"
        wiring_cfg = {}
    else:
        body = TPL["bodies"][t]
        wiring_cfg = cfg

    if t in LIST_INPUT:
        win = {"data": None, "data__sources": [
            {"node": i["node"], "output_port": i["output_port"], "name": i["node"], "df_name": i["node"]} for i in inputs]}
        data_list = "[\n" + ",\n".join(f'        ctx["{i["node"]}.{i["output_port"]}"]' for i in inputs) + "\n    ]" if inputs else "[]"
        inputs_txt = "{\n    \"data\": " + data_list + ",\n    \"data__sources\": " + _py(win["data__sources"], 4) + "\n}"
    elif inputs:
        inputs_txt = "{\n" + ",\n".join(f'    "{i["input_port"]}": ctx["{i["node"]}.{i["output_port"]}"]' for i in inputs) + "\n}"
    else:
        inputs_txt = "{}"

    wiring = ("# generated from the system\nctx = globals().setdefault(\"ctx\", {})\n"
              f"config = {_py(wiring_cfg)}\ninputs = {inputs_txt}\nout = run(config, inputs, spark)\n")
    op = OUT_PORT[t]
    if op:
        wiring += f'ctx["{node["id"]}.{op}"] = out["{op}"]\n'
    src = f'"""\n{yaml_txt}\n"""\n\n{body}\n\n{wiring}'
    meta = json.loads(json.dumps(TPL["code_cell_metadata"]))
    meta["application/vnd.databricks.v1+cell"]["nuid"] = str(uuid.uuid4())
    return {"cell_type": "code", "execution_count": 0, "metadata": meta, "outputs": [],
            "source": src.splitlines(keepends=True)}


def _md_cell(nid, md, x, y, w=760, h=330):
    hdr = {"id": nid, "template": "markdown", "name": nid, "position": {"x": x, "y": y},
           "dimensions": {"width": w, "height": h}, "config": {"md": md}}
    src = "---\n" + "\n".join(_y(hdr, 0)) + "\n---"
    meta = json.loads(json.dumps(TPL["md_cell_metadata"]))
    meta["application/vnd.databricks.v1+cell"]["nuid"] = str(uuid.uuid4())
    return {"cell_type": "markdown", "metadata": meta, "source": src.splitlines(keepends=True)}


def build(spec):
    nodes = [dict(n) for n in spec["nodes"]]
    by_id = {n["id"]: n for n in nodes}
    for n in nodes:
        if n["template"] not in SUPPORTED:
            raise ValueError(f"{n['id']}: template '{n['template']}' is not a verified template")
        if n["template"] == "output" and len(n.get("inputs", [])) != 1:
            raise ValueError(f"{n['id']}: output needs exactly one input")
    _layout(nodes, by_id)
    order, seen = [], set()
    def visit(nid):
        if nid in seen:
            return
        for i in by_id[nid].get("inputs", []):
            visit(i if isinstance(i, str) else i["node"])
        seen.add(nid); order.append(nid)
    for n in nodes:
        visit(n["id"])
    cells = []
    if spec.get("readme"):
        cells.append(_md_cell("readme", spec["readme"], 0, -40, spec.get("readme_w", 900), spec.get("readme_h", 360)))
    for nid in order:
        n = by_id[nid]
        cells.append(_code_cell(n, _norm_inputs(n, by_id)))
    md = json.loads(json.dumps(TPL["nb_metadata"]))
    nbm = md["application/vnd.databricks.v1+notebook"]
    nbm["notebookName"] = spec["name"] + ".designer.ipynb"
    nbm["environmentMetadata"] = {"base_environment": "", "dependencies": spec.get("dependencies", []), "environment_version": "5"}
    return {"cells": cells, "metadata": md, "nbformat": TPL.get("nbformat") or 4, "nbformat_minor": TPL.get("nbformat_minor") or 0}


def to_py_notebook(nb):
    """Same code as the Designer run, as a plain Databricks .py notebook (for headless testing)."""
    parts = ["# Databricks notebook source"]
    for c in nb["cells"]:
        if c["cell_type"] == "code":
            parts.append("".join(c["source"]))
    return "\n\n# COMMAND ----------\n\n".join(parts) + "\n"


if __name__ == "__main__":
    spec = json.load(open(sys.argv[1]))
    nb = build(spec)
    json.dump(nb, open(sys.argv[2], "w"), indent=1, ensure_ascii=False)
    print("wrote", sys.argv[2], "cells:", len(nb["cells"]))
