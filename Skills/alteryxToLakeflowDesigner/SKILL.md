---
name: alteryx-to-vdp
description: Convert Alteryx Designer workflows (.yxmd / .yxmc XML files) into Databricks Lakeflow Designer Visual Data Prep pipelines. Maps the full Alteryx tool palette (In/Out, Preparation, Join, Parse, Transform, Data Investigation, Predictive, Time Series, Spatial, Reporting, Documentation, Developer, Interface, Macros) to the actual VDP operators (Source, Output, AI Function, Aggregate, Combine, Filter, Join, Limit, Pivot, Sort, SQL, Transform, Python, Note, Group), handles all common input/output file formats (CSV, TSV, Excel, JSON, XML, Parquet, Avro, ORC, Delta, SAS, SPSS, R, geospatial, PDF, .yxdb), and always materializes output to a Unity Catalog Delta table. Validates against expected output when provided.
---

# Skill: Convert Alteryx Workflow (.yxmd / .yxmc) to Lakeflow Designer Visual Data Prep

## Objective

When a user provides an Alteryx workflow file (`.yxmd` or `.yxmc`), convert it into a fully functional Lakeflow Designer (Visual Data Prep) pipeline. The pipeline must:
1. Reproduce the exact logic of the Alteryx workflow.
2. Always materialize the final output to a Unity Catalog Delta table.
3. Validate results against expected output when provided by the user.
4. Cover the full Alteryx tool palette and all common file formats — flagging any tool/format that has no automatic VDP equivalent so the user can address it manually.

---

## VDP Operator Reference (the only templates you should emit)

> **Sources of truth**:
> - UI reference: [Built-in operators in Lakeflow Designer (Microsoft Learn)](https://learn.microsoft.com/en-us/azure/databricks/designer/built-in-operators)
> - YAML schema: verified against actual Designer exports (`*.designer.ipynb`)
>
> **The UI label and the YAML template name diverge in one case**: the operator the UI calls **Note** is exported with `template: markdown` and uses `config.md` for its content. Always emit `markdown`.

| Template (YAML) | UI label | Inputs (port → upstream output) | Output port | Required `config` keys |
|---|---|---|---|---|
| `source` | Source | (none) | `data` | `file_source: {path, format, header, inferSchema, ...}` **OR** `table_source: {tableName: "catalog.schema.table"}` |
| `output` | Output | `data` | (terminal) | `catalog`, `schema`, `table_name` |
| `ai_function` | AI Function | `data` | `ai_data` | `expressions: [SQL expressions calling ai_* functions]` (returns those columns plus `*`) |
| `aggregate` | Aggregate | `data` | `aggregated_data` | `group_bys: [{expr, type: expr}, ...]`, `aggregations: [{columnExpr: {expr, type: expr}, fn, alias}, ...]`. Supported `fn`: AVG, COUNT, MAX, MEAN, MEDIAN, MIN, PERCENTILE, STDDEV, SUM, VARIANCE |
| `combine` | Combine | `data_0`, `data_1` | `combined_data` | `operator`: UNION / INTERSECT / EXCEPT / MINUS; `quantifier`: ALL / DISTINCT |
| `filter` | Filter | `data` | `filtered_data` | `condition: "<SQL boolean expression>"` (the UI is a visual builder but the export is a SQL string — feel free to write SQL directly) |
| `join` | Join | `left`, `right` | `joined_data` | `join_type`: inner / left / right / full / cross_join; `join_conditions: "left.col_a = right.col_b AND ..."` (always use the `left.` / `right.` aliases — that's what the runtime aliases the inputs as); optional `expressions: [select-expressions]` |
| `limit` | Limit | `data` | `limited_data` | `n: <integer>` — verified |
| `pivot` | Pivot | `data` | `pivoted_data` | **Rows → Columns mode**: `mode: pivot`, `group_by: [<col>, ...]`, `pivot_column: <col>`, `value_column: <col>`, `agg: count\|sum\|avg\|min\|max`. **Columns → Rows mode**: `mode: unpivot`, `id_columns: [...]`, `value_columns: [...]`, `key_name: <out_key>`, `value_name: <out_value>` — verified |
| `sort` | Sort | `data` | `sorted_data` | `sort_expressions: [{columnExpr: {expr, type: expr}, sortBy: ASC / DESC}, ...]` |
| `sql` | SQL | `data` (LIST), plus `data__sources` (LIST of `{node, output_port, name, df_name}`) | `result` | `query: "<SQL>"` — references each upstream by its operator `name` as a temp view; supports `:param` widget bindings |
| `transform` | Transform | `data` | `transformed_data` | `expressions: ["*", "expr AS \`alias\`", ...]` — `selectExpr`-style strings; `"*"` keeps all upstream columns |
| `python` | Python | `data` (LIST) | `result` | `code: "<PySpark>"` — `inputs["data"][i]`; assign final DataFrame to `result` |
| `markdown` | Note | (none) | (none) | `md: "<Markdown body>"`; optional `dimensions: {width, height}` |
| `group` | Group | (visual only) | (visual only) | `config: {}`, `input: []`, plus `position: {x, y}` and `dimensions: {width, height}`. Children are not wired via the YAML; place child operator cells inside the bounding box visually. Verified — imports cleanly and renders as a labeled container. |

Anything Alteryx does that doesn't map to one of these uses `python`, `sql`, or `ai_function`. If none of them can express it (UI forms, rendered reports, etc.), mark it **MANUAL** and emit a `markdown` node explaining what the user must do outside the pipeline.

### Designer file format

Designer pipelines are stored as Jupyter notebooks named `<pipeline>.designer.ipynb`. Each operator is one notebook cell. A cell's `source` always has three parts:

1. A triple-quoted **YAML docstring** at the top describing the operator (this is what tooling reads).
2. The boilerplate **`def run(config, inputs, spark): ...`** body — auto-generated; do not edit.
3. A **wiring block** at the bottom that builds `config = {...}`, builds `inputs = {...}` from a shared `ctx` dict, calls `run`, and stores the output back in `ctx[<name>.<output_port>]`.

Canonical cell skeleton (use this template for every operator you emit):

```python
"""
id: <unique_snake_case_id>
template: <template_name>
name: <operator_name_no_spaces>
position:
  x: <int>
  y: <int>
description:
  text: <one-line description; UI auto-generates one if you omit it>
  hash: <content hash; UI manages this>
previewMode: "1000"
config:
  <template-specific keys — see operator reference table>
input:
  - node: <upstream_operator_id>
    input_port: <port name on this operator>
    output_port: <port name on the upstream operator>
"""
# the run() body and the wiring block below are auto-generated by Designer.
```

Markdown ("Note") cells use `cell_type: markdown` and the source is:

```
---
id: <id>
template: markdown
name: <name>
position: { x: 0, y: 0 }
dimensions: { width: 500, height: 280 }
config:
  md: |
    # Markdown body
    ...
---
```

Layout coordinates: x increases left → right (typical step ≈ 260px), y increases top → bottom (typical lane spacing ≈ 145px). The pipeline canvas accepts negative y for header notes.

**YAML quoting — non-negotiable**: any free-text scalar that may contain `:`, `#`, `{`, `}`, `[`, `]`, `,`, or leading/trailing whitespace MUST be double-quoted. The most common offender is `description.text` (e.g. `"Per-category metrics: avg / median / sum / count."`). If the YAML docstring fails to parse, Designer silently drops the cell from the dataflow graph and downstream cells fail with `'<this>.<port>' data is missing or not created before use` — which looks like a wiring problem but is actually a YAML problem in the upstream cell.

### When to reach for `ai_function`

Several Alteryx tools have no traditional SQL equivalent but map naturally to a Databricks AI function. Prefer `ai_function` over a hand-rolled `python` + LLM call. The full set of functions exposed in the operator dropdown:

| Function | Description | Alteryx pattern it replaces |
|---|---|---|
| `ai_analyze_sentiment` | Perform sentiment analysis on input text | Sentiment Analysis (predictive) |
| `ai_classify` | Classify text or parsed documents according to the labels you provide | Text categorization / rules-based classification |
| `ai_extract` | Extract structured data from text or parsed documents according to the fields you provide | Free-text → structured fields (no fixed schema for RegEx) |
| `ai_fix_grammar` | Correct grammatical errors in text | Data Cleansing (text quality), bespoke regex cleanups |
| `ai_gen` | Answer the user-provided prompt (generic LLM call) | Custom Python tool with an LLM call; "use a model to decide…" patterns |
| `ai_mask` | Mask specified entities in text | PII redaction (often built ad-hoc in Alteryx with regex chains) |
| `ai_similarity` | Compare two strings and compute the semantic similarity score | Fuzzy Match (beyond `levenshtein` / `soundex`) |
| `ai_summarize` | Generate a summary of text | Long-form text reduction; report-feeder summaries |
| `ai_translate` | Translate text to a specified target language | Multi-language normalization before downstream joins |

---

## Step 1: Read and Analyze the Alteryx Workflow

1. Read the `.yxmd` / `.yxmc` file — it is XML containing `<Node>` elements (tools) and `<Connection>` elements (wires).
2. For each `<Node>`, identify the tool from the `<Plugin>` / `<EngineSettings>` attribute (e.g. `AlteryxBasePluginsGui.DbFileInput.DbFileInput`).
3. Build a directed graph from `<Connection>` (`Origin`→`Destination`).
4. Extract per-node configuration from `<Properties>/<Configuration>` (formulas, filter expressions, join keys, group-by fields, etc.).
5. Identify:
   - Input sources (files, databases, tables, directories)
   - Transformation logic (formulas, filters, joins, aggregations, parses)
   - Output destinations (files, tables, reports)
   - Branching/splitting (T/F outputs of Filter, L/J/R outputs of Join, etc.)
   - Containers (Tool Container) and comments — preserve as `group` / `markdown`
   - Macros referenced — note `.yxmc` paths; handle per Step 11
   - Interface tools — note presence; handle per Step 12

---

## Step 2: Tool Palette → VDP Operator Mapping

The mapping is grouped by Alteryx's official tool categories so tools can be located the way they appear in Designer. Tools marked **MANUAL** have no automatic conversion; emit a `markdown` node and tell the user what to do.

### 2.1 In/Out

| Alteryx Tool | VDP Operator | Notes |
|---|---|---|
| Input Data (file) | `source` (file_source) | UC Volume path; `format` from extension |
| Input Data (DB / ODBC / OLEDB) | `source` (table_source) or `python` (JDBC) | Use UC Connections / Lakehouse Federation when possible |
| Output Data (table) | `output` | catalog + schema + table_name |
| Output Data (file) | `python` after `output` | Write to a UC Volume (see Step 9b) |
| Browse | *omit* | Browse is just a preview tile — no VDP analog needed |
| Text Input | `python` | `spark.createDataFrame(rows, schema)` |
| Directory | `python` | `os.listdir` over a Volume path; see Step 4 |
| Date/Time Now | `transform` | `current_timestamp()` / `current_date()` |
| Map Input | **MANUAL** | Designer-only; replace with a Volume-hosted file |

### 2.2 Preparation

| Alteryx Tool | VDP Operator | Notes |
|---|---|---|
| Auto Field | `transform` | Cast columns; let Spark infer or pick narrowest type |
| Data Cleansing (whitespace / case / nulls) | `transform` | TRIM, LOWER/UPPER, REPLACE, NULLIF |
| Data Cleansing (grammar / typos) | `ai_function` | `ai_fix_grammar(text)` |
| Data Cleansing (PII redaction) | `ai_function` | `ai_mask(text, ARRAY('EMAIL','PHONE','SSN',...))` |
| Filter | `filter` | `config.condition` is a free-form SQL boolean string (the UI is a visual builder; the export is SQL). Alteryx T/F outputs become two parallel `filter` operators with inverse conditions — Designer's Filter has only one output port (`filtered_data`). |
| Formula | `transform` | One row per output column with a SQL expression |
| Imputation | `sql` | `COALESCE(col, AVG(col) OVER ())` etc. |
| Multi-Field Formula | `transform` | Apply same expression to a list of columns |
| Multi-Row Formula | `sql` | `LAG`/`LEAD` window functions |
| Random % Sample | `sql` | `WHERE rand() < 0.1` (with seed if reproducibility needed) |
| Record ID | `sql` | `ROW_NUMBER() OVER (ORDER BY ...)` |
| Sample / First N / Last N / Skip 1st N | `limit` or `sql` | First N → `limit`; Last N → `ROW_NUMBER` desc + filter |
| Select | `transform` | Reorder, rename, drop, retype |
| Select Records | `sql` | Range-based: `WHERE rn BETWEEN a AND b` |
| Sort | `sort` | One or more `column ASC|DESC` |
| Tile | `sql` | `NTILE(n) OVER (...)` |
| Unique | `sql` | `ROW_NUMBER() ... WHERE rn = 1` (see Step 7) |

### 2.3 Join

| Alteryx Tool | VDP Operator | Notes |
|---|---|---|
| Join | `join` | Designer Join supports Full / Inner / Left / Right only. Alteryx's three outputs (L = unmatched left, J = matched, R = unmatched right) → recreate via a Left join (J + L by null check) and a parallel Right join (R), or use `sql` with `LEFT ANTI`/`RIGHT ANTI`. |
| Join Multiple | chain of `join` | Or one `sql` with multi-table FROM |
| Append Fields (cross join) | `sql` | Designer Join has no cross-join — emit `SELECT * FROM left CROSS JOIN right`. |
| Union | `combine` | `operator: UNION`, `quantifier: ALL` (= UNION ALL) or `DISTINCT` (= UNION DISTINCT) |
| Set difference (Alteryx Join L-only output, in isolation) | `combine` | `operator: EXCEPT` (or `MINUS`). Designer's Combine also exposes `INTERSECT` — Alteryx has no native equivalent for either. |
| Find Replace | `join` + `transform` | Left join to lookup table, COALESCE replacement, drop lookup cols |
| Make Group | `sql` | Connected-components — emit a stub + flag **REVIEW** (rare; ask user) |
| Fuzzy Match (string-distance) | `python` | `levenshtein`/`soundex`/`jaro_winkler`; flag **REVIEW** for tuning |
| Fuzzy Match (semantic) | `ai_function` | `ai_similarity(left_text, right_text)` then threshold |

### 2.4 Parse

| Alteryx Tool | VDP Operator | Notes |
|---|---|---|
| DateTime | `transform` | `to_date`, `to_timestamp`, `date_format`, `unix_timestamp` |
| RegEx (Parse) | `transform` | `regexp_extract(col, pattern, n)` per capture group |
| RegEx (Replace) | `transform` | `regexp_replace(col, pattern, repl)` |
| RegEx (Tokenize) | `sql` | `explode(split(regexp_extract_all(...)))` |
| Text To Columns | `sql` | `split` + index, or `explode` for rows |
| XML Parse | `python` | `from_xml` (spark-xml) or `pyspark.sql.functions.xpath_*` |
| JSON Parse | `transform` | `from_json(col, schema)` then expand struct |
| Free-text → fields (no fixed schema) | `ai_function` | `ai_extract(text, ARRAY('field_a','field_b',...))` returns a struct |

### 2.5 Transform

| Alteryx Tool | VDP Operator | Notes |
|---|---|---|
| Arrange | `python` | Reshape — typically `melt` then `pivot`; flag **REVIEW** |
| Count Records | `aggregate` | Single `COUNT(*)` aggregation, no group_bys |
| Cross Tab | `pivot` | **Rows → Columns** mode; pick pivot column + value/aggregation |
| Running Total | `sql` | `SUM(col) OVER (PARTITION BY ... ORDER BY ...)` |
| Summarize (Sum/Avg/Count/Min/Max/Median/Stddev/Variance/Percentile) | `aggregate` | Map directly to the supported aggregations |
| Summarize (First / Last / Concat) | `sql` | Designer's Aggregate does NOT expose first/last/collect_list — use `FIRST_VALUE`, `LAST_VALUE`, or `concat_ws(',', collect_list(col))` |
| Transpose | `pivot` | **Columns → Rows** mode |
| Weighted Average | `sql` | `SUM(value*weight) / SUM(weight)` per group |

### 2.6 Data Investigation

These are exploratory; in VDP they're typically intermediate `aggregate`/`sql` nodes, not pipeline outputs.

| Alteryx Tool | VDP Operator | Notes |
|---|---|---|
| Field Summary | `sql` | `describe`-style query: count/mean/stddev/min/max per column |
| Frequency Table | `aggregate` | GROUP BY col, COUNT(*) |
| Pearson / Spearman Correlation | `python` | `df.stat.corr(...)` per pair, or `Correlation.corr` (MLlib) |
| Histogram | `sql` | `WIDTH_BUCKET` or manual binning |
| Scatterplot / Distribution / Association | **MANUAL → Lakeview** | Build a Lakeview (AI/BI) dashboard chart on the Delta output instead |

### 2.7 Predictive

Predictive tools have no native VDP operator. Emit a `python` operator with the equivalent ML code, and add a `markdown` node telling the user to track the run in MLflow.

| Alteryx Tool | VDP Operator | Notes |
|---|---|---|
| Linear / Logistic Regression | `python` | `pyspark.ml.regression` / `classification`; log to MLflow |
| Decision Tree / Forest / Boosted | `python` | `pyspark.ml.classification` / `xgboost-spark` |
| Score | `python` | Load MLflow model URI; `.transform(df)` |
| Cross Validation | `python` | `pyspark.ml.tuning.CrossValidator` |
| Create Samples (train/valid/test) | `sql` | `randomSplit` via Python, or `WHERE rand() < ...` |
| Sentiment Analysis | `ai_function` | `ai_analyze_sentiment(text)` — no model training needed |
| Text Classification | `ai_function` | `ai_classify(text, ARRAY('cls_a','cls_b',...))` |
| Topic Modeling / Categorize | `ai_function` | `ai_classify` with the candidate topic list |
| AB Trend / AB Controls | **MANUAL** | Niche — flag for user review |

### 2.8 Time Series

| Alteryx Tool | VDP Operator | Notes |
|---|---|---|
| TS Filler | `sql` | `sequence(min(ts), max(ts), interval)` + LEFT JOIN |
| TS Plot | **MANUAL → Lakeview** | Build a line chart on the materialized table |
| ARIMA / ETS / TS Forecast | `python` | `statsmodels` / `prophet` / `pyspark.ml`; log to MLflow |
| TS Compare | `python` | Compute MAPE/RMSE per model; emit comparison table |

### 2.9 Spatial

Spatial tools require Sedona (or H3) on the cluster. If unavailable, mark **MANUAL**.

| Alteryx Tool | VDP Operator | Notes |
|---|---|---|
| Buffer | `python` (Sedona) | `ST_Buffer(geom, distance)` |
| Distance | `python` (Sedona) | `ST_Distance(a, b)` |
| Find Nearest | `python` (Sedona) | `ST_Distance` + `ROW_NUMBER` per point |
| Generalize | `python` (Sedona) | `ST_Simplify` |
| Make Grid | `python` (Sedona) | Generate fishnet via `ST_MakeBox2D` loop |
| Poly-Build / Poly-Split | `python` (Sedona) | `ST_MakePolygon`, `ST_Split` |
| Smooth | `python` (Sedona) | `ST_Smooth` if available; otherwise flag **MANUAL** |
| Spatial Info | `python` (Sedona) | `ST_GeometryType`, `ST_NPoints`, `ST_Area` |
| Spatial Match | `python` (Sedona) | `ST_Intersects` / `ST_Contains` join |
| Spatial Process | `python` (Sedona) | `ST_Union`, `ST_Intersection`, `ST_Difference` |
| Trade Area | `python` (Sedona) | `ST_Buffer` (radius) or `ST_ConvexHull` (drive-time → flag manual) |

### 2.10 Reporting

Reporting tools render PDFs / emails / dashboards. VDP doesn't render — output a Delta table and point users at AI/BI (Lakeview) or DBSQL alerts.

| Alteryx Tool | VDP Operator | Notes |
|---|---|---|
| Render | **MANUAL → Lakeview / scheduled email** | Output Delta; build a Lakeview dashboard |
| Email | **MANUAL → Workflow notification** | Use a Lakeflow Job email notification |
| Table / Chart / Map / Layout | **MANUAL → Lakeview widget** | One widget per Alteryx report element |
| Report Header / Footer / Text | **MANUAL** | Documentation only |

### 2.11 Documentation

| Alteryx Tool | VDP Operator | Notes |
|---|---|---|
| Comment / Annotation | `markdown` | Preserve text |
| Tool Container | `group` | Set `parentId` on children |
| Explorer Box | `markdown` | Convert URL to a link |

### 2.12 Developer

| Alteryx Tool | VDP Operator | Notes |
|---|---|---|
| Run Command | **MANUAL** | Replace with a Lakeflow Job task or notebook |
| Throttle | *omit* | Spark schedules its own work |
| Detour | **MANUAL** | Static branching by parameter; convert to two pipelines or `if` in `python` |
| Block Until Done | *omit* | Spark DAG already enforces ordering |
| Message | `markdown` | Static informational text |
| Test | `sql` | `SELECT CASE WHEN <invariant> THEN 'PASS' ELSE 'FAIL'`; or DLT expectations |
| Python Tool | `python` | Direct port; rewrite `Alteryx.read()` → `inputs["data"][i]` |
| R Tool | `python` | Re-implement in PySpark; flag **REVIEW** |

### 2.13 Interface

Interface tools build the form for an Alteryx Analytic App. VDP has no UI form.

| Alteryx Tool | VDP Operator | Notes |
|---|---|---|
| Text Box / List Box / Drop Down / Date / File Browse / Numeric Up Down | **MANUAL** | Map to Lakeflow Job parameters or a Databricks App on top of the pipeline |
| Action / Update / Control Parameter | **MANUAL** | Becomes pipeline parameters (Step 4 `env_config` pattern) |
| Macro Input / Macro Output | depends | Standard macro → reusable pipeline / Python (Step 11) |

### 2.14 Macros

See Step 11 for full handling.

| Macro Type | VDP Approach |
|---|---|
| Standard macro (`.yxmc`) | Inline as a sub-DAG, or extract to a `python` operator |
| Batch macro | `python` operator iterating with Spark |
| Iterative macro | **MANUAL** — orchestrate via Lakeflow Job loop |
| Analytic App | **MANUAL** — Databricks App or Lakeflow Job parameters |

---

## Step 3: File Format Coverage Matrix

| Category | Format | VDP Operator | Notes |
|---|---|---|---|
| **Tabular** | CSV / TSV | `source` (file_source, `format: csv`) | `header`, `delimiter`, `inferSchema` |
| | Fixed-width | `python` | `spark.read.text` + `substring` slices |
| | Excel `.xlsx` / `.xlsm` / `.xlsb` | `python` | `pandas.read_excel` → `spark.createDataFrame`; or `com.crealytics:spark-excel` |
| | Excel `.xls` (legacy) | `python` | Same as above; `pandas` + `xlrd` |
| **Semi-structured** | JSON | `source` (file_source, `format: json`) | Multiline option for pretty JSON |
| | XML | `python` | `spark-xml` (`com.databricks:spark-xml`) `rowTag` |
| | Avro | `source` (file_source, `format: avro`) | |
| | ORC | `source` (file_source, `format: orc`) | |
| | Parquet | `source` (file_source, `format: parquet`) | |
| | Delta | `source` (table_source) | Prefer table_source over a Delta path |
| **Stat packages** | SAS `.sas7bdat` | `python` | `spark-sas7bdat` connector or `pandas`+`pyreadstat` for small files |
| | SPSS `.sav` | `python` | `pandas`+`pyreadstat` |
| | R `.rds` | `python` | `pyreadr.read_r` |
| **Alteryx native** | `.yxdb` | **MANUAL** | Proprietary binary; user must export from Alteryx to CSV/Parquet first |
| | `.yxmd` | n/a | The workflow itself — input to this skill |
| | `.yxmc` | see Step 11 | Macro definition |
| | `.yxi` | **MANUAL** | Packaged tool; not data |
| **Geospatial** | Shapefile `.shp` | `python` (Sedona) | `ShapefileReader.readToGeometryRDD` |
| | GeoJSON | `python` (Sedona) | `ST_GeomFromGeoJSON` |
| | KML / KMZ | `python` | KMZ → unzip → KML; `geopandas.read_file` |
| | MapInfo TAB | **MANUAL** | Convert to Shapefile/GeoJSON externally |
| **Documents** | PDF (tabular) | `python` | `tabula-py` or Databricks AI Functions for table extraction |
| | HTML | `python` | `pandas.read_html` |
| **Compressed** | `.gz` / `.bz2` | `source` | Spark reads transparently if extension is preserved |
| | `.zip` | `python` | Unzip to Volume first; then read |
| | `.7z` | `python` | `py7zr`; unzip to Volume first |
| **Cloud / DB** | S3 / ADLS / GCS | `source` (file_source) | Use UC External Volume mounted to the bucket |
| | UC Tables | `source` (table_source) | Preferred over JDBC for Databricks-native data |
| | Snowflake / Redshift / SQL Server / Oracle / Postgres / Teradata | `python` (JDBC) or UC Federation | Prefer Lakehouse Federation foreign catalog → `table_source` |

**Rule of thumb**: if a format is in the `source.file_source.format` enum, use `source`. Otherwise use `python`. Always copy local files to a UC **Volume** first (Step 4).

---

## Step 4: Source Data Strategy

### Preferred: Unity Catalog Volumes

```yaml
- id: src_orders
  template: source
  name: src_orders
  config:
    file_source:
      path: /Volumes/my_catalog/raw/landing/orders.csv
      format: csv
      header: true
      inferSchema: true
  input: []
```

Under the hood, Designer's Source compiles `file_source` into `SELECT * FROM read_files("<path>", <opts>)`. Any option that `read_files` accepts will be passed through (e.g. `multiLine`, `delimiter`, `dataAddress` for spark-excel). Volume paths (`/Volumes/...`) are reliable for both preview and full execution.

### Avoid: Workspace Paths with `file:` Prefix

- Paths like `file:/Workspace/Users/user@company.com/...` FAIL on full reads — the `@` causes URI authority errors.
- **Workaround**: Always copy source files to a UC Volume first.

### Dynamic Table References (Parameterized Pipelines)

Use a Python `env_config` operator for environment-specific catalogs/schemas:

```yaml
- id: env_config
  template: python
  name: env_config
  config:
    code: |
      CATALOG = "my_catalog"
      SCHEMA  = "my_schema"
      result = spark.createDataFrame([(CATALOG, SCHEMA)], ["catalog", "schema"])
  input: []

- id: src_orders_table
  template: python
  name: src_orders_table
  config:
    code: |
      cfg = inputs["data"][0].collect()[0]
      result = spark.table(f"{cfg.catalog}.{cfg.schema}.orders")
  input:
    - node: env_config
      input_port: data
      output_port: result
```

### Reading Multiple Files from a Volume Folder

```yaml
- id: multi_file_reader
  template: python
  name: multi_file_reader
  config:
    code: |
      # inputs["data"] is a list of input DataFrames
      cfg_df = inputs["data"][0] if inputs["data"] else None
      cfg = cfg_df.collect()[0]

      import os
      vol = f"/Volumes/{cfg.catalog}/{cfg.schema}/raw/folder/"
      files = [f for f in os.listdir(vol) if f.endswith('.csv')]
      dfs = [spark.read.option("header","true").csv(vol + f) for f in files]
      result = dfs[0]
      for d in dfs[1:]:
        result = result.unionByName(d, allowMissingColumns=True)
  input:
    - node: env_config
      input_port: data
      output_port: result
```

> Per the docs, `inputs["data"]` is a **list** of upstream DataFrames in upstream order, and the operator details pane lists their names (e.g. `inputs["data"][0] (customers), inputs["data"][1] (sales)`). Always assign the final DataFrame to `result`.

### Excel via Python (covers .xlsx/.xls/.xlsm/.xlsb)

```yaml
- id: src_excel
  template: python
  name: src_excel
  config:
    code: |
      import pandas as pd
      pdf = pd.read_excel("/Volumes/cat/raw/landing/book.xlsx", sheet_name="Sheet1")
      pdf.columns = [c.strip().lower().replace(" ", "_") for c in pdf.columns]
      result = spark.createDataFrame(pdf)
  input: []
```

### Unity Catalog table source (incl. UC Federation foreign catalogs)

```yaml
- id: src_snowflake_orders
  template: source
  name: src_snowflake_orders
  config:
    table_source:
      tableName: snowflake_fed.sales.orders   # one dotted string: catalog.schema.table
  input: []
```

`table_source.tableName` is a **single dotted string**, not split into `catalog`/`schema`/`table` keys. The runtime calls `spark.table(tableName)`, so anything Unity Catalog can resolve (managed tables, foreign catalogs, views) works.

### AI Function (sentiment / classification / extraction / mask / generate)

`ai_function` is a thin wrapper over `selectExpr`: its `config.expressions` list contains SQL expressions that call the AI SQL functions, aliased to output column names. The runtime appends `*` to the expressions list, so all upstream columns flow through alongside the new AI columns. Output port: `ai_data`.

```yaml
- id: classify_review_sentiment
  template: ai_function
  name: classify_review_sentiment
  config:
    expressions:
      - ai_analyze_sentiment(review_text) AS `sentiment`
  input:
    - node: src_reviews
      input_port: data
      output_port: data

- id: classify_ticket_priority
  template: ai_function
  name: classify_ticket_priority
  config:
    expressions:
      - ai_classify(ticket_body, ARRAY('P0','P1','P2','P3')) AS `priority`
  input:
    - node: src_tickets
      input_port: data
      output_port: data

- id: extract_complaint_fields
  template: ai_function
  name: extract_complaint_fields
  config:
    expressions:
      - ai_extract(complaint_text, ARRAY('product','issue_type','severity')) AS `extracted`
  input:
    - node: src_complaints
      input_port: data
      output_port: data

- id: redact_pii
  template: ai_function
  name: redact_pii
  config:
    expressions:
      - ai_mask(free_text, ARRAY('EMAIL','PHONE','SSN','PERSON')) AS `free_text_masked`
  input:
    - node: src_messages
      input_port: data
      output_port: data

- id: summarize_and_translate
  template: ai_function
  name: summarize_and_translate
  config:
    expressions:
      - ai_summarize(article_body, 3) AS `summary`
      - ai_translate(article_body, 'en') AS `body_en`
  input:
    - node: src_articles
      input_port: data
      output_port: data

- id: generic_llm_call
  template: ai_function
  name: generic_llm_call
  config:
    expressions:
      - ai_gen(prompt_text) AS `response`
  input:
    - node: src_prompts
      input_port: data
      output_port: data
```

You can chain multiple AI calls in a single `expressions` list (one per output column). Empty `expressions: []` is valid — the operator becomes a passthrough. If you ever need fine control over arguments not exposed by the SQL function (e.g. an explicit serving endpoint), use a `sql` operator and call the function there.

---

## Step 5: Column Name Handling

**Delta tables CANNOT have spaces or special characters in column names.**

- Rename columns early: `Product Class` → `product_class`.
- Use backticks to reference source columns with spaces: `` `Product Class` AS product_class ``.
- For columns with `/`, `%`, `&`, `(`, `)`: rename immediately after source.
- Convention: `snake_case` for all internal column names.

---

## Step 6: Type Casting — Handle Dirty Data

Excel/CSV sources may contain error values like `#`, `#N/A`, `#VALUE!`, `#REF!`.

**Option A (preferred): Filter bad rows upstream**

```yaml
- id: remove_bad_data
  template: filter
  name: remove_bad_data
  config:
    condition: Category != '#' AND CAST(Amount AS STRING) != '#'
  input:
    - node: src_orders
      input_port: data
      output_port: data
```

**Option B: Use TRY_CAST in SQL operators**

```yaml
- id: safe_cast
  template: sql
  name: safe_cast
  config:
    query: |
      SELECT
        TRY_CAST(amount AS DOUBLE) AS amount,
        TRY_CAST(quantity AS INT)  AS quantity
      FROM src_orders
  input:
    - node: src_orders
      input_port: data
      output_port: data
```

> The `transform` template may silently revert `TRY_CAST` edits to `CAST`. If this happens, use `filter` or `sql` instead.

---

## Step 7: SQL Operator View Name Rules

- A `sql` operator registers each upstream DataFrame as a temp view using the upstream operator's **display name** (the `name` field).
- Use simple snake_case names (no spaces, no punctuation) for any operator that feeds into a SQL node.
- Reference in FROM clauses: `FROM operator_name`.
- Names with spaces produce `TABLE_OR_VIEW_NOT_FOUND`.

---

## Step 8: Deduplication Pattern (Alteryx Unique)

The `sql` operator registers each upstream DataFrame as a temp view named after the upstream operator's `name` field. Reference it directly in the `FROM` clause:

```yaml
- id: deduplicate
  template: sql
  name: deduplicate
  config:
    query: |
      SELECT * EXCEPT (_dedup_rn)
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY unique_id) AS _dedup_rn
        FROM add_validation
      )
      WHERE _dedup_rn = 1
  input:
    - node: add_validation
      input_port: data
      output_port: transformed_data
```

The runtime auto-builds `inputs["data__sources"]` for each upstream so the temp view name matches the upstream operator's `name`. The SQL operator also supports `:param_name` widget bindings — define widgets in the pipeline parameters panel and reference them as `:param_name` in the query.

---

## Step 9: Output — Always Materialize

### 9a. Canonical Delta output (REQUIRED)

Every converted workflow MUST end with an `output` operator:

```yaml
- id: output_final
  template: output
  name: output_final
  config:
    catalog:    target_catalog
    schema:     target_schema
    table_name: target_table
  input:
    - node: last_transform
      input_port: data
      output_port: <last_operator_output_port>
```

- Confirm the catalog and schema exist; otherwise tell the user: `CREATE SCHEMA IF NOT EXISTS catalog.schema`.
- Preview the output node — "no columns" with no error = success.
- "no compute attached" = user needs to click Run.

### 9b. Additional non-Delta outputs (optional, downstream of 9a)

When the original Alteryx workflow writes a CSV/Parquet/JSON file, add a `python` operator **after** the Delta `output`:

```yaml
- id: write_csv_to_volume
  template: python
  name: write_csv_to_volume
  config:
    code: |
      df = inputs["data"][0]
      (df.coalesce(1)
         .write.mode("overwrite")
         .option("header", "true")
         .csv("/Volumes/cat/exports/orders_csv/"))
      result = df    # pass-through so the node has an output
  input:
    - node: last_transform
      input_port: data
      output_port: <last_operator_output_port>
```

For external DB writes, use a `python` operator with `df.write.format("jdbc")` or write into a UC Federation catalog. The Delta table from 9a remains the canonical output — file/DB writes are sinks, not the source of truth.

---

## Step 10: Data Validation (When Expected Output Provided)

When the user provides an expected output file (CSV, Excel, table):

### 10a. Add a validation source

Read the expected output as a separate `source`/`python` node.

### 10b. Add a validation comparison node

```yaml
- id: validation
  template: sql
  name: validation
  config:
    query: |
      SELECT 'actual'   AS source,
             COUNT(*)                    AS row_count,
             COUNT(DISTINCT key_column)  AS unique_keys,
             ROUND(AVG(metric_column),4) AS avg_metric
      FROM actual_output
      UNION ALL
      SELECT 'expected' AS source,
             COUNT(*),
             COUNT(DISTINCT key_column),
             ROUND(AVG(metric_column),4)
      FROM expected_data
  input:
    - node: actual_output_node
      input_port: data
      output_port: <port>
    - node: expected_source_node
      input_port: data
      output_port: <port>
```

### 10c. Compare and report

- Row counts should match.
- Key column distinct counts should match.
- Numeric averages should be within tolerance.
- For row-level diff, use `EXCEPT`/`MINUS` both ways.

### 10d. Clean up after validation

Once the pipeline is confirmed correct, remove the validation source and comparison nodes.

---

## Step 11: Macro & Apps Handling

### Standard macro (`.yxmc`)
Two options, in order of preference:
1. **Inline** the macro nodes into the parent pipeline (rename to avoid collisions).
2. **Extract** to a single `python` operator that takes the macro inputs as `inputs[...]` and returns `result`. Note this in a `markdown` node so it can be reused.

### Batch macro
Each batch becomes a loop inside a `python` operator:
```python
ctrl = inputs["control"][0].collect()
out = []
for row in ctrl:
    df = inputs["data"][0].where(F.col("group") == row.group)
    out.append(df.withColumn("group", F.lit(row.group)))
result = out[0]
for d in out[1:]:
    result = result.unionByName(d)
```

### Iterative macro
**MANUAL** — VDP has no fixed-point loop. Convert to a Lakeflow **Job** with a `For Each` task that re-runs the pipeline until a stop condition.

### Analytic App
**MANUAL** — there is no VDP-native form. Options:
- Lakeflow Job with parameters (replaces interface tools).
- Databricks App on top of the materialized table (replaces report tools).

Always emit a `markdown` node that lists the manual steps and the macro path it came from.

---

## Step 12: Predictive / Spatial / Reporting / Interface — Default Policy

| Category | Default | Mark as |
|---|---|---|
| Predictive (classical ML) | Emit `python` with PySpark MLlib equivalent + MLflow `log_model` | **REVIEW** |
| Predictive (text/sentiment/classification) | Emit `ai_function` first; fall back to `python` only if labels are dynamic | **READY** |
| Time Series | Emit `python` with Prophet/statsmodels + MLflow | **REVIEW** |
| Spatial | Emit `python` with Sedona; if Sedona not enabled, mark **MANUAL** | **REVIEW / MANUAL** |
| Reporting (Render/Email/Chart/Map) | Out of scope — point user at Lakeview / Job email | **MANUAL** |
| Interface (Text Box, Drop Down, etc.) | Out of scope — point user at Job parameters / App | **MANUAL** |

For every **REVIEW** / **MANUAL** node, emit an adjacent `markdown` describing what was skipped and how the user should complete it.

---

## Step 13: Layout Conventions

- **Horizontal flow** (left → right): sources at x=0, transforms at x=260, 520, etc.
- **260px horizontal spacing** between operator tiers.
- **145px vertical spacing** between parallel branches.
- **Never overlap operators** — check positions before placing.
- Add a top-level `markdown` node documenting the pipeline purpose, source workflow filename, and any **MANUAL** items.
- For parameterized pipelines, add a `markdown` explaining the `env_config` parameters.

---

## Step 14: Known Limitations & Workarounds

| Issue | Symptom | Workaround |
|---|---|---|
| Workspace `file:` paths | `FAILED_READ_FILE` with `@` in path | Use UC Volume paths instead |
| Config stripping | Python/SQL configs reset to `{}` or `expressions: []` | Never change template type; recreate operator instead |
| `TRY_CAST` in `transform` | Edits silently revert to `CAST` | Use a `filter` upstream, or use `sql` instead |
| SQL view names with spaces | `TABLE_OR_VIEW_NOT_FOUND` | Rename operators to simple snake_case names |
| `.yxdb` files | Alteryx proprietary binary | Ask user to export to CSV/Parquet first |
| Complex Python code | Code field gets stripped | Keep code simple; split logic across multiple `python` operators |
| Iterative macros | No native loop | Lakeflow Job `For Each` task |
| Reporting/Render tools | No equivalent | Output Delta + Lakeview (AI/BI) dashboard |
| Interface tools | No form UI | Job parameters or Databricks App |
| Spatial without Sedona | `ST_*` not found | Enable Sedona on the cluster, or mark MANUAL |
| Excel `.xlsb` / `.xlsm` macros | Macros not executed | Pandas reads cell values only — VBA macros must be ported manually |
| Iterative joins on huge tables | OOM | Use broadcast hint in SQL: `/*+ BROADCAST(small) */` |
| Designer Filter is graphical, not free-form SQL | Cannot enter `REGEXP_LIKE`, `BETWEEN`, multi-AND-OR mixes directly | Fall back to `sql` operator |
| Designer Join has no cross-join | Append Fields cannot map to `join` | Use a `sql` operator with `CROSS JOIN` |
| Aggregate has no first/last/collect_list | Alteryx Summarize → Concat doesn't fit | Use `sql` with `concat_ws(',', collect_list(col))` |
| Combine requires matching schemas | Heterogeneous Alteryx Unions fail | Pre-align schemas with two `transform` operators before `combine` |
| YAML docstring colon in `description.text` breaks the cell | Downstream cells fail with `'<this>.<port>' data is missing or not created before use` because Designer never registers the broken cell in the dataflow graph | **Always quote** any free-text YAML scalar that may contain `:`, `#`, `{`, `}`, `[`, `]`, `,`, or leading/trailing whitespace. Concretely: emit `text: "Per-category metrics: avg / median / sum / count."` (double-quoted) rather than `text: Per-category metrics: avg / median / sum / count.` |

---

## Conversion Checklist

- [ ] Alteryx `.yxmd` / `.yxmc` analyzed — every `<Node>` and `<Connection>` mapped
- [ ] Each tool converted to a VDP operator OR explicitly flagged **MANUAL/REVIEW**
- [ ] All input file formats handled per Step 3 (and `.yxdb` flagged for export)
- [ ] Source files placed in UC Volume (not Workspace `file:` paths)
- [ ] All column names are Delta-compatible (no spaces, no special chars)
- [ ] Type casts handle dirty data (filter or `TRY_CAST`)
- [ ] SQL operators reference simple display names (no spaces)
- [ ] Deduplication uses `ROW_NUMBER()` pattern
- [ ] Joins preserve L / J / R branches required downstream
- [ ] Macros — standard inlined or extracted; iterative flagged for Job
- [ ] Predictive / spatial / time-series — Python operator emitted, MLflow noted, marked **REVIEW**
- [ ] Reporting / interface — flagged **MANUAL** with Lakeview / Job / App pointer
- [ ] Output operator configured with `catalog.schema.table_name`
- [ ] Optional non-Delta sinks added downstream of the Delta output (Step 9b)
- [ ] Output node previews with no errors
- [ ] Data validation performed (if expected output provided) and validation nodes removed afterwards
- [ ] Pipeline tested end-to-end
- [ ] Layout is clean and readable (horizontal flow, no overlaps)
- [ ] Top-level `markdown` documents pipeline purpose and any MANUAL items
