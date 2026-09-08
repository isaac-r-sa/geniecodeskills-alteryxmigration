---
name: alteryx-to-vdp
description: Convert Alteryx Designer workflows (.yxmd / .yxmc XML files) into Databricks Lakeflow Designer Visual Data Prep pipelines. Maps the full Alteryx tool palette (In/Out, Preparation, Join, Parse, Transform, Data Investigation, Predictive, Time Series, Spatial, Reporting, Documentation, Developer, Interface, Macros) to the actual VDP operators (Source, Output, AI Function, Aggregate, Combine, Enter Data, Filter, Join, Limit, Pivot, Prepare, Sort, SQL, Transform, Unique, Visualization, Python, Note, Group), handles all common input/output file formats (CSV, TSV, Excel, JSON, XML, Parquet, Avro, ORC, Delta, SAS, SPSS, R, geospatial, PDF, .yxdb), and always materializes output to a Unity Catalog Delta table. Validates against expected output when provided.
---

# Skill: Convert Alteryx Workflow (.yxmd / .yxmc) to Lakeflow Designer Visual Data Prep

## Objective

When a user provides an Alteryx workflow file (`.yxmd` or `.yxmc`), convert it into a fully functional Lakeflow Designer (Visual Data Prep) pipeline. The pipeline must:
1. Reproduce the exact logic of the Alteryx workflow.
2. Always materialize the final output to a Unity Catalog Delta table.
3. Validate results against expected output (see Step 10). If the user has not provided an expected output file, explicitly ask for one before finalizing the pipeline. Validation is required and must not be skipped.
4. Cover the full Alteryx tool palette and all common file formats — flagging any tool/format that has no automatic VDP equivalent so the user can address it manually.

---

## CRITICAL RULE: Operator Selection Priority

> ### MANDATORY PRE-CHECK — runs BEFORE every operator decision
> Before writing any `sql` or `python` operator, answer ALL of these:
> 1. Can a **Transform** express this? (CASE WHEN, CAST, COALESCE, TRIM, UPPER, REGEXP_EXTRACT, SPLIT + ELEMENT_AT, DATEDIFF, arithmetic, literals) → **USE TRANSFORM. STOP.**
> 2. Can a **Filter** express this? (boolean row condition) → **USE FILTER. STOP.**
> 3. Can an **Aggregate** express this? (GROUP BY + SUM/AVG/COUNT/MIN/MAX/MEDIAN/STDDEV/PERCENTILE) → **USE AGGREGATE. STOP.**
> 4. Can a **Join** express this? (equi-join on key columns) → **USE JOIN. STOP.**
> 5. Can a **Sort**, **Limit**, **Pivot**, **Combine**, or **Unique** express this? → **USE THE VISUAL OPERATOR. STOP.**
> 6. Can a **Prepare** action express this? (trim, cast, text_case, fill_null, replace_value, regex_replace, extract, parse_date, formula) → **USE PREPARE. STOP.**
> 7. Can an **Enter Data** express this? (small inline/lookup table) → **USE ENTER_DATA. STOP.**
>
> Only if ALL seven answers are NO may you proceed to `sql` or `python`.
> If you write `sql` or `python` without answering all seven, the operator choice is wrong.

**Always prefer visual/deterministic operators over custom code or AI.** For every Alteryx tool being converted, follow this strict priority order. MORE NODES is ALWAYS preferred over fewer consolidated nodes. Each logical step = its own operator.

### Priority 1: Visual Operators (ALWAYS try first)

| Operator | Use For |
|----------|---------|
| **Transform** | Column derivations, CASE WHEN, CAST, COALESCE, TRIM, UPPER, SOUNDEX, REGEXP_EXTRACT, SPLIT + ELEMENT_AT, DATEDIFF, literal values, `*` passthrough |
| **Filter** | Row filtering with boolean conditions |
| **Aggregate** | GROUP BY with SUM, AVG, COUNT, MIN, MAX, MEDIAN, STDDEV, VARIANCE, PERCENTILE |
| **Join** | Combining tables on key columns |
| **Sort** | ORDER BY |
| **Limit** | TOP N rows |
| **Pivot/Unpivot** | Reshape wide↔tall |
| **Combine** | UNION, INTERSECT, EXCEPT |
| **Unique** | Deduplicate rows (full-row or by column subset, with optional sort to control which row is kept) |
| **Prepare** | Ordered action list: formula, cast, replace_value, fill_null, text_case, trim, regex_replace, extract, parse_date |
| **Enter Data** | Inline lookup/constant tables (markdown-style table input) — replaces `spark.createDataFrame` for small static data |
| **Visualization** | Inline charts (bar, line, scatter, pie, histogram, box, heatmap, etc.) — replaces MANUAL→Lakeview for exploratory charts |

### Priority 2: AI Functions (ONLY when ALL 3 conditions are met)

1. Output is **creative/generative text** (summaries, profiles, semantic classifications of free-text)
2. Input table has **low cardinality** (thousands of rows, NOT millions)
3. There is **no deterministic equivalent** (no CASE WHEN, lookup table, or regex can do it)

✅ **Good AI use cases:** customer profile generation, free-text sentiment, text summarization, semantic classification
❌ **Bad AI use cases (use Transform instead):** product name standardization (finite mappings), region→coord lookup, rule-based segment assignment, data type conversion

#### AI Function Decision Flowchart:
1. Is the mapping finite and known? → **Transform CASE WHEN** or **Join to lookup table**
2. Does it need to be deterministic/reproducible? → **Transform** or **SQL**
3. Is the table millions of rows? → **NOT AI** (cost/latency explosion)
4. Is it genuinely creative/semantic with no deterministic equivalent? → **AI Function** ✅

### Priority 3: SQL (ONLY after the mandatory pre-check passes, and only for these specific patterns)

- **Window functions**: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, SUM/AVG/COUNT OVER(...)
- **COUNT(DISTINCT col)** — Aggregate operator doesn't support it
- **STDDEV, VARIANCE, PERCENTILE_APPROX** in aggregation context with COUNT DISTINCT in same query
- **CTEs** — ONLY when required for SEQUENCE/EXPLODE or self-referencing subqueries
- **SEQUENCE + EXPLODE** (calendar/date generation)
- **Subqueries** (SELECT FROM (SELECT ...)) for inline DISTINCT before window
- **Explode-to-rows tokenization** (for example, `EXPLODE(SPLIT(col, ','))`)

**Do NOT use SQL for:**
- Fixed-column string splitting — use **Transform** with `SPLIT` + `ELEMENT_AT`
- Finite mappings / small Find Replace rules — use **Transform** CASE WHEN
- Inline constant rows or tiny lookup tables — use `python` `spark.createDataFrame(...)` when you truly need rows, or **Transform** CASE WHEN when you only need deterministic mappings

### Priority 4: Python (ABSOLUTE LAST RESORT — only for)

- **File I/O**: CSV/Parquet writes to Volumes (4 lines max)
- **ML model training/scoring**: sklearn, pyspark.ml, statsmodels
- **External libraries** with no SQL/visual equivalent (e.g., ARIMA, Prophet)

#### Python must NEVER contain:
- `F.withColumn("col", F.soundex(...))` → use **Transform**: `SOUNDEX(col) AS alias`
- `F.withColumn("col", F.regexp_extract(...))` → use **Transform**: `REGEXP_EXTRACT(col, pattern, group) AS alias`
- `F.withColumn("col", F.when(...).otherwise(...))` → use **Transform**: `CASE WHEN ... END AS alias`
- `df.groupBy(...).agg(F.sum(), F.avg(), ...)` → use **Aggregate** operator
- `F.datediff(...)`, `F.current_date()` → use **Transform**: `DATEDIFF(...)`, `CURRENT_DATE()`
- `F.lit(value)` → use **Transform**: `5.0 AS col_name`
- `F.col("x").cast("double")` → use **Transform**: `CAST(x AS DOUBLE)`

---

### One Logical Step = One Operator Rule

Each distinct transformation purpose gets its own operator node. NEVER consolidate multiple unrelated steps into one SQL or Python operator.

**Default approach (ALWAYS use this):**

| Step | Operator | Purpose |
|------|----------|---------|
| 1 | **Transform** | Derive new columns (SOUNDEX, REGEXP_EXTRACT, CASE WHEN) |
| 2 | **SQL** | Window function A (e.g., DENSE_RANK for group assignment) |
| 3 | **SQL** (separate) | Window function B that depends on step 2's output (e.g., LAG partitioned by group_id) |
| 4 | **Transform** | Simple derivation on window results (e.g., DATEDIFF on LAG output) |

**❌ DO NOT consolidate into CTEs by default:**
```sql
-- WRONG: Merging unrelated purposes into one SQL
WITH step1 AS (SELECT *, DENSE_RANK() ... FROM upstream),
     step2 AS (SELECT *, LAG() ... FROM step1)
SELECT *, DATEDIFF(...) FROM step2
```

**✅ CORRECT: Separate operators for separate purposes:**
```
Transform (derivations) → SQL (DENSE_RANK only) → SQL (LAG + NTILE) → Transform (DATEDIFF)
```

#### When to ASK the user about consolidation:

If two adjacent SQL nodes both contain window functions, ASK the user:
> "These window functions could be combined into one SQL node for compactness, or kept as separate nodes for clarity. Which do you prefer?"

**Only consolidate if the user explicitly requests it.** Default is always MORE operators.

---

### Decomposition Rule

When an Alteryx tool's logic contains BOTH simple expressions AND complex operations (window functions, dedup), **always split into multiple operators**:
- **Transform** for: CASE WHEN, COALESCE, constants, regex, type casts, string functions, date functions, arithmetic, SOUNDEX
- **SQL** for: window functions (SUM OVER, ROW_NUMBER, NTILE, LAG/LEAD), CTEs, subqueries
- **AI Function** for: creative/generative text on low-cardinality results (profiles, summaries)

---

### What Transform CAN handle (do NOT use SQL or Python for these)

| Category | Functions/Patterns | Example |
|----------|-------------------|---------|
| **Arithmetic** | +, -, *, /, ROUND, ABS, FLOOR, CEIL | `quantity * unit_price * (1 - discount_pct) AS net_amount` |
| **Conditional** | CASE WHEN (up to ~10+ branches) | `CASE WHEN region = 'X' THEN val ... END AS col` |
| **Null handling** | COALESCE, NVL, IFNULL | `COALESCE(unit_price, list_price) AS price_final` |
| **String** | TRIM, UPPER, LOWER, INITCAP, CONCAT, SPLIT, ELEMENT_AT, SUBSTRING, LENGTH, REPLACE | `TRIM(INITCAP(name)) AS name`; `ELEMENT_AT(SPLIT(col, ','), 1) AS part1` |
| **Regex** | REGEXP_EXTRACT, REGEXP_REPLACE | `REGEXP_EXTRACT(email, '@(.+)$', 1) AS domain` |
| **Phonetic** | SOUNDEX | `SOUNDEX(customer_name) AS name_soundex` |
| **Date/Time** | TO_TIMESTAMP, TO_DATE, DATEDIFF, DATE_ADD, MONTHS_BETWEEN, YEAR, MONTH, DAYOFWEEK | `DATEDIFF(current_date(), last_date) AS days_ago` |
| **Type casting** | CAST | `CAST(quantity AS DOUBLE)` |
| **Literals/Constants** | Any fixed value | `5.0 AS trade_area_radius_km`, `'AUD' AS currency` |
| **Passthrough** | `*` to keep all existing columns | First expr `"*"`, then add computed cols |
| **Column selection** | List specific columns | `col1`, `col2`, `col3 AS renamed` |
| **Find/Replace (small)** | CASE WHEN for ≤10 mappings | `CASE WHEN col = 'old' THEN 'new' ... END AS col` |

### What REQUIRES SQL (cannot use Transform)

| Pattern | Why |
|---|---|
| Window functions | `SUM() OVER (PARTITION BY ... ORDER BY ...)` |
| Running totals | `SUM(col) OVER (... ROWS UNBOUNDED PRECEDING)` |
| Deduplication | `ROW_NUMBER() OVER (PARTITION BY key ...) WHERE rn = 1` |
| NTILE / ranking | `NTILE(10) OVER (ORDER BY ...)` |
| ~~COUNT DISTINCT~~ | Now supported by visual **Aggregate** operator as `COUNT_DISTINCT` — no SQL needed |
| Subqueries / CTEs | Multi-step logic referencing intermediate results |
| QUALIFY | Row-level filter on window results |
| LAG / LEAD | `LAG(col) OVER (PARTITION BY ... ORDER BY ...)` |
| SEQUENCE + EXPLODE | Calendar/date spine generation |

### Aggregate Operator — Capabilities & Limitations

**✅ Supported:** SUM, AVG, COUNT, COUNT_DISTINCT, MIN, MAX, MEDIAN, STDDEV, VARIANCE, PERCENTILE, FIRST, LAST, CONCAT

**✅ Workarounds:**
| Need | Workaround |
|------|-----------|
| FIRST(col) | Use Aggregate FIRST — now natively supported (non-deterministic without upstream Sort) |
| LAST(col) | Use Aggregate LAST — now natively supported (non-deterministic without upstream Sort) |
| CONCAT(col) | Use Aggregate CONCAT — concatenates values with configurable separator (default ", ") |

**❌ Must use SQL:** COLLECT_LIST/SET (as arrays), FIRST_VALUE/LAST_VALUE with window frame, any window function

### When to use Aggregate vs SQL for GROUP BY

| Pattern | Use |
|---|---|
| GROUP BY + SUM/AVG/COUNT/MIN/MAX/MEDIAN/STDDEV | **Aggregate operator** — always preferred |
| GROUP BY + COUNT DISTINCT | **Aggregate operator** — use `COUNT_DISTINCT` fn (now natively supported) |
| GROUP BY + FIRST/LAST | **Aggregate operator** — use `FIRST` / `LAST` fn (now natively supported; non-deterministic without upstream Sort) |
| GROUP BY + CONCAT (string agg) | **Aggregate operator** — use `CONCAT` fn with optional separator |
| GROUP BY + COLLECT_LIST/SET (array) | Must use **SQL** — returns arrays, not supported by Aggregate |

---

### Decomposition Patterns — Common Alteryx Tools

#### Fuzzy Match + Make Group
| Step | Operator | Expression |
|------|----------|-----------|
| 1 | **Transform** | `SOUNDEX(name) AS soundex`, `REGEXP_EXTRACT(email, ...) AS domain` |
| 2 | **SQL** | `DENSE_RANK() OVER (ORDER BY soundex, domain) AS group_id` |
| 3 | **SQL** (separate — depends on group_id) | `LAG(txn_dt) OVER (PARTITION BY group_id ...) AS prev_dt` |
| 4 | **Transform** | `DATEDIFF(txn_dt, prev_dt) AS days_since_prior` |

#### Customer Segmentation Macro (RFM)
| Step | Operator | Logic |
|------|----------|-------|
| 1 | **Aggregate** | GROUP BY customer with MAX, COUNT, SUM, AVG, MIN |
| 2 | **Transform** | `DATEDIFF(current_date(), last_txn_date) AS recency_days` |
| 3 | **SQL** | `NTILE(5) OVER (ORDER BY ...) AS r_score, f_score, m_score` |
| 4 | **Transform** | `CASE WHEN r_score >= 4 AND f_score >= 4 THEN 'Champions' ...` |
| 5 | **AI Function** (optional, low-cardinality) | `ai_gen(CONCAT('Profile: ', ...)) AS customer_profile` |

#### Geospatial (Coordinate Assignment + Ranking)
| Step | Operator | Logic |
|------|----------|-------|
| 1 | **Transform** | CASE WHEN region → lat/lon (≤10 branches) |
| 2 | **SQL** | DISTINCT + `ROW_NUMBER() OVER (PARTITION BY region ORDER BY id)` |

#### Summarize / GroupBy
| Scenario | Operator |
|----------|----------|
| Standard aggs (SUM/AVG/COUNT/MIN/MAX) | **Aggregate** |
| COUNT(DISTINCT col) | **SQL** |
| STDDEV / PERCENTILE alone | **Aggregate** (supported) |
| F.first() needed | **Aggregate** with MIN substitute |

#### Find Replace / Standardize
| Scenario | Operator |
|----------|----------|
| ≤10 known mappings | **Transform** CASE WHEN |
| 10-100 mappings | **Join** to lookup/reference table |
| Unknown variations (low cardinality, creative) | **AI Function** |
| Unknown variations (high cardinality) | Run AI on DISTINCT values once → save to table → **Join** |

---

### Anti-Patterns — What NOT to Do

| ❌ Anti-Pattern | ✅ Correct Approach |
|----------------|-------------------|
| Monolithic Python with groupBy + withColumn + CASE WHEN + CSV write | Decompose: Aggregate → Transform → SQL (windows) → Transform → Python (CSV only) |
| ai_gen() on millions of rows for standardization | Transform CASE WHEN or Join to lookup table |
| SQL for simple COALESCE/CAST/TRIM/SOUNDEX/DATEDIFF | Transform operator |
| Python F.lit(5.0) for constants | Transform: `5.0 AS col_name` |
| Python F.soundex() or F.regexp_extract() | Transform: SOUNDEX(), REGEXP_EXTRACT() |
| Merging multiple unrelated windows into one SQL via CTEs without asking user | Separate SQL nodes: one per logical step; ASK before consolidating |
| AI function when output must be deterministic | Transform CASE WHEN |
| AI function on high-cardinality table (millions of rows) | Run on DISTINCT values once → save → Join |
| Fewer nodes via consolidation without asking user | Always default to MORE operators; ask before merging |
| `sql` for Text To Columns into fixed N columns | `transform`: `ELEMENT_AT(SPLIT(col, ','), 1) AS part1` — use `SPLIT` + `ELEMENT_AT` in Transform |
| `sql` for inline constant rows or lookup tables with ≤10 rows | Use `python` `spark.createDataFrame(...)` for real inline row sources, or `transform` CASE WHEN for deterministic mappings |
| `sql` JOIN to a ≤10-row lookup table for Find Replace | `transform` CASE WHEN — small finite mappings should stay visual |
| Skipping the mandatory pre-check and jumping straight to `sql` | Answer all 5 visual-operator questions first; only then use `sql` |
| `sql` ROW_NUMBER for simple deduplication | Use visual `unique` operator — `unique_by_all_columns: false` + `columns` + optional `sort_expressions` |
| `python` `spark.createDataFrame(...)` for small inline/lookup tables | Use visual `enter_data` operator with markdown-style table syntax |
| `sql` COUNT(DISTINCT col) in GROUP BY | Use visual `aggregate` with `fn: COUNT_DISTINCT` — now natively supported |
| `sql` FIRST_VALUE / LAST_VALUE in GROUP BY | Use visual `aggregate` with `fn: FIRST` or `fn: LAST` — now natively supported |
| Two `filter` operators with inverse conditions for Alteryx T/F split | Use ONE `filter` with two output ports: `filtered_data` (T) and `excluded_data` (F) |
| `sql` LEFT ANTI / RIGHT ANTI for Alteryx Join L/R unmatched | Use `join` with `join_type: split_join` — produces `joined_data`, `left_unmatched`, `right_unmatched` |
| `python` for file output to Volume | Use `output` with `output_type: file` + `volume` + `file_name` + `file_type` (csv/json/excel) |


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
| `prepare` | Prepare | `data` | `prepared_data` | `actions: [{type, column, ...}, ...]` — ordered list of typed actions: `formula` (arbitrary SQL expr), `cast` (type change), `replace_value` (value substitution), `fill_null`, `text_case` (lower/upper/title), `trim`, `regex_replace`, `extract` (regex capture), `parse_date`. Each action mutates in sequence. |
| `unique` | Unique | `data` | `unique_data` | `unique_by_all_columns: true` (full-row dedup) or `false` + `columns: [key_cols]` (subset dedup). Optional `sort_expressions` to control which row survives. |
| `enter_data` | Enter Data | (none) | `data` | `data: "| col1 | col2 |\n| --- | --- |\n| val1 | val2 |"` — markdown-style inline table. Use for small lookup/constant tables. |
| `visualization` | Visualization | `data` | `data` | `editorSpec: {type, xAxis, yAxis, ...}` — inline chart. Types: bar, line, area, scatter, pie, table, histogram, box, heatmap, combo, counter, funnel, etc. Aggregates internally — do NOT add a separate aggregate upstream. |
| *(UDO name)* | User-Defined Operator | `data` | `result` | Registered via `.user_defined_operators.yaml`; the YAML `template` field matches the UDO's registered identifier (not a fixed string). Three subtypes: **`uc-udf`** — UC UDF, row-level column transform; **`uc-udtf`** — UC UDTF, multi-row/stateful (ML scoring, clustering); **`python-run-function`** — standalone Python callable, no UC dependency. Config varies by subtype — see [UDO reference](https://learn.microsoft.com/en-us/azure/databricks/designer/user-operators/). |

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

**IMPORTANT: Always check Priority 1 (visual operators) and Priority 2 conditions first.**
AI functions are appropriate ONLY when:
1. The output is **creative/generative** (summaries, profiles, semantic classifications)
2. The table has **low cardinality** (thousands of rows, NOT millions)
3. There is **no deterministic equivalent** (CASE WHEN, lookup table, regex cannot do it)

❌ **NEVER use AI for:** finite known mappings (product name standardization → CASE WHEN), coordinate lookups (→ CASE WHEN), rule-based assignments (RFM segments → CASE WHEN), any table with millions of rows.

✅ **Good AI use:** generating natural-language customer profiles from RFM scores, sentiment analysis on free-text reviews, semantic similarity for fuzzy matching when SOUNDEX is insufficient.

Several Alteryx tools have no traditional SQL equivalent but map naturally to a Databricks AI function. Prefer `ai_function` over a hand-rolled `python` + LLM call.

**TVF mode (table-valued functions):** The `ai_function` operator also supports `tvf_sql` for table-valued AI functions like `ai_forecast`. Use `tvf_sql` instead of `expressions` — they are mutually exclusive. Example: `tvf_sql: "SELECT * FROM ai_forecast(observed => TABLE(__lakebuilder_ai_function_input__), horizon => '2099-12-31', time_col => 'date', value_col => 'sales')"`. This replaces `python` Prophet/statsmodels for simple forecasting.

The full set of scalar functions exposed in the operator dropdown:

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

### When to reach for a User-Defined Operator (UDO)

UDOs are reusable visual operators backed by Unity Catalog functions or standalone Python callables. They appear in the operator palette after being registered via `.user_defined_operators.yaml`. Prefer a UDO over a raw `python` node when:

| Condition | Recommended UDO subtype |
|---|---|
| Row-level custom transform already exists (or should exist) as a UC UDF | `uc-udf` |
| Multi-row / stateful operation: ML scoring, clustering, UDTF-style aggregation | `uc-udtf` |
| Reusable Python callable with no UC dependency (e.g. external API call, email notification) | `python-run-function` |
| Same logic appears in multiple pipelines and should be maintained in one place | Any subtype |

**Do NOT use a UDO when:**
- The operation is expressible with a built-in operator (Transform, SQL, Aggregate, etc.) — built-ins are always preferred.
- The custom logic is one-off and pipeline-specific — use `python` instead.

**YAML note:** Unlike built-in operators, the `template` field for a UDO is the UDO's own registered identifier string (not a fixed value like `transform` or `sql`). The exact YAML schema depends on the UDO subtype — consult the [UDO YAML reference](https://learn.microsoft.com/en-us/azure/databricks/designer/user-operators/) before emitting a UDO cell.

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
| Output Data (file) | `output` (file mode) | `output_type: file` with `volume`, `file_name`, `file_type` (csv, json, excel). No Python needed. Fall back to `python` only for formats not supported by the output operator. |
| Browse | *omit* | Browse is just a preview tile — no VDP analog needed |
| Text Input | `enter_data` | Inline table with markdown-style syntax (header row + separator + data rows). Use `enter_data` for small static lookup/constant tables. Fall back to `python` `spark.createDataFrame` only for programmatic row generation. |
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
| Filter | `filter` | `config.condition` is a free-form SQL boolean string. Designer's Filter now has **two output ports**: `filtered_data` (rows where condition is true) and `excluded_data` (complement). This maps directly to Alteryx's T/F outputs — use ONE filter operator and wire T→`filtered_data`, F→`excluded_data`. No need for two filters with inverse conditions. |
| Formula | `transform` | One row per output column with a SQL expression |
| Imputation (simple null fill with constant/other col) | `transform` | `COALESCE(col, fallback_col) AS col` — use Transform when filling from another column or a constant. **Prefer Transform over SQL.** |
| Imputation (fill with aggregate like median/mean) | `sql` | `COALESCE(col, AVG(col) OVER ())` — use SQL only when the fill value requires a window/aggregate calculation |
| Multi-Field Formula | `transform` | Apply same expression to a list of columns |
| Multi-Row Formula | `sql` | `LAG`/`LEAD` window functions |
| Random % Sample | `sql` | `WHERE rand() < 0.1` (with seed if reproducibility needed) |
| Record ID | `sql` | `ROW_NUMBER() OVER (ORDER BY ...)` |
| Sample / First N / Last N / Skip 1st N | `limit` or `sql` | First N → `limit`; Last N → `ROW_NUMBER` desc + filter |
| Select | `transform` | Reorder, rename, drop, retype |
| Select Records | `sql` | Range-based: `WHERE rn BETWEEN a AND b` |
| Sort | `sort` | One or more `column ASC|DESC` |
| Tile | `sql` | `NTILE(n) OVER (...)` |
| Unique | `unique` | Visual **Unique** operator: `unique_by_all_columns: true` for full-row dedup, or set `false` with `columns: [key_cols]` for subset dedup. Add `sort_expressions` to control which row is kept. Output port: `unique_data`. Only use SQL ROW_NUMBER for complex dedup with multiple window partitions. |

### 2.3 Join

| Alteryx Tool | VDP Operator | Notes |
|---|---|---|
| Join | `join` | Designer Join now supports `split_join` mode with **three output ports**: `joined_data` (matched), `left_unmatched`, `right_unmatched` — this maps **directly** to Alteryx's L/J/R outputs. Also supports Inner / Left / Right / Full. Use `split_join` as default for Alteryx Join conversions. No need for LEFT ANTI/RIGHT ANTI SQL workarounds. |
| Join Multiple | chain of `join` | Or one `sql` with multi-table FROM |
| Append Fields (cross join) | `sql` | Designer Join has no cross-join — emit `SELECT * FROM left CROSS JOIN right`. |
| Union | `combine` | `operator: UNION`, `quantifier: ALL` (= UNION ALL) or `DISTINCT` (= UNION DISTINCT) |
| Set difference (Alteryx Join L-only output, in isolation) | `combine` | `operator: EXCEPT` (or `MINUS`). Designer's Combine also exposes `INTERSECT` — Alteryx has no native equivalent for either. |
| Find Replace (small static mapping, 10 or fewer values) | `transform` | Use CASE WHEN: `CASE WHEN col = 'old1' THEN 'new1' ... ELSE col END AS col`. **Prefer Transform for small lookups.** |
| Find Replace (large lookup table) | `join` + `transform` | Left join to lookup table, COALESCE replacement, drop lookup cols |
| Make Group | `sql` | Connected-components — emit a stub + flag **REVIEW** (rare; ask user) |
| Fuzzy Match (string-distance, SOUNDEX grouping) | `transform` + `sql` | **Transform**: `SOUNDEX(name) AS soundex`, `REGEXP_EXTRACT(email, ...) AS domain`. **SQL**: `DENSE_RANK() OVER (ORDER BY soundex, domain) AS group_id`. See Decomposition Patterns above. |
| Fuzzy Match (Levenshtein/Jaro-Winkler with tuning) | `python` | `levenshtein`/`jaro_winkler` with custom thresholds; flag **REVIEW** |
| Fuzzy Match (semantic similarity) | `ai_function` | `ai_similarity(left_text, right_text)` then threshold — only for truly semantic matching on low-cardinality data |

### 2.4 Parse

| Alteryx Tool | VDP Operator | Notes |
|---|---|---|
| DateTime | `transform` | `to_date`, `to_timestamp`, `date_format`, `unix_timestamp` |
| RegEx (Parse) | `transform` | `regexp_extract(col, pattern, n)` per capture group |
| RegEx (Replace) | `transform` | `regexp_replace(col, pattern, repl)` |
| RegEx (Tokenize) | `sql` | `explode(split(regexp_extract_all(...)))` |
| Text To Columns (fixed N columns) | `transform` | `ELEMENT_AT(SPLIT(col, ','), 1) AS part1`, `ELEMENT_AT(SPLIT(col, ','), 2) AS part2` — use Transform when the output is a fixed set of columns |
| Text To Columns (explode into rows) | `sql` | `EXPLODE(SPLIT(col, ',')) AS part` — SQL required only for row expansion |
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
| Summarize (Sum/Avg/Count/CountDistinct/Min/Max/Median/Stddev/Variance/Percentile/First/Last/Concat) | `aggregate` | **ALWAYS use Aggregate** — all these functions are natively supported. Only fall back to `sql` when: (a) aggregation involves UNION ALL across multiple granularities, (b) uses COLLECT_LIST/SET (array output), or (c) needs window functions. |
| Summarize with CountDistinct | `aggregate` | Use `COUNT_DISTINCT` fn — now natively supported by the visual Aggregate operator. No SQL needed. |
| Summarize (First / Last) | `aggregate` | Use `FIRST` / `LAST` fn — now natively supported. Non-deterministic without upstream Sort. |
| Summarize (Concat / string agg) | `aggregate` | Use `CONCAT` fn with optional separator — now natively supported. |
| Summarize (Collect List/Set as array) | `sql` | COLLECT_LIST / COLLECT_SET returns arrays — still requires SQL. |
| Transpose | `pivot` | **Columns → Rows** mode |
| Weighted Average | `sql` | `SUM(value*weight) / SUM(weight)` per group |

### 2.6 Data Investigation

These are exploratory; in VDP they're typically intermediate `aggregate`/`sql` nodes, not pipeline outputs.

| Alteryx Tool | VDP Operator | Notes |
|---|---|---|
| Field Summary | `sql` | `describe`-style query: count/mean/stddev/min/max per column |
| Frequency Table | `aggregate` | GROUP BY col, COUNT(*) |
| Pearson / Spearman Correlation | `python` | `df.stat.corr(...)` per pair, or `Correlation.corr` (MLlib) |
| Histogram | `visualization` | `type: histogram` — visual Visualization operator handles binning internally. Fall back to `sql` `WIDTH_BUCKET` only for custom bin boundaries. |
| Scatterplot / Distribution / Association | `visualization` | Use the visual **Visualization** operator for inline charts (bar, line, scatter, pie, histogram, box, heatmap, etc.). The operator aggregates internally — no separate aggregate needed. For production dashboards, also consider Lakeview (AI/BI). |
| Histogram | `visualization` | `type: histogram` with `xAxis` (numeric column) + `yAxis` (`COUNT(*)`) — replaces SQL WIDTH_BUCKET binning |

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
| ARIMA / ETS / TS Forecast | `ai_function` (TVF) or `python` | **Preferred**: `ai_function` with `tvf_sql` calling `ai_forecast(...)` for simple forecasting. Fall back to `python` (`statsmodels` / `prophet`) for custom ARIMA/ETS models; log to MLflow |
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
| Test | `sql` | `SELECT CASE WHEN <invariant> THEN 'PASS' ELSE 'FAIL'`; or pipeline expectations |
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

### 2.15 User-Defined Operators (UDO)

UDOs have no direct Alteryx equivalent — they are a VDP-native feature for packaging reusable logic as a first-class visual operator. Consider suggesting a UDO **only when** converting Alteryx tools that contain custom Python logic the user is likely to reuse across multiple pipelines.

| Alteryx Pattern | UDO Subtype | Notes |
|---|---|---|
| Python Tool with a row-level formula already registered (or planned) as a UC UDF | `uc-udf` | Maps each input row through the UDF; output is a new column. Prefer over `python` when the function is already in UC. |
| Python Tool performing ML scoring via an MLflow model | `uc-udtf` | UDTF accepts the full table, applies the model, returns scored rows. Cleaner and more reusable than `python` with `mlflow.pyfunc.load_model`. |
| Python Tool calling an external API (e.g. Slack, email, enrichment service) | `python-run-function` | Standalone Python callable; no UC required. Keeps pipeline logic consistent without embedding credentials in a raw `python` cell. |
| Custom Tool (`.yxi`) with a known Python implementation | `uc-udtf` or `python-run-function` | Assess whether the logic generalizes; if yes, register as UDO. If one-off, use a `python` node instead. |
| R Tool | `python` (flag **REVIEW**) | R has no UDO path; re-implement in PySpark or Python first. |

**Emit a `markdown` note** adjacent to any UDO cell that explains: the UDO name, the UC catalog path (for `uc-udf` / `uc-udtf`), and any one-time setup the user must perform before running the pipeline.

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
| **Alteryx native** | `.yxdb` | **MANUAL** | Proprietary binary. The Python `yxdb` library fails on newer "e2 Database" format files. Workarounds: (a) ask user to export from Alteryx to CSV/Parquet, (b) if an expected output file exists, extract historical `.yxdb` data from it via column alignment, (c) use the original `.xlsx`/`.csv` source if the `.yxdb` was just an intermediate cache. Always save extracted data as CSV to a UC Volume. |
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

**Preferred: Use the visual `unique` operator** — it handles deduplication without SQL.

```yaml
- id: deduplicate
  template: unique
  name: deduplicate
  config:
    unique_by_all_columns: false
    columns:
      - unique_id
    sort_expressions:
      - columnExpr:
          expr: updated_at
        sortBy: DESC
  input:
    - node: add_validation
      input_port: data
      output_port: transformed_data
```

**Config options:**
- `unique_by_all_columns: true` — full-row dedup (drop exact duplicate rows across all columns)
- `unique_by_all_columns: false` + `columns: [key_cols]` — dedup by a column subset
- `sort_expressions` — within each duplicate group, keep the row that sorts first (e.g. most recent `updated_at`)
- Output port: `unique_data`

**Only use SQL ROW_NUMBER when:** the dedup requires multiple different partitions in the same step, or complex window logic that the visual operator cannot express.

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

**Preferred: Use `output` with `output_type: file`** for CSV/JSON/Excel file output:

```yaml
- id: write_csv_to_volume
  template: output
  name: write_csv_to_volume
  config:
    output_type: file
    catalog: cat
    schema: exports
    volume: orders_csv
    file_name: orders.csv
    file_type: csv
  input:
    - node: last_transform
      input_port: data
      output_port: <last_operator_output_port>
```

Fall back to `python` only for formats not supported by the output operator (e.g. Parquet with custom options). When the original Alteryx workflow requires Python file writes:

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

## Step 10: Data Validation (REQUIRED)

### 10a. Ask for the expected output file

**This step is mandatory.** Before finalizing the pipeline, you MUST have an expected output file to validate against.

- If the user provided an expected output file (CSV, Excel, Parquet, or table) alongside the `.yxmd`, proceed to 10b.
- If the user has not provided one, stop and explicitly ask:

> "To validate the converted pipeline produces correct results, I need an expected output file — typically the CSV/Excel that the Alteryx workflow originally produced. Could you provide that file? (Upload it or place it in a UC Volume path.)"

Do not skip validation or assume the pipeline is correct without comparing against known-good output.

### 10b. Upload expected output to a UC Volume

Save the expected output file to the same UC Volume area as the source data, for example:

`/Volumes/<catalog>/<schema>/raw/<expected_output_filename>`

### 10c. Add a validation source node

Read the expected output as a separate `source` or `python` node (depending on format). Place it below the main pipeline flow at the same x-level as the final output.

### 10d. Run structural validation (row counts by granularity)

Add a `sql` validation node that compares row counts by key dimensions:

```yaml
- id: validation_row_counts
  template: sql
  name: validation_row_counts
  config:
    query: |
      SELECT 'actual' AS source, <granularity_column>, COUNT(*) AS row_count
      FROM actual_output
      GROUP BY <granularity_column>
      UNION ALL
      SELECT 'expected' AS source, <granularity_column>, COUNT(*) AS row_count
      FROM expected_data
      GROUP BY <granularity_column>
      ORDER BY <granularity_column>, source
  input:
    - node: <actual_output_node>
      input_port: data
      output_port: <port>
    - node: <expected_source_node>
      input_port: data
      output_port: <port>
```

**What to check:**
- Total row count: should match exactly or within a small margin (< 3%) if source data was regenerated
- Row count by each granularity/dimension: identify which specific categories differ
- If counts differ, investigate whether the source data has changed (common with dummy/test data)

### 10e. Run numeric validation (value comparison)

Add a second `sql` node comparing key numeric columns for a known subset:

```yaml
- id: validation_values
  template: sql
  name: validation_values
  config:
    query: |
      SELECT a.<key_cols>,
             a.<metric> AS actual_value,
             e.<metric> AS expected_value,
             ABS(a.<metric> - e.<metric>) AS abs_diff,
             CASE WHEN e.<metric> != 0
                  THEN ABS(a.<metric> - e.<metric>) / ABS(e.<metric>) * 100
                  ELSE NULL END AS pct_diff
      FROM actual_output a
      JOIN expected_data e ON a.<key1> = e.<key1> AND a.<key2> = e.<key2>
      WHERE ABS(a.<metric> - e.<metric>) > 0.0001
      ORDER BY pct_diff DESC
      LIMIT 20
  input:
    - node: <actual_output_node>
      input_port: data
      output_port: <port>
    - node: <expected_source_node>
      input_port: data
      output_port: <port>
```

### 10f. Numeric precision tolerance

Alteryx uses `FixedDecimal` types (typically 19.6 — 19 digits, 6 decimal places). Spark uses IEEE 754 double-precision. Expected differences:
- **< 0.1% difference**: Normal — Alteryx intermediate rounding vs Spark continuous precision
- **0.1% – 2%**: Likely due to regenerated dummy/test data (different random seed) — verify structural match instead
- **> 2%**: Logic error — investigate the specific aggregation path

**Structural match criteria (when values differ due to data regeneration):**
- Same number of time periods processed
- Same granularity types/labels present
- Same column names and types
- Row counts per granularity follow the same pattern (e.g. Period=8, Region=8×regions)

### 10g. Report validation results

Present results to the user in a concise summary covering:
- Structural match status by major granularity
- Total actual vs expected row counts
- Largest numeric percent difference
- Whether differences are explained by data regeneration or indicate a logic issue

### 10h. Clean up after validation

Once the pipeline is confirmed correct, remove the validation source and comparison nodes. They are temporary debugging tools, not part of the production pipeline.

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
| `.yxdb` files | Alteryx proprietary binary; `yxdb` Python lib fails on "e2 Database" format | Ask user to export to CSV/Parquet first; or extract subset from expected output file |
| Alteryx FixedDecimal precision | Numeric values differ ~0.04-1.7% vs expected output | Accept tolerance; verify structure (row counts, granularity types) rather than exact numeric match |
| Excel source with NaN/blank cells | NULL columns for future periods in CYTD reports | Correctly handled — stable assortment filters exclude NULLs; no action needed |
| Fan-out to 10+ Summarize/Join pairs | Verbose DAG; many intermediate Alteryx nodes | Consolidate into one `sql` per logical branch (e.g. Retail, Cost) using UNION ALL across granularities; remove downstream sort/rename transforms; see §15b and §15f |
| Complex Python code | Code field gets stripped | Keep code simple; split logic across multiple `python` operators |
| Iterative macros | No native loop | Lakeflow Job `For Each` task |
| Reporting/Render tools | No equivalent | Output Delta + Lakeview (AI/BI) dashboard |
| Interface tools | No form UI | Job parameters or Databricks App |
| Spatial without Sedona | `ST_*` not found | Enable Sedona on the cluster, or mark MANUAL |
| Excel `.xlsb` / `.xlsm` macros | Macros not executed | Pandas reads cell values only — VBA macros must be ported manually |
| Iterative joins on huge tables | OOM | Use broadcast hint in SQL: `/*+ BROADCAST(small) */` |
| Designer Filter is graphical, not free-form SQL | Cannot enter `REGEXP_LIKE`, `BETWEEN`, multi-AND-OR mixes directly | Fall back to `sql` operator |
| Designer Join has no cross-join | Append Fields cannot map to `join` | Use a `sql` operator with `CROSS JOIN` |
| Aggregate has no collect_list/set (array output) | Alteryx Summarize → Concat List doesn't fit | Use `sql` with `collect_list(col)` / `collect_set(col)`. Note: FIRST, LAST, CONCAT (string), and COUNT_DISTINCT are now natively supported by the Aggregate operator. |
| Combine requires matching schemas | Heterogeneous Alteryx Unions fail | Pre-align schemas with two `transform` operators before `combine` |
| YAML docstring colon in `description.text` breaks the cell | Downstream cells fail with `'<this>.<port>' data is missing or not created before use` because Designer never registers the broken cell in the dataflow graph | **Always quote** any free-text YAML scalar that may contain `:`, `#`, `{`, `}`, `[`, `]`, `,`, or leading/trailing whitespace. Concretely: emit `text: "Per-category metrics: avg / median / sum / count."` (double-quoted) rather than `text: Per-category metrics: avg / median / sum / count.` |

---

## Step 15: Common Alteryx Patterns & VDP Equivalents

### 15a. Fisher Index / Laspeyres-Paasche calculation

A common economic/inflation calculation:
1. Split into Retail/Cost branches
2. Compute Average Item Price = Value / Quantity
3. Cross-products: `Price_CY × Qty_PY` and `Price_PY × Qty_CY`
4. Laspeyres = SUM(Price_CY × Qty_PY) / SUM(Value_PY)
5. Paasche = SUM(Value_CY) / SUM(Price_PY × Qty_CY)
6. Fisher = SQRT(Laspeyres × Paasche)

**Critical**: Non-Region granularities aggregate at the article level first (summing across regions), then apply stable assortment filters, then compute prices. Region granularity keeps raw per-region data. This produces mathematically different results — always trace the Summarize tool's GROUP BY fields.

**Optimization**: The Fisher calculation should be a single `sql` operator that consumes the consolidated aggregation output (all granularities already unioned). Do not duplicate the Fisher formula per granularity — compute it once with a GROUP BY on `Granularity, Granularity_Value`.

### 15b. Fan-out aggregation (one source → many granularities)

**When to use `aggregate` vs `sql` for Alteryx Summarize:**
- **Use `aggregate`** for any standalone Summarize that does a single GROUP BY with supported aggregation functions (SUM, AVG, COUNT, MIN, MAX, MEDIAN, STDDEV, VARIANCE, PERCENTILE). The visual `aggregate` operator is easier for users to read, modify, and maintain — prefer it over `sql` for simple aggregations.
- **Use `sql`** only when the aggregation pattern is too complex for a single `aggregate` — e.g. multiple different GROUP BY clauses UNIONed together, literal string columns added per segment, NULL casts for columns that vary per granularity, or unsupported functions like FIRST_VALUE / LAST_VALUE / collect_list.

When one intermediate feeds 5+ parallel Summarize chains (common in inflation/index pipelines with Period, YTD, Region, etc.):
- Do NOT replicate each separate Summarize+Join — consolidate into **one `sql` operator per branch** (e.g. one for Retail, one for Cost) that computes ALL granularities via UNION ALL inside a single query
- Each UNION ALL segment performs its own GROUP BY and derives ratios in-line
- This replaces N×2 cells (N aggregates + N joins/transforms) with just 2 SQL cells
- Downstream Fisher/index logic then consumes the consolidated output directly

Pattern (one SQL cell replacing 5 separate aggregate+transform chains):

```sql
-- Retail branch: all granularities in one query
SELECT 'Period' AS Granularity, CAST(Period AS STRING) AS Granularity_Value,
       SUM(Retail_Value) AS Retail_Value, SUM(Retail_Quantity) AS Retail_Quantity
FROM source GROUP BY Period
UNION ALL
SELECT 'YTD' AS Granularity, CONCAT('YTD_', CAST(Period AS STRING)) AS Granularity_Value,
       SUM(Retail_Value) AS Retail_Value, SUM(Retail_Quantity) AS Retail_Quantity
FROM source GROUP BY <ytd_grouping>
UNION ALL
SELECT 'Region' AS Granularity, Region AS Granularity_Value,
       SUM(Retail_Value) AS Retail_Value, SUM(Retail_Quantity) AS Retail_Quantity
FROM source GROUP BY Region, Period
-- etc. for all granularity levels
```

**Typical reduction**: 38 → 25 cells (or more) by eliminating per-granularity transform, rename, and sort operators that become unnecessary once SQL handles formatting directly.

### 15c. Historical data union

Many workflows union computed results with historical `.yxdb` data:
- Add a second `source` for the historical CSV (extracted from expected output or exported from Alteryx)
- Align column names/types with a `transform` — ensure BOTH branches produce identical column names and order
- Use `combine` (UNION ALL) to merge
- **Critical after optimization**: when consolidating aggregation cells, ensure the dynamic branch's SELECT list exactly matches the historical source's columns. Common mismatches: `Granularity_Value` vs `Granularity Value`, extra/missing metric columns. Use a `select_columns` transform on the historical branch or adjust the SQL's aliases to align.

### 15d. Cleanse macro interpretation

The `Cleanse.yxmc` checkboxes:
- `Check Box (84) = True` → TRIM whitespace
- `Check Box (117) = True` → Remove duplicate whitespace
- `Drop Down (81) = "upper"` → Verify against actual output — may apply to column names only, not values

### 15e. Select tool with renames

`AlteryxSelect` does three things simultaneously:
1. Drops columns (`selected="False"`)
2. Renames columns (`rename="NewName"`)
3. Changes types (`type="Double"`)

Map to a single `transform` with explicit column list. `*Unknown selected="True"` means unlisted columns pass through.

### 15f. Post-conversion DAG optimization

After a faithful 1:1 Alteryx→VDP conversion, perform an optimization pass to reduce DAG complexity:

1. **Eliminate redundant `transform` (Select/Rename) cells**: If a downstream `sql` operator can produce correctly named columns directly in its SELECT list, remove the intermediate rename transform.
2. **Eliminate redundant `sort` cells**: Sort is only needed immediately before the `output` operator or for windowed operations. Sorts inserted to mirror Alteryx's Sort tools between aggregations are unnecessary — remove them.
3. **Consolidate parallel branches with UNION ALL in SQL**: When multiple parallel paths produce identically-structured rows (same columns, just different groupings), merge them into one `sql` cell using UNION ALL segments rather than separate aggregate → transform → combine chains.
4. **Absorb type formatting into SQL**: Rather than a post-aggregation `transform` for ROUND/CAST, include formatting directly in the SQL's SELECT list.
5. **Verify column consistency**: After removing intermediate transforms, confirm the final output columns still match (name, order, type) for any downstream `combine` or `output` operator.
6. **Prefer visual `aggregate` over `sql` for simple GROUP BY**: If a `sql` operator only does `SELECT <group_cols>, SUM/AVG/COUNT/MIN/MAX(<cols>) FROM ... GROUP BY ...` with no UNION ALL, window functions, or complex expressions, convert it to a visual `aggregate` operator. This makes the pipeline more accessible to non-SQL users and aligns with Designer's visual-first philosophy.

**Rule of thumb**: A well-optimized VDP pipeline should have ~60-65% of the cell count of a faithful 1:1 conversion.

---

## Conversion Checklist

- [ ] Alteryx `.yxmd` / `.yxmc` analyzed — every `<Node>` and `<Connection>` mapped
- [ ] Each tool converted to a VDP operator OR explicitly flagged **MANUAL/REVIEW**
- [ ] All input file formats handled per Step 3 (and `.yxdb` flagged for export)
- [ ] Source files placed in UC Volume (not Workspace `file:` paths)
- [ ] All column names are Delta-compatible (no spaces, no special chars)
- [ ] Type casts handle dirty data (filter or `TRY_CAST`)
- [ ] SQL operators reference simple display names (no spaces)
- [ ] Simple GROUP BY aggregations use visual `aggregate` operator (not `sql`); fixed-column Text To Columns use `transform` with `SPLIT` + `ELEMENT_AT`; reserve `sql` for multi-granularity UNION ALL, row explosion, window functions, or unsupported functions
- [ ] Mandatory 7-question visual-operator pre-check completed before every `sql` or `python` operator
- [ ] Each logical step has its own operator (no unnecessary CTE consolidation without user approval)
- [ ] AI functions used ONLY for creative/generative text on low-cardinality data (NOT for finite mappings)
- [ ] Python operators contain ONLY file I/O or ML code (no SOUNDEX, CASE WHEN, groupBy, datediff)
- [ ] Reusable custom Python logic assessed for UDO promotion (`uc-udf` / `uc-udtf` / `python-run-function`) — adjacent `markdown` node added if a UDO is used
- [ ] User was asked before consolidating multiple SQL window nodes into one
- [ ] Deduplication uses visual `unique` operator (not SQL ROW_NUMBER) — reserve SQL only for multi-partition dedup or complex window logic
- [ ] Joins preserve L / J / R branches required downstream
- [ ] Macros — standard inlined or extracted; iterative flagged for Job
- [ ] Predictive / spatial / time-series — Python operator emitted, MLflow noted, marked **REVIEW**
- [ ] Reporting / interface — flagged **MANUAL** with Lakeview / Job / App pointer
- [ ] Output operator configured with `catalog.schema.table_name`
- [ ] Optional non-Delta sinks added downstream of the Delta output (Step 9b)
- [ ] Output node previews with no errors
- [ ] Expected output file obtained from user (asked explicitly if not provided)
- [ ] Structural validation passed (row counts by granularity match or differences explained)
- [ ] Numeric validation passed (values within 0.1% tolerance, or data regeneration documented)
- [ ] Article-level pre-aggregation verified (if applicable — non-Region branches aggregate before price calculation)
- [ ] Historical data source included (if workflow unions computed results with prior-period `.yxdb` data)
- [ ] Data validation performed and validation nodes removed afterwards
- [ ] Post-conversion optimization performed (redundant sorts/renames removed, fan-out consolidated — §15f)
- [ ] Column schemas verified consistent across UNION branches after optimization
- [ ] Pipeline tested end-to-end
- [ ] Layout is clean and readable (horizontal flow, no overlaps)
- [ ] Top-level `markdown` documents pipeline purpose and any MANUAL items

