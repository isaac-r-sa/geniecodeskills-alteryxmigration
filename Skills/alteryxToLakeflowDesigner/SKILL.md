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
> Before writing any `sql`, `python`, or `ai_function` operator, answer ALL of these in order:
> 1. Can a **Transform** express this? (CASE WHEN, CAST, COALESCE, TRIM, UPPER, REGEXP_EXTRACT, SPLIT + ELEMENT_AT, DATEDIFF, arithmetic, literals) → **USE TRANSFORM. STOP.**
> 2. Can a **Filter** express this? (boolean row condition, with optional T/F split via v2.0.0) → **USE FILTER. STOP.**
> 3. Can an **Aggregate** express this? (GROUP BY + SUM/AVG/COUNT/COUNT_DISTINCT/FIRST/LAST/CONCAT/MIN/MAX/MEDIAN/STDDEV/PERCENTILE) → **USE AGGREGATE. STOP.**
> 4. Can a **Join** express this? (equi-join on key columns; use split_join for Alteryx L/J/R) → **USE JOIN. STOP.**
> 5. Can a **Sort**, **Limit**, **Pivot**, **Combine**, or **Unique** express this? → **USE THE VISUAL OPERATOR. STOP.**
> 6. Can a **Prepare** action express this? (trim, cast, text_case, fill_null, replace_value, regex_replace, extract, parse_date, formula) → **USE PREPARE. STOP.**
> 7. Can an **Enter Data** express this? (small inline/lookup table ≤ 20 rows) → **USE ENTER_DATA. STOP.**
> 8. Can a **Visualization** express this? (chart, counter, funnel — do NOT add a separate Aggregate upstream) → **USE VISUALIZATION. STOP.**
> 9. Is this reusable custom logic that exists (or should exist) as a UC function? → **USE A UDO. STOP.**
>
> Only if ALL nine answers are NO may you proceed to `sql` or `python`.
> If you write `sql` or `python` without answering all nine, the operator choice is wrong.

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

### Priority 3: User-Defined Operators (UDO) — for reusable custom functions

When custom logic is needed AND will be reused across pipelines, register it as a UDO rather than embedding it in a raw `python` cell. UDOs appear in the Designer palette as first-class visual operators.

| UDO Subtype | Use For | Example |
|---|---|---|
| **`uc-udf`** | Row-level custom transform registered as a UC UDF | Currency conversion, address normalization, custom hash |
| **`uc-udtf`** | Multi-row / stateful operation registered as a UC UDTF | ML model scoring, clustering, batch enrichment |
| **`python-run-function`** | Standalone Python callable, no UC dependency | External API call, email notification, PDF generation |

**Promote to UDO when:**
- The same logic appears in ≥2 pipelines (or is likely to)
- The function already exists (or should exist) as a UC UDF/UDTF
- The user has a library of reusable transforms (e.g., tax calculation, compliance rules)
- The Alteryx workflow calls a `.yxi` Custom Tool with a known Python implementation

**Do NOT promote to UDO when:**
- The operation is expressible with a built-in operator (Transform, Aggregate, etc.)
- The custom logic is one-off and pipeline-specific — use `python` instead

**Registration**: UDOs are defined in `.user_defined_operators.yaml` in the workspace. The `template` field in the YAML docstring is the UDO's registered identifier (not a fixed value like `transform`). See § 2.15 for YAML examples.

### Priority 4: SQL (ONLY after the mandatory pre-check passes, and only for these specific patterns)

- **Window functions**: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, SUM/AVG/COUNT OVER(...)
- **COLLECT_LIST / COLLECT_SET** — returns arrays, not supported by Aggregate
- **CTEs** — ONLY when required for SEQUENCE/EXPLODE or self-referencing subqueries
- **SEQUENCE + EXPLODE** (calendar/date generation)
- **Subqueries** (SELECT FROM (SELECT ...)) for inline DISTINCT before window
- **Explode-to-rows tokenization** (for example, `EXPLODE(SPLIT(col, ','))`)
- **QUALIFY** — row-level filter on window results

**Do NOT use SQL for:**
- Fixed-column string splitting — use **Transform** with `SPLIT` + `ELEMENT_AT`
- Finite mappings / small Find Replace rules — use **Transform** CASE WHEN
- Simple GROUP BY with SUM/COUNT/AVG — use **Aggregate** operator
- COUNT(DISTINCT col) — use **Aggregate** with `COUNT_DISTINCT`
- FIRST/LAST value per group — use **Aggregate** with `FIRST`/`LAST`
- Simple deduplication — use **Unique** operator
- Inline constant rows or tiny lookup tables — use **Enter Data** or **Transform** CASE WHEN

### Priority 5: Python (ABSOLUTE LAST RESORT — only for)

- **File I/O**: Excel read/write, PDF form filling (pypdf), binary file operations
- **ML model training/scoring**: sklearn, pyspark.ml, statsmodels
- **External libraries** with no SQL/visual equivalent (e.g., ARIMA, Prophet)
- **One-off custom logic** that does NOT warrant a UDO (≤1 pipeline uses it)

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
| ~~COUNT DISTINCT~~ | ✅ Now supported by visual **Aggregate** operator as `COUNT_DISTINCT` — no SQL needed |
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
| COUNT(DISTINCT col) | **Aggregate** (use `COUNT_DISTINCT` fn) |
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
| Skipping the mandatory pre-check and jumping straight to `sql` | Answer all 9 pre-check questions first; only then use `sql` |
| `sql` ROW_NUMBER for simple deduplication | Use visual `unique` operator — `unique_by_all_columns: false` + `columns` + optional `sort_expressions` |
| `python` `spark.createDataFrame(...)` for small inline/lookup tables | Use visual `enter_data` operator with markdown-style table syntax |
| `sql` COUNT(DISTINCT col) in GROUP BY | Use visual `aggregate` with `fn: COUNT_DISTINCT` — now natively supported |
| `sql` FIRST_VALUE / LAST_VALUE in GROUP BY | Use visual `aggregate` with `fn: FIRST` or `fn: LAST` — now natively supported |
| Two `filter` operators with inverse conditions for Alteryx T/F split | Use ONE `filter` with two output ports: `filtered_data` (T) and `excluded_data` (F) |
| `sql` LEFT ANTI / RIGHT ANTI for Alteryx Join L/R unmatched | Use `join` with `join_type: split_join` — produces `joined_data`, `left_unmatched`, `right_unmatched` |
| `python` for simple Excel/CSV/JSON file output to Volume | Use native `output` with `output_type: file`, `file_type: csv/json/excel`, `write_mode: overwrite/append` — Python is only needed for multi-sheet workbooks, formatted output, or template injection |
| `python` `pandas.read_excel()` for simple full-file Excel reads | Use native `source` operator with `format: excel` — no Python needed. Requires Excel File Format Support enabled. Fall back to `python` only for specific sheets, ranges, named ranges, or legacy `.xls`/`.xlsb` formats |
| `python` for JSON file output | Use native `output` with `output_type: file`, `file_type: json` — Python is only needed for pretty-printing, nested JSON, or custom envelopes |
| `enter_data` for lookup tables when preview crashes with `'DataFrame' object has no attribute 'map'` | The built-in `enter_data` template uses `pdf.map()` which requires pandas ≥ 2.1.0. **Fallback**: replace with a `python` operator using `spark.createDataFrame(data, schema)`. Update downstream wiring from `output_port: data` → `output_port: result`. |
| Join@1.0.0 with `expressions: []` (empty) | Always set explicit join expressions: `["left.*", "right.needed_col"]` — empty expressions pass ALL columns from both sides, duplicating the join key and causing `DLTAnalysisException: duplicate column name` downstream |
| Treating output v4.0.0 preview `TABLE_OR_VIEW_NOT_FOUND` as a bug | The v4.0.0 output preview only does `SELECT * FROM target` — it does NOT write. First-run failure is **expected**. The actual write happens on Run All. Same for file outputs (`CF_PATH_DOES_NOT_EXIST_FOR_READ_FILES`). Do not try to "fix" this. |
| Passing VARIANT columns to downstream Transform/Join operators | ai_parse_document and ai_extract return VARIANT. Designer preview fails with `UNSUPPORTED_OPERATION` on VARIANT. **Always CAST to STRING/DOUBLE/etc inline** in the same SQL — never let VARIANT flow downstream. |
| `TRY_TO_TIMESTAMP(col, 'MM/dd/yyyy')` on AI-extracted dates | AI-extracted dates often come in mixed formats (MM/dd/yyyy, yyyy-M-d, dd-MMM-yyyy). Use `COALESCE(TRY_TO_TIMESTAMP(col, 'MM/dd/yyyy'), TRY_TO_TIMESTAMP(col, 'yyyy-M-d'), TRY_TO_TIMESTAMP(col, 'dd-MMM-yyyy'))` in a Transform. |
| Reusable Python logic duplicated across pipelines | Promote to a **UDO** (`uc-udf` for row-level, `uc-udtf` for multi-row, `python-run-function` for external calls). Register once, reuse everywhere as a visual operator. |
| `python` node calling a UC UDF via `spark.sql("SELECT my_udf(...)")` | Use a **`uc-udf` UDO** instead — it makes the function a drag-and-drop operator in the palette, with proper type checking and documentation. |
| `sql` calling a UC UDTF via `SELECT * FROM my_udtf(TABLE(...))` | Use a **`uc-udtf` UDO** instead — wraps the UDTF as a visual operator with explicit input/output column mapping. |


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
| `output` | Output | `data` | (terminal) | Three output types: **Table** (`output_type: table`) — `catalog`, `schema`, `table_name`; **Materialized View** (`output_type: materialized_view`) — publishes as an MV in UC that refreshes on each run; **File** (`output_type: file`) — `catalog`, `schema`, `volume`, `file_name`, `file_type: csv/json/excel`. **Write modes** (table & file): `overwrite` (default), `append` (adds rows to existing), **`merge`** (table only — upserts by `merge_keys`). Preview is read-only (expected `TABLE_OR_VIEW_NOT_FOUND` on first run); actual write happens on Run All. |
| `ai_function` | AI Function | `data` | `ai_data` | `expressions: [SQL expressions calling ai_* functions]` (returns those columns plus `*`) |
| `aggregate` | Aggregate | `data` | `aggregated_data` | `group_bys: [{expr, type: expr}, ...]`, `aggregations: [{columnExpr: {expr, type: expr}, fn, alias}, ...]`. Supported `fn`: AVG, COUNT, **COUNT_DISTINCT**, **FIRST**, **LAST**, **CONCAT**, MAX, MEAN, MEDIAN, MIN, PERCENTILE, STDDEV, SUM, VARIANCE. FIRST/LAST are non-deterministic without upstream Sort. CONCAT joins strings with optional `separator` (default `", "`). |
| `combine` | Combine | `data_0`, `data_1` | `combined_data` | `operator`: UNION / INTERSECT / EXCEPT / MINUS; `quantifier`: ALL / DISTINCT |
| `filter` | Filter | `data` | `filtered_data` (v1.0.0) **or** `filtered_data` + `excluded_data` (v2.0.0) | `condition: "<SQL boolean expression>"`. **v2.0.0** (recommended) emits **two output ports**: `filtered_data` (matching rows — Alteryx True) and `excluded_data` (non-matching — Alteryx False). Maps directly to Alteryx Filter T/F. Always use `templateVersion: 2.0.0` when both branches are needed. |
| `join` | Join | `left`, `right` | `joined_data` (standard) **or** `joined_data` + `left_unmatched` + `right_unmatched` (split_join) | `join_type`: inner / left / right / full / cross_join / **split_join**; `join_conditions: "left.col_a = right.col_b AND ..."` (always use the `left.` / `right.` aliases). Optional `expressions: [select-expressions]` — **always set explicit expressions** (never empty `[]`). **split_join** (recommended for Alteryx Join) produces 3 output ports mapping directly to Alteryx L/J/R. |
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
| Output Data (table) | `output` | `output_type: table`, catalog + schema + table_name, `write_mode: overwrite/append/merge`. Merge upserts by `merge_keys`. |
| Output Data (file — CSV/JSON/Excel) | `output` (file mode) | `output_type: file`, `file_type: csv/json/excel`, `write_mode: overwrite/append`. Supports **Excel (.xlsx) natively** — no Python needed for simple data dumps. Fall back to `python` only for multi-sheet workbooks, formatted output, or template injection. |
| Browse | *omit* | Browse is just a preview tile — no VDP analog needed |
| Text Input | `enter_data` | Inline table with markdown-style syntax (header row + separator + data rows). Use `enter_data` for small static lookup/constant tables. Fall back to `python` `spark.createDataFrame` only for programmatic row generation. |
| Directory | `python` | `os.listdir` over a Volume path; see Step 4 |
| Date/Time Now | `transform` | `current_timestamp()` / `current_date()` |
| Input Data (.xlsx — full file, single sheet) | **`source`** (native) or `python` | **Preferred**: `source` with `file_source: {path, format: excel}` — reads natively via `read_files`. Requires [Excel File Format Support](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/volumes-read-write#excel) enabled. Fall back to `python` `pandas.read_excel()` only if the feature is disabled or for `.xlsb`/`.xls` legacy formats. See §4 Excel Ingest Patterns. |
| Input Data (.xlsx — specific sheet/range/named range) | `python` | `pandas.read_excel(sheet_name=, usecols=, skiprows=, nrows=, header=)`; see §4 Excel Ingest Patterns |
| Input Data (.xls legacy) | `python` | `pandas.read_excel(engine='xlrd')`; see §4 Excel Ingest Patterns |
| Output Data (.xlsx — simple) | **`output`** (file mode) | `output_type: file`, `file_type: excel`, `write_mode: overwrite/append` — **native, no Python** |
| Output Data (.xlsx — multi-sheet/formatted/template) | `python` | `toPandas()` → `openpyxl` write to UC Volume; see §4 Excel Write-back Patterns |
| Output Data (materialized view) | `output` (MV mode) | `output_type: materialized_view` — publishes as an MV in UC, refreshes on each run |
| Input Data (PDF — form/document) | `sql` or `python` | `ai_parse_document()` → `ai_extract()` chain; see §4 PDF Parse Patterns |
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
| PDF form data extraction | `sql` chain | `ai_parse_document(content, MAP('version','2.0'))` → `ai_extract(parsed, schema, MAP('version','2.1'))` — see §4 PDF Parse Patterns |

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

UDOs are a VDP-native feature for packaging reusable logic as a **first-class visual operator** that appears in the Designer palette. They are the preferred path for custom functions that are reused across pipelines — **always prefer a UDO over a raw `python` node** when the logic is reusable.

#### When to create/use a UDO

| Alteryx Pattern | UDO Subtype | Notes |
|---|---|---|
| Python Tool with a row-level formula already registered (or planned) as a UC UDF | `uc-udf` | Maps each input row through the UDF; output is a new column. Prefer over `python` when the function is already in UC. |
| Python Tool performing ML scoring via an MLflow model | `uc-udtf` | UDTF accepts the full table, applies the model, returns scored rows. Cleaner and more reusable than `python` with `mlflow.pyfunc.load_model`. |
| Python Tool calling an external API (e.g. Slack, email, enrichment service) | `python-run-function` | Standalone Python callable; no UC required. Keeps pipeline logic consistent without embedding credentials in a raw `python` cell. |
| Custom Tool (`.yxi`) with a known Python implementation | `uc-udtf` or `python-run-function` | Assess whether the logic generalizes; if yes, register as UDO. If one-off, use a `python` node instead. |
| Tax calculation / compliance formula reused across jurisdictions | `uc-udf` | Register once in UC, reuse across all tax pipelines. |
| Data quality / validation rule library | `uc-udf` or `uc-udtf` | Standard DQ checks become drag-and-drop operators. |
| R Tool | `python` (flag **REVIEW**) | R has no UDO path; re-implement in PySpark or Python first. |

#### UDO YAML templates

**`uc-udf` — Row-level UC UDF:**
```yaml
- id: apply_tax_calc
  template: my_tax_calculator    # matches the UDO registered name
  name: apply_tax_calc
  position: { x: 600, y: 140 }
  description:
    text: "Apply custom tax calculation UDF"
    hash: ""
  previewMode: "1000"
  config:
    function_name: catalog.schema.calculate_tax
    input_columns:
      - gross_amount
      - jurisdiction
    output_column: tax_due
  input:
    - node: upstream_op
      input_port: data
      output_port: transformed_data
```

**`uc-udtf` — Multi-row UC UDTF (ML scoring, batch enrichment):**
```yaml
- id: score_model
  template: my_ml_scorer    # matches the UDO registered name
  name: score_model
  position: { x: 900, y: 140 }
  description:
    text: "Score rows with ML model via UC UDTF"
    hash: ""
  previewMode: "1000"
  config:
    function_name: catalog.schema.score_churn_model
    input_columns:
      - recency
      - frequency
      - monetary
    output_columns:
      - churn_score
      - churn_segment
  input:
    - node: upstream_op
      input_port: data
      output_port: aggregated_data
```

**`python-run-function` — Standalone Python callable:**
```yaml
- id: call_enrichment_api
  template: my_enrichment_func    # matches the UDO registered name
  name: call_enrichment_api
  position: { x: 1200, y: 140 }
  description:
    text: "Enrich records via external API"
    hash: ""
  previewMode: "1000"
  config:
    function_module: my_package.enrichment
    function_name: enrich_vendor_data
    params:
      api_key_secret: scope/key
      batch_size: 100
  input:
    - node: upstream_op
      input_port: data
      output_port: data
```

#### UDO registration

UDOs are registered in `.user_defined_operators.yaml` in the workspace root:
```yaml
operators:
  - name: my_tax_calculator
    type: uc-udf
    display_name: Tax Calculator
    description: Calculate tax using jurisdiction-specific rules
    catalog: main
    schema: tax_functions
    function: calculate_tax
  - name: my_ml_scorer
    type: uc-udtf
    display_name: ML Churn Scorer
    description: Score customer churn risk
    catalog: main
    schema: ml_functions
    function: score_churn_model
```

**Emit a `markdown` note** adjacent to any UDO cell that explains: the UDO name, the UC catalog path (for `uc-udf` / `uc-udtf`), and any one-time setup the user must perform before running the pipeline.

#### UDO promotion checklist

When converting a `python` operator, always ask:
1. Does this logic appear in ≥2 pipelines (or is it likely to)? → **Promote to UDO**
2. Does a UC UDF/UDTF already exist for this? → **Use `uc-udf` / `uc-udtf` UDO**
3. Is the user building a library of reusable functions? → **Register as UDO**
4. Is this a one-off, pipeline-specific operation? → **Keep as `python`**

---

## Step 3: File Format Coverage Matrix

| Category | Format | VDP Operator | Notes |
|---|---|---|---|
| **Tabular** | CSV / TSV | `source` (file_source, `format: csv`) | `header`, `delimiter`, `inferSchema` |
| | Fixed-width | `python` | `spark.read.text` + `substring` slices |
| | Excel `.xlsx` / `.xlsm` (simple full file) | **`source`** (native) | `file_source: {path: /Volumes/.../file.xlsx, format: excel}` — reads via `read_files`. Requires Excel File Format Support enabled. |
| | Excel `.xlsx` (specific sheet/range/named range) | `python` | `pandas.read_excel(sheet_name=, usecols=, skiprows=, nrows=)` — native source cannot target specific sheets/ranges |
| | Excel `.xlsb` (binary) / `.xls` (legacy) | `python` | `pandas.read_excel(engine='pyxlsb')` or `engine='xlrd'` — binary/legacy formats not supported by native source |
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
| **Documents** | PDF (form data / structured extraction) | `sql` | `ai_parse_document()` → `ai_extract()` chain (preferred); see §4 PDF Parse Patterns |
| | PDF (tabular / table extraction) | `python` or `sql` | `ai_parse_document()` for AI-powered extraction (preferred); `tabula-py` as fallback for simple grids |
| | HTML | `python` | `pandas.read_html` |
| **Compressed** | `.gz` / `.bz2` | `source` | Spark reads transparently if extension is preserved |
| | `.zip` | `python` | Unzip to Volume first; then read |
| | `.7z` | `python` | `py7zr`; unzip to Volume first |
| **Cloud / DB** | S3 / ADLS / GCS | `source` (file_source) | Use UC External Volume mounted to the bucket |
| | UC Tables | `source` (table_source) | Preferred over JDBC for Databricks-native data |
| | Snowflake / Redshift / SQL Server / Oracle / Postgres / Teradata | `python` (JDBC) or UC Federation | Prefer Lakehouse Federation foreign catalog → `table_source` |

**Rule of thumb**: if a format is in the `source.file_source.format` enum (`csv`, `json`, `parquet`, `avro`, `orc`, `excel`), use `source`. For **output** file writes, the native `output` operator supports only `csv`, `json`, and `excel` — for Parquet/Avro/ORC/XML writes, use `python`. Always copy local files to a UC **Volume** first (Step 4).

**Source operator folder ingestion**: The `source` operator can target an entire UC Volume or folder path — Designer auto-detects the format and combines all files into one table. For structured formats (CSV, JSON, Excel, Parquet), all files in the folder are unioned. For PDF documents, each file becomes one row.

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

### Excel Ingest Patterns (covers .xlsx/.xls/.xlsm/.xlsb)

**Decision tree — pick the first that applies:**

| Scenario | Operator | Method |
|---|---|---|
| Full file, single sheet, no range filtering | **`source`** (native) or `python` | **Preferred**: `source` with `format: excel` (requires Excel File Format Support). Fallback: `pandas.read_excel(path)` |
| Specific sheet by name or index | `python` | `pandas.read_excel(path, sheet_name='Sheet2')` or `sheet_name=1` |
| Specific cell range (e.g. A1:G50) | `python` | `openpyxl` load + slice, then `pd.DataFrame` |
| Named range / defined name | `python` | `openpyxl` load → `wb.defined_names[name]` → cell range → slice |
| Skip header rows / footer rows | `python` | `pandas.read_excel(path, skiprows=3, nrows=100)` |
| Multiple sheets → union | `python` | Loop `sheet_name=[...]` or `sheet_name=None` (all sheets) |
| Legacy `.xls` | `python` | `pandas.read_excel(path, engine='xlrd')` |
| `.xlsb` (binary) | `python` | `pandas.read_excel(path, engine='pyxlsb')` |
| `.xlsm` with macros | `python` | Reads cell values only — VBA macros are NOT executed |
| Very large Excel (100k+ rows) | `python` | `openpyxl` read_only mode or spark-excel (if available) |

#### Pattern A0: Native source (PREFERRED for simple Excel reads)

```yaml
- id: src_excel
  template: source
  name: src_excel
  config:
    file_source:
      path: /Volumes/cat/raw/landing/book.xlsx
      format: excel
      header: true
      inferSchema: true
  input: []
```

> **Note**: Native Excel source requires [Excel File Format Support](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/volumes-read-write#excel) to be enabled on the workspace. If disabled, fall back to Pattern A (Python). The native source reads the first sheet by default — for specific sheets, ranges, or named ranges, use the Python patterns below.

#### Pattern A: Full file / single sheet (Python fallback)

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

#### Pattern B: Specific cell range (e.g. Alteryx "Data Address" B3:F50)

```yaml
- id: src_excel_range
  template: python
  name: src_excel_range
  config:
    code: |
      import openpyxl, pandas as pd
      wb = openpyxl.load_workbook("/Volumes/cat/raw/landing/report.xlsx", read_only=True, data_only=True)
      ws = wb["Summary"]
      data = [[cell.value for cell in row] for row in ws["B3":"F50"]]
      header = data[0]
      rows = data[1:]
      pdf = pd.DataFrame(rows, columns=header)
      pdf.columns = [str(c).strip().lower().replace(" ", "_") for c in pdf.columns]
      result = spark.createDataFrame(pdf)
  input: []
```

#### Pattern C: Named range

```yaml
- id: src_excel_named
  template: python
  name: src_excel_named
  config:
    code: |
      import openpyxl, pandas as pd
      wb = openpyxl.load_workbook("/Volumes/cat/raw/landing/model.xlsx", read_only=False, data_only=True)
      dest = wb.defined_names["SalesData"]
      sheet_title, cell_range = next(dest.destinations)
      ws = wb[sheet_title]
      data = [[cell.value for cell in row] for row in ws[cell_range]]
      pdf = pd.DataFrame(data[1:], columns=data[0])
      pdf.columns = [str(c).strip().lower().replace(" ", "_") for c in pdf.columns]
      result = spark.createDataFrame(pdf)
  input: []
```

#### Pattern D: All sheets unioned

```yaml
- id: src_excel_all_sheets
  template: python
  name: src_excel_all_sheets
  config:
    code: |
      import pandas as pd
      sheets = pd.read_excel("/Volumes/cat/raw/landing/multi.xlsx", sheet_name=None)
      frames = []
      for name, pdf in sheets.items():
          pdf.columns = [str(c).strip().lower().replace(" ", "_") for c in pdf.columns]
          pdf["_sheet_name"] = name
          frames.append(pdf)
      combined = pd.concat(frames, ignore_index=True)
      result = spark.createDataFrame(combined)
  input: []
```

#### Pattern E: spark-excel (large files, if library available on cluster)

```yaml
- id: src_excel_spark
  template: python
  name: src_excel_spark
  config:
    code: |
      result = (spark.read
          .format("com.crealytics.spark.excel")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("dataAddress", "'Sheet1'!A1")
          .load("/Volumes/cat/raw/landing/large_file.xlsx"))
  input: []
```

> **Note**: spark-excel is a third-party library (`com.crealytics:spark-excel_2.12`). It must be installed as a cluster library. On serverless compute it is NOT available — use pandas patterns instead.

### Excel Write-back Patterns

**Decision tree — pick the first that applies:**

| Scenario | Operator | Method |
|---|---|---|
| Simple data dump to `.xlsx` (overwrite) | **`output`** (native) | `output_type: file`, `file_type: excel`, `write_mode: overwrite` — **NO Python needed** |
| Append rows to existing `.xlsx` | **`output`** (native) | `output_type: file`, `file_type: excel`, `write_mode: append` — **NO Python needed** |
| Simple data dump to `.csv` | **`output`** (native) | `output_type: file`, `file_type: csv`, `write_mode: overwrite` or `append` |
| Simple data dump to `.json` | **`output`** (native) | `output_type: file`, `file_type: json`, `write_mode: overwrite` or `append` |
| Multiple sheets in one workbook | `python` | `pd.ExcelWriter` context manager (native output writes single-sheet only) |
| Formatted output (bold headers, number formats, column widths) | `python` | `openpyxl` Workbook + manual styling |
| Write into an existing Excel template | `python` | `openpyxl.load_workbook(template)` → write cells → save to Volume |

> **ALWAYS prefer the native `output` operator** for simple Excel/CSV/JSON file writes. Only fall back to `python` when you need multi-sheet workbooks, cell-level formatting, or template injection.

**IMPORTANT**: Always materialize to a Delta table first (Step 9a canonical output), then add the Excel write-back as a secondary sink. The Delta table is the source of truth.

#### Pattern A0: Native Excel output (PREFERRED for simple writes)

```yaml
- id: write_excel_native
  template: output
  templateVersion: 4.0.0
  name: write_excel_native
  config:
    output_type: file
    catalog: cat
    schema: exports
    volume: reports
    file_name: output_report.xlsx
    file_type: excel
    write_mode: overwrite
  input:
    - node: last_transform
      input_port: data
      output_port: <last_output_port>
```

#### Pattern A0-append: Native Excel append (add rows to existing file)

```yaml
- id: append_excel_native
  template: output
  templateVersion: 4.0.0
  name: append_excel_native
  config:
    output_type: file
    catalog: cat
    schema: exports
    volume: reports
    file_name: running_log.xlsx
    file_type: excel
    write_mode: append
  input:
    - node: new_data
      input_port: data
      output_port: <port>
```

#### Pattern A1: Python data dump (ONLY when native output is insufficient)

```yaml
- id: write_excel
  template: python
  name: write_excel
  config:
    code: |
      import pandas as pd
      df = inputs["data"][0]
      pdf = df.toPandas()
      pdf.to_excel("/Volumes/cat/exports/output.xlsx", index=False, sheet_name="Results")
      result = df  # pass-through
  input:
    - node: last_transform
      input_port: data
      output_port: <last_output_port>
```

#### Pattern B: Multiple sheets

```yaml
- id: write_excel_multi
  template: python
  name: write_excel_multi
  config:
    code: |
      import pandas as pd
      df_summary = inputs["data"][0].toPandas()
      df_detail  = inputs["data"][1].toPandas()
      with pd.ExcelWriter("/Volumes/cat/exports/report.xlsx", engine="openpyxl") as writer:
          df_summary.to_excel(writer, sheet_name="Summary", index=False)
          df_detail.to_excel(writer, sheet_name="Detail", index=False)
      result = inputs["data"][0]  # pass-through
  input:
    - node: summary_node
      input_port: data
      output_port: <port>
    - node: detail_node
      input_port: data
      output_port: <port>
```

#### Pattern C: Formatted output (bold headers, column widths, number formats)

```yaml
- id: write_excel_formatted
  template: python
  name: write_excel_formatted
  config:
    code: |
      from openpyxl import Workbook
      from openpyxl.styles import Font
      df = inputs["data"][0]
      pdf = df.toPandas()
      wb = Workbook()
      ws = wb.active
      ws.title = "Report"
      for col_idx, col_name in enumerate(pdf.columns, 1):
          cell = ws.cell(row=1, column=col_idx, value=col_name)
          cell.font = Font(bold=True)
          ws.column_dimensions[cell.column_letter].width = max(len(str(col_name)) + 4, 12)
      for row_idx, row in enumerate(pdf.itertuples(index=False), 2):
          for col_idx, value in enumerate(row, 1):
              ws.cell(row=row_idx, column=col_idx, value=value)
      wb.save("/Volumes/cat/exports/formatted_report.xlsx")
      result = df
  input:
    - node: last_transform
      input_port: data
      output_port: <port>
```

#### Pattern D: Write into existing Excel template

```yaml
- id: write_excel_template
  template: python
  name: write_excel_template
  config:
    code: |
      from openpyxl import load_workbook
      import shutil
      src = "/Volumes/cat/raw/templates/report_template.xlsx"
      dst = "/Volumes/cat/exports/filled_report.xlsx"
      shutil.copy2(src, dst)
      wb = load_workbook(dst)
      ws = wb["Data"]
      df = inputs["data"][0]
      pdf = df.toPandas()
      for row_idx, row in enumerate(pdf.itertuples(index=False), 2):
          for col_idx, value in enumerate(row, 1):
              ws.cell(row=row_idx, column=col_idx, value=value)
      wb.save(dst)
      result = df
  input:
    - node: last_transform
      input_port: data
      output_port: <port>
```

**Limitations of Excel write-back:**
- **No VBA macro execution**: `.xlsm` templates preserve macros but they won't run in Databricks.
- **Formatting preservation**: Writing into a template preserves existing formatting in untouched cells.
- **File size**: `toPandas()` collects all data to the driver. For datasets >1M rows, consider Parquet/CSV output instead.
- **Charts/pivot tables in templates**: Existing charts remain but won't auto-refresh until opened in Excel.



### CSV Write-back Patterns

**Decision tree — pick the first that applies:**

| Scenario | Operator | Method |
|---|---|---|
| Simple CSV output (overwrite or append) | **`output`** (native) | `output_type: file`, `file_type: csv`, `write_mode: overwrite/append` — **NO Python needed** |
| Custom delimiter (TSV, pipe-separated) | `python` | Native output writes comma-delimited only; use `.write.option("delimiter", "\t").csv(...)` |
| Single-file output (no Spark partitioning) | `python` | `.coalesce(1).write.csv(...)` for a single file — native output may write multiple part files for large data |
| CSV with specific encoding (UTF-16, Latin-1) | `python` | `pandas.to_csv(encoding=...)` |

> **ALWAYS prefer the native `output` operator** for simple CSV file writes. Only fall back to `python` for custom delimiters, single-file guarantees, or encoding requirements.

#### Pattern A: Native CSV output (PREFERRED)

```yaml
- id: write_csv_native
  template: output
  templateVersion: 4.0.0
  name: write_csv_native
  config:
    output_type: file
    catalog: cat
    schema: exports
    volume: data_feeds
    file_name: orders_export.csv
    file_type: csv
    write_mode: overwrite
  input:
    - node: last_transform
      input_port: data
      output_port: <last_output_port>
```

### JSON Write-back Patterns

**Decision tree — pick the first that applies:**

| Scenario | Operator | Method |
|---|---|---|
| Simple JSON file output (overwrite) | **`output`** (native) | `output_type: file`, `file_type: json`, `write_mode: overwrite` — **NO Python needed** |
| Append JSON to existing file | **`output`** (native) | `output_type: file`, `file_type: json`, `write_mode: append` — **NO Python needed** |
| JSON Lines (NDJSON) format | **`output`** (native) | Same as above — Spark writes JSON as JSON Lines by default |
| Pretty-printed JSON (indented) | `python` | Native output writes JSON Lines; use `json.dumps(indent=2)` for human-readable formatting |
| Nested/hierarchical JSON from flat DataFrame | `python` | Build nested structs with `to_json(struct(...))` or custom Python logic |
| JSON with custom schema/envelope | `python` | Wrap data in a custom root key or metadata envelope |

> **ALWAYS prefer the native `output` operator** for simple JSON file writes. Only fall back to `python` when you need pretty-printing, custom nesting, or envelope structures.

#### Pattern A: Native JSON output (PREFERRED)

```yaml
- id: write_json_native
  template: output
  templateVersion: 4.0.0
  name: write_json_native
  config:
    output_type: file
    catalog: cat
    schema: exports
    volume: api_feeds
    file_name: results.json
    file_type: json
    write_mode: overwrite
  input:
    - node: last_transform
      input_port: data
      output_port: <last_output_port>
```

#### Pattern B: Pretty-printed JSON (Python fallback)

```yaml
- id: write_json_pretty
  template: python
  name: write_json_pretty
  config:
    code: |
      import json
      df = inputs["data"][0]
      records = [row.asDict() for row in df.collect()]
      with open("/Volumes/cat/exports/api_feeds/results_pretty.json", "w") as f:
          json.dump(records, f, indent=2, default=str)
      result = df  # pass-through
  input:
    - node: last_transform
      input_port: data
      output_port: <port>
```

**Limitations of JSON write-back:**
- Native output writes JSON Lines (one JSON object per line) — not a single JSON array. Most downstream systems accept NDJSON, but if the consumer expects a JSON array, use the Python pattern.
- `df.collect()` loads all data to the driver. For datasets >1M rows, consider writing as JSON Lines (native output) or Parquet instead.

### PDF Parse Patterns (ai_parse_document → ai_extract)

**When to use**: Any Alteryx workflow that reads PDF form data, extracts invoice fields, processes scanned documents, or parses unstructured PDF content.

**Decision tree:**

| Scenario | Operator | Method |
|---|---|---|
| Extract structured fields from PDF forms (invoices, receipts, contracts) | `sql` chain | `ai_parse_document()` → `ai_extract()` with typed schema |
| Classify document type | `sql` chain | `ai_parse_document()` → `ai_classify()` |
| Summarize / free-form Q&A over PDF | `sql` chain | `ai_parse_document()` → flatten to text → `ai_query()` |
| Extract tables from PDF (grid data) | `sql` | `ai_parse_document()` — tables come as HTML in elements |
| Specific pages only (large PDFs) | `sql` | `ai_parse_document(content, MAP('version','2.0','pageRange','1-3'))` |

> **CRITICAL — ai_extract v2.1 response structure (validated Sep 2026):**
> `ai_extract` wraps the result in an envelope: `{error_message, metadata: {version: "2.1"}, response: {...}}`.
> Your extracted fields live under `ex:response:field_name`, **NOT** `ex:field_name`.
> Each property is further wrapped as `{value: "actual_data"}`, so access is `ex:response:field_name:value`.
> For array fields, the FROM_JSON schema must use `STRUCT<value:STRING>` for each nested property, and the SELECT must unwrap via `.value`.
> **Always use snake_case aliases** in the final SELECT — column names with spaces cause `DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES` on Delta writes.

#### Pattern A: Structured form extraction (invoices, receipts, applications)

This is a 3-operator chain: Source (read binary) → SQL (parse + extract) → Transform (flatten VARIANT to columns).

```yaml
- id: src_pdf_files
  template: python
  name: src_pdf_files
  config:
    code: |
      result = spark.read.format("binaryFile").load("/Volumes/cat/raw/landing/invoices/")
  input: []

- id: parse_and_extract
  template: sql
  name: parse_and_extract
  config:
    query: |
      WITH parsed AS (
        SELECT
          path,
          ai_parse_document(content, MAP('version', '2.0')) AS parsed_content
        FROM src_pdf_files
      )
      SELECT
        path,
        ai_extract(
          parsed_content,
          '{
            "invoice_id": {"type": "string"},
            "vendor_name": {"type": "string", "description": "Legal business name"},
            "invoice_date": {"type": "string", "description": "Date in YYYY-MM-DD format"},
            "line_items": {
              "type": "array",
              "items": {
                "type": "object",
                "properties": {
                  "description": {"type": "string"},
                  "quantity": {"type": "integer"},
                  "unit_price": {"type": "number"}
                }
              }
            },
            "total_amount": {"type": "number"}
          }',
          MAP('version', '2.1', 'instructions', 'Extract all invoice fields and line items.')
        ) AS extracted
      FROM parsed
      WHERE is_variant_null(parsed_content:error_status)
  input:
    - node: src_pdf_files
      input_port: data
      output_port: result

- id: flatten_extracted
  template: transform
  name: flatten_extracted
  config:
    expressions:
      - "path"
      - "CAST(extracted:response:invoice_id:value AS STRING) AS invoice_id"
      - "CAST(extracted:response:vendor_name:value AS STRING) AS vendor_name"
      - "CAST(extracted:response:invoice_date:value AS DATE) AS invoice_date"
      - "CAST(extracted:response:total_amount:value AS DOUBLE) AS total_amount"
  input:
    - node: parse_and_extract
      input_port: data
      output_port: result
```

#### Pattern B: Explode line items from PDF (array fields)

```yaml
- id: explode_line_items
  template: sql
  name: explode_line_items
  config:
    query: |
      SELECT
        path,
        CAST(extracted:response:invoice_id:value AS STRING) AS invoice_id,
        li.description.value AS description,
        CAST(li.quantity.value AS INT) AS quantity,
        CAST(li.unit_price.value AS DOUBLE) AS unit_price,
        CAST(li.quantity.value AS INT) * CAST(li.unit_price.value AS DOUBLE) AS line_total
      FROM parse_and_extract
      LATERAL VIEW EXPLODE(
        FROM_JSON(
          CAST(extracted:response:line_items AS STRING),
          'ARRAY<STRUCT<description:STRUCT<value:STRING>, quantity:STRUCT<value:STRING>, unit_price:STRUCT<value:STRING>>>'
        )
      ) AS li
  input:
    - node: parse_and_extract
      input_port: data
      output_port: result
```

#### Pattern C: Document classification

```yaml
- id: classify_documents
  template: sql
  name: classify_documents
  config:
    query: |
      WITH parsed AS (
        SELECT
          path,
          ai_parse_document(content, MAP('version', '2.0')) AS parsed_content
        FROM src_pdf_files
      )
      SELECT
        path,
        ai_classify(
          parsed_content,
          '{"invoice": "Billing document with line items",
           "contract": "Legal agreement with terms",
           "receipt": "Proof of payment",
           "application": "Form with applicant details"}',
          MAP('version', '2.1')
        ) AS doc_type
      FROM parsed
      WHERE is_variant_null(parsed_content:error_status)
  input:
    - node: src_pdf_files
      input_port: data
      output_port: result
```

#### Pattern D: Specific pages only (large PDFs)

```yaml
- id: parse_first_pages
  template: sql
  name: parse_first_pages
  config:
    query: |
      SELECT
        path,
        ai_parse_document(content, MAP('version', '2.0', 'pageRange', '1-3')) AS parsed_content
      FROM src_pdf_files
  input:
    - node: src_pdf_files
      input_port: data
      output_port: result
```

**PDF Parse limitations:**
- **Cost**: `ai_parse_document` calls an AI model per page. Large PDFs (100+ pages) or large batches (1000+ files) incur significant cost — use `pageRange` to limit when possible.
- **Supported formats**: PDF, JPG, JPEG, PNG, TIFF, TIF, DOC, DOCX, PPT, PPTX.
- **Error handling**: Always filter with `WHERE is_variant_null(parsed_content:error_status)` — corrupted or unscannable pages produce errors in the VARIANT.
- **VARIANT type**: ai_parse_document and ai_extract return VARIANT. Designer preview fails with `UNSUPPORTED_OPERATION` if VARIANT reaches a downstream Transform or Join. **Always CAST VARIANT to concrete types (STRING, DOUBLE, DATE) inline in the same SQL operator** — never let VARIANT flow downstream.
- **Nested response envelope**: ai_extract v2.1 wraps results under `response`. Access `ex:response:field:value`, not `ex:field`. See Pattern A/B above.
- **Mixed date formats**: AI-extracted dates often arrive in inconsistent formats (MM/dd/yyyy, yyyy-M-d, dd-MMM-yyyy). Use `COALESCE(TRY_TO_TIMESTAMP(col, 'MM/dd/yyyy'), TRY_TO_TIMESTAMP(col, 'yyyy-M-d'), TRY_TO_TIMESTAMP(col, 'dd-MMM-yyyy'))` in a downstream Transform.
- **Handwritten text**: OCR quality varies; printed forms work well, handwritten text may be partial.
- **Tables in PDFs**: Extracted as HTML in the `elements` array (type = `table`). For complex multi-page tables, results may need manual cleanup.

#### Pattern E: PDF form filling (AcroForm — Python only, no visual operator)

Alteryx fills PDF forms via the Python tool with pypdf. VDP equivalent is a `python` operator. This is a valid Python use case (external library with no SQL equivalent).

```yaml
- id: fill_pdf_forms
  template: python
  name: fill_pdf_forms
  config:
    code: |
      import subprocess, sys
      subprocess.check_call([sys.executable, "-m", "pip", "install", "pypdf", "-q"])

      from pypdf import PdfReader, PdfWriter
      import os

      TEMPLATE = "/Volumes/catalog/schema/volume/template_form.pdf"
      OUTDIR   = "/Volumes/catalog/schema/volume/filled_forms"
      ON = "/Yes"   # AcroForm checkbox-on value

      df = inputs["data"][0]
      rows = df.collect()
      os.makedirs(OUTDIR, exist_ok=True)

      manifest = []
      for rec in rows:
          reader = PdfReader(TEMPLATE)
          writer = PdfWriter()
          writer.append(reader)
          writer.set_need_appearances_writer(True)

          # Map DataFrame columns → PDF form field names
          field_values = {
              "form_field_name": str(rec["column_name"]),
              # ... add all field mappings
          }
          # Checkboxes: set to ON ("/Yes") for the matching option
          # field_values["checkbox_field"] = ON

          slug = "".join(c if c.isalnum() else "_" for c in str(rec["id_col"])).strip("_")
          outp = os.path.join(OUTDIR, f"filled_{slug}.pdf")

          for pg in writer.pages:
              writer.update_page_form_field_values(pg, field_values, auto_regenerate=False)
          with open(outp, "wb") as o:
              writer.write(o)

          manifest.append((str(rec["id_col"]), outp, "filled"))

      from pyspark.sql.types import StructType, StructField, StringType
      schema = StructType([
          StructField("id", StringType()),
          StructField("filled_pdf_path", StringType()),
          StructField("status", StringType()),
      ])
      result = spark.createDataFrame(manifest, schema)
  input:
    - node: upstream_source
      input_port: data
      output_port: data
```

**Notes:**
- `pip install pypdf` is required — include in the code block.
- `set_need_appearances_writer(True)` ensures form fields render in viewers that don't re-render AcroForms.
- Checkboxes use `/Yes` (or `/Off`) — inspect the PDF field names with `PdfReader(path).get_fields()`.
- Output filled PDFs to a UC Volume path; produce a manifest DataFrame for downstream tracking.
- For the manifest output, use `output_type: file` (CSV) to the same Volume folder, NOT a Delta table — the filled PDFs are the primary deliverable.

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

Every converted workflow MUST end with an `output` operator. The output operator (v4.0.0) supports **table**, **materialized view**, and **file** output types with **overwrite**, **append**, and **merge** (table only) write modes:

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

### 9b. Additional output types (optional, downstream of or instead of 9a)

The `output` operator (v4.0.0) supports three output types and three write modes:

| Output Type | Config | Description |
|---|---|---|
| **Table** | `output_type: table` | Managed UC Delta table (default, canonical) |
| **Materialized View** | `output_type: materialized_view` | Publishes as an MV in UC; refreshes on each pipeline run |
| **File** | `output_type: file` | Writes CSV, Excel, or JSON to a UC Volume |

| Write Mode | Applies To | Description |
|---|---|---|
| **overwrite** | table, file | Replace existing contents (default) |
| **append** | table, file | Add new rows to existing data |
| **merge** | table only | Upsert by `merge_keys` — inserts new rows, updates existing matches |

**Preferred: Use `output` with `output_type: file`** for CSV/JSON/Excel file output:

```yaml
- id: write_csv_to_volume
  template: output
  templateVersion: 4.0.0
  name: write_csv_to_volume
  config:
    output_type: file
    catalog: cat
    schema: exports
    volume: orders_csv
    file_name: orders.csv
    file_type: csv
    write_mode: overwrite
  input:
    - node: last_transform
      input_port: data
      output_port: <last_operator_output_port>
```

#### Merge (upsert) example — table output only

```yaml
- id: output_upsert
  template: output
  templateVersion: 4.0.0
  name: output_upsert
  config:
    output_type: table
    catalog: main
    schema: silver
    table_name: customers
    write_mode: merge
    merge_keys:
      - customer_id
  input:
    - node: deduped_customers
      input_port: data
      output_port: unique_data
```

Fall back to `python` only for formats not supported by the native output operator. **Formats requiring Python for file output**: Parquet, Avro, ORC, XML, TSV (custom delimiter), fixed-width — the native output only supports CSV, Excel, and JSON as file types. When the original Alteryx workflow requires Python file writes:

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
| Excel write-back formatting | Charts/pivots don't auto-refresh | Existing charts remain in template but won't update until opened in Excel |
| Native Excel output is single-sheet only | Multi-sheet workbooks need Python | Use native `output` for simple single-sheet writes; fall back to `python` + `pd.ExcelWriter` for multi-sheet |
| Excel write-back large datasets | OOM on driver | `toPandas()` collects all data; for >1M rows use Parquet/CSV instead |
| PDF parse cost | AI model call per page | Use `pageRange` option to limit pages; batch large volumes off-peak |
| PDF handwritten text | Partial OCR | Printed forms work well; handwritten fields may be incomplete — flag **REVIEW** |
| Iterative joins on huge tables | OOM | Use broadcast hint in SQL: `/*+ BROADCAST(small) */` |
| Designer Filter is graphical, not free-form SQL | Cannot enter `REGEXP_LIKE`, `BETWEEN`, multi-AND-OR mixes directly | Fall back to `sql` operator |
| Designer Join has no cross-join | Append Fields cannot map to `join` | Use a `sql` operator with `CROSS JOIN` |
| Aggregate has no collect_list/set (array output) | Alteryx Summarize → Concat List doesn't fit | Use `sql` with `collect_list(col)` / `collect_set(col)`. Note: FIRST, LAST, CONCAT (string), and COUNT_DISTINCT are now natively supported by the Aggregate operator. |
| Combine requires matching schemas | Heterogeneous Alteryx Unions fail | Pre-align schemas with two `transform` operators before `combine` |
| YAML docstring colon in `description.text` breaks the cell | Downstream cells fail with `'<this>.<port>' data is missing or not created before use` because Designer never registers the broken cell in the dataflow graph | **Always quote** any free-text YAML scalar that may contain `:`, `#`, `{`, `}`, `[`, `]`, `,`, or leading/trailing whitespace. Concretely: emit `text: "Per-category metrics: avg / median / sum / count."` (double-quoted) rather than `text: Per-category metrics: avg / median / sum / count.` |
| `enter_data` crashes: `'DataFrame' object has no attribute 'map'` | The generated enter_data runtime uses `pdf.map()` (pandas ≥ 2.1.0 only) | Replace with `python` operator using `spark.createDataFrame(data, schema)`. Update downstream wiring: `output_port: data` → `output_port: result`. |
| ai_extract fields are NULL despite successful parse | ai_extract v2.1 nests data under `response` envelope | Use `ex:response:field:value` path, NOT `ex:field`. See PDF Parse Patterns §Pattern A. |
| VARIANT column breaks downstream Transform/Join | `UNSUPPORTED_OPERATION` on VARIANT type in Designer preview | CAST all VARIANT to STRING/DOUBLE/DATE inline in the same SQL operator |
| Join@1.0.0 `expressions: []` duplicates columns | `DLTAnalysisException: duplicate column name` or `DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES` on output | Always set explicit expressions: `["left.*", "right.needed_col"]`, excluding the right-side join key |
| Output v4.0.0 preview fails with TABLE_OR_VIEW_NOT_FOUND | Preview only does `SELECT * FROM target` — does NOT write | Expected on first run. The actual write happens on Run All. Same for file outputs. Do not "fix" this. |
| Mixed AI-extracted date formats | `TRY_TO_TIMESTAMP` returns NULL for non-matching formats | Use COALESCE with multiple format patterns in a Transform |

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
- [ ] Excel ingest uses native `source` operator when possible (full file, single sheet, Excel File Format Support enabled); Python only for sheet/range/named range or legacy formats
- [ ] JSON/CSV file outputs use native `output` operator (not Python)
- [ ] Excel write-back is a secondary sink AFTER Delta output (Step 9a) — never the only output
- [ ] PDF parse uses `ai_parse_document()` → `ai_extract()` chain with error filtering (`is_variant_null`)
- [ ] ai_extract fields accessed via `ex:response:field:value` path (v2.1 nested envelope)
- [ ] No VARIANT columns flow to downstream operators (all CAST inline in the SQL)
- [ ] All join operators have explicit `expressions` (no empty `[]` that duplicates keys)
- [ ] enter_data operator previews successfully (fallback to Python if pandas `.map()` fails)
- [ ] Source files placed in UC Volume (not Workspace `file:` paths)
- [ ] All column names are Delta-compatible (no spaces, no special chars)
- [ ] Type casts handle dirty data (filter or `TRY_CAST`)
- [ ] SQL operators reference simple display names (no spaces)
- [ ] Simple GROUP BY aggregations use visual `aggregate` operator (not `sql`); fixed-column Text To Columns use `transform` with `SPLIT` + `ELEMENT_AT`; reserve `sql` for multi-granularity UNION ALL, row explosion, window functions, or unsupported functions
- [ ] Mandatory 9-question visual-operator pre-check completed before every `sql`, `python`, or `ai_function` operator
- [ ] Each logical step has its own operator (no unnecessary CTE consolidation without user approval)
- [ ] AI functions used ONLY for creative/generative text on low-cardinality data (NOT for finite mappings)
- [ ] Python operators contain ONLY file I/O or ML code (no SOUNDEX, CASE WHEN, groupBy, datediff)
- [ ] Python operators assessed for UDO promotion (reusable logic → `uc-udf` / `uc-udtf` / `python-run-function`)
- [ ] Filter operators use v2.0.0 when both T/F branches are needed (filtered_data + excluded_data)
- [ ] Join operators use split_join when Alteryx L/J/R outputs are all needed (3 output ports)
- [ ] Aggregate operators use COUNT_DISTINCT, FIRST, LAST, CONCAT where applicable (no SQL needed)
- [ ] Reusable custom Python logic assessed for UDO promotion (`uc-udf` / `uc-udtf` / `python-run-function`) — adjacent `markdown` node added if a UDO is used
- [ ] User was asked before consolidating multiple SQL window nodes into one
- [ ] Deduplication uses visual `unique` operator (not SQL ROW_NUMBER) — reserve SQL only for multi-partition dedup or complex window logic
- [ ] Joins preserve L / J / R branches required downstream
- [ ] Macros — standard inlined or extracted; iterative flagged for Job
- [ ] Predictive / spatial / time-series — Python operator emitted, MLflow noted, marked **REVIEW**
- [ ] Reporting / interface — flagged **MANUAL** with Lakeview / Job / App pointer
- [ ] Output operator configured with `catalog.schema.table_name`
- [ ] Optional non-Delta sinks added downstream of the Delta output (Step 9b)
- [ ] Simple Excel/CSV/JSON file outputs use native `output` operator (not Python)
- [ ] Excel append scenarios use `output` with `write_mode: append` (not Python)
- [ ] Merge/upsert scenarios use `output` with `write_mode: merge` + `merge_keys` (not Python/SQL MERGE)
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

