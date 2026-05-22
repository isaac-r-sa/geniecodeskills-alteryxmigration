---
name: tableau-prep-to-ldp
description: "Converts Tableau Prep flows (.tflx / .tfl) into runnable Databricks Lakeflow Declarative Pipelines (LDP, also known as SDP / Spark Declarative Pipelines). Emits two interchangeable output flavors: pure SQL (CREATE OR REFRESH STREAMING TABLE / MATERIALIZED VIEW) and PySpark (@dp.table / @dp.materialized_view) — caller picks one. Bronze/silver/gold medallion layout, Asset Bundle scaffolding (databricks.yml + pipeline.yml), and a MANUAL_STEPS.md for everything that can't be auto-converted (SQL Server / Hyper / Tableau Server publishes, .hyper file outputs, Excel reads, SMB shares). Use when a user gives you a Tableau Prep flow and asks to migrate it to Databricks Lakeflow Declarative Pipelines, SDP, DLT, or 'Lakeflow pipelines'."
---

# Tableau Prep → Lakeflow Declarative Pipelines (LDP)

Converts a Tableau Prep flow (`.tflx`, `.tfl`) into a runnable **Databricks Lakeflow Declarative Pipeline** (LDP, a.k.a. SDP / Spark Declarative Pipelines, formerly DLT). The skill produces two output flavors and the caller picks one:

- **SQL** — `CREATE OR REFRESH STREAMING TABLE` / `CREATE OR REFRESH MATERIALIZED VIEW` files. Default. Best for analyst-readable pipelines.
- **PySpark** — `@dp.table` / `@dp.materialized_view` decorated functions. Best when transformations need Python (UDFs, branching, dynamic schemas, complex unpivots).

Each pipeline node = one DAG step in LDP. A companion `MANUAL_STEPS.md` lists Tableau Prep operations that need human action (SQL Server / Hyper sources, `.hyper` / Tableau Server outputs, Excel reads, SMB share paths, embedded extracts, custom R/Python scripts).

**Companion skills (authoritative for LDP patterns):**
- `databricks-spark-declarative-pipelines` — SQL & PySpark patterns, `read_files()`, `STREAM`, expectations, AUTO CDC, Unity Catalog.
- `databricks-asset-bundles` — multi-environment deployment.
- `alteryx-to-databricks-sdp` — sibling skill, same SDP target.

When in doubt about LDP syntax, defer to those skills.

---

## Critical Rules

1. **One flavor per pipeline.** Emit either all-SQL files or all-PySpark files — never mix in the same project. Reading mixed sources is supported by LDP but harder to maintain.
2. **`CREATE OR REFRESH`**, not `CREATE OR REPLACE`. SDP SQL requires it.
3. **`@dp.table` / `@dp.materialized_view`** for PySpark. Import from `pyspark import pipelines as dp`. Never use the older `@dlt.table` decorators in new pipelines; mention "Lakeflow Declarative Pipelines", not "DLT" or "Delta Live Tables".
4. **`CLUSTER BY`** (Liquid Clustering), never `PARTITION BY` or `ZORDER`.
5. **Unqualified table names between pipeline steps** — `FROM bronze_supplier`, not `catalog.schema.bronze_supplier`. Catalog/schema is set on the pipeline.
6. **Medallion layers**:
   - **bronze** — raw ingestion. Files via `read_files()`, federated SQL Server via `<catalog>.<schema>.<table>`, existing Delta tables passthrough.
   - **silver** — cleaning, joins, unions, computed columns, type casts.
   - **gold** — aggregates, rollups, final outputs consumed by dashboards / Genie.
7. **Bronze for files = `STREAMING TABLE` using `FROM STREAM read_files(...)`** with `_ingested_at`, `_metadata.file_path AS _source_file` lineage columns.
8. **Bronze for federated/Delta sources = `MATERIALIZED VIEW`** (no streaming semantics).
9. **Silver / Gold = `MATERIALIZED VIEW`** by default. Promote to `STREAMING TABLE` only when source is append-only and you want incremental processing.
10. **Promote Tableau Prep filters to expectations** where they encode data-quality intent: `CONSTRAINT <name> EXPECT (<expr>) ON VIOLATION DROP ROW`.
11. **Every unconvertible Tableau Prep operation** gets a `-- MANUAL STEP:` block (or `# MANUAL STEP:` in PySpark) **and** an entry in `MANUAL_STEPS.md`.
12. **Drop dead-end leaf "preview" branches** — Tableau Prep nodes whose only purpose was the in-flow data preview pane. Note them in `MANUAL_STEPS.md` so the user can confirm.
13. **Drop `.hyper` / Tableau Server publish nodes** entirely. Replace with downstream consumers reading the gold table; document in `MANUAL_STEPS.md`.

---

## Workflow

### Step 1 — Parse the flow file

Tableau Prep files come in two forms:

- **`.tfl`** — bare JSON.
- **`.tflx`** — zip archive. The flow definition lives in a top-level entry named `flow` (JSON). `Data/...` entries hold embedded extracts/CSVs.

```python
import json, zipfile
from pathlib import Path

def parse_flow(path):
    if path.suffix == ".tfl":
        return json.loads(path.read_text()), []
    with zipfile.ZipFile(path) as z:
        flow = json.loads(z.read("flow").decode())
        embedded = [(i.filename, z.read(i.filename))
                    for i in z.infolist() if i.filename.startswith("Data/")]
    return flow, embedded
```

Top-level keys you care about:

- `nodes` — dict keyed by node id. Each node has `nodeType`, `name`, `nextNodes` (list of `{nextNodeId}`), and a body that depends on `nodeType` (see [Node Mapping](#node-mapping)).
- `initialNodes` — entry points (sources).
- `connections` — sometimes used; `nextNodes` is canonical.

### Step 2 — Build the DAG

Walk forward from `initialNodes` via `nextNodes`. A Kahn topological sort gives deterministic ordering for emit. Identify:

- **Sources** (`.v1.LoadCsv`, `.v1.LoadExcel`, `.v1.LoadSql`, `.v1.LoadHyper`) → bronze.
- **Cleaning containers** (`.v1.Container` wrapping AddColumn/RenameColumn/RemoveColumns/ChangeColumnType/FilterOperation/Remap) → silver step (or split filter vs transform).
- **Joins/unions/pivots/unpivots/aggregates** → silver (joins/unions) or gold (aggregates).
- **Outputs** (`.v1.PublishExtract`, `.v1.WriteToHyper`, `.v1.WriteToCsv`) → drop, replace with the final gold MV. List in `MANUAL_STEPS.md`.

### Step 3 — Name tables

- `snake_case`. Strip non-alphanumerics. Tableau column names in `[brackets]` (e.g. `[Item Code]`) → backticked identifiers (`` `Item  Code` ``) at the SQL level, with friendly aliases (`itemCode`).
- Prefix `bronze_`, `silver_`, `gold_`.
- Use the node `name` if meaningful (Cleaning step "Supplier Clean" → `silver_supplier_clean`). Fall back to `<source>_<verb>` (e.g. `silver_forecast_enriched`).
- Tableau allows duplicate node names — de-dup with `_2`, `_3` suffixes.

### Step 4 — Emit pipeline files

Default Asset Bundle layout (compatible with `databricks bundle deploy`):

```
<pipeline_name>/                       # e.g. fy_capacity_gap
├── databricks.yml                     # Asset Bundle entrypoint
├── resources/
│   └── <pipeline_name>.pipeline.yml   # Pipeline config: catalog, schema, libraries
├── src/
│   ├── 01_bronze.<ext>                # ext = sql or py
│   ├── 02_silver.<ext>
│   └── 03_gold.<ext>
└── MANUAL_STEPS.md
```

For SQL flavor: `.sql` files, three of them. For PySpark flavor: `.py` files, three of them, each starting with `from pyspark import pipelines as dp`.

A flatter layout (no Asset Bundle) is acceptable when the user only wants the source files:

```
<pipeline_name>/
├── 01_bronze.<ext>
├── 02_silver.<ext>
├── 03_gold.<ext>
└── MANUAL_STEPS.md
```

Default to the Asset Bundle layout. Flatten only on explicit request.

### Step 5 — Emit `MANUAL_STEPS.md`

Cross-references each unconvertible item back to the file/line it appears in. Always includes:

- Pipeline config (catalog/schema/host).
- File migrations (Excel → CSV → UC Volume; embedded extracts → Volume).
- External connections (SQL Server → Lakehouse Federation foreign catalog; Hyper extracts → re-source from Delta).
- Output replacement (`.hyper` / Tableau Server / SMB share → gold MV + downstream consumer rewiring).
- Run checklist (`databricks bundle validate`, deploy, trigger, verify row counts).

### Step 6 — Comment every step

Top of each generated step references its Tableau Prep origin:

```sql
-- Converted from Tableau Prep:
--   nodeId 11      Cleaning step "Supplier Clean"
--     AddColumn supplierWithName = LPAD(STR([supplierNo]), 5, '0') + ' - ' + [supplier]
--     FilterOperation [divNo] >= 0
```

```python
# Converted from Tableau Prep:
#   nodeId 11      Cleaning step "Supplier Clean"
#     AddColumn supplierWithName = LPAD(STR([supplierNo]), 5, '0') + ' - ' + [supplier]
#     FilterOperation [divNo] >= 0
```

Reviewers should be able to map every LDP node back to the Prep flow.

---

## Node Mapping

### Source nodes → bronze

| Tableau `nodeType` | Notes | LDP target |
|---|---|---|
| `.v1.LoadCsv` | `connectionAttributes.filename` is the embedded CSV name. Embedded files live under `Data/` inside the `.tflx` zip. | **MANUAL STEP:** extract embedded file → upload to `/Volumes/<catalog>/<schema>/raw/<name>/`. Bronze = `STREAMING TABLE` via `read_files(..., format=>'csv')`. |
| `.v1.LoadExcel` | `.xlsx` / `.xls`. Tableau-Prep–embedded sheets must be exported. | **MANUAL STEP:** convert sheet → CSV → upload to Volume. Bronze = `STREAMING TABLE` via `read_files(..., format=>'csv')`. (LDP does not natively read Excel.) |
| `.v1.LoadSql` | Connection block (`connection`, `relation`) describes a SQL Server / Postgres / etc. table or query. No data is embedded. | **MANUAL STEP:** set up Lakehouse Federation foreign catalog. Bronze = `MATERIALIZED VIEW AS SELECT ... FROM <foreign_catalog>.<schema>.<table>`. |
| `.v1.LoadHyper` | `.hyper` extract reference. | **MANUAL STEP:** re-source from the upstream Delta table or re-export from the SQL backend; `.hyper` is not directly readable from LDP. |

**Bronze SQL template (file source):**

```sql
-- Converted from Tableau Prep nodeId=<id>, type=LoadCsv
-- Original: <connectionAttributes.filename>
-- ACTION: upload the source file to /Volumes/${catalog}/${schema}/raw/<name>/ (see MANUAL_STEPS.md)
CREATE OR REFRESH STREAMING TABLE bronze_<name>
CLUSTER BY (<first_id_col>)
COMMENT '<source description>'
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/raw/<name>/',
  format       => 'csv',
  header       => true,
  schemaHints  => '<col1> <type>, <col2> <type>, ...'
);
```

**Bronze PySpark template (file source):**

```python
# Converted from Tableau Prep nodeId=<id>, type=LoadCsv
# Original: <connectionAttributes.filename>
# ACTION: upload the source file to /Volumes/<catalog>/<schema>/raw/<name>/ (see MANUAL_STEPS.md)
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

@dp.table(
    name="bronze_<name>",
    cluster_by=["<first_id_col>"],
    comment="<source description>",
)
def bronze_<name>():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaHints",
                    "<col1> <type>, <col2> <type>, ...")
            .load("/Volumes/<catalog>/<schema>/raw/<name>/")
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
    )
```

**Bronze SQL template (federated SQL source):**

```sql
CREATE OR REFRESH MATERIALIZED VIEW bronze_<name>
COMMENT 'Federated read from <foreign_catalog>.<schema>.<table>'
AS
SELECT
  CAST(<col1> AS <type>) AS <alias1>,
  CAST(<col2> AS <type>) AS <alias2>
FROM <foreign_catalog>.<schema>.<table>
WHERE <filter from Tableau connection.condition>
;
```

**Bronze PySpark template (federated SQL source):**

```python
@dp.materialized_view(
    name="bronze_<name>",
    comment="Federated read from <foreign_catalog>.<schema>.<table>",
)
def bronze_<name>():
    return (
        spark.read.table("<foreign_catalog>.<schema>.<table>")
            .selectExpr(
                "CAST(<col1> AS <type>) AS <alias1>",
                "CAST(<col2> AS <type>) AS <alias2>",
            )
            .where("<filter from Tableau connection.condition>")
    )
```

### Cleaning containers (`.v1.Container`) → silver

A Tableau Prep "Cleaning step" wraps an inner DAG of micro-actions in `loomContainer.nodes`. Walk those actions and combine into one `MATERIALIZED VIEW`:

| Inner `nodeType` | What it does | SQL | PySpark |
|---|---|---|---|
| `.v1.AddColumn` | new column from formula | `<expr> AS <col>` in `SELECT` | `.withColumn(col, expr(<expr>))` |
| `.v1.RenameColumn` | rename | `<old> AS <new>` | `.withColumnRenamed(old, new)` |
| `.v1.RemoveColumns` | drop | omit from `SELECT` | `.drop(*cols)` |
| `.v1.ChangeColumnType` | type cast | `CAST(<col> AS <type>) AS <col>` | `.withColumn(col, col.cast(type))` |
| `.v2018_3_3.Remap` | value remapping | `CASE WHEN <col>=<old> THEN <new> ... END AS <col>` | `.withColumn(col, F.when(...).when(...).otherwise(col))` |
| `.v1.FilterOperation` | calculated filter | `WHERE <expr>` | `.where(<expr>)` |
| `.v1.ValueFilter` | keep/exclude values | `WHERE <col> [NOT] IN (...)` | `.where(F.col(c).isin(vals))` |
| `.v1.CalculatedFilter` | expression filter | `WHERE <expr>` | `.where(<expr>)` |
| `.v1.RangeFilter` | bounded range | `WHERE <col> BETWEEN <min> AND <max>` | `.where((col(c)>=min) & (col(c)<=max))` |

**Container classification heuristic** (when emitting one step per container):

- Only filters → emit a filter-only step (often promotes well to LDP expectations).
- Only computes (Add/Rename/Remove/Cast/Remap) → emit a transform step.
- Mixed → emit one step that does both (CTE → filter → projection).

**Filters that encode data-quality intent should be promoted to expectations**:

```sql
CREATE OR REFRESH MATERIALIZED VIEW silver_<name>
(
  CONSTRAINT valid_product_code  EXPECT (productCode IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT non_alcohol         EXPECT (cgComplete NOT IN ('02 - Sparkling wine','03 - Wine','04 - Beer'))
)
AS
SELECT ...
```

```python
@dp.materialized_view(name="silver_<name>")
@dp.expect_or_drop("valid_product_code", "productCode IS NOT NULL")
@dp.expect("non_alcohol", "cgComplete NOT IN ('02 - Sparkling wine','03 - Wine','04 - Beer')")
def silver_<name>():
    ...
```

### Join (`.v2018_2_3.SuperJoin`) → silver

| Tableau `joinType` | LDP join |
|---|---|
| `inner` | `INNER JOIN` |
| `left` | `LEFT JOIN` |
| `right` | `RIGHT JOIN` |
| `full` | `FULL OUTER JOIN` |
| `leftOnly` | `LEFT ANTI JOIN` |
| `rightOnly` | `RIGHT ANTI JOIN` (or swap inputs and use `LEFT ANTI`) |

`actionNode.conditions` is a list of `{leftExpression, rightExpression, comparator}`. Build the ON clause:

```sql
CREATE OR REFRESH MATERIALIZED VIEW silver_<name>
AS
SELECT l.*, r.<extra_cols>
FROM <left_upstream>  l
LEFT JOIN <right_upstream> r
  ON l.<lcol> = r.<rcol>
 AND l.<lcol2> = r.<rcol2>
;
```

```python
@dp.materialized_view(name="silver_<name>")
def silver_<name>():
    l = spark.read.table("<left_upstream>")
    r = spark.read.table("<right_upstream>")
    return l.join(r, [l.<lcol> == r.<rcol>, l.<lcol2> == r.<rcol2>], "left")
```

### Union (`.v2018_2_3.SuperUnion`) → silver

Tableau Prep unions are by-position by default. Match column lists explicitly when types/names differ. In LDP:

```sql
WITH a_aligned AS (SELECT CAST(... AS ...) AS ..., ... FROM <upstream_a>),
     b_aligned AS (SELECT CAST(... AS ...) AS ..., ... FROM <upstream_b>)
SELECT * FROM a_aligned
UNION ALL
SELECT * FROM b_aligned
```

```python
a = spark.read.table("<upstream_a>").selectExpr(...)
b = spark.read.table("<upstream_b>").selectExpr(...)
return a.unionByName(b)   # or a.union(b) for by-position
```

### Aggregate (`.v2018_2_3.SuperAggregate`) → gold

`actionNode.groupByFields` and `actionNode.aggregateFields`:

| Tableau `function` | LDP function |
|---|---|
| `GroupBy` | (column appears in `GROUP BY`, no agg) |
| `SUM` | `SUM(col)` / `F.sum(col)` |
| `AVG` | `AVG(col)` / `F.avg(col)` |
| `COUNT` | `COUNT(col)` / `F.count(col)` |
| `CountDistinct` | `COUNT(DISTINCT col)` / `F.countDistinct(col)` |
| `MIN` / `MAX` | `MIN(col)` / `MAX(col)` |
| `MEDIAN` | `MEDIAN(col)` (Databricks SQL builtin) / `F.expr("median(col)")` |
| `STDEV` | `STDDEV(col)` / `F.stddev(col)` |

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold_<name>
CLUSTER BY (<group_by_first_col>)
AS
SELECT <group_by_cols>, SUM(<col>) AS <alias>, AVG(<col2>) AS <alias2>
FROM <upstream>
GROUP BY <group_by_cols>
;
```

### Pivot (`.v2018_3_3.SuperPivot`) → silver/gold

Tableau Prep pivots are rows-to-columns aggregations. LDP supports `PIVOT` in plain SQL but **not directly inside an LDP `MATERIALIZED VIEW` query** — wrap the pivot in a CTE/subquery, or rewrite as `SUM(CASE WHEN ... THEN ... END)`:

```sql
CREATE OR REFRESH MATERIALIZED VIEW silver_team_pivot
AS
WITH src AS (
  SELECT <group_by_cols>, <pivot_col>, <value_col> FROM <upstream>
)
SELECT * FROM src
PIVOT (SUM(<value_col>) FOR <pivot_col> IN ('SST', 'VCO'))
;
```

```python
@dp.materialized_view(name="silver_team_pivot")
def silver_team_pivot():
    return (
        spark.read.table("<upstream>")
            .groupBy(*group_by_cols)
            .pivot("<pivot_col>", ["SST", "VCO"])
            .agg(F.sum("<value_col>"))
    )
```

### Unpivot (`.v2018_3_4.SuperUnpivotExtended`) → silver

Tableau Prep's "Pivot columns to rows" maps multiple column groups onto labeled values. Use SQL `UNPIVOT` (works inside CTEs in LDP MVs):

```sql
WITH joined AS (
  SELECT <key_cols>, capacity2027, capacity2028, ..., fcYear1, fcYear2, ...
  FROM <upstream>
)
SELECT <key_cols>, capacity, forecast, year
FROM joined
UNPIVOT (
  (capacity, forecast)
  FOR year IN (
    (capacity2027, fcYear1) AS '2027',
    (capacity2028, fcYear2) AS '2028',
    (capacity2029, fcYear3) AS '2029',
    (capacity2030, fcYear4) AS '2030',
    (capacity2031, fcYear5) AS '2031'
  )
)
```

PySpark equivalent (use `stack` since `unpivot` accepts only single value columns):

```python
@dp.materialized_view(name="silver_capacity_gap_long")
def silver_capacity_gap_long():
    df = spark.read.table("<upstream>")
    return df.selectExpr(
        "<key_cols>",
        "stack(5, "
        "  '2027', capacity2027, fcYear1,"
        "  '2028', capacity2028, fcYear2,"
        "  '2029', capacity2029, fcYear3,"
        "  '2030', capacity2030, fcYear4,"
        "  '2031', capacity2031, fcYear5"
        ") AS (year, capacity, forecast)"
    )
```

### Outputs (`.v1.PublishExtract`, `.v1.WriteToHyper`, `.v1.WriteToCsv`)

Drop these node types entirely. The final gold `MATERIALIZED VIEW` is the new contract — downstream consumers (Tableau, Excel, dashboards) repoint to it via Delta or Partner Connect / Delta Sharing.

Always emit a `MANUAL_STEPS.md` entry per output node listing:
- The original target path / Tableau Server site & project.
- The replacement gold table name.
- The action: rewire the consumer.

---

## Tableau Formula → Spark SQL

Tableau Prep formulas (calc expressions, filter expressions) need translation:

| Tableau | Spark SQL |
|---|---|
| `[col]` | `` `col` `` (backtick when name has special chars) |
| `IIF(c, t, f)` | `IF(c, t, f)` or `CASE WHEN c THEN t ELSE f END` |
| `IF c THEN t ELSEIF c2 THEN t2 ELSE f END` | `CASE WHEN c THEN t WHEN c2 THEN t2 ELSE f END` (replace `ELSEIF` with `WHEN`) |
| `IFNULL(x, y)` | `COALESCE(x, y)` |
| `ISNULL([x])` | `x IS NULL` |
| `STR(x)` | `CAST(x AS STRING)` |
| `INT(x)` | `CAST(x AS INT)` |
| `FLOAT(x)` | `CAST(x AS DOUBLE)` |
| `DATE(x)` | `CAST(x AS DATE)` |
| `DATETIME(x)` | `CAST(x AS TIMESTAMP)` |
| `TRIM([x])` / `LTRIM` / `RTRIM` | `TRIM` / `LTRIM` / `RTRIM` |
| `LEFT/RIGHT/MID` | `LEFT` / `RIGHT` / `SUBSTRING` |
| `UPPER` / `LOWER` | same |
| `LEN([x])` | `LENGTH(x)` |
| `CONTAINS([x], 's')` | `contains(x, 's')` or `x LIKE '%s%'` |
| `STARTSWITH` / `ENDSWITH` | `startswith` / `endswith` |
| `REPLACE([x], a, b)` | `REPLACE(x, a, b)` |
| `REGEXP_REPLACE([x], p, r)` | `regexp_replace(x, p, r)` |
| `REGEXP_MATCH([x], p)` | `x RLIKE p` |
| `SPLIT([x], sep, n)` | `split(x, sep)[n-1]` |
| `DATEPARSE(fmt, [x])` | `to_date(x, fmt)` / `to_timestamp(x, fmt)` |
| `DATEPART('year', [x])` | `year(x)` (similarly `month`, `day`, `hour`, …) |
| `DATEADD('day', n, [x])` | `date_add(x, n)` / `x + INTERVAL n DAY` |
| `DATEDIFF('day', a, b)` | `datediff(b, a)` |
| `TODAY()` / `NOW()` | `current_date()` / `current_timestamp()` |
| `MIN(a, b)` / `MAX(a, b)` | `LEAST(a, b)` / `GREATEST(a, b)` |
| `ABS` / `ROUND` / `CEILING` / `FLOOR` | `ABS` / `ROUND` / `CEIL` / `FLOOR` |
| `ZN([x])` | `COALESCE(x, 0)` |

For the PySpark flavor, prefer `F.expr("<spark sql expression>")` for anything more complex than a single function call — the SQL translation table above is reusable inside `expr()`.

### Date format mapping

Tableau uses the same Java SimpleDateFormat tokens Spark uses, with one subtlety:

| Tableau | Spark |
|---|---|
| `yyyy` | `yyyy` |
| `MM` (month) | `MM` |
| `dd` | `dd` |
| `HH` (24h) | `HH` |
| `hh` (12h) | `hh` (Spark accepts both; prefer `HH`) |
| `mm` (minute) | `mm` |
| `ss` | `ss` |
| `'literal'` (quoted) | same |

---

## Tableau column-name handling

Tableau preserves spaces and punctuation in column names: `Item  Code`, `CG No`, `Test Name`. Two strategies:

1. **Preserve at bronze, rename at silver.** Bronze `read_files()` reads with `header=>true` and the names land verbatim. Silver renames them to camelCase or snake_case in the first projection.

   ```sql
   CREATE OR REFRESH MATERIALIZED VIEW silver_apt_in_test AS
   SELECT
     CAST(`Legacy Code` AS INT) AS legacyCode,
     `Test Name`                AS testName,
     Status                     AS status
   FROM bronze_apt
   ;
   ```

2. **Rename in `schemaHints` at bronze.** Use this only when the source name is invalid even with backticks (rare).

Always use backticks (`` ` ``) around bracketed names — never double-quotes.

---

## SQL flavor — file templates

### `databricks.yml`

```yaml
bundle:
  name: <pipeline_name>

include:
  - resources/*.pipeline.yml

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: <workspace_url>
    variables:
      catalog: <catalog>
      schema:  <schema>

  prod:
    mode: production
    workspace:
      host: <workspace_url>
      root_path: /Workspace/Shared/.bundle/${bundle.target}/${bundle.name}
    variables:
      catalog: <catalog>
      schema:  <schema>

variables:
  catalog: { description: UC catalog,           default: <catalog> }
  schema:  { description: UC schema,            default: <schema>  }
```

### `resources/<pipeline_name>.pipeline.yml`

```yaml
resources:
  pipelines:
    <pipeline_name>_etl:
      name: "[${bundle.target}] <Pipeline Name>"
      catalog: ${var.catalog}
      schema:  ${var.schema}
      serverless: true
      development: ${bundle.target == "dev"}
      continuous: false
      photon: true

      libraries:
        - file: { path: ../src/01_bronze.sql }
        - file: { path: ../src/02_silver.sql }
        - file: { path: ../src/03_gold.sql }

      configuration:
        catalog: ${var.catalog}
        schema:  ${var.schema}
```

### `src/01_bronze.sql`

Mix of `STREAMING TABLE` (file ingestion) and `MATERIALIZED VIEW` (federated / passthrough). See [Source nodes](#source-nodes--bronze) for full templates. One `CREATE OR REFRESH` per source.

### `src/02_silver.sql`

One `CREATE OR REFRESH MATERIALIZED VIEW` per cleaning container / join / union, in topological order. Promote DQ-intent filters to `EXPECT` constraints.

### `src/03_gold.sql`

Aggregates (per buyingDirector × year, per supplier × year, per category × year, etc.) plus the headline fact MV. Each is a `MATERIALIZED VIEW`.

---

## PySpark flavor — file templates

### `resources/<pipeline_name>.pipeline.yml`

Same as SQL flavor but `libraries` points to `.py`:

```yaml
      libraries:
        - file: { path: ../src/01_bronze.py }
        - file: { path: ../src/02_silver.py }
        - file: { path: ../src/03_gold.py }
```

### `src/01_bronze.py`

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
SCHEMA  = spark.conf.get("schema")
RAW     = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

# ─── Source: <Tableau node "FY Forecast"> ─────────────────────────────────
@dp.table(
    name="bronze_fyforecast",
    cluster_by=["productCode", "supplierNo"],
    comment="Multi-year forecasted cases per product/supplier/division",
)
def bronze_fyforecast():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaHints",
                    "divNo INT, mainCode INT, productCode INT, "
                    "supplierNo INT, fcYear1 DOUBLE, fcYear2 DOUBLE, "
                    "fcYear3 DOUBLE, fcYear4 DOUBLE, fcYear5 DOUBLE, "
                    "fcYear6 DOUBLE, lySales BIGINT")
            .load(f"{RAW}/fy_forecast/")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
    )


# ─── Source: <federated SQL Server "Division"> ────────────────────────────
@dp.materialized_view(
    name="bronze_division",
    comment="Division dimension — federated read from CBIS499p.dbo.DIVISION",
)
def bronze_division():
    return (
        spark.read.table("cbis499p.dbo.division")
            .where("CountryID = 'U' AND DivisionTyp = 1 AND DivNo NOT IN (445)")
            .selectExpr(
                "CAST(DivNo AS INT) AS divNo",
                "DivID AS divId",
                "SortID AS sortByDivOrder",
            )
    )
```

### `src/02_silver.py`

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ─── Cleaning container: <Tableau node "Supplier Clean"> ──────────────────
@dp.materialized_view(
    name="silver_forecast_enriched",
    cluster_by=["productCode", "supplierNo"],
    comment="Forecast joined with division/contract/product/supplier "
            "with regional supplier overrides and computed columns",
)
@dp.expect_or_drop("valid_product_code",  "productCode IS NOT NULL")
@dp.expect_or_drop("valid_supplier_code", "supplierNo  IS NOT NULL")
def silver_forecast_enriched():
    fy = spark.read.table("bronze_fyforecast")
    c  = spark.read.table("bronze_contract")
    d  = spark.read.table("bronze_division")
    p  = spark.read.table("bronze_product")
    s  = spark.read.table("bronze_supplier")

    return (
        fy.alias("fy")
          .join(c.alias("c"),
                ["productCode", "supplierNo"], "left")
          .join(d.alias("d"), F.col("fy.divNo") == F.col("d.divNo"), "left")
          .join(p.alias("p"), F.col("fy.productCode") == F.col("p.productCode"), "left")
          .join(s.alias("s"), F.col("fy.supplierNo") == F.col("s.supplierNo"), "left")
          .withColumn(
              "supplier",
              F.when(F.col("fy.divNo") == 997, F.lit("West Region"))
               .when(F.col("fy.divNo") == 998, F.lit("Southeast Region"))
               .when(F.col("fy.divNo") == 999, F.lit("Northeast Region"))
               .otherwise(F.col("s.supplier")),
          )
          .withColumn(
              "fiveYearTotal",
              sum(F.coalesce(F.col(f"fy.fcYear{i}"), F.lit(0)) for i in range(2, 7)),
          )
          .selectExpr("fy.*", "c.casesPerPallet", "d.divId",
                      "p.description", "p.CGNo", "p.cgComplete",
                      "supplier", "fiveYearTotal", ...)
    )
```

### `src/03_gold.py`

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="gold_buyer_summary",
    cluster_by=["year", "buyingDirector"],
    comment="Per buyingDirector × year: shortfall counts + total gap",
)
def gold_buyer_summary():
    return (
        spark.read.table("gold_capacity_gap")
            .groupBy("buyingDirector", "groupBuyingDirector", "year")
            .agg(
                F.count("*").alias("items"),
                F.count_if(F.col("gapStatus") == "shortfall").alias("shortfall_items"),
                F.sum("capacityGap").alias("net_gap_cases"),
                F.sum(F.greatest(F.col("forecast") - F.col("capacity"), F.lit(0)))
                    .alias("shortfall_cases"),
                F.sum(F.col("forecast") * F.col("retail")).alias("forecast_revenue"),
            )
    )
```

---

## Manual-step block templates

### SQL

```sql
-- ============================================================
-- MANUAL STEP — <Short title>
-- Tableau Prep nodeId: <id>   nodeType: <type>
-- Context:   <what the Prep node did>
-- Action:    <what the user must do before the pipeline can run>
-- See MANUAL_STEPS.md §<anchor>.
-- ============================================================
```

### PySpark

```python
# ============================================================
# MANUAL STEP — <Short title>
# Tableau Prep nodeId: <id>   nodeType: <type>
# Context:   <what the Prep node did>
# Action:    <what the user must do before the pipeline can run>
# See MANUAL_STEPS.md §<anchor>.
# ============================================================
```

Canonical manual-step categories:

- **Embedded extract / Excel migration** — extract `Data/<file>` from the `.tflx` zip, convert `.xlsx` → `.csv` if needed, upload to `/Volumes/<catalog>/<schema>/raw/<name>/`.
- **SQL Server / Postgres / Oracle source** — set up Lakehouse Federation foreign catalog or Lakeflow Connect ingestion.
- **`.hyper` source / sink** — `.hyper` is a Tableau-only format. Re-source from the upstream Delta or operational DB; replace `.hyper` outputs with the gold MV (Tableau reads via Databricks connector).
- **Tableau Server publish** — replace with Partner Connect or Delta Sharing; document the target site/project so the consumer can repoint.
- **SMB / UNC network paths** — drop the literal path; consumer reads the gold table from UC.
- **Custom R / Python script step** — Prep `Insert Script` nodes need full re-implementation. Surface the script as a comment block; ask the user to port to PySpark or a UDF.
- **Embedded TableauPrep extract refresh schedule** — replace with the LDP pipeline trigger / job schedule.

---

## `MANUAL_STEPS.md` template

```markdown
# Manual steps — Tableau Prep → Lakeflow Declarative Pipelines

**Source flow:** <flow_name>.tflx
**Generated on:** <date>
**Target pipeline:** <pipeline_name>
**Output flavor:** SQL | PySpark

## 1 · Pipeline config
- [ ] Pick Unity Catalog target: `catalog = <...>`, `schema = <...>`
- [ ] Confirm workspace host in `databricks.yml`
- [ ] (Optional) Replace inline `read_files` schema hints with autoloader inference

## 2 · File migration
For every `.v1.LoadCsv` / `.v1.LoadExcel` source, upload the source to a UC Volume:
- [ ] `<original filename>` → `/Volumes/<catalog>/<schema>/raw/<name>/`
- [ ] `<embedded extract Data/...>` → extract from `.tflx` zip → upload

## 3 · External SQL connections
For every `.v1.LoadSql` source, set up Lakehouse Federation:
- [ ] Connection `<server>/<database>` → foreign catalog `<foreign_catalog>`
- [ ] Verify the bronze MV reads `<foreign_catalog>.<schema>.<table>` and returns expected row counts.

## 4 · Output replacement
- [ ] Drop `.hyper` output: `<original path>` → consumers read `gold_<name>` instead
- [ ] Drop Tableau Server publish: `<site>/<project>/<datasource>` → repoint Tableau workbook to the Databricks connector pointing at `gold_<name>`
- [ ] Drop CSV / SMB output: `<unc path>` → repoint downstream consumer

## 5 · Data quality
- [ ] Review converted `WHERE` clauses — promote DQ-intent filters to `EXPECT` constraints.
- [ ] Add row-count expectations on bronze if the source had row-level reasonableness checks.

## 6 · Disabled / preview-only branches
- [ ] <list any Prep nodes whose only purpose was the in-flow preview pane — left as comments>

## 7 · Run checklist
- [ ] `databricks bundle validate -t dev`
- [ ] `databricks bundle deploy -t dev`
- [ ] Trigger the pipeline; confirm it goes green
- [ ] Bronze row counts match source files
- [ ] Gold row counts match Tableau Prep extract row counts (within tolerance for type coercions)
- [ ] Repoint Tableau workbooks / dashboards to the new gold tables
```

---

## Pipeline run checklist (for users)

1. Pick catalog + schema (e.g. `aldi_aus.demo`).
2. Create the UC Volume `/Volumes/<catalog>/<schema>/raw/` and upload all source files listed in `MANUAL_STEPS.md §2`.
3. Set up foreign catalogs for any `.v1.LoadSql` sources (`MANUAL_STEPS.md §3`).
4. `databricks bundle validate -t dev` from the pipeline folder.
5. `databricks bundle deploy -t dev`.
6. Open Workflows → Pipelines → `[dev] <Pipeline Name>` → **Start**.
7. Each `CREATE OR REFRESH` (or `@dp.table` / `@dp.materialized_view`) appears as a node in the LDP graph.
8. Verify bronze row counts vs source files; verify gold row counts vs the legacy Tableau Prep extract.
9. Repoint Tableau / consumer workbooks to the gold tables.

---

## Choosing flavor

| Situation | Pick |
|---|---|
| Pipeline is mostly source → clean → join → aggregate, all expressible as SQL | **SQL** |
| Tableau Prep flow uses Insert Script (Python/R), custom UDFs, or row-by-row procedural logic | **PySpark** |
| Pipeline uses unpivots that span > 5–10 column groups, dynamic schemas, or complex `Remap` rules | **PySpark** (easier to express programmatically) |
| Analyst team owns ongoing maintenance, wants SQL Editor authoring | **SQL** |
| Data engineering team owns ongoing maintenance, wants tests / type checking / shared helpers | **PySpark** |
| User asks for both | Emit SQL first; offer PySpark as a follow-up if they want to extend with Python |

**Default = SQL.** It matches the existing `fy_capacity` reference output and is what most Tableau Prep migrations need.

---

## Related skills

- **[databricks-spark-declarative-pipelines](../databricks-spark-declarative-pipelines/SKILL.md)** — authoritative LDP/SDP patterns (ingestion, streaming, expectations, AUTO CDC, performance, both SQL and PySpark).
- **[databricks-asset-bundles](../databricks-asset-bundles/SKILL.md)** — multi-environment deployment.
- **[databricks-unity-catalog](../databricks-unity-catalog/SKILL.md)** — Volume creation, foreign catalog setup, permissions.
- **[alteryx-to-databricks-sdp](../alteryx-to-databricks-sdp/SKILL.md)** — sibling skill, same SDP target from Alteryx `.yxmd` workflows.
- **[alteryx-to-lakeflow-designer](../alteryx-to-lakeflow-designer/SKILL.md)** — sibling skill, targets the Lakeflow *Designer* visual editor instead of declarative pipelines.
