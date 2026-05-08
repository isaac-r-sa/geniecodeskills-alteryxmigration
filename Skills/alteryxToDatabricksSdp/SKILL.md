---
name: alteryx-to-databricks-sdp
description: "Converts Alteryx workflows (.yxmd / .yxmc XML files) into runnable Databricks Lakeflow Spark Declarative Pipelines (SDP) as pure SQL. Produces bronze/silver/gold .sql files plus a MANUAL_STEPS.md for anything that cannot be auto-converted (file uploads, SAP/ODBC sources, email, Tableau, reporting). Use when a user provides an Alteryx .yxmd file and asks to convert it to a Databricks SDP / Lakeflow pipeline, DLT pipeline, or Lakeflow Declarative Pipeline."
---

# Alteryx → Databricks SDP (SQL) Conversion

Converts Alteryx Designer workflows (`.yxmd`, `.yxmc`) into a runnable **Databricks Lakeflow Spark Declarative Pipeline (SDP)** expressed in **pure SQL**. Each output `.sql` file contains one or more `CREATE OR REFRESH STREAMING TABLE` / `CREATE OR REFRESH MATERIALIZED VIEW` statements — each becomes a node in the SDP DAG. A companion `MANUAL_STEPS.md` lists the Alteryx tools that need human action (file migration, SAP/ODBC, Tableau, Email, Reports, Spatial, etc.).

**Companion skill (authoritative for SDP patterns):** `databricks-spark-declarative-pipelines`. Always follow its rules for SQL syntax, `CREATE OR REFRESH`, `CLUSTER BY`, `read_files()`, `STREAM`, Unity Catalog, and Asset Bundle layout. When in doubt, defer to that skill.

---

## Critical Rules

1. **SQL only.** Output `.sql` files, no Python, no notebooks. This pipeline must run as a Lakeflow SDP.
2. **`CREATE OR REFRESH`** — never `CREATE OR REPLACE` (SDP SQL requires `CREATE OR REFRESH STREAMING TABLE` / `CREATE OR REFRESH MATERIALIZED VIEW`).
3. **Lakeflow naming** — call it SDP / Lakeflow Declarative Pipelines. Never "DLT" or "Delta Live Tables" in output or comments.
4. **`CLUSTER BY`** (Liquid Clustering), never `PARTITION BY` or `ZORDER`.
5. **Unqualified table names** between pipeline steps (e.g. `FROM bronze_students_1`, not `FROM catalog.schema.bronze_students_1`) — target catalog/schema is set at the pipeline level.
6. **Medallion layers** — bronze (raw ingestion), silver (clean + validate + join), gold (aggregate / business outputs).
7. **Bronze = `STREAMING TABLE` using `FROM STREAM read_files(...)`** for file ingestion.
8. **Silver / Gold = `MATERIALIZED VIEW`** by default; use `STREAMING TABLE` with `STREAM()` only if source is streaming and append-only.
9. **Add lineage columns in bronze**: `current_timestamp() AS _ingested_at`, `_metadata.file_path AS _source_file`.
10. **Every unconvertible Alteryx tool** gets a `-- MANUAL STEP:` block in the SQL **and** an entry in `MANUAL_STEPS.md`.
11. **Remove `Browse` tools** — Alteryx debugging, no production equivalent.
12. **Remove `BlockUntilDone`** — SDP handles DAG ordering automatically.
13. **Control Containers are layout, not logic** — their execution order comes from data dependencies in SDP; don't try to model them.

---

## Workflow

### Step 1 — Parse the `.yxmd` XML

An Alteryx workflow is XML. Extract:

- `<Node ToolID="...">` — each tool. Key sub-elements:
  - `GuiSettings/@Plugin` — tool type (see [Tool Mapping](#tool-mapping))
  - `Properties/Configuration` — tool-specific settings (file path, SQL, formula, filter expression, join keys, summarize actions)
  - `Properties/Annotation/AnnotationText` / `DefaultAnnotationText` — user label, use for naming
  - `EngineSettings/@Macro` — if set, this is a macro (e.g. `Cleanse.yxmc`, `SelectRecords.yxmc`) — see [Macro Handling](#macro-handling)
  - Nested `<ChildNodes>` under `ToolContainer` / `ControlContainer` — these are groupings
- `<Connection>` — data flow edges: `Origin ToolID → Destination ToolID`, with `Connection` attribute (`Output`, `Input`, `True`, `False`, `Left`, `Right`, `Join`)
- `ToolContainer` with a `<Caption>` — logical section (often a medallion layer)
- `Control` connections (`Origin Connection="Log"` → `Destination Connection="Control"`) — these sequence Control Containers in Alteryx. **Ignore for SDP** — the data DAG is what matters.

### Step 2 — Build the DAG

1. Walk connections forward from each input to outputs.
2. Identify:
   - **Sources** — `DbFileInput` nodes → bronze streaming tables.
   - **Transformations** — Filter / Formula / Select / Sort / Sample / RegEx / Unique / Cleanse macro → silver materialized views.
   - **Joins / Unions** — combine multiple streams → silver materialized view.
   - **Aggregations** (`Summarize`, `CrossTab`, `Transpose`) → gold materialized views.
   - **Outputs** (`DbFileOutput`, `Email`, `TableauOutput`, `BrowseV2`) → gold materialized view (for file/DB out) or MANUAL STEP (for Email/Tableau/Browse).
3. Prefer **one `CREATE OR REFRESH` per logical step** rather than stuffing everything into one query. SDP renders each as a node; downstream steps reference upstream by table name.

### Step 3 — Name tables

- `snake_case`, no spaces, no slashes. Columns like `race/ethnicity` → `race_ethnicity`.
- Prefix: `bronze_`, `silver_`, `gold_`.
- Use `AnnotationText` or the ToolContainer `Caption` when meaningful (e.g. container "Student Performance 1" → `silver_students_performance_1`). Fall back to `<input_name>`, `<operation>_<input>`, etc.

### Step 4 — Emit SQL files

Default project layout (compatible with `databricks pipelines init` / Asset Bundles):

```
<pipeline_name>/
├── databricks.yml                              # Asset Bundle (MANUAL STEP: configure targets)
├── resources/
│   └── <pipeline_name>.pipeline.yml            # Pipeline config: catalog, schema, params
└── src/
    └── transformations/
        ├── bronze_ingest.sql
        ├── silver_<section>.sql
        ├── gold_<output>.sql
        └── MANUAL_STEPS.md
```

Simpler layout if the user only wants raw SQL files (no Asset Bundle):

```
<pipeline_name>/
├── bronze_ingest.sql
├── silver_<section>.sql
├── gold_<output>.sql
└── MANUAL_STEPS.md
```

Default to the simpler layout unless the user asks for a full Asset Bundle project. If they do, follow `databricks-spark-declarative-pipelines` §Quick Start and run `databricks pipelines init`.

### Step 5 — Emit `MANUAL_STEPS.md`

One top-level `MANUAL_STEPS.md` that:
- Lists every Alteryx tool that required human action (SAP, ODBC, Excel input, Email, Tableau, Reports, Spatial, Network shares, `.yxmc` macros without a clear SQL equivalent, etc.).
- Provides concrete next actions (upload files to a UC Volume, configure Lakehouse Federation, swap Email for a SQL Alert, etc.).
- Includes a **deploy & run** checklist (set catalog/schema, create Volume, upload files, run pipeline).
- Cross-references each item to the `.sql` file / line where the placeholder lives.

### Step 6 — Comment every converted step

At the top of each `CREATE OR REFRESH`, add a comment indicating the Alteryx origin:

```sql
-- Converted from Alteryx:
--   ToolID 13 — Formula: Total Score = [math score] + [reading score] + [writing score]
--   ToolID 5  — Filter: [gender] = "female"
--   ToolID 6  — Sample: first 300 rows
```

This makes reviewers able to trace each SDP node back to the original workflow.

---

## Tool Mapping

### Input tools → bronze `STREAMING TABLE`

| Alteryx Plugin | SDP SQL pattern | Notes |
|---|---|---|
| `DbFileInput` (CSV, `FileFormat="0"`) | `CREATE OR REFRESH STREAMING TABLE bronze_* AS SELECT *, current_timestamp() AS _ingested_at, _metadata.file_path AS _source_file FROM STREAM read_files('/Volumes/...', format => 'csv', header => true)` | **MANUAL STEP:** upload file to a UC Volume. Rename columns with special characters via `SELECT` + `AS`. |
| `DbFileInput` (Excel, `FileFormat="25"`) | `read_files(..., format => 'csv')` after converting `.xlsx` → `.csv`, OR use a one-time `spark.read.format('com.crealytics.spark.excel')` loader outside the pipeline | **MANUAL STEP:** SDP does not natively read Excel. Convert to CSV or pre-land as a Delta table. |
| `TextInput` | Inline `VALUES` clause wrapped in a CTE, or a seed table | Small hardcoded lookup. |
| `LockInInput` / ODBC | `CREATE OR REFRESH MATERIALIZED VIEW ... AS SELECT * FROM <catalog>.<schema>.<table>` | Use Unity Catalog if already loaded, or Lakehouse Federation foreign catalog. |
| SAP (Theobald / XtractUniversal) | Placeholder MV + **MANUAL STEP: SAP** | Recommend Lakeflow Connect SAP connector or Lakehouse Federation. |
| `AlteryxDirectory` | `LIST` / `read_files()` with glob | File discovery. |

**Bronze template:**

```sql
-- Bronze: raw ingestion of <source_name>
-- Converted from Alteryx ToolID=<id>, Plugin=DbFileInput
-- Original path: <windows_path_from_yxmd>
-- ACTION: upload the source file to a Unity Catalog Volume (see MANUAL_STEPS.md)
CREATE OR REFRESH STREAMING TABLE bronze_<name>
CLUSTER BY (<first_id_col>)
AS
SELECT
  *,
  current_timestamp()    AS _ingested_at,
  _metadata.file_path    AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/raw/<name>/',
  format       => 'csv',
  header       => true,
  schemaHints  => '<col1> <type>, <col2> <type>, ...'
);
```

### Transformation tools → silver `MATERIALIZED VIEW`

| Alteryx Plugin | SDP SQL | Notes |
|---|---|---|
| `AlteryxSelect` | `SELECT col1, col2 AS renamed, CAST(col3 AS INT) AS col3 FROM <prev>` | Use for rename, drop, type changes. Uncheck = exclude column. |
| `Filter` (Mode=Simple) | `WHERE <Field> <Operator> '<Operand>'` — `IsNotEmpty` → `<field> IS NOT NULL AND <field> <> ''`, `IsEmpty` → `(<field> IS NULL OR <field> = '')` | Simple filter. True/False outputs → two views OR one with inverse WHERE. |
| `Filter` (Mode=Custom) | WHERE with the raw expression (translate Alteryx functions, see [Formula Mapping](#formula-mapping)) | |
| `Formula` | Added columns in `SELECT`: `<expr> AS <field>` | Translate Alteryx functions. |
| `Sort` | `ORDER BY col <ASC|DESC>` | **Generally drop** — SDP output is unordered by design, consumers sort at query time. Keep only when required for `Sample` `First N`. |
| `Sample` (First N) | `ORDER BY ... LIMIT N` (combine with upstream Sort when present) | Preserve the upstream sort inside a CTE if `First N` depends on order. |
| `Sample` (Last N) | `ORDER BY ... DESC LIMIT N` | |
| `Sample` (RandomN) | `TABLESAMPLE (n ROWS)` | |
| `Unique` | `QUALIFY ROW_NUMBER() OVER (PARTITION BY <key_fields> ORDER BY 1) = 1` | Dedupe on key fields. |
| `RegEx` (Method=Replace) | `regexp_replace(<field>, '<pattern>', '<replacement>')` | Case-insensitive: prepend `(?i)` to the pattern. |
| `RegEx` (Method=ParseSimple) | `regexp_extract_all(<field>, '<pattern>')` + `explode` | More complex — may need MANUAL STEP. |
| `Cleanse.yxmc` macro | Combine `TRIM`, `REGEXP_REPLACE` (punct), `UPPER`/`LOWER`, `COALESCE`. Inspect the `<Value name=...>` configs to know which checkboxes are on. | See [Macro Handling](#macro-handling). |
| `SelectRecords.yxmc` macro | `LIMIT` with offset via `ROW_NUMBER()` | `Ranges` like `30-330` → `ROW_NUMBER() OVER (ORDER BY 1) BETWEEN 30 AND 330`. |
| `DateTime` | `TO_DATE(f, fmt)`, `TO_TIMESTAMP(f, fmt)`, `DATE_FORMAT(f, fmt)` | See [Date format mapping](#date-format-mapping). |
| `Find Replace` | `CASE WHEN` / `REPLACE` / `regexp_replace` | Literal or regex replacement. |
| `MultiRowFormula` | `LAG() / LEAD()` over window | Row-relative calc. |
| `MultiFieldFormula` | Apply same expr to multiple columns | May need explicit per-column. |
| `RecordID` | `ROW_NUMBER() OVER (ORDER BY ...)` | |
| `RunningTotal` | `SUM(col) OVER (ORDER BY ... ROWS UNBOUNDED PRECEDING)` | |
| `Tile` | `NTILE(n) OVER (...)` | |
| `Transpose` | `UNPIVOT` or `stack(n, 'name1', c1, 'name2', c2, ...) AS (name, value)` | Key fields stay as GROUP BY equivalents; data fields become rows. |
| `CrossTab` | `PIVOT` | `PIVOT` is supported in plain SQL but **not supported inside an SDP `MATERIALIZED VIEW` query** — wrap in a subquery or use an intermediate MV that materializes the pivot via `SUM(CASE WHEN ... THEN ... END)` instead. |

### Join / combine tools → silver

| Alteryx Plugin | SDP SQL | Notes |
|---|---|---|
| `Join` — `Connection="Join"` (inner) | `INNER JOIN` | |
| `Join` — `Connection="Left"` (left-unmatched) | `LEFT ANTI JOIN` | |
| `Join` — `Connection="Right"` | `RIGHT ANTI JOIN` | |
| `JoinMultiple` | Chain `JOIN` clauses | |
| `Union` (Mode=ByName) | `UNION ALL BY NAME` (if supported) or explicit `SELECT` of the common columns from each side then `UNION ALL` | |
| `Union` (Mode=ByPosition) | `UNION ALL` | |
| `AppendFields` | `CROSS JOIN` | |
| `FindNearest` | `ST_Distance`, `ST_DWithin`, or H3 functions | **MANUAL STEP: Geospatial.** |

### Aggregation tools → gold

| Alteryx Summarize `action` | SDP SQL |
|---|---|
| `GroupBy` | `GROUP BY col` |
| `Sum` | `SUM(col)` |
| `Avg` / `Mean` | `AVG(col)` |
| `Count` | `COUNT(col)` |
| `CountDistinct` | `COUNT(DISTINCT col)` |
| `Min` / `Max` | `MIN(col)` / `MAX(col)` |
| `First` / `Last` | `FIRST(col)` / `LAST(col)` |
| `Concat` | `CONCAT_WS(sep, COLLECT_LIST(col))` |

### Output tools

| Alteryx Plugin | SDP equivalent | Notes |
|---|---|---|
| `DbFileOutput` (file / DB) | Final `MATERIALIZED VIEW` — consumers read the table | Drop file-path literals. Note in `MANUAL_STEPS.md` which consumers need repointing. |
| `DbFileOutput` (`.xlsx` sheet, e.g. `file.xlsx|||SheetName`) | One gold MV per sheet, named after the sheet | Each "sheet" becomes a separate gold table. |
| `Email` | **MANUAL STEP: Email** — Databricks SQL Alert, workflow notification, or webhook | |
| `BrowseV2` | Drop entirely | |
| `TableauOutput` | **MANUAL STEP: Tableau** — Partner Connect or Delta Sharing | |
| `ReportHeader` / `ComposerTable` / `ComposerLayout` | **MANUAL STEP: Reporting** — move to AI/BI Dashboards | |

### Control flow — ignore

- `BlockUntilDone` → drop, SDP manages order.
- `ToolContainer` / `ControlContainer` → layout hints only; use the `<Caption>` for naming.
- `Detour` / `DetourEnd` → map to `CASE WHEN` or two separate pipeline branches.

---

## Macro Handling

Nodes with `<EngineSettings Macro="..."/>` reference a `.yxmc` file. You usually don't have the macro XML, but the name + `<Value name="Check Box (N)">` settings often give enough signal:

| Macro | Heuristic SQL |
|---|---|
| `Cleanse.yxmc` | Per the list of selected fields in `List Box (11)`: `TRIM()`, strip punct via `REGEXP_REPLACE(f, '[[:punct:]]', '')`, `UPPER(f)` or `LOWER(f)` based on `Drop Down (81)`, `COALESCE(NULLIF(f,''), f)` for nulls. Preserve only the columns where corresponding checkboxes are `True`. |
| `SelectRecords.yxmc` | Take `<Value name="Ranges">` (e.g. `30-330`) and apply `ROW_NUMBER() OVER (ORDER BY 1) BETWEEN 30 AND 330`. |
| Unknown macro | **Add MANUAL STEP** describing the macro name and surfacing the configuration as comments, so the user can either re-implement in SQL or replace with a Databricks feature. |

---

## Formula Mapping

| Alteryx | Databricks SQL |
|---|---|
| `IIF(cond, t, f)` | `IF(cond, t, f)` or `CASE WHEN cond THEN t ELSE f END` |
| `Contains([f], 'x')` | `contains(f, 'x')` or `f LIKE '%x%'` |
| `IsNull([f])` | `f IS NULL` |
| `IsEmpty([f])` | `(f IS NULL OR f = '')` |
| `Trim([f])` | `TRIM(f)` |
| `Left/Right/Substring` | `LEFT` / `RIGHT` / `SUBSTRING` |
| `UpperCase` / `LowerCase` | `UPPER` / `LOWER` |
| `PadLeft/PadRight` | `LPAD` / `RPAD` |
| `ReplaceChar` / `Replace` | `REPLACE(f, a, b)` |
| `REGEX_Replace([f], p, r)` | `regexp_replace(f, p, r)` |
| `REGEX_Match([f], p)` | `f RLIKE p` |
| `ToNumber` / `ToString` | `CAST(f AS <type>)` / `CAST(f AS STRING)` |
| `DateTimeFormat` | `date_format(f, fmt)` |
| `DateTimeParse` | `to_date(f, fmt)` / `to_timestamp(f, fmt)` |
| `DateTimeAdd(f, n, 'days')` | `date_add(f, n)` / `f + INTERVAL n DAY` |
| `DateTimeDiff(f1, f2, 'days')` | `datediff(f1, f2)` |
| `DateTimeNow` / `DateTimeToday` | `current_timestamp()` / `current_date()` |
| `MIN(a,b)` / `MAX(a,b)` | `LEAST(a,b)` / `GREATEST(a,b)` |
| `Round/Ceil/Floor/Abs` | `ROUND` / `CEIL` / `FLOOR` / `ABS` |
| `NULL()` | `NULL` |

### Date format mapping

| Alteryx | Databricks |
|---|---|
| `yyyy` | `yyyy` |
| `MM` | `MM` |
| `dd` | `dd` |
| `HH` | `HH` |
| `mm` (minute) | `mm` |
| `ss` | `ss` |
| `%m/%d/%Y` | `MM/dd/yyyy` |
| `yyyy-MM-dd hh:mm:ss` | `yyyy-MM-dd HH:mm:ss` |

---

## Manual-Step Comment Template

For every unconvertible tool, emit both a SQL comment block *and* a `MANUAL_STEPS.md` entry:

```sql
-- ============================================================
-- MANUAL STEP — <Short title>
-- Alteryx ToolID: <id>   Plugin: <plugin>
-- Context:   <what the Alteryx tool did>
-- Action:    <what the user must do before the pipeline can run>
-- See MANUAL_STEPS.md §<anchor>.
-- ============================================================
```

Canonical manual steps (one each if relevant):

- **File migration (UNC / local paths)** — upload to `/Volumes/<catalog>/<schema>/raw/<name>/`. Excel → CSV first.
- **SAP (Theobald / XtractUniversal)** — Lakeflow Connect SAP connector or Lakehouse Federation.
- **ODBC / JDBC source** — verify the table exists in UC or set up a foreign catalog via Lakehouse Federation.
- **Email** — Databricks SQL Alert, workflow notification, or webhook.
- **Tableau Output** — Partner Connect or Delta Sharing.
- **Report Header / Composer** — rebuild in AI/BI Dashboards.
- **Spatial (`FindNearest`, `Trade Area`, `Buffer`)** — H3 or Mosaic.
- **Unknown `.yxmc` macro** — attach the macro configuration as a comment and ask the user to supply behavior, or re-implement in SQL.
- **Pipeline config** — pick target catalog/schema; add Asset Bundle / `databricks.yml` if the user wants one.

---

## `MANUAL_STEPS.md` Template

```markdown
# Manual steps — Alteryx → SDP conversion

**Source workflow:** <file>.yxmd
**Generated on:** <date>
**Target pipeline:** <pipeline_name>

## 1 · Pipeline config
- [ ] Pick Unity Catalog target: `catalog = <...>`, `schema = <...>`
- [ ] Create pipeline (Workspace → Workflows → Pipelines → Create) and point it at the `src/transformations/` folder. Default to **serverless**.
- [ ] (Optional) Convert to an Asset Bundle project via `databricks pipelines init`.

## 2 · File migration
For every Alteryx file input, upload the source to a UC Volume:
- [ ] `C:\...\StudentsPerformance.csv` → `/Volumes/<catalog>/<schema>/raw/students_performance_1/`
- [ ] `C:\...\StudentPerformance2.xlsx` → **convert to CSV first** → `/Volumes/<catalog>/<schema>/raw/students_performance_2/`

## 3 · External connections (ODBC / SAP)
- [ ] <list every ODBC / SAP input tool + recommended replacement>

## 4 · Outputs
- [ ] Any consumer reading old Alteryx output files (Tableau, Excel reports, emails) should repoint to the gold Delta tables.

## 5 · Data quality
- [ ] Review converted `WHERE` clauses — consider promoting to SDP expectations (`CONSTRAINT ... EXPECT (...) ON VIOLATION DROP ROW`).

## 6 · Disabled / skipped sections
- [ ] <list any Alteryx container with `Disabled="True"` — left as comments in SQL>

## 7 · Run checklist
- [ ] `databricks bundle validate` (if using Asset Bundle)
- [ ] Deploy and trigger the pipeline
- [ ] Confirm bronze row counts match source files
- [ ] Confirm gold row counts match Alteryx output
```

---

## Pipeline run checklist (for users)

1. Pick catalog + schema.
2. Create a UC Volume at `/Volumes/<catalog>/<schema>/raw/` and upload source files.
3. Open Databricks → **Workflows → Pipelines → Create pipeline** (or `databricks pipelines init` for Asset Bundle).
4. Point **Source code** to the folder containing the generated `.sql` files (glob `**/*.sql`).
5. Set **Target catalog** and **Target schema**.
6. Select **Serverless** compute (default, required for `read_files()` in most patterns).
7. **Validate** → **Start**. Each `CREATE OR REFRESH` appears as a node in the SDP graph.

---

## Related skills

- **[databricks-spark-declarative-pipelines](../databricks-spark-declarative-pipelines/SKILL.md)** — authoritative SDP SQL patterns (ingestion, streaming, expectations, AUTO CDC, performance).
- **[databricks-asset-bundles](../databricks-asset-bundles/SKILL.md)** — multi-environment deployment.
- **[databricks-unity-catalog](../databricks-unity-catalog/SKILL.md)** — Volume creation, permissions.
- **[alteryx-to-lakeflow-designer](../alteryx-to-lakeflow-designer/SKILL.md)** — sibling skill targeting the Lakeflow *Designer* visual editor (same SQL output; use this SDP skill when the user specifically asks for SDP / Lakeflow pipelines / DLT migration).
