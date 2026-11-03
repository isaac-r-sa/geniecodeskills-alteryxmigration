---
name: dataiku-to-vdp
description: Convert Dataiku DSS flows (project exports / bundles with datasets/*.json and recipes/*.json + payload files) into Databricks Lakeflow Designer Visual Data Prep pipelines (.designer.ipynb). Maps every visual recipe (Prepare/shaker with its processors, Join incl. pre-filters/computed columns, Group, Window, Pivot, Split, Stack, Distinct, Top N, Sort, Sample/Filter, Sync, Fuzzy join) and code recipes (SQL query, Python/pandas, PySpark, R) plus the DSS formula language, project variables and scenarios to Designer operators and Lakeflow Jobs. Preserves DSS output column naming (col_sum, col_distinct, {value}_{col}_{agg}), always materializes output to Unity Catalog Delta tables and validates against expected output.
---

# Skill: Convert Dataiku DSS Flows to Lakeflow Designer Visual Data Prep

## Objective

When a user provides a Dataiku DSS project export (zip or unzipped folder, a bundle, or individual recipe JSON/payload files), convert the Flow into a fully functional Lakeflow Designer (Visual Data Prep) pipeline that:

1. Reproduces the exact logic of every recipe between the selected input datasets and output datasets, including DSS-specific semantics: empty = NULL **or** `""`, Prepare steps applied in order, Group/Window/Pivot output column naming, Split default/missing routing, and Stack by column name.
2. Always materializes every output dataset that is consumed downstream of DSS (dashboards, exports, other projects) to a Unity Catalog Delta table.
3. Is validated against expected output (Step 10). If the user has none, ask for an export of the DSS output datasets from the same input snapshot.
4. Converts **code recipes to visual operators first** (pandas → SQL windows/aggregates), and keeps `python` for what genuinely needs it.
5. Flags scenarios, metrics/checks, ML models, webapps, plugins and partitioning in the top-level `markdown` note. Never silently drop logic.

Companion skill: `alteryx-to-vdp` uses the same operator catalog and Designer file format.

---

## CRITICAL RULE: Operator Selection Priority

> ### MANDATORY PRE-CHECK: run it before every operator decision
> 1. Can a **Transform** express it? (Prepare string/number/date processors, formulas, renames, column selection, FindReplace with ≤ 10 values, literal columns) → **USE TRANSFORM. STOP.**
> 2. Can a **Filter** express it? (FilterOnValue / range / date, Join pre/post-filter, one Split branch) → **USE FILTER. STOP.**
> 3. Can an **Aggregate** express it? (Group recipe with SUM/AVG/COUNT/MIN/MAX/STDDEV/… on plain columns) → **USE AGGREGATE. STOP.**
> 4. Can a **Join** express it? (Join recipe, lookups) → **USE JOIN. STOP.**
> 5. Can **Combine** (Stack), **Pivot** (Pivot / Fold / Unfold), or **Limit** express it? → **USE THE VISUAL OPERATOR. STOP.**
>
> Only then use `sql` (Window recipe, Top N per group, COUNT DISTINCT/first/last/concat in Group, SQL recipes, variable-dependent logic). Use `python` only as the last resort (pandas logic that has no SQL form, external libraries, file I/O).

**One recipe = one or more operators; one Prepare step group = one Transform.** Name operators after the recipe or step (`prep_clean`, `join_customers`, `group_sales`). Keep Prepare steps in order: a Transform evaluates every expression against its *input* columns, so a formula that uses a column cleaned in an earlier step goes into the **next** Transform.

### Verified operator templates (emit only these headless)

These templates were verified by running Designer files as serverless jobs. `designer_gen.py` (next to this file) only emits these. `prepare`, `sort`, `unique`, `enter_data`, `filter` v2 and `split_join` exist in the UI. Use them there, or after verifying their body in a real export.

| Template | Inputs → output port | Config | Verified behaviour (tested) |
|---|---|---|---|
| `source` | → `data` | `table_source: {tableName}` / `file_source` | |
| `transform` | `data` → `transformed_data` | `expressions` | each expression sees the **input** columns only |
| `filter` (1.0.0) | `data` → `filtered_data` | `condition` | NULL condition drops the row |
| `join` (1.0.0) | `left`, `right` → `joined_data` | `join_type`, `join_conditions`, `expressions` | `join_type` goes to Spark as-is: `inner`, `left`, `right`, `full`, `cross`, `left_anti`, `left_semi`. **`cross_join` fails** (`UNSUPPORTED_JOIN_TYPE`). Always list explicit `expressions` |
| `aggregate` (1.0.0) | `data` → `aggregated_data` | `group_bys`, `aggregations` | SUM, AVG, COUNT, MIN, MAX, MEAN, MEDIAN, STDDEV, VARIANCE, PERCENTILE on **plain columns**. No COUNT_DISTINCT/FIRST/LAST/CONCAT in this body, so use `sql` |
| `combine` | `data_0`, `data_1` → `combined_data` | `operator`, `quantifier` | unions **by position**. DSS Stack unions by name, so align columns first |
| `pivot` (1.0.0) | `data` → `pivoted_data` | `mode`, `pivot_column`, `value_column`, `agg_fn` (SUM/AVG/COUNT/MIN/MAX), `exclude_columns`, `null_behavior` | output columns are named after the pivot **values** (`WEB`, not `WEB_amount_sum`), so add a renaming Transform |
| `limit` (1.0.0) | `data` → `limited_data` | `limit` | |
| `sql` (1.0.0) | `data` list + `data__sources` → `result` | `query` | upstream operators are temp views named by id |
| `python` (1.0.0) | `data` list → `result` | `code` | |
| `output` (1.0.0) | `data` | `catalog`, `schema`, `table_name` | overwrite **without** `overwriteSchema`: drop the table once after a type change (`DELTA_FAILED_TO_MERGE_FIELDS`) |
| `markdown` | | `md` | Note |

---

## Designer file format

`<pipeline>.designer.ipynb`: one cell per operator, made of a triple-quoted YAML header (`id`, `template`, `templateVersion`, `name`, `position`, `description`, `config`, `input`), the generated `run()` body, and `ctx` wiring (`ctx["<id>.<port>"]`). Notes are markdown cells with a `---` YAML block. **Double-quote YAML scalars** containing `:`, `#`, `{`, `}`, `[`, `]`, `,`. DSS formulas and `${variables}` break unquoted YAML. A header that fails to parse is silently dropped.

**Generator:** `designer_gen.py` + `designer_templates.json` build a valid Designer file from a JSON spec, handling quoting, layout, verified bodies and wiring:

```bash
python3 designer_gen.py spec.json CUSTOMER_360.designer.ipynb
databricks workspace import /Users/<me>/migrations/CUSTOMER_360.designer.ipynb --file CUSTOMER_360.designer.ipynb --format AUTO
```

The import lands as a `DESIGNER_FILE`, opens in the editor, and runs headless as a job `notebook_task`.

---

## Step 1: Read the export

| Path | Content | Use |
|---|---|---|
| `params.json` | project key, name, flow zones | pipeline name; zones → layout groups |
| `variables.json` (or `project-variables` in the UI) | `standard` + `local` variables | `env_config` (Step 6) |
| `datasets/<name>.json` | `type` (PostgreSQL, Snowflake, Filesystem, S3, Azure Blob, UploadedFiles, Inline/Editable), `params.connection/table/path`, `schema.columns`, `managed`, `partitioning` | sources (non-managed or flow inputs), outputs (managed and consumed) |
| `recipes/<name>.json` | `type`, `inputs.<role>.items[].ref`, `outputs.<role>.items[].ref`, `params` | graph edges + recipe type |
| `recipes/<name>.shaker` | Prepare: ordered `steps[]` (`type`, `params`, `disabled`, `metaType: GROUP` for step groups) | Transform/Filter chain |
| `recipes/<name>.join` | `virtualInputs` (pre-filters, computed columns), `joins` (type, `on` conditions, `maxMatches`), `selectedColumns` (aliases), `computedColumns`, `postFilter` | Filter + Join + Transform |
| `recipes/<name>.grouping` | `keys`, `values[]` with flags (sum, avg, min, max, count, countDistinct, first, last, concat, stddev…), `globalCount`, pre/post filters, computed columns | Aggregate or SQL |
| `recipes/<name>.window` | `windows[]` (partitioning, orders, frame: limitPreceding/Following, rowNumber/rank/denseRank), `values[]` (sum, avg, lag, lead, lagDiff…) | SQL window |
| `recipes/<name>.pivot` | `explicitIdentifiers` (row keys), `keyColumns`, `valueColumns`, `explicitValues`, `computeCount`, `otherColumnsAggregation` | Pivot + rename |
| `recipes/<name>.split` | `mode` (VALUES, FILTER rules, RANDOM, CENTILE), `values[]→outputIndex`, `defaultOutputIndex`, `missingValueOutputIndex` | one Filter per output |
| `recipes/<name>.vstack` | `mode` (UNION / INTERSECT / CUSTOM column selection), `addOriginColumn`, `originColumnName/Mapping` | Transform per input + Combine |
| `recipes/<name>.topn` / `.distinct` / `.sort` / `.sampling` | Top N (keys, orders, firstRows/lastRows, row number column), Distinct (keys), Sort, Sample/Filter | SQL / Filter / Limit |
| `recipes/<name>.sql` / `.py` / `.R` / `.scala` | code recipes (DSS expands `${var}` in SQL before execution) | SQL operator / visual rewrite / Python |
| `scenarios/<id>.json` | triggers, steps (build, checks, SQL, Python), reporters | Lakeflow Jobs (Step 8) |

Procedure:
1. Build the Flow graph from recipe inputs/outputs. Pick the target outputs (datasets read by dashboards, exports or other projects) and walk back to the source datasets.
2. Skip `disabled: true` Prepare steps, and expand step groups (`metaType: GROUP`) in order.
3. Note each dataset's connection. **Managed** intermediate datasets don't need their own UC table unless something outside the flow reads them.
4. Read the project variables and every `${...}` in recipes.

---

## Step 2: Recipe → operator mapping

| DSS recipe | Designer | Notes |
|---|---|---|
| **Sync** | `source` → `output` | type casts to the target schema in a Transform |
| **Prepare** (shaker) | `transform` / `filter` chain | Step 3 maps each processor |
| **Join**: pre-filter per input | `filter` before the join | |
| Join INNER / LEFT / RIGHT / OUTER / CROSS | `join` inner / left / right / full / `cross` | multi-way joins are chained in the order of `joins[]`; `table1` = left |
| Join LEFT ANTI / advanced (`maxMatches: 1`) | `join` `left_anti`; dedup the right side first (`sql` ROW_NUMBER) | `maxMatches: 1` = first match only |
| Join non-equi conditions (`GT`, `LTE`, `WITHIN_RANGE`, `CONTAINS`) | `join` with an expression condition | `join_conditions: "left.a <= right.b"` |
| Join `selectedColumns` (+ `alias`) and `computedColumns` | `join` `expressions` (aliases) + `transform` after the join | computed columns can reference unselected columns, so keep them in the join and drop them in the Transform |
| Join `postFilter` | `filter` after the join | |
| **Group**: sum/avg/min/max/count/stddev/variance | `aggregate` | name aliases like DSS: `col_sum`, `col_avg`, `col_min`, `col_max`, `col_count`, `col_stddev`, `col_var` |
| Group `globalCount` | COUNT on a non-null key (`aggregate`) or `COUNT(*) AS count` (`sql`) | output column `count` |
| Group countDistinct / first / last / concat / percentile with order | `sql` | `COUNT(DISTINCT c) AS c_distinct`, `MIN_BY/MAX_BY`, `ARRAY_JOIN(COLLECT_LIST(c), ',') AS c_concat` |
| Group pre/post filter, computed columns | `filter` / `transform` around the aggregate | |
| **Window** | `sql` | `ROW_NUMBER()` / `RANK()` / `DENSE_RANK()`; value aggregates with the frame (`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` when ordered without limits); `LAG(c, n)` → `c_lag{n}`; `lagDiff` → `c_lagdiff{n}` (`DATEDIFF` in the `lagDiffUnit`) |
| **Pivot** (sum/avg/min/max/count) | `pivot` + `transform` rename to `{value}_{col}_{agg}` / `{value}_count` | `explicitValues` fixes the column set. With dynamic values, list them from the data at conversion time |
| Pivot with several value columns/aggs | one `pivot` per value column + `join`, or `sql` `PIVOT` | |
| **Split** VALUES | one `filter` per value; default output = `c IS NULL OR c NOT IN (...)` (respect `missingValueOutputIndex`) | `dropOutputIndex` = dropped rows |
| Split FILTER (rules in order, first match wins) | filters with exclusions: rule 2 = `c2 AND NOT COALESCE(c1,false)` | unlike Informatica Router, DSS rules are exclusive |
| Split RANDOM / CENTILE | `sql` with `PMOD(XXHASH64(key), 100)` (deterministic) / `NTILE` | DSS random splits are seeded; warn that rows differ |
| **Stack** (UNION / column mapping) | `transform` per input (same columns, same order, missing → `NULL AS col`) + `combine` UNION ALL (chain for > 2) | origin column = literal `'dataset_name' AS original_dataset` |
| **Distinct** | `sql` `SELECT DISTINCT` / `GROUP BY keys` (+ `count`) | |
| **Top N** | global: `sql` ROW_NUMBER + `QUALIFY`; per group: `ROW_NUMBER() OVER (PARTITION BY keys ORDER BY …)` | `firstRows` / `lastRows`; the row number column keeps its name |
| **Sort** | drop, or keep for order-sensitive steps | Delta tables are unordered |
| **Sample / Filter** | `filter` + `limit` | random sampling → hash-based filter |
| **Fuzzy join** | `sql` with `LEVENSHTEIN` / `SOUNDEX` / `ai_similarity` (low cardinality only) | document thresholds |
| **Geo join** | `sql` `ST_` functions | |
| **SQL query recipe** | `sql` | rewrite the dialect (Step 5); `${var}` → `env_config` |
| **SQL script recipe** | split statements; DDL → job SQL task | |
| **Python (pandas) recipe** | rewrite visually first (sort+shift → `LAG`, cumsum → `SUM OVER`, groupby.agg → `aggregate`, merge → `join`, `np.where` → CASE); else `python` with `inputs["data"][i]` (Spark) instead of `dataiku.Dataset` | `dataiku.get_custom_variables()` → `env_config` |
| **PySpark recipe** | `python` (already Spark) | replace `dkuspark` reads/writes |
| **R / Scala / Shell recipes** | MANUAL (`python` rewrite or a job task) | |
| **Prediction/clustering train & score, Evaluate** | MANUAL: MLflow model + `ai_query` / UC function in a Transform | |
| **Export / Download / Push to editable / Upload** | MANUAL: Volume + `read_files`, or Output `file` | |
| Editable / inline dataset | small: CASE or a Transform literal table; otherwise a UC table | |
| Plugin recipes | MANUAL or a UDO | |

---

## Step 3: Prepare processors → Transform/Filter

| DSS processor | Designer expression | Notes |
|---|---|---|
| `ColumnRenamer` | `old AS new` | |
| `ColumnsSelector` keep / remove | list kept columns / omit removed | `appliesTo: PATTERN` → expand the regex now |
| `ColumnReorder` | expression order | |
| `StringTransformer` TRIM / TO_LOWER / TO_UPPER / CAPITALIZE / NORMALIZE_WHITESPACE | `TRIM` / `LOWER` / `UPPER` / `CONCAT(UPPER(LEFT(c,1)), SUBSTR(c,2))` / `REGEXP_REPLACE(TRIM(c), '\\s+', ' ')` | TO_TITLE → `INITCAP` |
| `FillEmptyWithValue` | `CASE WHEN c IS NULL OR c = '' THEN v ELSE c END`; numeric: `COALESCE(c, v)` | **DSS "empty" = NULL or empty string** |
| `RemoveRowsOnEmpty` | `filter c IS NOT NULL AND c <> ''` | |
| `FilterOnValue` KEEP_ROW / REMOVE_ROW / CLEAR_CELL | `filter c IN (...)` / `filter c IS NULL OR c NOT IN (...)` / `CASE WHEN c IN (...) THEN NULL ELSE c END` | `matchingMode` SUBSTRING → `LIKE '%v%'`, PATTERN → `RLIKE`; `normalizationMode` LOWERCASE → compare `LOWER()` |
| `FilterOnNumericalRange` / `FilterOnDate` | `filter c BETWEEN a AND b` | REMOVE_ROW inverts, NULLs kept |
| `FlagOnValue` | `CASE WHEN c IN (...) THEN 1 ELSE 0 END AS flag` | |
| `FindReplace` (mapping, `output` column) | `CASE c WHEN 'from' THEN 'to' … ELSE c END` (unmatched keep the value); > 10 pairs → `join` to a mapping table | SUBSTRING matching → nested `REPLACE` |
| `RegexpExtractor` | `REGEXP_EXTRACT(c, pattern, n) AS out_n` | one column per group |
| `ColumnSplitter` | `ELEMENT_AT(SPLIT(c, sep), i) AS c_i` | `split to rows` → `sql` `EXPLODE` |
| `DateParser` (formats list) | `COALESCE(CAST(TRY_TO_TIMESTAMP(c, f1) AS DATE), CAST(TRY_TO_TIMESTAMP(c, f2) AS DATE))` | DSS tries the formats in order. Output is a UTC timestamp, so cast to DATE when the target is a date |
| `DateFormatter` | `DATE_FORMAT(c, fmt)` | |
| `DateComponentsExtractor` | `YEAR(c) AS c_year`, `MONTH(c) AS c_month`, … | |
| `DateDifference` | `DATEDIFF(...)` / `TIMESTAMPDIFF(unit, a, b)` | |
| `RoundProcessor` (ROUND/FLOOR/CEIL, places) | `ROUND(c, n)` / `FLOOR` / `CEIL` | |
| `NumericalCombinator` | `a + b`, `a * b`, … | |
| `Binner` | `CASE WHEN c < b1 THEN … END` | |
| `Coalesce` | `COALESCE(...)` | |
| `CreateColumnWithGREL` (Formula) | Step 4 translation | |
| `Fold` / `FoldMultipleColumns` | `pivot` `mode: unpivot` | |
| `Unfold` (dummy columns) | `pivot` COUNT, or CASE per value | |
| `JSONFlattener` / `ArrayFold` | `sql` `from_json` / `explode` | |
| `GeoIP`, `UserAgent`, `CurrencyConverter`, `Translator` | MANUAL or `ai_function` / lookup join | |
| Python processor (`CustomPython`) | rewrite as an expression, else `python` | |

---

## Step 4: DSS formula language → Databricks SQL

| DSS formula | Databricks SQL | Watch out |
|---|---|---|
| `a + " " + b` (strings) | `COALESCE(a,'') \|\| ' ' \|\| COALESCE(b,'')` | DSS concatenates empty for null; `+` is numeric on numbers |
| `if(cond, a, b)` | `CASE WHEN cond THEN a ELSE b END` | |
| `==`, `!=`, `&&`, `\|\|`, `!` | `=`, `<>`, `AND`, `OR`, `NOT` | |
| `isBlank(x)` / `isNull(x)` / `isNotBlank(x)` | `x IS NULL OR TRIM(x) = ''` / `x IS NULL` / negation | |
| `coalesce(a, b)` | `COALESCE(a, b)` | |
| `round(x)` | `FLOOR(x + 0.5)` | **no digits argument**; `round(x*100)/100` = `FLOOR(x*100 + 0.5)/100` (Java `Math.round`) |
| `floor`, `ceil`, `abs`, `pow`, `sqrt`, `log`, `exp` | same | |
| `toString(x)` / `toNumber(x)` / `toInt` | `CAST(x AS STRING)` / `TRY_CAST(x AS DOUBLE)` / `TRY_CAST(x AS INT)` | |
| `substring(s, from, to)` | `SUBSTRING(s, from + 1, to - from)` | **0-based, end-exclusive** in DSS |
| `length`, `trim`, `toLowercase`, `toUppercase`, `replace(s, a, b)` | `LENGTH`, `TRIM`, `LOWER`, `UPPER`, `REPLACE` | |
| `startsWith`, `endsWith`, `contains` | `STARTSWITH`, `ENDSWITH`, `CONTAINS` / `LIKE` | |
| `asDate(s)` / `parseDate(s, fmt)` | `CAST(s AS DATE)` / `TRY_TO_TIMESTAMP(s, fmt)` | |
| `datePart(d, "month")` | `MONTH(d)` (`year`, `day`, `dayofweek`, `hours`…) | DSS dayofweek is ISO (Mon = 1); Spark `DAYOFWEEK` is Sun = 1, so use `WEEKDAY(d) + 1` |
| `diff(d1, d2, "days")` | `DATEDIFF(d1, d2)` | sign = d1 − d2; other units → `TIMESTAMPDIFF` |
| `inc(d, n, "days")` / `trunc(d, "month")` | `DATE_ADD(d, n)` / `DATE_TRUNC('MONTH', d)` | |
| `now()` | `CURRENT_TIMESTAMP()` | |
| `${variable}` | column from `env_config` (Step 6) | |
| `val("col with space")` / `numval` / `strval` | `` `col with space` `` | |

---

## Step 5: Semantics that silently break conversions

1. **Empty = NULL or `""`.** DSS storage blurs them, and FillEmpty / RemoveRowsOnEmpty / isBlank catch both. Test `IS NULL OR = ''` (after TRIM when an earlier step trimmed).
2. **Prepare is sequential.** A formula sees the result of every earlier step. Split Transforms where a step reads a column an earlier step produced.
3. **Output column naming** is part of the contract: Group → `col_sum`, `col_avg`, `col_distinct`, `count`; Window → `row_number`, `rank`, `col_sum` (running), `col_lag1`, `col_lagdiff1`; Pivot → `{value}_{col}_{agg}` / `{value}_count`. Downstream recipes, dashboards and exports use these names.
4. **Stack unions by name; Combine unions by position.** Always add an aligning Transform per input.
5. **Window frames.** An ordered window without limits is a running frame (unbounded preceding → current row). Without ordering it covers the whole partition.
6. **Split** routes each row to exactly one output (first matching rule). Default and missing-value outputs need explicit conditions.
7. **SQL recipes run in the source engine's dialect** (PostgreSQL, Snowflake, Oracle…): `::numeric` → `CAST(... AS DECIMAL(p,s))`, `ILIKE` → `ILIKE` (supported), `DATE 'x'` ok, `NOW()` → `CURRENT_TIMESTAMP()`, quoted `"schema"."table"` → UC names, `LIMIT` ok. **Keep exact decimals**: summing DOUBLE instead of NUMERIC moved 6 of 380 values by 0.01 in testing.
8. **Python recipes on pandas**: `sort_values` + `groupby().shift()` + `cumsum()` = `LAG` + running `SUM`; `groupby().agg()` = `aggregate`. Validate that the sort keys are deterministic (tie-breakers).
9. **Top N ties**: DSS row numbers are arbitrary among ties, so add a tie-break column to make it deterministic and mention it.
10. **Partitioned datasets** and "build only this partition": MANUAL. Use a job parameter and filter, or Lakeflow incremental sources.
11. **Dates** are timestamps in UTC in DSS. When a dataset column is a date, cast to `DATE` and check timezone shifts around midnight.

## Step 6: Project variables

Standard and local variables become one `env_config` `python` operator that returns a 1-row DataFrame (tested pattern):

```python
result = spark.createDataFrame([("2025-07-01", "2025-11-15", 30)],
                               ["min_order_date", "analysis_end", "session_gap_minutes"])
```

Consume it with `CROSS JOIN env_config` in `sql`, or a `join` with `join_type: cross` + Transform. In production, set the values from Lakeflow Job parameters. Scenario-level variable overrides become job parameters.

## Step 7: Datasets

- SQL connections (PostgreSQL, Snowflake, Oracle, SQL Server, BigQuery, Redshift): use the migrated UC table, or a Lakehouse Federation foreign catalog.
- Filesystem / S3 / ADLS / GCS / uploaded files: UC Volume or external location + `file_source`.
- Managed outputs: UC tables (`output`), named after the DSS dataset. Intermediates stay as operators unless something reads them.

## Step 8: Scenarios, metrics, checks → Lakeflow Jobs (document in the note)

| DSS | Databricks |
|---|---|
| Scenario with time trigger | Lakeflow Job schedule (cron + timezone) |
| Dataset-change / SQL trigger | table update trigger / file arrival trigger |
| Build step (recursive / forced) | job task running the pipeline |
| Checks (record count, value ranges), metrics | validation SQL task, SQL alert, or Lakeflow expectations |
| Reporters (mail, Slack, Teams, webhook) | job notifications |
| Python/SQL scenario steps | job notebook / SQL tasks |

## Step 9: Output (always)

Every consumed DSS output dataset becomes an `output` writing a UC Delta table. A final Transform sets the column names and types the consumers expect. After a type change, drop the existing table once (Output overwrites without `overwriteSchema`).

## Step 10: Validation (required)

1. Expected output: export the DSS output datasets (CSV) from the same input snapshot, or compute a reference independently.
2. Compare **as sets** in both directions:

```sql
WITH a AS (SELECT <cols normalized> FROM cat.sch.actual),
     e AS (SELECT <cols normalized> FROM read_files('/Volumes/.../expected.csv', format => 'csv', header => true))
SELECT (SELECT COUNT(*) FROM a) AS actual_rows, (SELECT COUNT(*) FROM e) AS expected_rows,
       (SELECT COUNT(*) FROM (SELECT * FROM a EXCEPT ALL SELECT * FROM e)) AS only_in_actual,
       (SELECT COUNT(*) FROM (SELECT * FROM e EXCEPT ALL SELECT * FROM a)) AS only_in_expected
```

   Normalize numerics `ROUND(CAST(x AS DOUBLE), 2)`, dates `CAST(x AS DATE)`, and NULLs `COALESCE(CAST(x AS STRING), '<null>')`. Exclude the `_rescued_data` column that `read_files` adds.
3. Report per output and iterate until both differences are 0.

## Step 11: Layout

x = dependency depth (≈ 300 px), parallel branches on y (≈ 155 px). The `readme` note goes top-left, with: the project key, a recipe → operator table, variables, and MANUAL items (scenarios, checks, ML, plugins). `designer_gen.py` does the layout.

---

## Samples (all executed in a workspace and validated row-for-row)

| Sample | Project | Patterns | Designer operators | Result |
|---|---|---|---|---|
| `01_simple_customer_prep` | CUSTOMER_PREP | Prepare: TRIM, case, FillEmpty, DateParser (2 formats), 2 formulas, FilterOnValue, ColumnsSelector, Renamer | 6 | 315/315 rows match |
| `02_medium_sales_by_country_channel` | SALES_BY_COUNTRY_CHANNEL | 3-way Join (pre-filter, INNER + LEFT, computed column), Prepare, Group (globalCount, countDistinct, sum/avg/max) with DSS names | 10 | 15/15 |
| `03_medium_order_channel_split` | ORDER_CHANNEL_SPLIT | Prepare formulas (datePart), **Split by value with default + missing**, **Stack with origin column** | 11 | 1,906 + 339 |
| `04_complex_customer_order_behaviour` | CUSTOMER_ORDER_BEHAVIOUR | Join + SQL computed column, Group, **Window** (row number, running sum, lagDiff days), Group, **Pivot** naming, Join, Prepare (GREL `round`), **Top N** | 17 | 397 + 10 |
| `05_very_complex_customer_360` | CUSTOMER_360 | **project variables**, **PostgreSQL SQL recipe** (numeric → DECIMAL), **pandas sessionization recipe → SQL windows**, Group, Pivot count, Prepare (FindReplace, REMOVE_ROW), 3 × LEFT join, cross join to variables, Prepare formulas, Window rank, **scenario → job** | 24 | 380/380 |

`gen_test_samples.py` regenerates the five DSS project exports. Each sample folder holds the project export folder and the converted `.designer.ipynb`.
