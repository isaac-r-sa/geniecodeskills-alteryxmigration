---
name: informatica-to-vdp
description: Convert Informatica PowerCenter mappings, mapplets and workflows (POWERMART XML exports, also Informatica Cloud / IDMC CDI mapping exports) into Databricks Lakeflow Designer Visual Data Prep pipelines (.designer.ipynb). Maps every PowerCenter transformation (Source Qualifier, Expression, Filter, Router, Joiner, Lookup connected/unconnected/SQL override, Aggregator, Rank, Sorter, Sequence Generator, Update Strategy, Union, Normalizer, Mapplet, SQL, Java, Stored Procedure, Transaction Control) and the Informatica expression language (IIF, DECODE, ISNULL, IS_DATE, TO_DATE, TO_CHAR, ADD_TO_DATE, :LKP, $$parameters) to Designer operators, handles SCD Type 1/2 and Update Strategy targets, documents workflow/session settings for Lakeflow Jobs, always materializes output to Unity Catalog Delta tables and validates against expected output.
---

# Skill: Convert Informatica PowerCenter Mappings to Lakeflow Designer Visual Data Prep

## Objective

When a user provides an Informatica export (PowerCenter `POWERMART` XML from Repository Manager / `pmrep objectexport`, or an IDMC/Informatica Cloud CDI mapping export), convert each **mapping** into a fully functional Lakeflow Designer (Visual Data Prep) pipeline that:

1. Reproduces the exact logic of the mapping, including Informatica-specific semantics (Router multi-group routing, NULL-as-empty string concatenation, Joiner master/detail outer joins, Lookup multiple-match policy, Rank ties, Sequence Generator, Update Strategy).
2. Always materializes every target to a Unity Catalog Delta table.
3. Is validated against expected output (Step 10). If the user has no expected output, ask for one (a target-table extract from the last production run is ideal) before you call the conversion done.
4. Flags everything with no automatic equivalent (session/workflow settings, Transaction Control, Java/Custom transformations, stored procedures) in the pipeline's top-level `markdown` note. Never silently drop logic.

Companion skill: `alteryx-to-vdp` uses the same operator catalog and Designer file format. This skill repeats the parts you need, and adds Informatica semantics.

---

## CRITICAL RULE: Operator Selection Priority

> ### MANDATORY PRE-CHECK: run it before every operator decision
> Before writing any `sql` or `python` operator, answer these in order:
> 1. Can a **Transform** express it? (Expression transformation output ports, IIF/DECODE as CASE, CAST, COALESCE, TRIM, string/date functions, arithmetic, literals) → **USE TRANSFORM. STOP.**
> 2. Can a **Filter** express it? (Filter transformation, Source Qualifier Source Filter, one Router group) → **USE FILTER. STOP.**
> 3. Can an **Aggregate** express it? (Aggregator with SUM/AVG/COUNT/MIN/MAX/MEDIAN/STDDEV/VARIANCE/PERCENTILE on plain columns) → **USE AGGREGATE. STOP.**
> 4. Can a **Join** express it? (Joiner, connected Lookup, unconnected Lookup) → **USE JOIN. STOP.**
> 5. Can **Combine** (Union transformation), **Pivot** (Normalizer, de-normalizing Aggregator), or **Limit** express it? → **USE THE VISUAL OPERATOR. STOP.**
>
> Only when all answers are NO may you use `sql` (window functions, RANK, Sequence Generator, COUNT DISTINCT, parameter-dependent logic) or, as the last resort, `python`.

**More operators beat fewer.** One Informatica transformation = at least one Designer operator, named after it (`exp_standardize` for `EXP_STANDARDIZE`). Never consolidate several transformations into one SQL node unless the user asks.

### Verified operator templates (emit only these headless)

The table below lists the templates whose generated `run()` bodies were verified by executing Designer files as serverless jobs. `designer_gen.py` (next to this file) only emits these. Other Designer operators (`sort`, `unique`, `prepare`, `enter_data`, `filter` v2.0.0 with `excluded_data`, `join` `split_join`, `visualization`) exist in the UI. Use them only when you build in the UI, or after you have verified their template body in a real export.

| Template | Inputs → output port | Config | Verified behaviour (tested) |
|---|---|---|---|
| `source` | → `data` | `table_source: {tableName}` or `file_source: {path, format, header, ...}` | |
| `transform` | `data` → `transformed_data` | `expressions: ["*", "expr AS alias", ...]` | `selectExpr`; each expression sees only the **input** columns, so chain two Transforms when a port uses another output port |
| `filter` (1.0.0) | `data` → `filtered_data` | `condition` | rows where the condition is NULL are dropped (same as Informatica Filter) |
| `join` (1.0.0) | `left`, `right` → `joined_data` | `join_type`, `join_conditions` (`left.` / `right.`), `expressions` | `join_type` goes straight to Spark: use `inner`, `left`, `right`, `full`, `cross`, `left_anti`, `left_semi`. **`cross_join` fails** (`UNSUPPORTED_JOIN_TYPE`). Always list explicit `expressions` |
| `aggregate` (1.0.0) | `data` → `aggregated_data` | `group_bys`, `aggregations: [{columnExpr: {expr, type}, fn, alias}]` | `fn` ∈ SUM, AVG, COUNT, MIN, MAX, MEAN, MEDIAN, STDDEV, VARIANCE, PERCENTILE, applied to a **plain column name** (not an expression). COUNT_DISTINCT/FIRST/LAST/CONCAT are **not** in this body: use `sql` |
| `combine` | `data_0`, `data_1` → `combined_data` | `operator: UNION/INTERSECT/EXCEPT`, `quantifier: ALL/DISTINCT` | unions **by position**: align column order and types with a Transform first |
| `pivot` (1.0.0) | `data` → `pivoted_data` | `mode: pivot`, `pivot_column`, `value_column`, `agg_fn` (SUM/AVG/COUNT/MIN/MAX), `exclude_columns`, `null_behavior: null/zero`; or `mode: unpivot` | output columns are named after the pivot **values**; group-by = all other non-excluded columns |
| `limit` (1.0.0) | `data` → `limited_data` | `limit: "N"` | |
| `sql` (1.0.0) | `data` (list) + `data__sources` → `result` | `query` | upstream operators are temp views named by operator id; `:param` widget binding supported |
| `python` (1.0.0) | `data` (list) → `result` | `code` (assign `result`) | |
| `output` (1.0.0) | `data` → (terminal) | `catalog`, `schema`, `table_name` | `saveAsTable` mode overwrite **without** `overwriteSchema`. If you change a column type, drop the existing table once (`DELTA_FAILED_TO_MERGE_FIELDS` otherwise) |
| `markdown` | none | `md` | Note on the canvas |

---

## Designer file format

A Designer pipeline is a notebook named `<pipeline>.designer.ipynb`. Each operator is one cell: a triple-quoted **YAML header** (id, template, templateVersion, name, position, description, config, input), the generated `run()` body, and a wiring block that reads upstream results from a shared `ctx` dict (`ctx["<id>.<output_port>"]`). Notes are markdown cells with a `---` YAML block.

```python
"""
id: exp_standardize
template: transform
templateVersion: 1.0.0
name: exp_standardize
position:
  x: 300
  y: 360
description:
  text: "EXP_STANDARDIZE: trim, case and default customer attributes."
  hash: ""
previewMode: "1000"
config:
  expressions:
    - "*"
    - "LOWER(TRIM(email)) AS o_email"
input:
  - node: src_customers
    input_port: data
    output_port: data
"""
# generated run() body + ctx wiring follow
```

**YAML quoting is non-negotiable.** Double-quote any scalar containing `:`, `#`, `{`, `}`, `[`, `]`, `,` or edge whitespace (Informatica descriptions often contain `:` and `$$`). If a header fails to parse, Designer drops the cell silently, and downstream cells fail with `'<id>.<port>' data is missing`.

### Generator (recommended)

`designer_gen.py` + `designer_templates.json` (in this folder) build a valid Designer file from a JSON spec. They handle YAML quoting, canvas layout, the verified run() bodies and the `ctx` wiring:

```bash
python3 designer_gen.py spec.json m_MY_MAPPING.designer.ipynb
databricks workspace import /Users/<me>/migrations/m_MY_MAPPING.designer.ipynb --file m_MY_MAPPING.designer.ipynb --format AUTO
```

The spec is a list of `{"id", "template", "desc", "config", "inputs"}` nodes; see the docstring and the samples. The import lands as a `DESIGNER_FILE`, so it opens in the visual editor. It also runs headless as a job `notebook_task`, which is how the samples were tested.

---

## Step 1: Read the export

A PowerCenter export is `POWERMART > REPOSITORY > FOLDER`, containing:

| Element | Meaning | Use |
|---|---|---|
| `SOURCE` / `SOURCEFIELD` | source definitions (DB type, owner, columns, keys) | map to UC tables (Lakehouse Federation or migrated), or flat files to Volumes |
| `TARGET` / `TARGETFIELD` | target definitions | UC output tables; target column names are the output contract |
| `MAPPLET` | reusable sub-mapping with `Input Transformation` / `Output Transformation` | inline its transformations (prefix ids `mplt_`); a mapplet used in ≥ 2 pipelines can become a shared UDO |
| `MAPPING` | `TRANSFORMATION` (definitions), `INSTANCE` (placements, incl. sources/targets/mapplets and *shortcuts*), `CONNECTOR` (port-level wires), `MAPPINGVARIABLE` (`$$` parameters/variables) | the pipeline |
| `TRANSFORMFIELD` | ports: `PORTTYPE` = INPUT, OUTPUT, INPUT/OUTPUT, LOCAL VARIABLE, LOOKUP, RETURN, MASTER; `EXPRESSION`, `EXPRESSIONTYPE` (GENERAL/GROUPBY) | columns and expressions |
| `TABLEATTRIBUTE` | transformation properties (Filter Condition, Join Type, Lookup condition, Sql Query, Source Filter, Update Strategy Expression, Top/Bottom, …) | operator config |
| `GROUP` (Router) | output groups with `EXPRESSION`; `OUTPUT/DEFAULT` = default group | one Filter per group |
| `WORKFLOW` / `SESSION` / `TASKINSTANCE` / `WORKFLOWLINK` | orchestration, session overrides (pre/post SQL, truncate, treat-rows-as, parameter file, commit interval, email) | Lakeflow Jobs + MANUAL notes; **session overrides beat mapping properties** (e.g. a session-level SQL override) |

Procedure:
1. Build the graph from `CONNECTOR` (FROMINSTANCE → TOINSTANCE). Ports matter: one transformation can feed several downstream instances, and a Router or Mapplet has several output groups.
2. Resolve `INSTANCE` → `TRANSFORMATION_NAME` (instance names can differ from transformation names; reusable transformations and shortcuts live elsewhere in the folder or in a shared folder).
3. For every transformation, collect: output ports with expressions, variable ports (in port order), group-by ports, and table attributes.
4. Trace each **target instance** back to its sources. Several target instances of the same table (e.g. insert and update paths) mean one logical target. See Step 7.
5. List `$$` parameters/variables and their defaults, and look for the session's parameter file.
6. Informatica Cloud (CDI) exports are JSON with the same transformation types and semantics. Read them the same way.

---

## Step 2: Transformation → operator mapping

| PowerCenter transformation | Designer | Notes |
|---|---|---|
| Source Qualifier (no override) | `source` (table) | one `source` per SQ |
| SQ **Source Filter** | `source` + `filter` | filter directly after the source |
| SQ **Sql Query** override / **User Defined Join** | `sql` (rewrite to Databricks SQL), or `source` per table + `join` | prefer visual joins when the override is a plain join |
| SQ **Select Distinct** | `sql` `SELECT DISTINCT` | (`unique` once verified) |
| SQ **Number Of Sorted Ports** | drop | ordering only matters for order-sensitive downstream logic (Step 4) |
| Flat-file source | `source` with `file_source` on a UC Volume | fixed-width → `python` or `read_files` + `SUBSTR` in a Transform |
| Expression | `transform` | output ports become `expr AS name`; **variable ports**: see Step 4 |
| Filter | `filter` | |
| Router | one `filter` **per group** + one for DEFAULT | rows go to *every* matching group, so no T/F chain. DEFAULT = `NOT COALESCE(c1,false) AND NOT COALESCE(c2,false) …` |
| Joiner Normal | `join` inner | |
| Joiner **Master Outer** | `join` with the **detail** side kept | keeps all *detail* rows: `left` = detail, `join_type: left` |
| Joiner **Detail Outer** | `join` keeping the master side | keeps all *master* rows |
| Joiner Full Outer | `join` full | |
| Lookup (connected) | `join` left | policy **Use First/Last Value/Any** with a non-unique key: dedup the lookup first (`sql` ROW_NUMBER over the lookup ports order); **Report Error**: assert uniqueness in validation |
| Lookup (unconnected, `:LKP.x(args)`) | `join` left + `transform` | one join per lookup **table**, not per call; use the RETURN port |
| Lookup SQL override | `filter` / `transform` on the lookup source, or `sql` | `WHERE`, computed lookup ports (e.g. hashes) go before the join |
| Lookup non-equality condition (`<=`, `BETWEEN`) | `join` with a range condition | for "latest effective row" add `sql` ROW_NUMBER |
| Dynamic lookup cache | `sql` window or Output `merge` | usually an upsert, so model it as the target state (Step 7) |
| Aggregator | `aggregate` (+ `transform` for expressions around aggregates) | `ROUND(SUM(x),2)` becomes Aggregate SUM, then Transform ROUND. Non-group, non-aggregated ports return the *last row* value: group by them if they're functionally dependent, else `sql` with `MAX_BY`/`ANY_VALUE` and say so |
| Aggregator with COUNT(DISTINCT) | `sql` | not in Aggregate 1.0.0 |
| Rank | `sql` `RANK() OVER (PARTITION BY <group-by ports> ORDER BY <rank port> DESC/ASC)` + `filter` rank ≤ N | ties share a rank and the next value is skipped (RANK, not ROW_NUMBER) |
| Sorter | drop, or keep the keys for the next order-sensitive step | **Distinct** option → `SELECT DISTINCT` |
| Sequence Generator | `sql` `<start-1> + ROW_NUMBER() OVER (ORDER BY <business key>)` | Informatica numbers in arrival order; choose a deterministic order and document it. Persisted Current Value → read `MAX(sk)` from the target, or use an identity column |
| Update Strategy | target-state modelling (Step 7) or Output `merge` | DD_INSERT / DD_UPDATE / DD_DELETE / DD_REJECT |
| Union | `combine` UNION ALL (chain for > 2 inputs) | align columns by position first |
| Normalizer | `pivot` `mode: unpivot`, or `sql` `stack()` / `explode` | GK/GCID ports → `ROW_NUMBER` / `posexplode` |
| Mapplet | inline (`mplt_` prefix), or a shared UDO if reused | |
| SQL transformation | `sql` | |
| Stored Procedure | MANUAL: rewrite as a SQL UDF / Lakeflow Job SQL task | |
| Java / Custom / HTTP / Web Service | `python` (last resort) or a UDO | document side effects |
| XML Parser / Generator | `sql` `from_xml` / `to_xml`, or `python` | |
| Transaction Control | MANUAL | Delta writes are atomic per operator; commit points do not exist |
| Data Masking | `transform` with `mask()` / `sha2()` or UC column masks | |

---

## Step 3: Expression language translation

| Informatica | Databricks SQL (Transform) | Watch out |
|---|---|---|
| `IIF(c, a, b)` | `CASE WHEN c THEN a ELSE b END` | `IIF(c,a)` without else returns a type-default (0 / '' / NULL); check the port type |
| `DECODE(x, v1, r1, v2, r2, def)` | `CASE x WHEN v1 THEN r1 … ELSE def END` | DECODE matches NULL to NULL; CASE does not, so add `WHEN x IS NULL` if needed |
| `DECODE(TRUE, c1, r1, c2, r2, def)` | `CASE WHEN c1 THEN r1 WHEN c2 THEN r2 ELSE def END` | |
| `ISNULL(x)` | `x IS NULL` | |
| `a \|\| b` | `COALESCE(a,'') \|\| COALESCE(b,'')` | Informatica treats NULL as empty in concatenation (NULL only if both are NULL); Spark returns NULL |
| `CONCAT(a, b)` | same as `\|\|` | |
| `LTRIM(RTRIM(x))` | `TRIM(x)` | `LTRIM(x, 'chars')` → `LTRIM('chars', x)` (argument order flips) |
| `UPPER/LOWER/INITCAP/LENGTH` | same | |
| `SUBSTR(s, start, len)` | `SUBSTRING(s, start, len)` | both 1-based; Informatica start 0 = 1 |
| `INSTR(s, sub)` | `INSTR(s, sub)` / `LOCATE` | occurrence/start args: use `LOCATE(sub, s, start)` |
| `LPAD/RPAD` | same | |
| `REPLACESTR(case, s, old1, old2, …, new)` | nested `REPLACE` | case flag 0 = case-insensitive: `REGEXP_REPLACE(s, '(?i)old', new)` |
| `REPLACECHR(case, s, chars, new)` | `TRANSLATE` / `REGEXP_REPLACE(s, '[chars]', new)` | |
| `REG_EXTRACT(s, pat, n)` / `REG_MATCH` / `REG_REPLACE` | `REGEXP_EXTRACT` / `s RLIKE pat` / `REGEXP_REPLACE` | Perl vs Java regex: mostly compatible |
| `TO_DATE(s, fmt)` | `CAST(TRY_TO_TIMESTAMP(s, fmt*) AS DATE)` (fmt* = translated format codes, below) | serverless runs ANSI mode, so `TO_DATE` raises on bad input: use `TRY_` |
| `IS_DATE(s, fmt)` | `TRY_TO_TIMESTAMP(s, fmt*) IS NOT NULL` | |
| `TO_CHAR(d, fmt)` | `DATE_FORMAT(d, fmt*)` | |
| Format codes | `YYYY→yyyy, YY→yy, MM→MM, MON→MMM, MONTH→MMMM, DD→dd, DY→EEE, HH24→HH, HH/HH12→hh, MI→mm, SS→ss, MS→SSS, AM→a` | `MM` is month in both; `MI` is minutes in Informatica |
| `ADD_TO_DATE(d, 'DD', n)` | `DATE_ADD(d, n)` / `d + INTERVAL n DAY` | 'MM' → `ADD_MONTHS`, 'HH' → `+ INTERVAL n HOUR` |
| `DATE_DIFF(d1, d2, 'DD')` | `DATEDIFF(d1, d2)` | 'MM' → `MONTHS_BETWEEN`, 'HH' → `(UNIX_TIMESTAMP(d1)-UNIX_TIMESTAMP(d2))/3600` |
| `TRUNC(d, 'MM')` / `LAST_DAY(d)` | `DATE_TRUNC('MONTH', d)` / `LAST_DAY(d)` | `TRUNC(number, n)` → `TRUNC(x, n)` on numbers |
| `GET_DATE_PART(d, 'MM')` | `MONTH(d)` / `EXTRACT` | |
| `SYSDATE` / `SESSSTARTTIME` | `CURRENT_TIMESTAMP()` | SESSSTARTTIME is constant per run; so is `current_timestamp()` per query |
| `ROUND(x, n)` | `ROUND(x, n)` | both round half away from zero. Use `DECIMAL` inputs when the source used NUMBER/decimal (see Step 4) |
| `TO_INTEGER / TO_DECIMAL / TO_FLOAT / TO_CHAR(n)` | `CAST(… AS INT/DECIMAL(p,s)/DOUBLE/STRING)` | `TO_INTEGER` truncates; use `TRY_CAST` for dirty data |
| `MD5(s)` / `CRC32` / `SHA256` | `MD5(s)` / `CRC32` / `SHA2(s, 256)` | hash the *same* NULL-normalized concatenation on both sides |
| `:LKP.lkp_name(args)` | column from a left `join` (Step 2) | |
| `ERROR('msg')` / `ABORT('msg')` | `RAISE_ERROR('msg')` or route to an error table | |
| `$$PARAM`, `$PMxxx` | `env_config` operator (Step 6) | |
| `SETVARIABLE / SETMAXVARIABLE` | MANUAL: state table + job parameter | |

---

## Step 4: Semantics that silently break conversions

1. **Router.** Each row is tested against *every* group and lands in all matching groups. The DEFAULT group gets rows matching none. In the tested sample, 158 of 7,482 lines went to two targets. A CASE/if-else chain loses them.
2. **NULL concatenation.** `'A' || NULL = 'A'` in Informatica, `NULL` in Spark. Wrap each operand in `COALESCE(x,'')`, especially inside hash/MD5 change detection.
3. **Joiner outer types are named from the master's point of view.** *Master Outer* keeps all **detail** rows, *Detail Outer* keeps all **master** rows. Check which source is the master (`PORTTYPE` contains `MASTER`).
4. **Variable ports** are evaluated top to bottom after input ports and before output ports, and they **keep their value across rows**. `v_PREV = CURR` / running totals / "same key as previous row" are row-order dependent. Convert them to `sql` window functions (`LAG`, `SUM OVER`) with an explicit `ORDER BY` (from the Sorter or SQ sorted ports). Stateless variable ports (helpers) are inlined into the output expression.
5. **Aggregator pass-through ports** return the last row of the group (arbitrary without Sorted Input). Group by functionally dependent columns, or say which row you pick.
6. **Lookup multiple matches.** *Use First/Last Value* follows the lookup cache order (lookup ports ascending). Reproduce it with ROW_NUMBER over those ports. *Report Error* means duplicates were a failure, so add a uniqueness check.
7. **Rank ties** share a rank and skip the next (SQL `RANK`). Top N can return more than N rows.
8. **Sequence Generator** values depend on row order. Use `ROW_NUMBER() OVER (ORDER BY <deterministic key>)`, start from the persisted Current Value (or `MAX(sk)+1` from the target), and tell the user surrogate keys will not be byte-identical to an Informatica rerun.
9. **Decimal precision.** Oracle NUMBER and Informatica decimals are exact. Spark DOUBLE sums drift and flip `ROUND(…, 2)` at `.xx5` boundaries. In testing, 6 of 380 rows were 0.01 off until the inputs were cast to `DECIMAL(p,s)` before `SUM`. Cast money columns to `DECIMAL` whenever the totals are compared to the cent.
10. **Strings and CHAR.** CHAR ports are blank-padded in Oracle, and comparisons behave differently with trailing spaces. `TRIM` before comparing.
11. **Case-sensitive string comparison** (Rank, Sorter, Lookup) defaults differ per session. Spark is case-sensitive.
12. **Filter / Router conditions that evaluate to NULL** drop the row (Filter) or skip the group (Router). `COALESCE(cond, false)` keeps that behaviour explicit in DEFAULT conditions.
13. **Treat source rows as Data driven** + Update Strategy: the target is written row by row. Model the final target state (Step 7).

---

## Step 5: Sources and targets

- **Relational sources** (Oracle, SQL Server, DB2, Teradata, …): use the UC table if the data has been migrated, or a **Lakehouse Federation** foreign catalog (`source` with `table_source: {tableName: foreign_cat.schema.table}`). Rewrite SQ overrides in Databricks SQL (`NVL`→`COALESCE`, `SYSDATE`→`CURRENT_TIMESTAMP()`, `ROWNUM`→`ROW_NUMBER()`/`LIMIT`, `(+)` outer joins → ANSI joins, `||` NULL semantics as above).
- **Flat files** on shared drives / FTP: land them in a UC Volume (`/Volumes/<cat>/<sch>/<vol>/...`) and read with `file_source` (`format: csv`, `header`, `delimiter`). Indirect file lists → read the folder with a glob.
- **Targets**: one `output` per target table. *Truncate target table option = YES* → overwrite (the default). Insert-only appends → `append` mode (UI) or model the full state. *Update else Insert* / *Update as Update* → `merge` mode keyed on the target key (UI), or compute the final state headless as below.

## Step 6: Parameters and variables

`$$PARAM` defaults, parameter files and `$PM` built-ins become one `env_config` `python` operator that returns a 1-row DataFrame (this pattern is tested):

```python
LOAD_DATE = "2026-10-31"   # $$LOAD_DATE from dim_customer.par
result = spark.createDataFrame([(LOAD_DATE,)], ["load_date"])
```

Consume it with `CROSS JOIN env_config` in a `sql` operator, or with a `join` `join_type: cross` + Transform. In production, set the value from a Lakeflow Job parameter (`:param` widget binding in `sql`). **Persistent mapping variables** (`SETMAXVARIABLE($$LAST_RUN_TS, …)` for incremental loads) need a state table or a job parameter. Mark them MANUAL, with the recommended pattern (watermark table, or a Lakeflow Connect / Auto Loader incremental source).

## Step 7: Update Strategy and SCD targets

Designer has no row-level DD_UPDATE. Two tested options:

- **Final-state rebuild (default headless):** compute the complete target table and overwrite it. SCD2 = existing rows with expirations applied + unchanged rows + new versions:
  1. Standardize the incoming rows (mapplet inlined), then hash the tracked attributes.
  2. Look up the current dimension rows (`current_flag = 'Y'`), hashed the same way.
  3. Set `change_flag` = NEW / CHANGED / NOCHANGE.
  4. New versions (`NEW` + `CHANGED`) get keys from `sql` ROW_NUMBER + start value, with `valid_from = load date`.
  5. Expire old versions by left-joining the dimension with the CHANGED keys and setting `valid_to = load_date - 1`, `current_flag = 'N'`.
  6. `combine` UNION ALL, with columns aligned first, then `output` overwrite.
  See `samples/05_very_complex_dim_customer_scd2`.
- **Merge (UI):** Output in `merge` mode keyed on the surrogate key for updates, plus `append` for inserts. Use this for large dimensions where a rebuild is too expensive.

DD_DELETE → final-state rebuild without those keys, or a separate `sql` DELETE job task. DD_REJECT rows → an `output` to a reject table.

## Step 8: Workflow and session → Lakeflow Jobs (document in the markdown note)

| Informatica | Databricks |
|---|---|
| Workflow / worklet, links, decision tasks | Lakeflow Job with tasks, dependencies, `if/else` condition tasks |
| Scheduler | job schedule (cron + timezone) |
| Session Pre SQL / Post SQL | job SQL task before/after, often obsolete (`ALTER SESSION`, `DBMS_STATS` → predictive optimization) |
| Parameter file | job parameters → `:param` / `env_config` |
| On Success/Failure Email, Email task | job notifications |
| Command task | job notebook / shell-free alternatives |
| Commit interval, DTM buffer, partitions, pushdown optimization | not needed (serverless, atomic Delta writes) |
| Event Wait (file watch) | file arrival trigger on a Volume |

## Step 9: Output (always)

Every Informatica target becomes an `output` operator writing a UC Delta table. Map target port names with a final `transform` (`tgt_map_*`), so column names match the target definition exactly. Keep the Informatica name in the operator description.

## Step 10: Validation (required)

1. Get expected output: target table extract from the last Informatica run (same input snapshot), or an independent reference computation.
2. Upload it to a Volume. Compare it **as sets**, in both directions, outside the data flow:

```sql
WITH a AS (SELECT <cols normalized> FROM cat.sch.actual),
     e AS (SELECT <cols normalized> FROM read_files('/Volumes/.../expected.csv', format => 'csv', header => true))
SELECT (SELECT COUNT(*) FROM a)                                   AS actual_rows,
       (SELECT COUNT(*) FROM e)                                   AS expected_rows,
       (SELECT COUNT(*) FROM (SELECT * FROM a EXCEPT ALL SELECT * FROM e)) AS only_in_actual,
       (SELECT COUNT(*) FROM (SELECT * FROM e EXCEPT ALL SELECT * FROM a)) AS only_in_expected
```

   Normalize numerics with `ROUND(CAST(x AS DOUBLE), 2)`, dates with `CAST(x AS DATE)`, and NULLs with `COALESCE(CAST(x AS STRING), '<null>')`. `read_files` adds `_rescued_data`, so exclude it.
3. Report row counts and mismatches per target. Fix and re-run until both differences are 0. For surrogate keys, compare with the documented ordering rule (Step 4.8).

## Step 11: Layout

x grows with dependency depth (≈ 300 px per step), and parallel branches stack on y (≈ 155 px). Put the `readme` note at the top-left: source mapping name, an Informatica → Designer table, parameters and MANUAL items. `designer_gen.py` lays this out automatically.

---

## Samples (all executed in a workspace and validated row-for-row)

| Sample | Mapping | Patterns | Designer operators | Result |
|---|---|---|---|---|
| `01_simple_customer_clean` | m_LOAD_CUSTOMER_CLEAN | SQ → Expression (INITCAP, IIF/ISNULL default, IS_DATE + 2 date formats, `\|\|`) → Filter | 5 | 315/315 rows match |
| `02_medium_sales_by_customer_month` | m_SALES_BY_CUSTOMER_MONTH | SQ Source Filter, Joiner (master/detail), variable port, TO_CHAR, Aggregator + ROUND | 8 | 1,741/1,741 |
| `03_medium_order_line_router` | m_ORDER_LINE_ROUTER | connected Lookup, IIF/DECODE, **Router with overlapping groups**, 3 targets | 13 | 636 + 2,120 + 4,884 rows match (158 rows in 2 groups) |
| `04_complex_top_products_per_store` | m_TOP_PRODUCTS_PER_STORE | Joiner, Lookup, Aggregator, **Rank (ties)**, **unconnected :LKP**, Sorter + Sequence | 16 | 36/36 |
| `05_very_complex_dim_customer_scd2` | m_DIM_CUSTOMER_SCD2 | **mapplet**, MD5 change detection, Lookup SQL override, Router NEW/CHANGED, shared Sequence, 2 Update Strategies (insert + expire), `$$LOAD_DATE`, session Pre/Post SQL + email | 18 | 464/464 (52 expired, 152 new versions) |

`gen_test_samples.py` regenerates the five POWERMART XML files. Each sample folder holds the source `.xml` and the converted `.designer.ipynb`.
