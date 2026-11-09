# New Operator Details for Lakeflow Designer Migrations

This reference covers operators that were previously marked **MANUAL** or had only brief entries in the main skill. They are now native visual operators and should always be preferred over `python` or `sql` workarounds.

---

## 1. Notifications (`template: notifications`)

Maps the **Alteryx Email** tool. Sends email notifications with optional data attachments.

### Config

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `subscribers` | array | `[]` | `{type: "email", id: "<address>", name: "<address>"}`. **Must be a workspace member.** |
| `subject` | string | `""` | Subject line (static). |
| `subject_from_column` | bool | `false` | Read subject from a column. |
| `subject_column` | string | `""` | Column for subject (first row). |
| `body` | string | `""` | Body text (plain). |
| `body_from_column` | bool | `false` | Read body from a column. |
| `body_column` | string | `""` | Column for body (first row). |
| `attach_data` | bool | `false` | Attach input data as a file. |
| `attachment_format` | string | `"csv"` | `csv` or `xlsx`. |
| `show_email_input` | bool | `false` | Enable `email` input port for column-driven recipients. |
| `email_column` | string | `""` | Column with addresses. Sends ONE message to ALL addresses, not one per row. |

### Ports

- **Input:** `data` (variadic), `email` (optional, enabled by `show_email_input`)
- **Output:** terminal (no downstream)

### Pattern A: Simple (one email to all)

```yaml
- id: notif_report
  template: notifications
  templateVersion: 1.0.0
  name: Send report
  config:
    subscribers:
      - type: email
        id: user@company.com
        name: user@company.com
    subject: "Weekly Report"
    body: "Please find the report attached."
    attach_data: true
    attachment_format: csv
  input:
    - node: final_select
      input_port: data
      output_port: transformed_data
```

### Pattern B: Per-Recipient (Filter -> Notifications pairs)

Alteryx sent personalised emails by filtering data per person. Replicate with Filter -> Notifications per recipient:

```
Select (all data) --+-- Filter (Region=A) -> Notifications (manager_a@)
                    +-- Filter (Region=B) -> Notifications (manager_b@)
```

Each Filter narrows to one person's rows; Notifications attaches only that filtered data.

### Constraint

Subscribers must be **workspace members**. External addresses are rejected at runtime.

---

## 2. Visualization (`template: visualization`)

Maps **Alteryx Charting Tool, Interactive Chart, Scatterplot, Histogram**. Renders charts inline on the canvas.

### Config

| Key | Type | Description |
| --- | --- | --- |
| `editorSpec.type` | string | Chart type (see below). |
| `editorSpec.xAxis` | object | `{columnName, columnExpression}` for X axis. |
| `editorSpec.yAxis` | object | `{columnName, columnExpression}` for Y axis. |
| `editorSpec.color` | object | Series/group encoding. |
| `editorSpec.size` | object | Bubble size (scatter). |

### Chart Types

| Type | Description | Key Channels |
| --- | --- | --- |
| `bar` | Vertical bar | xAxis, yAxis, color |
| `line` | Line chart | xAxis, yAxis, color |
| `area` | Area chart | xAxis, yAxis, color |
| `scatter` | Scatter plot | xAxis, yAxis, color, size |
| `pie` | Pie/donut | angle, color |
| `table` | Tabular view | all columns |
| `histogram` | Frequency distribution | xAxis (numeric) |
| `box` | Box-and-whisker | xAxis (category), yAxis (numeric) |
| `heatmap` | 2D heat grid | xAxis, yAxis, color |
| `combo` | Mixed bar+line | xAxis, yAxis, yAxis2 |
| `counter` | Single KPI | value |
| `funnel` | Funnel | category, value |
| `waterfall` | Waterfall | xAxis, yAxis |
| `pivot` | Pivot table | rows, columns, values |

### Key Rule

> **Aggregates internally.** Never add a separate Aggregate upstream for charting.

---

## 3. HTML (`template: html`)

Maps **Alteryx Render, Layout, Report Text**. Custom formatted reports beyond what Visualization can produce.

### Config

| Key | Type | Description |
| --- | --- | --- |
| `code` | string | HTML fragment. No `<!DOCTYPE>`, no external fetches. Sandboxed iframe. |

### Data Binding

`window.inputs[0]` -> `{name, columns, rows, truncated}`

### Constraints

- No external fetches (no `fetch()`, no external `<script>`).
- Inline `<style>` and `<script>` are allowed.

---

## 4. Guardrails (`template: guardrails`, v2.0.0)

Maps **Alteryx Test tool, Message tool (error/warning)**, reject branches.

### Config

| Key | Type | Description |
| --- | --- | --- |
| `on_failure.mode` | string | `warn` (continue), `block` (empty passed), `stop` (halt run). |
| `checks` | array | Ordered list of check objects. |

### Check Types (9)

| Type | Description | Fields |
| --- | --- | --- |
| `required_columns` | Columns exist | `columns` |
| `row_count` | Row count in range | `min`, `max` |
| `not_null` | No NULLs | `columns` |
| `unique` | Values unique | `columns` |
| `accepted_values` | Allowlist | `column`, `values` |
| `disallowed_values` | Denylist | `column`, `values` |
| `value_range` | Numeric bounds | `column`, `min`, `max` |
| `regex_match` | Regex pattern | `column`, `pattern` |
| `expression` | Custom SQL boolean | `expression` |

### Output Ports

- `passed` -- rows passing all checks
- `failed` -- rows failing one or more checks
- `errors` -- metadata about which checks failed

---

## 5. Group (`template: group`)

Maps **Alteryx Tool Container**. Visual container for organising operators.

### Config

| Key | Type | Description |
| --- | --- | --- |
| `color` | string | Hex border colour (e.g. `"#ffe0b2"`). |
| `collapsed` | bool | Visually collapsed in UI. |

### Child Membership

Set `parentId: <group_id>` on each child operator. Position is relative to group top-left.

### Disabling

`config_overrides: { disabled: true }` or `config_overrides: { disabled: ":skip_param" }` for parameterised disable.

---

## 6. Inline Custom Operator (`template: inline_udo`)

Maps **Alteryx .yxi Custom Tool**. Pipeline-local custom operator with declared ports.

### When to Use

- No visual operator fits AND logic is complex enough for a named operator.
- Preferred over raw `python` for non-trivial transforms.

### Definition Structure

`config.operator`: `schema`, `type` (python-run-function), `name`, `description`, `config.properties`, `ports.input`, `ports.output`, `version`, `run_function` (inline code).

---

## 7. Enter Data (`template: enter_data`)

Maps **Alteryx Text Input**. Inline table with markdown syntax.

### Config

`data`: markdown table (header + separator + rows). Types auto-detected.

### Known Issue

Crashes with `'DataFrame' object has no attribute 'map'` on pandas < 2.1.0. Fallback: `python` with `spark.createDataFrame()`.

---

## 8. Unique (`template: unique`)

Maps **Alteryx Unique**. Deduplicates rows.

### Config

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `unique_by_all_columns` | bool | `true` | Full-row dedup. |
| `columns` | array | `[]` | Key columns (when false). |
| `sort_expressions` | array | `[]` | Controls which row survives. |

**Anti-pattern:** Do NOT use SQL `ROW_NUMBER() WHERE rn = 1` for simple dedup.

---

## 9. Markdown Note (`template: markdown`)

Maps **Alteryx Comment/Annotation**. Documentation node, no data flow.

`config.md`: markdown string. Always set `dimensions` (default is very small -- use width ~480, height ~120-300).

---

## 10. Pipeline Parameters

Maps **Alteryx Interface tools** (Text Box, Drop Down, Date, Action, Control Parameter).

Create with `manage_parameters`. Reference with `:parameter_name` in operator configs. Works in source, output, sql, filter, combine, limit, group (disabled).

---

## 11. Updated Anti-Patterns

| Alteryx Tool | Old (WRONG) | Correct (NOW) |
| --- | --- | --- |
| Email | MANUAL | `notifications` (Pattern A broadcast, Pattern B Filter pairs) |
| Charting / Chart | MANUAL -> Lakeview | `visualization` (aggregates internally) |
| Render / Layout | MANUAL -> Lakeview | `html` (window.inputs data binding) |
| Test / Message | SQL assertion | `guardrails` (typed checks, on_failure) |
| Tool Container | `group` (no membership) | `group` with `parentId` on children |
| Text Input | `python` createDataFrame | `enter_data` (markdown table) |
| Unique | SQL ROW_NUMBER | `unique` operator |
| Custom Tool (.yxi) | `python` inline | `inline_udo` (named, declared ports) |
| Comment | omit | `markdown` (set dimensions) |
| Interface tools | MANUAL -> App | Pipeline parameters (`:param`) |

---

## 12. AI Function -- Complete Function List

15 functions in the current Designer palette:

| Function | Description | New? |
| --- | --- | --- |
| `ai_analyze_sentiment` | Sentiment analysis | |
| `ai_classify` | Classify by labels | |
| `ai_decide` | Decisions with probabilities/scores | NEW (Beta) |
| `ai_extract` | Extract structured fields | |
| `ai_fix_grammar` | Grammar correction | |
| `ai_forecast` | Time-series forecasting (TVF) | |
| `ai_gen` | Generic LLM prompt | |
| `ai_mask` | PII masking | |
| `ai_parse_document` | Extract text/tables from PDFs/images | NEW |
| `ai_prep_search` | Chunk docs for RAG | NEW |
| `ai_query` | Prompt any foundation model | NEW |
| `ai_similarity` | Semantic similarity score | |
| `ai_summarize` | Summarise text | |
| `ai_transcribe` | Audio transcription | NEW |
| `ai_translate` | Language translation | |

---

## 13. Output -- Advanced Features

- **Split by distinct values**: one file per unique value (up to 50). Excel: separate worksheets.
- **Excel cell range writes**: starting cell + optional end cell. Preserves formatting outside range.
- **Sheet name**: target worksheet for Excel output.
- **Outgoing port**: after write, reads from the persisted asset (table/MV/file). Enables chaining Output -> Notifications.
- **Merge (upsert)**: `merge_keys` for matching. Updates existing, inserts new.

---

## 14. Join -- Multi-Table and Case-Insensitive Keys

- **Multi-table (3+ inputs)**: Advanced -> Join more than two tables -> Add input. Inner joins, single output.
- **Case-insensitive keys**: Match case is **off by default**. String keys match case-insensitively and ignore leading/trailing whitespace.
- **Custom expression columns**: computed columns on the joined result, directly in the Join config.

---

## 15. Aggregate -- Type-Specific Functions

| Data Type | Functions |
| --- | --- |
| Numeric | AVG, CONCAT, COUNT, FIRST, LAST, MAX, MEAN, MEDIAN, MIN, STDDEV, SUM, VARIANCE |
| Text/Date/Timestamp | CONCAT, COUNT, FIRST, LAST, MAX, MIN |
| Boolean | ANY, CONCAT, COUNT, EVERY, FIRST, LAST, NONE |
| Complex/Binary/Geo | COUNT, FIRST, LAST |

Boolean functions `ANY`, `EVERY`, `NONE` map Alteryx "Any True" / "All True" / "None True".

---

## 16. Filter -- Port Name Update

Docs now show: **Included** (matching) and **Excluded** (non-matching, including nulls). Verify YAML port names against the live catalog at Step 0.

---

## 17. Source -- Metric View Support

Sources can target a **Unity Catalog metric view**. Select dimensions and measures; Designer generates the aggregated query.

---

## Complete Operator Summary

| Category | Operators |
| --- | --- |
| Data I/O | Source, Output, Enter Data |
| Transformation | Prepare (12 actions), Select, Filter, Aggregate, Join, Combine, Sort, Limit, Pivot/Unpivot, Unique |
| AI | AI Function (15 functions) |
| Quality and Ops | Guardrails, Notifications |
| Reporting | Visualization, HTML |
| Code | SQL, Python |
| Documentation | Markdown (Note), Group |
| Custom | Inline UDO (local), Shared UDO (registered) |
| Configuration | Parameters (pipeline-level) |
