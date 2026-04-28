# alteryxToLakeflowDesigner

Converts Alteryx workflows (`.yxmd` / `.yxmc`) into Databricks **Lakeflow Designer** pipelines — the visual, no-code Visual Data Prep ETL surface that lives inside Lakeflow.

The output is a `.designer.ipynb` file you can open directly in Databricks, edit on the canvas, run, and ship through a Job. The skill always materializes the final result to a Unity Catalog Delta table.

## What it does

- Maps every Alteryx tool category (In/Out, Preparation, Join, Parse, Transform, Data Investigation, Predictive, Time Series, Spatial, Reporting, Documentation, Developer, Interface, Macros) to the actual Lakeflow Designer operators: **Source, Output, AI Function, Aggregate, Combine, Filter, Join, Limit, Pivot, Sort, SQL, Transform, Python, Note, Group**.
- Handles all common file formats — CSV, TSV, Excel, JSON, XML, Parquet, Avro, ORC, Delta, SAS, SPSS, R, geospatial, PDF, and `.yxdb`.
- Routes Alteryx text-analytics tools (Sentiment, RegEx parse, Data Cleansing/PII) to the new Databricks AI functions: `ai_analyze_sentiment`, `ai_classify`, `ai_extract`, `ai_fix_grammar`, `ai_gen`, `ai_mask`, `ai_similarity`, `ai_summarize`, `ai_translate`.
- Flags anything that has no automatic equivalent (interface forms, rendered reports, iterative macros) so the user can address it manually.
- YAML schemas verified against actual Designer exports.

## Files

| Path | Purpose |
|---|---|
| `SKILL.md` | The full skill — operator schemas, file-format coverage, conversion patterns, known limitations |
| `samples/customer_feedback.yxmd` | A small synthetic Alteryx workflow that exercises every operator |
| `samples/customer_feedback.designer.ipynb` | The converted Lakeflow Designer pipeline — drop into a Databricks workspace and open |
| `samples/data/*.csv` | Synthetic input data referenced by the workflow |
| `samples/generate_data.py` | Regenerate the synthetic CSVs (deterministic, seed=42) |
| `samples/build_designer_ipynb.py` | Reference generator showing how each operator's cell is constructed |

## Quick start (verifying the sample)

1. Upload `samples/data/current_feedback.csv` and `historical_feedback.csv` to a UC Volume (e.g. `/Volumes/<catalog>/<schema>/test/`).
2. Create the products lookup table:
   ```sql
   CREATE TABLE <catalog>.<schema>.products AS
   SELECT * FROM read_files('/path/to/products.csv', format => 'csv', header => true);
   ```
3. Import `samples/customer_feedback.designer.ipynb` into the workspace and open it.
4. Click **Run** on the `output_gold` operator.
5. Query the result: `SELECT * FROM <catalog>.<schema>.gold_feedback_summary`.
