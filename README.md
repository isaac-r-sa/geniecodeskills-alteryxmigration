# GenieCodeSkills — Alteryx Migration

A set of [Databricks Genie Code](https://docs.databricks.com/aws/en/genie-code/skills) agent skills for migrating Alteryx Designer workflows onto Databricks. Each skill is a self-contained `SKILL.md` (plus supporting files) under `Skills/`.

## Skills


| Skill folder                       | Name                                           | Description                                                                                                                                                                                                                                                                                                                         |
| ---------------------------------- | ---------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Skills/alteryxToPythonSpark`      | **Alteryx Migration to PySpark on Databricks** | Converts Alteryx workflows (`.yxmd`, `.yxmc`, `.yxwz`) into Python / PySpark notebooks following a medallion (bronze / silver / gold) layout, with mandatory output validation against an expected result file.                                                                                                                     |
| `Skills/alteryxToLakeflowDesigner` | **alteryx-to-vdp**                             | Converts Alteryx workflows into Databricks **Lakeflow Designer** (Visual Data Prep ETL) pipelines. Maps the full Alteryx tool palette to VDP operators (Source, Output, Aggregate, Combine, Filter, Join, Pivot, Sort, SQL, Transform, Python, AI Function, etc.) and materializes the final output to a Unity Catalog Delta table. See [`Skills/alteryxToLakeflowDesigner/Samples/`](Skills/alteryxToLakeflowDesigner/Samples/) for end-to-end conversion examples. |
| `Skills/alteryxToDatabricksSdp`    | **alteryx-to-databricks-sdp**                  | Converts Alteryx workflows into a runnable Databricks **Lakeflow Spark Declarative Pipeline (SDP)** expressed in pure SQL. Emits `CREATE OR REFRESH STREAMING TABLE` / `MATERIALIZED VIEW` files in bronze/silver/gold layers plus a `MANUAL_STEPS.md` for anything that can't be auto-converted.                                   |




## Installing to the Genie Code skills folder

Genie Code looks for skills in one of two workspace folders:

- **Personal (just you):** `/Workspace/Users/<your-email>/.assistant/skills/`
- **Shared (whole workspace):** `/Workspace/.assistant/skills/`

Each skill must live in its own subfolder containing a `SKILL.md` at the root, e.g. `…/.assistant/skills/alteryx-to-vdp/SKILL.md`.

You can install via the Databricks CLI, a Git folder, or the UI.

### Option 1 — Databricks CLI (recommended)

Authenticate once, then import each skill folder:

```bash
# Authenticate (one-time)
databricks auth login --host https://<your-workspace>.cloud.databricks.com

# Pick a destination root
DEST=/Users/$(databricks current-user me | jq -r .userName)/.assistant/skills
# or for a shared install:
# DEST=/.assistant/skills

# Import each skill
for skill in alteryxToPythonSpark alteryxToLakeflowDesigner alteryxToDatabricksSdp; do
  databricks workspace import-dir "Skills/$skill" "$DEST/$skill" --overwrite
done
```

### Option 2 — Databricks Git folder

1. In the Databricks UI: **Workspace → Users → **** → .assistant → skills**, create the `.assistant/skills` path if it does not exist.
2. From the Databricks UI, **Add → Git folder** and clone this repo into a scratch location (e.g. `/Workspace/Users/<you>/repos/GenieCodeSkills_AlteryxMigration`).
3. Move (or symlink/copy) each subdirectory under `Skills/` into `…/.assistant/skills/`. Genie Code will pick up changes automatically when you next open the panel.

### Option 3 — UI upload

1. In the Genie Code panel, click **Settings → Open skills folder**. Databricks opens `/Workspace/Users/<you>/.assistant/skills/` (or the shared folder).
2. For each skill in this repo, create a subfolder with the same name and drag-and-drop its `SKILL.md` (plus any subdirectories) into the workspace folder.

### Verify the install

In a Genie Code chat (Agent mode), type `@` — the three skills should appear in the autocomplete list. You can also invoke one directly, e.g. `@alteryx-to-vdp convert this workflow …`.

## Feedback

For feedback, bug reports, or feature requests, reach out to:

- Isaac Rahnema — [isaac.r@databricks.com](mailto:isaac.r@databricks.com)
- Daphne Koch — [daphne.koch@databricks.com](mailto:daphne.koch@databricks.com)

