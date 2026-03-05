# Module 5 Homework: Data Platforms with Bruin

**Course:** [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)  
**Module:** 05 - Data Platforms  
**Tool:** [Bruin CLI](https://getbruin.com/docs/bruin/)  
**Dataset:** NYC Yellow Taxi Trip Records  
**Year:** 2026

---

## Overview

This module introduces **Bruin**, an end-to-end data framework that combines data ingestion, SQL/Python transformations, and built-in data quality checks into a single CLI-driven workflow. The focus is on building a complete ELT pipeline for NYC taxi data — from raw ingestion to reporting — while applying software engineering best practices like lineage tracking, incremental materialization, and schema validation.

Key learning areas covered in this module:

- Understanding the Bruin **project and pipeline structure**
- Configuring environments and connections via `.bruin.yml`
- Applying **materialization strategies** (append, replace, time_interval, merge, etc.)
- Defining and overriding **pipeline variables** at runtime
- Running assets with **downstream dependency propagation**
- Adding **data quality checks** to asset column definitions
- Visualizing **asset lineage and dependencies**
- Deploying pipelines locally (DuckDB) and to the cloud (BigQuery)

### Dataset Information

| Property | Value |
|---|---|
| **Source** | NYC Taxi & Limousine Commission |
| **Data** | Yellow Taxi trip records |
| **Tool** | Bruin CLI (`bruin init zoomcamp`) |
| **Destination** | DuckDB (local) / BigQuery (cloud) |
| **Template** | `zoomcamp` |

---

## Project Setup

### Step 1: Install Bruin CLI

```bash
curl -LsSf https://getbruin.com/install/cli | sh
```

Verifies Bruin is available on your system. The CLI is written in Go and ships as a single binary — no Python environment required.

### Step 2: Initialize the Zoomcamp Template

```bash
bruin init zoomcamp my-pipeline
cd my-pipeline
```

This scaffolds a complete Bruin project from the `zoomcamp` template, including:
- A `.bruin.yml` file at the project root (connection/environment config)
- A `pipeline.yml` defining pipeline metadata and variables
- An `assets/` directory containing sample ingestion, transformation, and reporting assets

### Step 3: Configure DuckDB Connection

Edit `.bruin.yml` to add or verify your DuckDB connection:

```yaml
default_environment: default
environments:
  default:
    connections:
      duckdb:
        - name: "duckdb-default"
          path: "duckdb.db"
```

### Step 4: Run the Pipeline

```bash
bruin run
```

For a first-time run on a fresh database, use `--full-refresh` to ensure all tables are created from scratch:

```bash
bruin run --full-refresh
```

### Step 5: Run Quality Checks Only

```bash
bruin run --only checks
```

---

## Project Structure

```
my-pipeline/
├── .bruin.yml              # Project-level config: connections, environments, secrets
├── pipeline.yml            # Pipeline definition: schedule, variables, defaults
└── assets/
    ├── ingestion/
    │   └── trips.py        # Ingest NYC taxi data
    ├── transformations/
    │   └── trips_cleaned.sql
    └── reporting/
        └── monthly_summary.sql
```

### Pipeline Configuration

| Property | Value |
|---|---|
| **Tool** | Bruin CLI |
| **Destination** | DuckDB |
| **Config file** | `.bruin.yml` |
| **Pipeline definition** | `pipeline.yml` |
| **Assets location** | `assets/` |

---

## Homework Solutions

### Question 1: Bruin Pipeline Structure

**In a Bruin project, what are the required files/directories?**

**Options:**
- [ ] `bruin.yml` and `assets/`
- [ ] `.bruin.yml` and `pipeline.yml` (assets can be anywhere)
- [x] `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`
- [ ] `pipeline.yml` and `assets/` only

**Answer:** `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`

**Reference:**
```
# Official Bruin pipeline definition structure (from docs):
my-pipeline/
├─ pipeline.yml        # pipeline definition
└─ assets/             # all assets MUST be in this folder
    ├─ some.asset.yml
    ├─ another.asset.py
    └─ yet_another.asset.sql

# At the project (repository) root:
.bruin.yml             # project-level connections and environments
```

**Explanation:**

A Bruin **project** is a Git repository whose root contains `.bruin.yml` — the configuration file that stores environments, connection credentials, and secrets. Within the project, each **pipeline** is defined by a `pipeline.yml` file inside a named directory (e.g., `my-pipeline/`), and the [pipeline definition docs](https://getbruin.com/docs/bruin/pipelines/definition.html) explicitly state that **all assets must be placed under a folder called `assets/` next to `pipeline.yml`**. The second option is incorrect because assets cannot be placed arbitrarily — they must live in the `assets/` subdirectory alongside `pipeline.yml`. The project-level `.bruin.yml` is required for all Bruin commands that need connection configuration.

---

### Question 2: Materialization Strategies

**You're building a pipeline that processes NYC taxi data organized by month based on `pickup_datetime`. Which incremental strategy is best for processing a specific interval period by deleting and inserting data for that time period?**

**Options:**
- [ ] `append` - always add new rows
- [ ] `replace` - truncate and rebuild entirely
- [x] `time_interval` - incremental based on a time column
- [ ] `view` - create a virtual table only

**Answer:** `time_interval`

**Reference:**
```yaml
# Asset definition using time_interval strategy
materialization:
  type: table
  strategy: time_interval
  time_granularity: timestamp   # or 'date'
  incremental_key: pickup_datetime
```

```bash
# Run for a specific time window
bruin run --start-date "2024-01-01" --end-date "2024-01-31" assets/trips.sql
```

**Explanation:**

The `time_interval` strategy is specifically designed for incrementally loading time-based data within defined windows. According to the [Bruin materialization docs](https://getbruin.com/docs/bruin/assets/materialization.html), it works by: (1) starting a transaction, (2) **deleting existing records within the specified time interval**, and (3) inserting fresh records from the query. This makes it the ideal strategy for reprocessing a specific month of taxi data without touching records outside that window. The `append` strategy never deletes — it only adds rows, risking duplicates on reruns. The `replace` strategy is a full-table rebuild every time, which is expensive for large historical datasets. `view` does not materialize data at all.

> **Note:** The `time_interval` strategy requires `incremental_key` (the time column) and `time_granularity` (`date` or `timestamp`) to be specified in the asset's materialization config.

---

### Question 3: Pipeline Variables

**You have the following variable defined in `pipeline.yml`:**

```yaml
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```

**How do you override this when running the pipeline to only process yellow taxis?**

**Options:**
- [ ] `bruin run --taxi-types yellow`
- [ ] `bruin run --var taxi_types=yellow`
- [x] `bruin run --var 'taxi_types=["yellow"]'`
- [ ] `bruin run --set taxi_types=["yellow"]`

**Answer:** `bruin run --var 'taxi_types=["yellow"]'`

**Reference:**
```bash
# Override a simple string variable (use JSON quoting)
bruin run --var env='"prod"'

# Override with JSON for complex/array types
bruin run --var 'taxi_types=["yellow"]'

# Multiple overrides
bruin run --var target_segment='"self_serve"' --var forecast_horizon_days=60
```

**Explanation:**

The Bruin [Variables documentation](https://getbruin.com/docs/bruin/core-concepts/variables.html) shows that runtime variable overrides use the `--var` flag (not `--set` or named flags). For **array-typed variables**, the value must be provided as a JSON-formatted string — e.g., `'taxi_types=["yellow"]'`. The option `--var taxi_types=yellow` is incorrect because `yellow` without quotes is not valid JSON for an array type and would cause a parse error. The `--taxi-types` and `--set` options do not exist in the Bruin CLI. The shell single-quotes are required to prevent the shell from interpreting the brackets and quotes.

---

### Question 4: Running with Dependencies

**You've modified the `ingestion/trips.py` asset and want to run it plus all downstream assets. Which command should you use?**

**Options:**
- [ ] `bruin run ingestion.trips --all`
- [x] `bruin run ingestion/trips.py --downstream`
- [ ] `bruin run pipeline/trips.py --recursive`
- [ ] `bruin run --select ingestion.trips+`

**Answer:** `bruin run ingestion/trips.py --downstream`

**Reference:**
```bash
# Run a specific asset and all assets that depend on it
bruin run ingestion/trips.py --downstream

# Flags table from bruin run docs:
# --downstream  bool  false  Run all downstream assets as well.

# Combine with other flags for fine-grained control:
bruin run ingestion/trips.py --downstream --only checks
bruin run ingestion/trips.py --downstream --exclude-tag experimental
```

**Explanation:**

The Bruin [run command documentation](https://getbruin.com/docs/bruin/commands/run.html) defines the `--downstream` flag as: *"Run all downstream assets as well."* The asset path uses the **file system path** (`ingestion/trips.py`), not a dot-notation name like `ingestion.trips`. The `--all` and `--recursive` flags do not exist in Bruin's CLI. The `--select ingestion.trips+` syntax is a **dbt convention** (using the `+` suffix selector) and is not valid Bruin CLI syntax. Bruin's approach is file-path-based with explicit `--downstream` flag.

---

### Question 5: Quality Checks

**You want to ensure the `pickup_datetime` column in your trips table never has NULL values. Which quality check should you add to your asset definition?**

**Options:**
- [ ] `name: unique`
- [x] `name: not_null`
- [ ] `name: positive`
- [ ] `name: accepted_values, value: [not_null]`

**Answer:** `name: not_null`

**Reference:**
```yaml
# Asset definition with not_null quality check
columns:
  - name: pickup_datetime
    type: timestamp
    description: "The date and time when the trip started"
    checks:
      - name: not_null
```

**Explanation:**

The Bruin [Available Checks documentation](https://getbruin.com/docs/bruin/quality/available_checks.html) defines `not_null` as: *"This check will verify that none of the values of the checked column are null."* This is exactly the constraint required for `pickup_datetime` — a timestamp that must always be present. The `unique` check only validates for duplicate values, not nulls. The `positive` check applies to numeric columns and verifies values are greater than zero. The `accepted_values` option requires a specific list of allowed values and is not designed for null prevention. The `not_null` check is the standard, idiomatic way to enforce column presence in Bruin.

---

### Question 6: Lineage and Dependencies

**After building your pipeline, you want to visualize the dependency graph between assets. Which Bruin command should you use?**

**Options:**
- [ ] `bruin graph`
- [ ] `bruin dependencies`
- [x] `bruin lineage`
- [ ] `bruin show`

**Answer:** `bruin lineage`

**Reference:**
```bash
# Show upstream and downstream dependencies of an asset
bruin lineage assets/reporting/monthly_summary.sql

# Show all direct and indirect dependencies
bruin lineage --full assets/reporting/monthly_summary.sql

# Output as structured JSON
bruin lineage --output json assets/reporting/monthly_summary.sql
```

**Explanation:**

The Bruin [lineage command documentation](https://getbruin.com/docs/bruin/commands/lineage.html) states: *"The `lineage` command helps you understand how a specific asset fits into your pipeline by showing its dependencies — what the asset relies on (upstream), and what relies on the asset (downstream)."* The commands `bruin graph`, `bruin dependencies`, and `bruin show` do not exist in the Bruin CLI. The `lineage` command supports a `--full` flag to include all indirect dependencies, and a `--output json` flag for machine-readable output, making it suitable for both human inspection and CI/CD integration.

---

### Question 7: First-Time Run

**You're running a Bruin pipeline for the first time on a new DuckDB database. What flag should you use to ensure tables are created from scratch?**

**Options:**
- [ ] `--create`
- [ ] `--init`
- [x] `--full-refresh`
- [ ] `--truncate`

**Answer:** `--full-refresh`

**Reference:**
```bash
# First-time run: ensure all tables are created from scratch
bruin run --full-refresh

# From the flags table in bruin run docs:
# --full-refresh  bool  false
# Truncate the table before running.
# Also sets the full_refresh Jinja variable to True
# and BRUIN_FULL_REFRESH environment variable to 1.

# Protect specific tables from being dropped on full refresh:
# refresh_restricted: true  (set in asset definition)
```

**Explanation:**

The `--full-refresh` flag instructs Bruin to **drop and recreate tables** before running, ensuring a clean state on a fresh database. According to the [run command docs](https://getbruin.com/docs/bruin/commands/run.html), it also sets the `full_refresh` Jinja template variable to `true` and the `BRUIN_FULL_REFRESH` environment variable to `1`, allowing assets to branch their logic accordingly. The flags `--create`, `--init`, and `--truncate` do not exist in the Bruin CLI. For subsequent incremental runs without `--full-refresh`, Bruin applies the configured materialization strategy (e.g., `delete+insert`, `time_interval`) to process only the required data window.

> **Note:** If certain tables should never be dropped even during a full refresh (e.g., seed tables or critical reference data), set `refresh_restricted: true` in the asset definition to protect them.

---

## Key Concepts

### `.bruin.yml` — Project Configuration

The `.bruin.yml` file is the heart of a Bruin project. It lives at the **repository root** and stores all connection credentials, environment definitions, and optional secrets configuration. It is automatically added to `.gitignore` on creation to keep credentials out of version control.

```yaml
default_environment: default
environments:
  default:
    connections:
      duckdb:
        - name: "duckdb-default"
          path: "duckdb.db"
  production:
    connections:
      bigquery:
        - name: "bq-prod"
          project: "my-gcp-project"
          credentials_file: "${GOOGLE_APPLICATION_CREDENTIALS}"
```

### Materialization Strategies

Bruin supports a rich set of materialization strategies that convert a simple `SELECT` query into a fully managed table update pattern:

| Strategy | Behavior | Use Case |
|---|---|---|
| `create+replace` | Drop and recreate on every run | Lookup tables, small dimensions |
| `delete+insert` | Delete matching rows, insert new | Incremental by a key column |
| `truncate+insert` | Clear all rows, insert fresh | Full refresh without schema change |
| `append` | Only add new rows | Event/log data, immutable records |
| `merge` | Upsert based on primary key | Slowly changing records |
| `time_interval` | Delete + insert within a time window | **Time-partitioned NYC taxi data** |
| `scd2_by_column` | Type 2 SCD tracking column changes | Historical dimension tracking |

### Pipeline Variables with JSON Schema

Variables allow pipelines to be parameterized without code changes. They are defined in `pipeline.yml` using JSON Schema vocabulary and can be overridden at runtime with `--var`:

```yaml
# pipeline.yml
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
  start_year:
    type: integer
    minimum: 2009
    maximum: 2024
    default: 2024
```

```bash
# Override at runtime
bruin run --var 'taxi_types=["yellow"]' --var start_year=2023
```

### Data Quality Checks

Quality checks are defined inline in asset definitions and run automatically after every asset execution. Available built-in checks include `not_null`, `unique`, `positive`, `non_negative`, `negative`, `accepted_values`, `pattern`, `min`, and `max`:

```yaml
# In a SQL asset definition (e.g., assets/trips.sql)
columns:
  - name: pickup_datetime
    type: timestamp
    checks:
      - name: not_null
  - name: passenger_count
    type: integer
    checks:
      - name: positive
  - name: payment_type
    type: string
    checks:
      - name: accepted_values
        value: ["cash", "credit_card", "no_charge", "dispute"]
```

---

## Resources

| Resource | Link |
|---|---|
| Bruin Documentation | [getbruin.com/docs/bruin](https://getbruin.com/docs/bruin/) |
| Pipeline Definition | [pipelines/definition](https://getbruin.com/docs/bruin/pipelines/definition.html) |
| Materialization Strategies | [assets/materialization](https://getbruin.com/docs/bruin/assets/materialization.html) |
| Pipeline Variables | [core-concepts/variables](https://getbruin.com/docs/bruin/core-concepts/variables.html) |
| Run Command Reference | [commands/run](https://getbruin.com/docs/bruin/commands/run.html) |
| Lineage Command | [commands/lineage](https://getbruin.com/docs/bruin/commands/lineage.html) |
| Quality Checks | [quality/available_checks](https://getbruin.com/docs/bruin/quality/available_checks.html) |
| Project Structure | [core-concepts/project](https://getbruin.com/docs/bruin/core-concepts/project.html) |
| Homework Submission | [courses.datatalks.club/de-zoomcamp-2026/homework/hw5](https://courses.datatalks.club/de-zoomcamp-2026/homework/hw5) |
