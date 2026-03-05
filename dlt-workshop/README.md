# dlt Workshop Homework: Build Your Own dlt Pipeline

**Course:** [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)  
**Module:** dlt Workshop  
**Dataset:** NYC Yellow Taxi Trip Records  
**Year:** 2026

---

## Overview

This workshop focuses on building a data ingestion pipeline using **dlt (data load tool)** with a custom REST API. The key areas covered include:

- Building a REST API source for paginated JSON data using dlt
- Using the **dlt MCP Server** for AI-assisted pipeline development
- Loading data into **DuckDB** as a local data warehouse
- Inspecting pipeline metadata and querying data via the dlt MCP Server
- Exploring data with the **dlt Dashboard** and **marimo notebooks**

### Dataset Information

| Property | Value |
|----------|-------|
| **Source** | NYC Taxi & Limousine Commission (custom API) |
| **Base URL** | `https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api` |
| **Format** | Paginated JSON |
| **Page Size** | 1,000 records per page |
| **Pagination** | Stop when an empty page is returned |
| **Target** | DuckDB (local) |

---

## Project Setup

### Step 1: Create the Project

```bash
mkdir taxi-pipeline
cd taxi-pipeline
```

### Step 2: Set Up the dlt MCP Server

Add the following to `.vscode/mcp.json`:

```json
{
  "servers": {
    "dlt": {
      "command": "uv",
      "args": [
        "run",
        "--with",
        "dlt[duckdb]",
        "--with",
        "dlt-mcp[search]",
        "python",
        "-m",
        "dlt_mcp"
      ]
    }
  }
}
```

### Step 3: Install dlt

```bash
pip install "dlt[workspace]"
```

### Step 4: Initialize the Project

```bash
dlt init dlthub:taxi_pipeline duckdb
```

This creates the dlt project files and Cursor rules for AI assistance, but **no YAML file** since this is a custom (non-scaffolded) API.

### Step 5: Build the Pipeline

Prompt the AI agent with the API details:

```
Build a REST API source for NYC taxi data.

API details:
- Base URL: https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api
- Data format: Paginated JSON (1,000 records per page)
- Pagination: Stop when an empty page is returned

Place the code in taxi_pipeline.py and name the pipeline taxi_pipeline.
Use @dlt rest_api as a tutorial.
```

### Step 6: Run the Pipeline

```bash
python taxi_pipeline.py
```

---

## Pipeline Structure

```
dlt-workshop/
├── ny_taxi_pipeline.py      # Main pipeline script
├── requirements.txt         # Python dependencies
├── taxi_pipeline.duckdb     # DuckDB database (generated after run)
└── .dlt/
    └── config.toml          # dlt configuration
```

### Pipeline Configuration

- **Pipeline name:** `taxi_pipeline`
- **Dataset name:** `ny_taxi`
- **Destination:** DuckDB (`taxi_pipeline.duckdb`)
- **Write disposition:** `append`

---

## Homework Solutions

### Question 1: What is the start date and end date of the dataset?

**Options:**
- [ ] 2009-01-01 to 2009-01-31
- [x] 2009-06-01 to 2009-07-01
- [ ] 2024-01-01 to 2024-02-01
- [ ] 2024-06-01 to 2024-07-01

**Answer:** 2009-06-01 to 2009-07-01

**SQL Query:**
```sql
SELECT
    MIN(trip_pickup_date_time)  AS start_date,
    MAX(trip_dropoff_date_time) AS end_date
FROM ny_taxi;
```

**Result:**

| start_date | end_date |
|---|---|
| 2009-06-01 11:33:00 UTC | 2009-07-01 00:03:00 UTC |

**Explanation:**

The dataset contains NYC Yellow Taxi trip data spanning approximately one month — from **June 1, 2009** to **July 1, 2009**. The start date is derived from the earliest `trip_pickup_date_time` and the end date from the latest `trip_dropoff_date_time` in the dataset. This is a historical slice of the NYC TLC trip records exposed through the custom API.

---

### Question 2: What proportion of trips are paid with credit card?

**Options:**
- [ ] 16.66%
- [x] 26.66%
- [ ] 36.66%
- [ ] 46.66%

**Answer:** 26.66%

**SQL Query:**
```sql
SELECT
    payment_type,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM ny_taxi
GROUP BY payment_type
ORDER BY count DESC;
```

**Result:**

| payment_type | count | percentage |
|---|---|---|
| CASH | 7,235 | 72.35% |
| **Credit** | **2,666** | **26.66%** |
| Cash | 97 | 0.97% |
| No Charge | 1 | 0.01% |
| Dispute | 1 | 0.01% |

**Explanation:**

Out of 10,000 total trips, **2,666 trips (26.66%)** were paid with a credit card (recorded as `Credit`). The dominant payment method is cash — with `CASH` (7,235) and `Cash` (97) being the same category stored with inconsistent casing, totaling ~73.32% when combined.

> **Note:** The raw data has inconsistent casing for payment types (`CASH` vs `Cash`). A cleaned pipeline would normalize these values to a standard format using `UPPER()` or a lookup table.

---

### Question 3: What is the total amount of money generated in tips?

**Options:**
- [ ] $4,063.41
- [x] $6,063.41
- [ ] $8,063.41
- [ ] $10,063.41

**Answer:** $6,063.41

**SQL Query:**
```sql
SELECT ROUND(SUM(tip_amt), 2) AS total_tips
FROM ny_taxi;
```

**Result:**

| total_tips |
|---|
| $6,063.41 |

**Explanation:**

The `tip_amt` column records the monetary value of tips for each trip. Summing across all 10,000 trips gives a total tip revenue of **$6,063.41** for the dataset period (June–July 2009). Tips are only typically recorded for credit card transactions in NYC taxi data, so the ~26.66% credit card usage aligns with this tip total relative to the overall fare revenue.

---

## Key Concepts

### dlt MCP Server

The **dlt MCP (Model Context Protocol) Server** gives AI assistants direct access to:
- dlt documentation and code examples
- Pipeline metadata and schema information
- The ability to query the loaded data via SQL

This enables an **AI-assisted data engineering** workflow where you can ask questions like:
- *"What is the schema of the ny_taxi table?"*
- *"What are the start and end dates of the dataset?"*
- *"What proportion of trips are paid with credit card?"*

### Pagination Strategy

The custom API uses **empty-page pagination** — the pipeline continues fetching pages of 1,000 records until an empty page (no records) is returned. This is a common pattern for REST APIs without explicit total count headers.

```python
# Pagination configuration in dlt REST API source
"paginator": {
    "type": "page_number",
    "page_param": "page",
    "stop_after_empty_page": True
}
```

### Write Disposition

The pipeline uses `append` write disposition. This means each pipeline run **adds new records** to the existing table rather than replacing it. For incremental loads, this is combined with a cursor field like `trip_pickup_date_time`.

---

## Resources

| Resource | Link |
|----------|------|
| dlt Documentation | [dlthub.com/docs](https://dlthub.com/docs) |
| dlt REST API Source | [dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api) |
| dlt Dashboard Docs | [dlthub.com/docs/general-usage/dashboard](https://dlthub.com/docs/general-usage/dashboard) |
| marimo + dlt Guide | [dlthub.com/docs/general-usage/dataset-access/marimo](https://dlthub.com/docs/general-usage/dataset-access/marimo) |
| Workshop Notebook | [dlt Pipeline Overview (Google Colab)](https://colab.research.google.com/github/anair123/data-engineering-zoomcamp/blob/workshop/dlt_2026/cohorts/2026/workshops/dlt/dlt_Pipeline_Overview.ipynb) |
| Homework Submission | [courses.datatalks.club/de-zoomcamp-2026/homework/dlt](https://courses.datatalks.club/de-zoomcamp-2026/homework/dlt) |
