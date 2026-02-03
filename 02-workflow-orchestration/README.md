# Module 2: Workflow Orchestration - NYC Taxi Data Pipeline

**Course:** [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)  
**Module:** 02 - Workflow Orchestration  
**Orchestrator:** Prefect (instead of Kestra)  
**Year:** 2026

---

## Overview

This module extends existing data pipelines to process NYC taxi data covering:
- **2020:** All months (01-12) for baseline analysis
- **2021:** January through July (01-07) for extending the dataset

The solution implements ETL workflows using **Prefect** as the orchestration tool, processing both **Yellow** and **Green** taxi datasets.

### Key Features

- ✅ Complete ETL pipeline orchestration using Prefect
- ✅ Backfill functionality for historical data (2020 full year + 01/2021 - 07/2021)
- ✅ Support for both Yellow and Green taxi datasets
- ✅ Database schema creation and data population
- ✅ Comprehensive error handling and logging
- ✅ Task-based architecture with subflows

---

## Workflow Architecture

### Workflow Summary

The `workflow.py` file defines a complete ETL orchestration system with the following structure:

#### **1. Core Components**

| Component | Type | Purpose |
|-----------|------|---------|
| `TaxiColor` | Enum | Defines taxi types (GREEN, YELLOW) |
| `download_data()` | Task | Downloads compressed taxi data from GitHub with retry logic |
| `get_file_name()` | Task | Generates standardized file names for taxi data |
| `create_yellow_taxi_schema()` | Task | Creates production and staging tables for yellow taxi data |
| `populate_yellow_taxi()` | Task | Loads yellow taxi data into database using COPY and MERGE |
| `create_green_taxi_schema()` | Task | Creates production and staging tables for green taxi data |
| `populate_green_taxi()` | Task | Loads green taxi data into database using COPY and MERGE |

#### **2. Subflows**

- **`yellow_taxi_etl_subflow()`**: Orchestrates schema creation and data population for yellow taxis
- **`green_taxi_etl_subflow()`**: Orchestrates schema creation and data population for green taxis

#### **3. Main Flows**

- **`etl_process()`**: Routes to appropriate subflow based on taxi color; handles file download and schema setup

#### **4. Data Flow**

```
flow-entry
    ├── etl_process(YELLOW, 7, 2021)
    │   ├── get_file_name() → yellow_tripdata_2021-07.csv
    │   ├── download_data() → data/yellow_tripdata_2021-07.csv
    │   └── yellow_taxi_etl_subflow()
    │       ├── create_yellow_taxi_schema()
    │       └── populate_yellow_taxi()
    │           ├── COPY from CSV to staging table
    │           ├── UPDATE unique_row_id via MD5 hash
    │           ├── MERGE into production table
    │           └── TRUNCATE staging table
    │
    └── etl_process(GREEN, 7, 2021)
        ├── get_file_name() → green_tripdata_2021-07.csv
        ├── download_data() → data/green_tripdata_2021-07.csv
        └── green_taxi_etl_subflow()
            ├── create_green_taxi_schema()
            └── populate_green_taxi()
                ├── COPY from CSV to staging table
                ├── UPDATE unique_row_id via MD5 hash
                ├── MERGE into production table
                └── TRUNCATE staging table
```

#### **5. Database Schema**

**Yellow Taxi Tables:**
- `yellow_tripdata`: Production table
- `yellow_tripdata_staging`: Temporary staging table

**Green Taxi Tables:**
- `green_tripdata`: Production table
- `green_tripdata_staging`: Temporary staging table

**Key Fields:**
- `unique_row_id` (MD5 hash for deduplication)
- `filename` (source file tracking)
- `tpep_pickup_datetime` / `lpep_pickup_datetime` (pickup timestamp)
- Trip details: distance, fare, payment, locations, etc.

---

## Backfill Implementation

The `backfills.py` script provides backfilling for historical data:

### Features

- **Date Range:** 
  - 2020: January - December (12 months each for green and yellow taxis)
  - 2021: January - July (7 months each for green and yellow taxis)
  - **Total:** 19 months × 2 colors = 38 individual ETL runs
- **Error Handling:** Individual month failures don't stop the pipeline
- **Logging:** Descriptive logs with timestamps and status indicators
- **Reporting:** Comprehensive pass/fail report with success rates and failure details

### Usage

```bash
uv run backfills.py
```

---

## Homework Solutions

### Assignment

**Objective:** Extend existing flows to include NYC taxi data for 2020 (baseline) and 2021 (extension to July).

**Data Coverage:**
- **2020:** 12 months (Jan-Dec) for both Yellow and Green taxi data
- **2021:** 7 months (Jan-Jul) for both Yellow and Green taxi data
- **Total Records:** 38 monthly loads (12 months × 2 colors) + (7 months × 2 colors) = 38 total ETL runs

#### Solution Approach

- [x] **Option 1 (Implemented):** Leverage backfill functionality using the `backfills.py` script
  - Processes 2020 full year: 2020-01-01 to 2020-12-31
  - Processes 2021 partial year: 2021-01-01 to 2021-07-31
  - Processes both Yellow and Green taxi datasets
  - Runs all combinations (12 months + 7 months = 19 months × 2 colors = 38 total loads)

- [x] **Option 2 (Implemented):** Loop over combinations using dynamic execution
  - Both backfill script and workflow support parametrized execution
  - Can run manually for each month if needed

---

### Quiz Questions

#### Question 1: Uncompressed File Size for Yellow Taxi 2020-12

**Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the extract task)?**

Options:
- [ ] 128.3 MiB
- [x] 134.5 MiB
- [ ] 364.7 MiB
- [ ] 692.6 MiB

**Answer:** 

---

#### Question 2: Rendered Value of `file` Variable

**What is the rendered value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04` during execution?**

Options:
- [ ] `{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv`
- [x] `green_tripdata_2020-04.csv`
- [ ] `green_tripdata_04_2020.csv`
- [ ] `green_tripdata_2020.csv`

**Answer:** 

**Explanation:**

```python
# From workflow.py get_file_name() function
return f"{taxi_color}_tripdata_{year}-{month:02d}.csv"
# With taxi='green', year=2020, month=04:
# Returns: green_tripdata_2020-04.csv
```

---

#### Question 3: Row Count - Yellow Taxi 2020 (All Months)

**How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?**

Options:
- [ ] 13,537,299
- [x] 24,648,499
- [ ] 18,324,219
- [ ] 29,430,127

**Answer:** 

---

#### Question 4: Row Count - Green Taxi 2020 (All Months)

**How many rows are there for the Green Taxi data for all CSV files in the year 2020?**

Options:
- [ ] 5,327,301
- [ ] 936,199
- [x] 1,734,051
- [ ] 1,342,034

**Answer:** 

---

#### Question 5: Row Count - Yellow Taxi March 2021

**How many rows are there for the Yellow Taxi data for the March 2021 CSV file?**

Options:
- [ ] 1,428,092
- [ ] 706,911
- [x] 1,925,152
- [ ] 2,561,031

**Answer:** 

---

#### Question 6: Timezone Configuration

**How would you configure the timezone to New York in a Schedule trigger?**

Options:
- [ ] Add a `timezone` property set to `EST` in the `Schedule` trigger configuration
- [x] Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration
- [ ] Add a `timezone` property set to `UTC-5` in the `Schedule` trigger configuration
- [ ] Add a `location` property set to `New_York` in the `Schedule` trigger configuration

**Answer:** 

**Explanation (Prefect):**

In Prefect, you would configure timezone in deployment or schedule settings using IANA timezone format:

```python
from prefect.schedules import Schedule
from prefect.utilities.schedules import cron_schedule

schedule = Schedule(
    clocks=[
        CronClock(
            cron="0 0 * * *",  # Daily at midnight
            timezone="America/New_York"  # IANA timezone format
        )
    ]
)
```

---

## Running the Workflows

### Prerequisites

```bash
# Install dependencies
pip install prefect prefect-sqlalchemy

# Or with uv
uv pip install prefect prefect-sqlalchemy
```

### Execute Main Workflow

```bash
# Run the main workflow
python workflow.py

# Or with uv
uv run workflow.py
```

### Execute Backfill

```bash
# Run complete backfill (Jan-Jul 2021 for both colors)
python backfills.py

# Or with uv
uv run backfills.py

# Run specific backfill with custom parameters
python -c "from backfills import green_taxi_backfill; green_taxi_backfill(1, 7, 2021)"
```

---

## File Structure

```
02-workflow-orchestration/
├── workflow.py              # Main ETL workflow with Prefect flows and tasks
├── backfills.py             # Backfill script for historical data (01-07/2021)
├── docker-compose.yaml      # Database service configuration
├── docker-compose.yml       # Alternative compose file
├── data/                    # Local data storage
│   └── yellow_tripdata_2021-07.csv
├── README.md                # This file
└── homework-queries.sql     # Database queries for homework verification
```

---

## Implementation Notes

### Prefect vs Kestra

This solution uses **Prefect** instead of the course's **Kestra** orchestrator:

| Aspect | Prefect | Kestra |
|--------|---------|--------|
| **Language** | Python-native | YAML-based |
| **Task Definition** | `@task` decorator | YAML steps |
| **Flow Definition** | `@flow` decorator | YAML flows |
| **Scheduling** | Python/Deployment API | Built-in schedule triggers |
| **Backfill** | Manual iteration (backfills.py) | Built-in backfill UI |

### Key Implementation Details

1. **Database Connection:**
   - Uses `SqlAlchemyConnector` from `prefect_sqlalchemy`
   - Credentials loaded from Prefect secrets: `taxi-trip-db-creds`

2. **Deduplication:**
   - Implements MD5-based `unique_row_id` for row uniqueness
   - MERGE operation handles insert-only logic

3. **Staging Pattern:**
   - Staging tables used as temporary buffers
   - TRUNCATE after each successful MERGE

4. **Error Resilience:**
   - Individual month failures don't block pipeline
   - Comprehensive error logging in backfill script
   - Retry logic (2 retries) on data download task

---

## Data Sources

- **Yellow Taxi:** https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/
- **Green Taxi:** https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/
- **Taxi Zone Lookup:** https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv

---

## Submission

- **Assignment Status:** ✅ Completed
- **Backfill:** ✅ Implemented (01-07/2021 for both colors)
- **Quiz Answers:** [See homework section above]

---

## Learning Resources

- [Prefect Documentation](https://docs.prefect.io/)
- [Prefect Tutorial](https://docs.prefect.io/v2/learn/)
- [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)
- [NYC Taxi Dataset](https://github.com/DataTalksClub/nyc-tlc-data)

---

**Last Updated:** February 3, 2026  
**Status:** Active & Maintained
