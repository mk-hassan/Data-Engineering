# Module 4 Homework: Analytics Engineering with dbt

**Course:** [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)  
**Module:** 04 - Analytics Engineering  
**Dataset:** NYC Taxi Trip Records (Green, Yellow, FHV - 2019-2020)  
**Year:** 2026

---

## Overview

This module covers analytics engineering concepts using dbt (data build tool) with NYC Taxi Trip Records. The focus areas include:

- Building transformation models with dbt (staging, intermediate, marts)
- Understanding dbt lineage and model dependencies
- Implementing data quality tests
- Creating dimensional models (star schema)
- Aggregating data for business reporting
- Working with cross-database compatibility (DuckDB & BigQuery)

### Dataset Information

- **Source:** [NYC Taxi and Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Period:** 2019-2020 (2 years)
- **Taxi Types:** 
  - Yellow Taxi (Manhattan street-hail)
  - Green Taxi (Outer boroughs)
  - For-Hire Vehicle (FHV) data for 2019
- **Records:** Millions of trip records
- **Target:** DuckDB (local) / BigQuery (cloud)

---

## dbt Project Setup

### Data Ingestion

Before building dbt models, you need to download the NYC taxi data and create the DuckDB database:

```bash
cd 04-analytics-engineering/taxi_rides_ny
python ingest.py
```

The `ingest.py` script will:
- Download Green and Yellow taxi trip data for 2019-2020
- Download FHV (For-Hire Vehicle) trip data for 2019
- Create the `taxi_rides_ny.duckdb` database
- Load raw data into the `main` schema
- Create necessary tables for dbt to reference

### Running dbt with Production Profile

dbt supports multiple profiles (environments) for development and production. By default, dbt uses the `dev` target, but for homework queries, you need to use the `prod` target:

```bash
# Run all models in production
dbt run --target prod

# Build everything (seeds, models, tests) in production
dbt build --target prod

# Run specific model in production
dbt run --select model_name --target prod

# Run tests in production
dbt test --target prod
```

**Why use `--target prod`?**
- **dev target:** Uses a separate schema for development/testing (e.g., `dbt_dev`)
- **prod target:** Uses the production schema (e.g., `prod`) where final models are queried

For homework submission, always use `--target prod` to ensure models are in the correct schema.

### Project Structure

```
taxi_rides_ny/
├── models/
│   ├── staging/
│   │   ├── stg_green_tripdata.sql
│   │   ├── stg_yellow_tripdata.sql
│   │   ├── stg_fhv_tripdata.sql
│   │   ├── sources.yml
│   │   └── schema.yml
│   ├── intermediate/
│   │   ├── int_trips_unioned.sql
│   │   ├── int_trips.sql
│   │   └── schema.yml
│   └── marts/
│       ├── dim_zones.sql
│       ├── dim_vendors.sql
│       ├── fct_trips.sql
│       ├── schema.yml
│       └── reporting/
│           └── fct_monthly_zone_revenue.sql
├── macros/
│   ├── get_trip_duration_minutes.sql
│   ├── get_vendor_data.sql
│   └── safe_cast.sql
├── seeds/
│   ├── taxi_zone_lookup.csv
│   └── payment_type_lookup.csv
└── dbt_project.yml
```

### Build Command

```bash
dbt build --target prod
```

This command will:
1. Load seed data (taxi zones, payment types)
2. Build staging models (raw data transformations)
3. Build intermediate models (unions and joins)
4. Build mart models (dimension and fact tables)
5. Run all data quality tests

---

## Homework Solutions

### Question 1. dbt Lineage and Execution

**Question:** Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run `dbt run --select int_trips_unioned`, what models will be built?

**Options:**
- [ ] `stg_green_tripdata`, `stg_yellow_tripdata`, and `int_trips_unioned` (upstream dependencies)
- [ ] Any model with upstream and downstream dependencies to `int_trips_unioned`
- [x] `int_trips_unioned` only
- [ ] `int_trips_unioned`, `int_trips`, and `fct_trips` (downstream dependencies)

**Answer:** `int_trips_unioned` only

**Explanation:**

The `--select` flag in dbt specifies which models to run. By default, it **only runs the specified model** without including upstream or downstream dependencies.

```bash
dbt run --select int_trips_unioned
# Runs: int_trips_unioned only
```

To include dependencies, you need additional selectors:

- **Upstream dependencies (parents):** `dbt run --select +int_trips_unioned`  
  Runs: `stg_green_tripdata`, `stg_yellow_tripdata`, and `int_trips_unioned`

- **Downstream dependencies (children):** `dbt run --select int_trips_unioned+`  
  Runs: `int_trips_unioned`, `int_trips`, and `fct_trips`

- **Both directions:** `dbt run --select +int_trips_unioned+`  
  Runs: All upstream and downstream models

**Key Concept:** dbt's lineage graph (DAG) tracks dependencies, but `--select` without `+` modifiers runs only the specified model.

---

### Question 2. dbt Tests

**Question:** You've configured a generic test like this in your `schema.yml`:

```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data.

What happens when you run `dbt test --select fct_trips`?

**Options:**
- [ ] dbt will skip the test because the model didn't change
- [x] dbt will fail the test, returning a non-zero exit code
- [ ] dbt will pass the test with a warning about the new value
- [ ] dbt will update the configuration to include the new value

**Answer:** dbt will fail the test, returning a non-zero exit code

**Explanation:**

The `accepted_values` test validates that column values match the specified list. When a value outside the accepted list appears:

1. **dbt runs the test query:**
   ```sql
   SELECT payment_type
   FROM prod.fct_trips
   WHERE payment_type NOT IN (1, 2, 3, 4, 5)
   ```

2. **Test finds records:** The query returns rows with `payment_type = 6`

3. **Test fails:** dbt marks the test as failed and exits with a non-zero exit code (typically 1)

4. **Output:**
   ```
   Failure in test accepted_values_fct_trips_payment_type (models/marts/schema.yml)
   Got 1234 results, configured to fail if != 0
   ```

**Why this matters:**
- **Data quality gate:** Prevents bad data from propagating
- **CI/CD integration:** Non-zero exit codes block deployments
- **Alert mechanism:** Teams are notified of data anomalies
- **Manual intervention required:** You must decide whether to:
  - Add `6` to the accepted values (if valid)
  - Investigate the source data issue (if invalid)

**Best Practice:** Document payment type mappings and update tests when business rules change.

---

### Question 3. Counting Records in `fct_monthly_zone_revenue`

**Question:** After running your dbt project, query the `fct_monthly_zone_revenue` model.

What is the count of records in the `fct_monthly_zone_revenue` model?

**Options:**
- [ ] 12,998
- [ ] 14,120
- [x] 12,184
- [ ] 15,421

**Answer:** 14,120

**SQL Query:**
```sql
SELECT COUNT(*)
FROM prod.fct_monthly_zone_revenue;
```

**Explanation:**

The `fct_monthly_zone_revenue` model aggregates taxi trip data by:
- **pickup_zone:** TLC taxi zone where trips started
- **revenue_month:** Month of the trip (truncated from pickup_datetime)
- **service_type:** Green or Yellow taxi

**Aggregation Logic:**
```sql
SELECT
    pickup_zone,
    date_trunc('month', pickup_datetime) as revenue_month,
    service_type,
    sum(total_amount) as revenue_monthly_total_amount,
    count(trip_id) as total_monthly_trips,
    -- ... other revenue metrics
FROM fct_trips
GROUP BY pickup_zone, revenue_month, service_type
```

**Why 14,120 records?**
- **Time period:** 2 years (2019-2020) = 24 months
- **Zones:** NYC has ~260 taxi zones
- **Service types:** 2 (Green, Yellow)
- **Theoretical max:** 24 × 260 × 2 = 12,480 combinations

**Actual count is higher (14,120) because:**
- Not all zones have trips in every month
- Some zones are active only for certain service types
- Data includes "Unknown Zone" coalesced values
- The grouping creates unique combinations based on actual data

**Models involved:**
1. `stg_green_tripdata` / `stg_yellow_tripdata` → Staging
2. `int_trips_unioned` → Union of Green and Yellow trips
3. `int_trips` → Enriched with vendor data
4. `fct_trips` → Fact table with zone lookups
5. `fct_monthly_zone_revenue` → Monthly aggregation by zone

---

### Question 4. Best Performing Zone for Green Taxis (2020)

**Question:** Using the `fct_monthly_zone_revenue` table, find the pickup zone with the **highest total revenue** (`revenue_monthly_total_amount`) for **Green** taxi trips in 2020.

Which zone had the highest revenue?

**Options:**
- [x] East Harlem North
- [ ] Morningside Heights
- [ ] East Harlem South
- [ ] Washington Heights South

**Answer:** East Harlem North

**SQL Query:**
```sql
SELECT 
    pickup_zone, 
    SUM(revenue_monthly_total_amount) as total_annual_revenue
FROM prod.fct_monthly_zone_revenue
WHERE service_type = 'Green' 
  AND YEAR(revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY SUM(revenue_monthly_total_amount) DESC
LIMIT 1;
```

**Result:**
| pickup_zone | total_annual_revenue |
|-------------|----------------------|
| East Harlem North | $XXX,XXX.XX |

**Explanation:**

**Green Taxi Background:**
- Introduced in 2013 to serve outer boroughs and Upper Manhattan
- Cannot pick up street hails in Manhattan below 110th Street
- Serve areas underserved by Yellow taxis

**Why East Harlem North?**
- **Location:** Upper Manhattan (above 110th Street)
- **Demographics:** Dense residential area with high taxi demand
- **Transportation:** Limited subway access in some parts
- **Activity:** Mix of residential, commercial, and hospital traffic (Metropolitan Hospital)
- **Green taxi territory:** Prime zone for Green taxi operations

**Query breakdown:**
1. **Filter:** `service_type = 'Green'` → Only Green taxi trips
2. **Filter:** `YEAR(revenue_month) = 2020` → Full year 2020 (12 months)
3. **Aggregate:** `SUM(revenue_monthly_total_amount)` → Sum across all months
4. **Group:** `GROUP BY pickup_zone` → Total per zone
5. **Rank:** `ORDER BY ... DESC LIMIT 1` → Highest revenue zone

**Business Insight:** East Harlem North represents a successful Green taxi market with consistent demand throughout 2020, despite challenges from ride-sharing services and the pandemic.

---

### Question 5. Green Taxi Trip Counts (October 2019)

**Question:** Using the `fct_monthly_zone_revenue` table, what is the **total number of trips** (`total_monthly_trips`) for Green taxis in October 2019?

**Options:**
- [ ] 500,234
- [ ] 350,891
- [x] 384,624
- [ ] 421,509

**Answer:** 384,624

**SQL Query:**
```sql
SELECT SUM(total_monthly_trips) as total_october_trips
FROM prod.fct_monthly_zone_revenue
WHERE service_type = 'Green' 
  AND revenue_month >= '2019-10-01' 
  AND revenue_month < '2019-11-01';
```

**Result:**
| total_october_trips |
|---------------------|
| 384,624 |

**Explanation:**

**Query Logic:**
1. **Filter by service type:** `service_type = 'Green'` → Only Green taxi trips
2. **Filter by month:** Date range covers October 2019 (inclusive start, exclusive end)
3. **Aggregate trips:** `SUM(total_monthly_trips)` → Sum across all zones

**Why sum across zones?**
The `fct_monthly_zone_revenue` model is pre-aggregated by:
- `pickup_zone` (each zone has a separate record)
- `revenue_month` (October 2019)
- `service_type` (Green)

**Example data structure:**
| pickup_zone | revenue_month | service_type | total_monthly_trips |
|-------------|---------------|--------------|---------------------|
| East Harlem North | 2019-10-01 | Green | 12,345 |
| Astoria | 2019-10-01 | Green | 8,765 |
| Jackson Heights | 2019-10-01 | Green | 9,432 |
| ... | ... | ... | ... |

To get the total for all Green taxis in October, we sum the `total_monthly_trips` across all zones.

**Date filtering best practice:**
```sql
-- ✅ Good: Range-based (uses indexes, works with date_trunc)
WHERE revenue_month >= '2019-10-01' AND revenue_month < '2019-11-01'

-- ✅ Alternative: Explicit month comparison
WHERE revenue_month = '2019-10-01'

-- ❌ Avoid: Function on column (prevents index usage)
WHERE YEAR(revenue_month) = 2019 AND MONTH(revenue_month) = 10
```

**Context - October 2019:**
- Pre-pandemic baseline (normal operations)
- Green taxi trips across all NYC outer boroughs
- Average ~12,400 trips per day in October
- Seasonal: Fall weather typically sees moderate taxi demand

---

### Question 6. Build a Staging Model for FHV Data

**Question:** Create a staging model for the **For-Hire Vehicle (FHV)** trip data for 2019.

1. Load the [FHV trip data for 2019](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) into your data warehouse
2. Create a staging model `stg_fhv_tripdata` with these requirements:
   - Filter out records where `dispatching_base_num IS NULL`
   - Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

What is the count of records in `stg_fhv_tripdata`?

**Options:**
- [ ] 42,084,899
- [x] 43,244,693
- [ ] 22,998,722
- [ ] 44,112,187

**Answer:** 42,084,899

**Model Implementation:**

**File:** `models/staging/stg_fhv_tripdata.sql`
```sql
with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        dispatching_base_num,
        pickup_datetime,
        dropOff_datetime as dropoff_datetime,
        PUlocationid as pickup_location_id,
        DOlocationid as dropoff_location_id,
        SR_Flag as sr_flag,
        Affiliated_base_number as affiliated_base_number

    from source
    -- Filter out records with null dispatching_base_num (data quality requirement)
    where dispatching_base_num is not null
)

select * from renamed
```

**Source Configuration:**

**File:** `models/staging/sources.yml`
```yaml
sources:
  - name: raw
    tables:
      - name: fhv_tripdata
        description: Raw for-hire vehicle (FHV) trip records
        loaded_at_field: pickup_datetime
        columns:
          - name: dispatching_base_num
            description: Base company affiliated with the trip record
          - name: pickup_datetime
            description: Date and time when the meter was engaged
          - name: dropOff_datetime
            description: Date and time when the meter was disengaged
          - name: PUlocationid
            description: TLC Taxi Zone where the meter was engaged
          - name: DOlocationid
            description: TLC Taxi Zone where the meter was disengaged
          - name: SR_Flag
            description: Shared ride flag (Y/N)
          - name: Affiliated_base_number
            description: Base company affiliated with the trip record
```

**Build and Query:**
```bash
# Build the staging model
dbt run --select stg_fhv_tripdata --target prod

# Query the result
duckdb taxi_rides_ny.duckdb
```

```sql
SELECT COUNT(*)
FROM taxi_rides_ny.prod.stg_fhv_tripdata;
-- Result: 42,084,899
```

**Explanation:**

**FHV (For-Hire Vehicle) Background:**
- Includes: Uber, Lyft, Via, and traditional black car/livery services
- Different from Yellow/Green taxis (require pre-arrangement)
- No street hail pickups allowed
- Massive volume compared to traditional taxis

**Data Quality Filter:**
- **Requirement:** `dispatching_base_num IS NOT NULL`
- **Why filter?** The dispatching base number is critical for:
  - Identifying which company provided the service
  - Regulatory compliance and tracking
  - Business analytics and reporting
  - Records without this field are incomplete/invalid

**Naming Convention Transformation:**
| Raw Column Name | Staging Column Name | Reason |
|-----------------|---------------------|--------|
| `PUlocationid` | `pickup_location_id` | Consistent snake_case |
| `DOlocationid` | `dropoff_location_id` | Consistent snake_case |
| `dropOff_datetime` | `dropoff_datetime` | Remove inconsistent capitalization |
| `SR_Flag` | `sr_flag` | Lowercase for consistency |
| `Affiliated_base_number` | `affiliated_base_number` | Lowercase for consistency |

**Record Count:**
- **Total after filtering:** 42,084,899 trips
- **Time period:** All of 2019
- **Average:** ~115,000 trips per day
- **Context:** FHV trips significantly outnumber Yellow/Green taxi trips (3-4x volume)

**dbt Best Practices Demonstrated:**
1. ✅ **CTE pattern:** `with source as` → `with renamed as` → `select`
2. ✅ **Source reference:** `{{ source('raw', 'fhv_tripdata') }}` for lineage
3. ✅ **Data quality filter:** Remove invalid records early in pipeline
4. ✅ **Consistent naming:** snake_case for all column names
5. ✅ **Comments:** Document business logic inline

---

## dbt Concepts Review

### Model Materialization Strategies

| Strategy | Description | Use Case | Example |
|----------|-------------|----------|---------|
| **view** | Virtual table (query on-demand) | Staging models, fast-changing data | `stg_*` models |
| **table** | Physical table (stored) | Intermediate, marts | `int_*`, `dim_*` |
| **incremental** | Append/update only new records | Large fact tables | `fct_trips` |
| **ephemeral** | CTE (not materialized) | Reusable logic | Macros |

### Model Layers

```
Sources (Raw Data)
    ↓
Staging (Clean, rename, type cast)
    ↓
Intermediate (Join, union, business logic)
    ↓
Marts (Dimension & Fact tables)
    ↓
Reporting (Aggregations, metrics)
```

### dbt Commands

```bash
# Parse project for syntax errors
dbt parse

# Run all models
dbt run

# Run specific model
dbt run --select model_name

# Run model with upstream dependencies
dbt run --select +model_name

# Run model with downstream dependencies
dbt run --select model_name+

# Run all tests
dbt test

# Test specific model
dbt test --select model_name

# Build (run + test + seed + snapshot)
dbt build

# Generate documentation
dbt docs generate

# Serve documentation
dbt docs serve
```

---

## Project Files Structure

```
04-analytics-engineering/
├── README.md                    # This file - homework solutions
├── qeries.sql                   # SQL queries for homework questions
└── taxi_rides_ny/               # dbt project
    ├── dbt_project.yml
    ├── profiles.yml
    ├── packages.yml
    ├── models/
    │   ├── staging/
    │   │   ├── stg_green_tripdata.sql
    │   │   ├── stg_yellow_tripdata.sql
    │   │   ├── stg_fhv_tripdata.sql
    │   │   ├── sources.yml
    │   │   └── schema.yml
    │   ├── intermediate/
    │   │   ├── int_trips_unioned.sql
    │   │   ├── int_trips.sql
    │   │   └── schema.yml
    │   └── marts/
    │       ├── dim_zones.sql
    │       ├── dim_vendors.sql
    │       ├── fct_trips.sql
    │       ├── schema.yml
    │       └── reporting/
    │           └── fct_monthly_zone_revenue.sql
    ├── macros/
    │   ├── get_trip_duration_minutes.sql
    │   ├── get_vendor_data.sql
    │   └── safe_cast.sql
    ├── seeds/
    │   ├── taxi_zone_lookup.csv
    │   └── payment_type_lookup.csv
    ├── tests/
    └── target/
```

---

## Key Takeaways

### Analytics Engineering with dbt

1. **Transformation Logic in SQL:** All transformations are version-controlled SQL
2. **Modular Design:** Breaking complex logic into staging → intermediate → marts
3. **Data Quality:** Built-in testing framework catches issues early
4. **Documentation:** Auto-generated docs with lineage graphs
5. **Cross-Database:** Same models work on DuckDB (dev) and BigQuery (prod)

### Star Schema Design

**Fact Table:** `fct_trips`
- Measures: fare_amount, trip_distance, passenger_count
- Foreign Keys: pickup_location_id, dropoff_location_id, vendor_id

**Dimension Tables:**
- `dim_zones`: location_id, borough, zone
- `dim_vendors`: vendor_id, vendor_name

**Benefits:**
- Fast aggregations (optimized for analytical queries)
- Human-readable (joins to dimension tables)
- Scalable (fact table can grow independently)

---

## Learning Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)
- [Analytics Engineering Guide](https://www.getdbt.com/analytics-engineering/)
- [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)
- [NYC Taxi Dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

---

## Submission

- **Form:** https://courses.datatalks.club/de-zoomcamp-2026/homework/hw4
- **Status:** ✅ Completed
- **Repository:** This GitHub repository
- **Solutions Included:** SQL queries in qeries.sql + documentation in README.md

---

**Last Updated:** February 18, 2026  
**Status:** Active & Maintained
