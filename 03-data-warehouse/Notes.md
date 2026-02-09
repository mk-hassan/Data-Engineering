# Notes for data warehouse

## Partitioning

Paritioing in OLAP is like indexing in OLTP, you group rows by certain column

- You can parition using only a **single** column
- Partitions:
  - Time-unit (Timestamp column exists on your data)
    This represents actual data time your data cares about
  - Ingestion time (_PARTITIONTIME, _PARTITIONDATE)
    * you have no control on these data, it's internal in bigquery
    * depend on this paritioning only if you don't have a reliable event timestamp
  - Integer range partitioning
    ```sql
    PARTITION BY RANGE_BUCKET(user_id, GENERATE_ARRAY(0, 1_000_000, 10_000))
    ```
    Partitions are defined by ranges of integers instead of dates \
    0–9999 \
    10000–19999 \
    20000–29999 \
    ...

    * Easy to design poorly (hot partitions)
    * less common

### Time stamp based partitioning

- most common way
- most predicatable and controlled
- **`DAY`** is the default partitioning key
- Other keys exists like `Hourly`, `Monthly`, `Yearly`, and of course `Daily` (default)

> [!WARNING] Partitioning LIMIT
> Big query limits you to 4000 partitions, when you partition `Hourly` you will exceed this limit within 5 months 
>
> Hourly parition isn't just 24 paritions in total, but it's 24 paritions per day (it's like Yearly -> Monthly -> Daily)
>
> So based on that you will need to define some expiting strategy for the partitions 
> ```sql
> ALTER TABLE dataset.events
> SET OPTIONS (
>   partition_expiration_days = 90
> );
> ```


## Clustering

- You can use up to 4 columns
- It impreoves (Filter and Aggregate queries)
- It's a fancy way to say sort the columns data as the columns appear in order
- columns  order is crucial, clustering on a, b, c, and d isnot the same as clustering on b, a, d, and c 
- can't use repeated columns

> [!TIP] Overhead
> Partitioning and clustering on a small amount of data is an overhead, take care that partitioning and clustering have meta data and their own complexities so you will experience downgrade in performance for small data sizes


## Clustering VS Partitioning
- paritioning cost known up front \
  You know the expected cost of your query before executing it which is not possible in clustering. This comes with the advantage of defining big query to prevent queries costs more than a certain limit

- Because as we mentioned paritions are limited to 4000, so if the column you want to parition upon using has high cardinality clustering is more suitable

-  Use clustering to get fine-grained filtering inside partitions
> [!NOTE] Granularity means:
> How detailed or fine-grained something is

so use clustering when you need to fine grain the data you are quering, that is somthing partitioning can't achieve

so you want details down to the level hours, this much details means high granularity which is some thing partitioning can't achieve (limited partitions).

- If you update the data frequently and this update causes changes in multiple partitions so clustering is more suitable in this case.
