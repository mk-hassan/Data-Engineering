with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        dispatching_base_num,
        pickup_datetime,
        dropOff_datetime as dropoff_datetime,
        PUlocationid as  pickup_location_id,
        DOlocationid as dropoff_location_id,
        SR_Flag as sr_flag,
        Affiliated_base_number as affiliated_base_number

    from source
    -- Filter out records with null dispatching_base_num (data quality requirement)
    where dispatching_base_num is not null
)

select * from renamed