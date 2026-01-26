select COUNT(*)
from green_taxi_trip 
where lpep_pickup_datetime >= '2025-11-01' and lpep_pickup_datetime < '2025-12-01' and trip_distance <= 1;

select Date(lpep_pickup_datetime)
from green_taxi_trip
where trip_distance < 100
order by trip_distance desc
limit 1;

select zone."zone"
from green_taxi_trip
join zone on zone.location_id = green_taxi_trip.pulocation_id 
where lpep_pickup_datetime > '2025-11-17' and lpep_pickup_datetime < '2025-11-19'
group by zone."zone" 
order by SUM(total_amount) desc 
limit 1;

select dozone."zone"
from green_taxi_trip
join zone as puzone on puzone.location_id = green_taxi_trip.pulocation_id 
join zone as dozone on dozone.location_id  = green_taxi_trip.dolocation_id 
where puzone."zone"  = 'East Harlem North' and lpep_pickup_datetime >= '2025-11-01' and lpep_pickup_datetime < '2025-12-01'
order by tip_amount desc
limit 1;