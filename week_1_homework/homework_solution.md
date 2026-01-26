## ***Week 1 Homework***

`In this homework we'll prepare the environment 
and practice with Docker and SQL`


## Question 1. Understanding docker first run

Run the command 
```shell
>>> docker run -it --entrypoint bash python:3.13
>>> pip --version
```
**Solution :** `25.3`

## Question 2. Understanding Docker networking and docker-compose

**Solution :** `postgres:5432` 

## Question 3. Trip Segmentation Count

```sql
select 
	COUNT(*)
from 
	green_taxi_trip 
where 
	lpep_pickup_datetime >= '2025-11-01' and 
	lpep_pickup_datetime < '2025-12-01' and 
	trip_distance <= 1;
```

**Solution :** `8,007`

## Question 4. Largest trip for each day

```sql
select 
	Date(lpep_pickup_datetime)
from 
	green_taxi_trip
where 
	trip_distance < 100
order by trip_distance desc
limit 1;
```
**Solution :** `2025-11-14`

## Question 5. Three biggest pickup zones

```sql
select 
	zone."zone"
from 
	green_taxi_trip
join 
	zone on zone.location_id = green_taxi_trip.pulocation_id 
where 
	lpep_pickup_datetime > '2025-11-17' and lpep_pickup_datetime < '2025-11-19'
group by zone."zone" 
order by SUM(total_amount) desc 
limit 1;
```

**Solution :** `East Harlem North`

## Question 6. Largest tip

```sql
select 
	dozone."zone"
from 
	green_taxi_trip
join 
	zone as puzone on puzone.location_id = green_taxi_trip.pulocation_id 
join 
	zone as dozone on dozone.location_id  = green_taxi_trip.dolocation_id 
where 
	puzone."zone"  = 'East Harlem North' and lpep_pickup_datetime >= '2025-11-01' and lpep_pickup_datetime < '2025-12-01'
order by tip_amount desc
limit 1;
```

**Solutiion :** `Yorkville West`

## Question 7. Terraform Workflow

**Solution :** `terraform init, terraform apply -auto-approve, terraform destroy`

