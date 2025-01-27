## ***Week 1 Homework***

`In this homework we'll prepare the environment 
and practice with Docker and SQL`


## Question 1. Understanding docker first run

Run the command 
```shell
>>> docker run -it --entrypoint bash python:3.12.8
>>> pip --version
```
**Solution :** `24.3.1`

## Question 2. Understanding Docker networking and docker-compose

**Solution :** `db:5432` 

## Question 3. Trip Segmentation Count

> [!TIP]
> As assumption: I used `lpep_dropoff_datetime` as the date column which makes more accure results than `lpep_pickup_datetime` .

```sql
-- running just the second question query which differentiate the answers
SELECT COUNT(*)
FROM public.green_taxi_trip
WHERE lpep_dropoff_datetime >= '2019-10-01 00:00:00'
  AND lpep_dropoff_datetime < '2019-11-01 00:00:00'
  AND trip_distance > 1
  AND trip_distance <= 3;
```

**Solution :** `104,802; 198,924; 109,603; 27,678; 35,189`

## Question 4. Largest trip for each day

```sql
SELECT lpep_pickup_datetime::DATE FROM public.green_taxi_trip WHERE trip_distance = (SELECT MAX(trip_distance) FROM public.green_taxi_trip);
```
**Solution :** `2019-01-31`

## Question 5. Three biggest pickup zones

```sql
SELECT zone_item.zone
FROM zone AS zone_item
join (
	SELECT pulocation_id
	FROM green_taxi_trip
	WHERE DATE(lpep_pickup_datetime) = '2019-10-18'
	GROUP BY pulocation_id
	HAVING SUM(total_amount) > 13000
	ORDER BY SUM(total_amount) DESC
) AS tops ON zone_item.location_id = tops.pulocation_id
```

**Solution :** `East Harlem North, East Harlem South, Morningside Heights`

## Question 6. Largest tip

```sql
SELECT t.tip_amount, t.dolocation_id, zdo.zone 
FROM public.green_taxi_trip t 
JOIN public.zone zpu ON t.pulocation_id = zpu.location_id 
JOIN public.zone zdo ON t.dolocation_id = zdo.location_id 
WHERE zpu.zone LIKE '%East Harlem North%' 
ORDER BY t.tip_amount DESC 
LIMIT(1);
```

**Solutiion :** `JFK Airport`

## Question 7. Terraform Workflow

**Solution :** `terraform init, terraform apply -auto-approve, terraform destroy`

