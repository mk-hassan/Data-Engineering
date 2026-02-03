import os
from enum import Enum

from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector


DATA_BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
TAXI_ZONES_DATA = "wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

class TaxiColor(Enum):
    GREEN = "green"
    YELLOW = "yellow"

@task(name="Download Data", retries=2)
def download_data(taxi_color: str, file_name: str):
	# make data directory if not exists
	if not os.path.exists("data"):
		os.makedirs("data")
	os.system(f'wget -qO- {DATA_BASE_URL}/{taxi_color}/{file_name}.gz | gunzip > data/{file_name}')

@task(name="Get File Name")
def get_file_name(taxi_color: str, month: int, year: int):
    # check if taxi_color is valid
	if taxi_color not in TaxiColor._value2member_map_:
		raise ValueError("Invalid taxi color. Choose 'green' or 'yellow'.")

	return f"{taxi_color}_tripdata_{year}-{month:02d}.csv"


@task(name="Create Yellow Taxi Schema")
def create_yellow_taxi_schema(taxi_color: str):
	if taxi_color != TaxiColor.YELLOW.value:
		raise ValueError("This function only handles yellow taxi data.")
	
	with SqlAlchemyConnector.load("taxi-trip-db-creds") as connector:
		connector.execute(
			"""
			CREATE TABLE IF NOT EXISTS yellow_tripdata (
				unique_row_id            text,
				filename                 text,
				"VendorID"               text,
				tpep_pickup_datetime     timestamp,
				tpep_dropoff_datetime    timestamp,
				passenger_count          integer,
				trip_distance            double precision,
				"RatecodeID"             text,
				store_and_fwd_flag       text,
				"PULocationID"           text,
				"DOLocationID"           text,
				payment_type             integer,
				fare_amount              double precision,
				extra                    double precision,
				mta_tax                  double precision,
				tip_amount               double precision,
				tolls_amount             double precision,
				improvement_surcharge    double precision,
				total_amount             double precision,
				congestion_surcharge     double precision
          	);
						
			CREATE TABLE IF NOT EXISTS yellow_tripdata_staging (
				unique_row_id            text,
				filename                 text,
				"VendorID"               text,
				tpep_pickup_datetime     timestamp,
				tpep_dropoff_datetime    timestamp,
				passenger_count          integer,
				trip_distance            double precision,
				"RatecodeID"             text,
				store_and_fwd_flag       text,
				"PULocationID"           text,
				"DOLocationID"           text,
				payment_type             integer,
				fare_amount              double precision,
				extra                    double precision,
				mta_tax                  double precision,
				tip_amount               double precision,
				tolls_amount             double precision,
				improvement_surcharge    double precision,
				total_amount             double precision,
				congestion_surcharge     double precision
          	);	
			"""
		)

@task(name="Populate Yellow Taxi Data")
def populate_yellow_taxi(file_name: str):
	COLUMNS = [
		"\"VendorID\"",
		"tpep_pickup_datetime",
		"tpep_dropoff_datetime",
		"passenger_count",
		"trip_distance",
		"\"RatecodeID\"",
		"store_and_fwd_flag",
		"\"PULocationID\"",
		"\"DOLocationID\"",
		"payment_type",
		"fare_amount",
		"extra",
		"mta_tax",
		"tip_amount",
		"tolls_amount",
		"improvement_surcharge",
		"total_amount",
		"congestion_surcharge"
	]

	with SqlAlchemyConnector.load("taxi-trip-db-creds") as connector:
		connector.execute(
			f"""
			COPY yellow_tripdata_staging ({', '.join(COLUMNS)})
			FROM '/data/{file_name}'
			WITH (FORMAT CSV, HEADER);

			UPDATE yellow_tripdata_staging
			SET 
				unique_row_id = md5(
				COALESCE(CAST("VendorID" AS text), '') ||
				COALESCE(CAST(tpep_pickup_datetime AS text), '') || 
				COALESCE(CAST(tpep_dropoff_datetime AS text), '') || 
				COALESCE("PULocationID", '') || 
				COALESCE("DOLocationID", '') || 
				COALESCE(CAST(fare_amount AS text), '') || 
				COALESCE(CAST(trip_distance AS text), '')      
				),
            filename = '{file_name}';
						
			MERGE INTO yellow_tripdata AS T
			USING yellow_tripdata_staging AS S
			ON T.unique_row_id = S.unique_row_id
			WHEN NOT MATCHED THEN
			INSERT (
				unique_row_id, filename, "VendorID", tpep_pickup_datetime, tpep_dropoff_datetime,
				passenger_count, trip_distance, "RatecodeID", store_and_fwd_flag, "PULocationID",
				"DOLocationID", payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
				improvement_surcharge, total_amount, congestion_surcharge
			)
			VALUES (
				S.unique_row_id, S.filename, S."VendorID", S.tpep_pickup_datetime, S.tpep_dropoff_datetime,
				S.passenger_count, S.trip_distance, S."RatecodeID", S.store_and_fwd_flag, S."PULocationID",
				S."DOLocationID", S.payment_type, S.fare_amount, S.extra, S.mta_tax, S.tip_amount, S.tolls_amount,
				S.improvement_surcharge, S.total_amount, S.congestion_surcharge
			);

			TRUNCATE TABLE yellow_tripdata_staging;
			"""
		)


@task(name="Create Green Taxi Schema")
def create_green_taxi_schema(taxi_color: str):
	if taxi_color != TaxiColor.GREEN.value:
		raise ValueError("This function only handles green taxi data.")
	
	with SqlAlchemyConnector.load("taxi-trip-db-creds") as connector:
		connector.execute(
			"""
			CREATE TABLE IF NOT EXISTS green_tripdata (
				unique_row_id            text,
				filename                 text,
				"VendorID"               text,
				lpep_pickup_datetime     timestamp,
				lpep_dropoff_datetime    timestamp,
				store_and_fwd_flag       text,
				"RatecodeID"             text,
				"PULocationID"           text,
				"DOLocationID"           text,
				passenger_count          integer,
				trip_distance            double precision,
				fare_amount              double precision,
				extra                    double precision,
				mta_tax                  double precision,
				tip_amount               double precision,
				tolls_amount             double precision,
				ehail_fee                double precision,
				improvement_surcharge    double precision,
				total_amount             double precision,
				payment_type             integer,
				trip_type                integer,
				congestion_surcharge     double precision
          	);
					
			CREATE TABLE IF NOT EXISTS green_tripdata_staging (
				unique_row_id            text,
				filename                 text,
				"VendorID"               text,
				lpep_pickup_datetime     timestamp,
				lpep_dropoff_datetime    timestamp,
				store_and_fwd_flag       text,
				"RatecodeID"             text,
				"PULocationID"           text,
				"DOLocationID"           text,
				passenger_count          integer,
				trip_distance            double precision,
				fare_amount              double precision,
				extra                    double precision,
				mta_tax                  double precision,
				tip_amount               double precision,
				tolls_amount             double precision,
				ehail_fee                double precision,
				improvement_surcharge    double precision,
				total_amount             double precision,
				payment_type             integer,
				trip_type                integer,
				congestion_surcharge     double precision
          	);
			"""
		)

@task(name="Populate Green Taxi Data")
def populate_green_taxi(file_name: str):
	COLUMNS = [
		"\"VendorID\"",
		"lpep_pickup_datetime",
		"lpep_dropoff_datetime",
		"store_and_fwd_flag",
		"\"RatecodeID\"",
		"\"PULocationID\"",
		"\"DOLocationID\"",
		"passenger_count",
		"trip_distance",
		"fare_amount",
		"extra",
		"mta_tax",
		"tip_amount",
		"tolls_amount",
		"ehail_fee",
		"improvement_surcharge",
		"total_amount",
		"payment_type",
		"trip_type",
		"congestion_surcharge"
	]

	with SqlAlchemyConnector.load("taxi-trip-db-creds") as connector:
		connector.execute(
			f"""
			COPY green_tripdata_staging ({", ".join(COLUMNS)})
			FROM '/data/{file_name}'
			WITH (FORMAT csv, HEADER true);

			UPDATE green_tripdata_staging
			SET 
				unique_row_id = md5(
				COALESCE(CAST("VendorID" AS text), '') ||
				COALESCE(CAST(lpep_pickup_datetime AS text), '') || 
				COALESCE(CAST(lpep_dropoff_datetime AS text), '') || 
				COALESCE("PULocationID", '') || 
				COALESCE("DOLocationID", '') || 
				COALESCE(CAST(fare_amount AS text), '') || 
				COALESCE(CAST(trip_distance AS text), '')      
				),
            filename = '{file_name}';
						
			MERGE INTO green_tripdata AS T
			USING green_tripdata_staging AS S
			ON T.unique_row_id = S.unique_row_id
			WHEN NOT MATCHED THEN
			INSERT (
				unique_row_id, filename, "VendorID", lpep_pickup_datetime, lpep_dropoff_datetime,
				store_and_fwd_flag, "RatecodeID", "PULocationID", "DOLocationID", passenger_count,
				trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee,
				improvement_surcharge, total_amount, payment_type, trip_type, congestion_surcharge
			)
			VALUES (
				S.unique_row_id, S.filename, S."VendorID", S.lpep_pickup_datetime, S.lpep_dropoff_datetime,
				S.store_and_fwd_flag, S."RatecodeID", S."PULocationID", S."DOLocationID", S.passenger_count,
				S.trip_distance, S.fare_amount, S.extra, S.mta_tax, S.tip_amount, S.tolls_amount, S.ehail_fee,
				S.improvement_surcharge, S.total_amount, S.payment_type, S.trip_type, S.congestion_surcharge
			);

			TRUNCATE TABLE green_tripdata_staging;
			"""
		)


@flow(name="Yellow Taxi ETL Subflow")
def yellow_taxi_etl_subflow(file_name: str):
	"""Subflow for yellow taxi ETL process"""
	create_yellow_taxi_schema(TaxiColor.YELLOW.value)
	populate_yellow_taxi(file_name)

@flow(name="Green Taxi ETL Subflow")
def green_taxi_etl_subflow(file_name: str):
	"""Subflow for green taxi ETL process"""
	create_green_taxi_schema(TaxiColor.GREEN.value)
	populate_green_taxi(file_name)

@flow(name="NYC Taxi Data Pipeline", description="Main workflow for NYC taxi data ETL")
def etl_process(taxi_color: TaxiColor, month: int, year: int):
	"""ETL orchestration subflow for taxi data"""
	file_name = get_file_name(taxi_color.value, month, year)
	download_data(taxi_color.value, file_name)
	
	if taxi_color == TaxiColor.YELLOW:
		yellow_taxi_etl_subflow(file_name)
	elif taxi_color == TaxiColor.GREEN:
		green_taxi_etl_subflow(file_name)
	else:
		raise ValueError("Invalid taxi color")

if __name__ == "__main__":
	etl_process.serve(name="nyc-taxi-etl", limit=1, print_starting_message=True)