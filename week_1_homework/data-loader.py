import os
import argparse
import pandas as pd

from sqlalchemy import create_engine

GREEN_TAXI_TRIP_DATA = "wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
TAXI_ZONES_DATA = "wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

GREEN_TAXI_TRIP_DATA_OUTPUT = "./taxiData/green_taxi_data.parquet"
TAXI_ZONES_DATA_OUTPUT = "./taxiData/taxi_zone_lookup.csv"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--passcode', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')

    args = parser.parse_args()

    os.system(f'wget {GREEN_TAXI_TRIP_DATA} -O {GREEN_TAXI_TRIP_DATA_OUTPUT}')
    os.system(f'wget {TAXI_ZONES_DATA} -O {TAXI_ZONES_DATA_OUTPUT}')
    
    engine = create_engine(f'postgresql://{args.user}:{args.passcode}@{args.host}:{args.port}/ny_taxi')

    try:
        engine.connect()
    except Exception as e:
        e.show()
        exit(1)
    
    taxi_data = pd.read_parquet("./taxiData/green_taxi_data.parquet", engine='pyarrow')
    taxi_data.rename(columns = {'VendorID':'vendor_id', 'RatecodeID':'rate_code_id', 
                            'PULocationID':'pulocation_id', 'DOLocationID':'dolocation_id'
                           }, inplace=True)
    taxi_data.to_sql(name="green_taxi_trip", con=engine, if_exists='replace', index=False)

    zone_data = pd.read_csv("./taxiData/taxi_zone_lookup.csv")
    zone_data.rename(columns = {'LocationID':'location_id', 'Borough':'borough', 'Zone':'zone'}, inplace=True)
    zone_data.to_sql(name="zone", con=engine, if_exists='replace', index=False)