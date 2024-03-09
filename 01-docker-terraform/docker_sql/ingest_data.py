import pandas as pd
import pyarrow.parquet as pq
import argparse
from sqlalchemy import create_engine
import requests

def download_file(url, filename):
    response = requests.get(url)
    response.raise_for_status()  
    with open(filename, 'wb') as f:
        f.write(response.content)
def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_name = 'yellow_tripdata_2021-01.parquet'

    download_file(url, parquet_name)

    df = pq.read_table(parquet_name).to_pandas()

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    chunk_size = 100000
    total_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)

    for i, start in enumerate(range(0, len(df), chunk_size)):
        chunk = df[start:start + chunk_size]
        chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        print(f"Chunk {i+1}/{total_chunks} procesado y cargado en la base de datos")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data to PG')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()
    main(args)
