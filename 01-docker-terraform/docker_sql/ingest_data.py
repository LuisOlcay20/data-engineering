import os
import argparse
from time import time
import pandas as pd
import pyarrow.parquet as pq
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
    
   
    if url.endswith('.parquet'):
        parquet_file = 'output.parquet'
    else:
        raise ValueError("URL must point to a Parquet file")

   
    download_file(url, parquet_file)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

   
    parquet_iter = pq.ParquetFile(parquet_file).iter_batches(batch_size=100000)
    batch = next(parquet_iter)
    df = pd.DataFrame(batch.to_pandas())

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        try:
            t_start = time()
            
            batch = next(parquet_iter)
            df = pd.DataFrame(batch.to_pandas())

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('Inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='URL of the Parquet file')

    args = parser.parse_args()

    main(args)

