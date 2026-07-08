#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import pyarrow.parquet as pq
import fsspec
import click
from sqlalchemy import create_engine
from tqdm import tqdm

#function for data ingestion from parquet file
def ingest_data_parquet(url: str, engine: str, target_table: str, chunksize: int):

    with fsspec.open(url, mode="rb") as f:
        parquet_file = pq.ParquetFile(f)
        first = True

        for batch in tqdm(parquet_file.iter_batches(batch_size=chunksize)):
            df_chunk = batch.to_pandas()

            if first:
                df_chunk.head(n=0).to_sql(name=target_table, con=engine, if_exists='replace', index=False)
                first = False

            df_chunk.to_sql(
                name=target_table,
                con=engine, if_exists='append',
                index=False
            )


#function for data ingestion from csv file
def ingest_data_csv(url: str, engine: str, target_table: str, chunksize: int):
    df_iter = pd.read_csv(
        filepath_or_buffer=url,
        iterator=True,
        chunksize=chunksize
    )

    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(n=0).to_sql(
                name=target_table,
                con=engine,
                if_exists='replace',
                index=False
            )
            first = False

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='append',
            index=False
        )

#CLI interactivity via click package
@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5433', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--datasource', default='csv', help='PostgreSQL database name')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for ingestion')
@click.option('--target-table', default='taxi_zone', help='Target table name')

def main(pg_user, pg_pass, pg_host, pg_port, pg_db, datasource, year, month, chunksize, target_table):

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    if datasource == 'parquet':
        url_prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
        url = f'{url_prefix}/green_tripdata_{year:04d}-{month:02d}.parquet'

        ingest_data_parquet(
            url=url,
            engine=engine,
            target_table=target_table,
            chunksize=chunksize
        )

    if datasource == 'csv':
        url_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc'
        url = f'{url_prefix}/taxi_zone_lookup.csv'

        ingest_data_csv(
            url=url,
            engine=engine,
            target_table=target_table,
            chunksize=chunksize
        )

if __name__ == '__main__':
    main()
    
    

