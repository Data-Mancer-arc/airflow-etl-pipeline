from airflow import DAG
from airflow.decorators import task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from datetime import timedelta, datetime
import requests
import pandas as pd



with DAG(
     dag_id='market_etl',
     start_date=datetime(2025, 11, 7),
     schedule='@daily',
     catchup=True,
     default_args={
          'owner':'harsh',
          'retries': 1,
          'retry_delay': timedelta(minutes=5)
      },
) as dag:

   @task()
   def hit_polygon_api(**context):
        stock_ticker = 'AMZN'
        polygon_api_key = 'YPalEhpzEBj6Xp2atmtq2EQuB4ohzDa9'
        ds = context.get('ds')
        url = f'https://api.polygon.io/v1/open-close/{stock_ticker}/{ds}?adjusted=true&apiKey={polygon_api_key}'
        response = requests.get(url)
        return response.json()
   
   @task()
   def flatten_market_data(polygon_response, **context):
       columns = {
          'status': 'closed',
          'from': context.get('ds'),
          'symbol': 'AMZN',
          'open': None,
          'high': None,
          'low': None,
          'close': None,
          'volume': None
       }
       flattened_record = []
       for header_name, default_value in columns.items():
           flattened_record.append(polygon_response.get(header_name, default_value))
       flattened_dataframe = pd.DataFrame([flattened_record], columns=columns.keys())
       return flattened_dataframe

   @task()
   def load_market_data(flattened_dataframe):
        market_database_hook = SqliteHook('market_database_conn')
        market_database_conn = market_database_hook.get_sqlalchemy_engine()
        flattened_dataframe.to_sql(
           name = 'market_data',
           con = market_database_conn,
           if_exists = 'append',
           index = False
        )
        
        print(market_database_hook.get_records('SELECT * FROM market_data;'))
           
   
   raw_market_data = hit_polygon_api()
   transformed_market_data = flatten_market_data(raw_market_data)
   load_market_data(transformed_market_data)
   
   print(transformed_market_data)


       
