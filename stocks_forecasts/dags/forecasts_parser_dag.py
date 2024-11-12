"""
DAG forecasts_parser_dag скачивает новые прогнозы и заносит в БД
"""

#	imports

import logging as _log 
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)) + '/modules')  
from datetime import date, datetime, timedelta
from TPulseParser import TPulseParser

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DB_CONN_ID = "stocks_forecasts" 

def _parse_forecasts(**context):
  TPP = TPulseParser()
  TPP.run()
 
# dag init 

args = {'owner': 'airflow'}
with DAG (
   dag_id = "forecasts_parser_dag", 
   default_args = args, 
   description = 'DAG скачивает новые прогнозы и заносит в БД', 
   start_date = datetime(2024, 11, 7), 
   tags = ['forecasts', '@d_ivanov'], 
   schedule = '5 */4 * * *', 
   catchup = False, 
   max_active_runs = 1, 
   max_active_tasks = 1, 
) as dag:
  # task init
  parse_forecasts = PythonOperator( 
    task_id='parse_forecasts', 
    python_callable=_parse_forecasts, 
  )

parse_forecasts