"""
DAG forecasts_result_calc_dag считает результаты прогнозов
"""

#	imports

import logging as _log 
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)) + '/modules')  
from ForecastDb import ForecastDb
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DB_CONN_ID = "stocks_forecasts" 

def _get_forecasts(**context):
  fDb = ForecastDb()
  
  forecasts = fDb.getNotCalcedForecasts()
  context["task_instance"].xcom_push(key="forecasts", value=forecasts)

def _calc_forecasts_result(**context):
  fDb = ForecastDb()

  forecasts = context["task_instance"].xcom_pull(task_ids="get_forecasts", key="forecasts")

  calcedForecasts = {}

  for fc in forecasts:
    fcFinal = fDb.getForecastFinalData(fc)

    if not fcFinal:
      continue

    key = str(fcFinal['start_date']) + '-' + fcFinal['ticker']

    if key not in calcedForecasts:
      calcedForecasts[key] = fcFinal

  context["task_instance"].xcom_push(key="forecasts", value=calcedForecasts)

def _write_forecasts_result(**context): 
  fDb = ForecastDb()

  forecasts = context["task_instance"].xcom_pull( task_ids="calc_forecasts_result", key="forecasts")
  fDb.saveForecasts(forecasts)
   
# dag init 

args = {'owner': 'airflow'}
with DAG (
   dag_id = "forecasts_result_calc_dag", 
   default_args = args, 
   description = 'DAG Расчёт результатов прогнозов', 
   start_date = datetime(2024, 11, 7), 
   tags = ['forecasts', '@d_ivanov'], 
   schedule = '5 */4 * * *', 
   catchup = False, 
   max_active_runs = 1, 
   max_active_tasks = 1, 
) as dag:
  # task init
  get_forecasts = PythonOperator( 
    task_id='get_forecasts', 
    python_callable=_get_forecasts, 
  )

  calc_forecasts_result = PythonOperator(
     task_id = 'calc_forecasts_result', 
     python_callable = _calc_forecasts_result, 
  )

  write_forecasts_result = PythonOperator( 
    task_id = 'write_forecasts_result', 
    python_callable = _write_forecasts_result, 
    provide_context = True, 
    trigger_rule = "all_success", 
  )


get_forecasts >> calc_forecasts_result >> write_forecasts_result