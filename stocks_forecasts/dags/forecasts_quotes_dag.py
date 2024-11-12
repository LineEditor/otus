"""
DAG forecasts_quotes_dag получает котировки по текущим прогнозам
"""

#	imports

import logging as _log 
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)) + '/modules')  
from ForecastDb import ForecastDb
from TInvestAPI import TInvestAPI

from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DB_CONN_ID = "stocks_forecasts" 

def _get_quotes_maxdates(**context):
  fDb = ForecastDb()
  
  forecasts = fDb.getActualForecasts()
   
  tickers = [] 
  for fc in forecasts: 
     tickers.append(fc['ticker'])
  
  quotesMaxDates = fDb.getQuotesMaxDates(tickers) 

  context["task_instance"].xcom_push(key="quotes", value=quotesMaxDates)  


def _get_quotes(**context):
  fDb = ForecastDb()
  client = TInvestAPI()
  stocks = fDb.getStocks()
  client.setStocks(stocks)
  
  qoutesMaxDates = context["task_instance"].xcom_pull(task_ids="get_quotes_maxdates", key="quotes")

  allQuotes = []
  
  if qoutesMaxDates:
    for qmd in qoutesMaxDates:
      qmd['end_date'] = None
      quotes = client.getPricesForForecast(qmd, "CANDLE_INTERVAL_10_MIN")

      if quotes:
        allQuotes += quotes


  context["task_instance"].xcom_push(key="quotes", value=allQuotes)

def _write_quotes(**context): 
  fDb = ForecastDb()

  quotes = context["task_instance"].xcom_pull( task_ids="get_quotes", key="quotes")
  fDb.addQuotes(quotes)
   
# dag init 

args = {'owner': 'airflow'}
with DAG (
   dag_id = "forecasts_quotes_dag", 
   default_args = args, 
   description = 'DAG получение котировок текущих прогнозов', 
   start_date = datetime(2024, 11, 7), 
   tags = ['forecasts', '@d_ivanov'], 
   schedule = '*/10 * * * *', 
   catchup = False, 
   max_active_runs = 1, 
   max_active_tasks = 1, 
) as dag:
  # task init
  get_quotes_maxdates = PythonOperator( 
    task_id = 'get_quotes_maxdates', 
    python_callable = _get_quotes_maxdates, 
  )

  get_quotes = PythonOperator(
     task_id = 'get_quotes', 
     python_callable = _get_quotes, 
  )

  write_quotes = PythonOperator( 
    task_id = 'write_quotes', 
    python_callable = _write_quotes, 
    provide_context = True, 
    trigger_rule = "all_success", 
  )


get_quotes_maxdates >> get_quotes >> write_quotes