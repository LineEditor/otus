import sys
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
import psycopg2.extras as pg_extras
from datetime import datetime, timedelta
from dotenv import dotenv_values
import logging as _log 
import os
from pathlib import Path

class ForecastDb:
  conn = None
  indicators = []
  stocks = []
  DB_CONN_ID = "stocks_forecasts"

  def __init__(self):
    try:
      dest = PostgresHook(postgres_conn_id=self.DB_CONN_ID)
      self.conn = dest.get_conn() 
    except Exception as err:
      _log.info(type(err))
      _log.info(err.args) 
      _log.info(err)  
    return

  #получение актуальных прогнозов
  def getActualForecasts(self, analyst_company_id = 0):
    analystCond = ""
    analyst_company_id = int(analyst_company_id)

    if analyst_company_id > 0:
      analystCond = f" analyst_company_id = {analyst_company_id} AND "
    cursor = self.conn.cursor(cursor_factory=pg_extras.DictCursor)
    cursor.execute(f"select * from stocks_forecasts sf where {analystCond} forecast_result is NULL and end_date >= CURRENT_DATE and price_start is not NULL order by id ASC")
    column_names = [desc[0] for desc in cursor.description]
    forecasts = cursor.fetchall()
    cursor.close()

    result = []
    if type(forecasts) is list:
      for fc in forecasts:
        result.append(dict(zip(column_names, fc)))
    
    return result  

  #получение ещё нерассчитанных прогнозов, у которых end_date уже меньше текущей
  def getNotCalcedForecasts(self, analyst_company_id = 0):
    analystCond = ""
    analyst_company_id = int(analyst_company_id)

    if analyst_company_id > 0:
      analystCond = f" analyst_company_id = {analyst_company_id} AND "
    cursor = self.conn.cursor(cursor_factory=pg_extras.DictCursor)
    cursor.execute(f"select * from stocks_forecasts sf where {analystCond} forecast_result is NULL and end_date < CURRENT_DATE and price_start is not NULL order by id ASC")
    column_names = [desc[0] for desc in cursor.description]
    forecasts = cursor.fetchall()
    cursor.close()

    result = []
    if type(forecasts) is list:
      for fc in forecasts:
        result.append(dict(zip(column_names, fc)))
    
    return result  
  
  #Получение последних дат котировок для заданных тикеров
  def getQuotesMaxDates(self, tickers):
    if not tickers:
      return None

    tickersStr = "'" + "','".join(tickers) + "'"
    
    sql = f"select ticker, max(datetime_end) as date_inserted from stocks_quotes where ticker in ({tickersStr}) group by ticker"

    cursor = self.conn.cursor(cursor_factory=pg_extras.DictCursor)
    cursor.execute(sql)
    column_names = [desc[0] for desc in cursor.description]
    maxdates = cursor.fetchall()
    cursor.close()

    result = []
    if type(maxdates) is list:
      for md in maxdates:
        result.append(dict(zip(column_names, md)))
    
    return result  


  # Метод возвращает словарь индикаторов
  def getIndicators(self):
    cursor = self.conn.cursor(cursor_factory=pg_extras.DictCursor)
    cursor.execute('SELECT id, indicator_name, success_proc FROM indicators')
    column_names = [desc[0] for desc in cursor.description]
    indicators = cursor.fetchall()
    cursor.close()

    result = {}
    if type(indicators) is list:
      for indicator in indicators:
        ind = dict(zip(column_names, indicator))
        result[ind['indicator_name']] = ind
    
    return result

  # Метод добавляет индикаторы в словарь
  def addIndicators(self, indicators):
    sql = """
      INSERT INTO indicators (indicator_name, success_proc)
       VALUES %s
      ON CONFLICT (indicator_name) DO UPDATE
      SET
        success_proc = excluded.success_proc,
        updated_at=NOW()    

    """
    values_list = []
    
    for row in indicators:
      values = (row['indicator_name'], row['success_proc'])
      values_list.append(values)

    cursor = self.conn.cursor(cursor_factory=pg_extras.DictCursor)
    pg_extras.execute_values(cursor, sql, values_list)
    self.conn.commit()
    cursor.close()
    
    self.indicators = self.getIndicators()
    return self.indicators

  # Метод возвращает словарь акций
  def getStocks(self):
    cursor = self.conn.cursor(cursor_factory=pg_extras.DictCursor)
    cursor.execute('SELECT ticker, figi, stock_name, class_code FROM stocks')
    column_names = [desc[0] for desc in cursor.description]
    stocks = cursor.fetchall()
    cursor.close()

    result = {}
    if type(stocks) is list:
      for stock in stocks:
        st = dict(zip(column_names, stock))
        result[st['ticker']] = st
    
    return result
  
  # Метод добавляет компании в словарь
  def addStocks(self, stocks):

    sql = """
    INSERT INTO stocks(ticker, figi, stock_name, country_id, currency, lot, class_code, sector)
    VALUES %s
    ON CONFLICT (ticker) DO UPDATE
    SET
        figi = excluded.figi,
        stock_name = excluded.stock_name, 
        country_id = excluded.country_id, 
        currency = excluded.currency, 
        lot = excluded.lot,
        class_code = excluded.class_code,
        sector = excluded.sector,        
        updated_at=NOW()    
    """
    
    chunk_size = 1000
    
    while stocks:
      #Разбиваем на чанки, чтобы записывать в БД по частям
      chunk, stocks = stocks[:chunk_size], stocks[chunk_size:]
      values_list = []    
      for row in chunk:
        values = (row['ticker'], row['figi'], row['stock_name'], 
                  row['country_id'], row['currency'], row['lot'],
                  row['class_code'], row['sector'])
        values_list.append(values)

        cursor = self.conn.cursor(cursor_factory=pg_extras.DictCursor)
        pg_extras.execute_values(cursor, sql, values_list)
        self.conn.commit()
        cursor.close()
    
    self.stocks = self.getStocks()
    return self.stocks
  
  # Метод добавляет прогнозы
  def saveForecasts(self, forecasts):

    sql = """
INSERT INTO stocks_forecasts(
  analyst_company_id,
  ticker,
  start_date,
  end_date,
  forecast_direction,   
  price_start,
  price_forecast,
  price_delta_forecast,
  price_end,
  price_max_real,
  price_max_date,
  price_min_real,
  price_min_date,
  end_real_price_delta,
  price_max_delta,
  price_min_delta,
  forecast_result,
  external_id,
  indicator_id
)
VALUES %s
  ON CONFLICT (analyst_company_id, ticker, start_date) DO UPDATE
SET
  end_date = excluded.end_date,
  forecast_direction = excluded.forecast_direction,   
  price_start = excluded.price_start,
  price_forecast = excluded.price_forecast,
  price_delta_forecast = excluded.price_delta_forecast,
  price_end = excluded.price_end,
  price_max_real = excluded.price_max_real,
  price_max_date = excluded.price_max_date,
  price_min_real = excluded.price_min_real,
  price_min_date = excluded.price_min_date,
  end_real_price_delta = excluded.end_real_price_delta,
  price_max_delta = excluded.price_max_delta,
  price_min_delta = excluded.price_min_delta,
  forecast_result = excluded.forecast_result,
  external_id = excluded.external_id,
  indicator_id = excluded.indicator_id,  
  updated_at=NOW()    
"""
  
    values_list = []
    
    for row in forecasts.values():      
      values = (row['analyst_company_id'], row['ticker'], str(row['start_date']), 
                str(row['end_date']),
                row['forecast_direction'],   
                row['price_start'],
                row['price_forecast'],
                row['price_delta_forecast'],
                row['price_end'],
                row['price_max_real'],
                row['price_max_date'],
                row['price_min_real'],
                row['price_min_date'],
                row['end_real_price_delta'],
                row['price_max_delta'],
                row['price_min_delta'],
                row['forecast_result'],
                row['external_id'],
                row['indicator_id']
                )
      values_list.append(values)

    cursor = self.conn.cursor(cursor_factory=pg_extras.DictCursor)
    pg_extras.execute_values(cursor, sql, values_list)
    self.conn.commit()
    cursor.close()

  # Метод добавляет котировки
  def addQuotes(self, quotes):

    sql = """
INSERT INTO stocks_quotes(
   ticker,
   datetime_start,
   datetime_end,
   open_price,
   close_price,   
   high_price,   
   low_price,   
   volume
)
VALUES %s
  ON CONFLICT (ticker, datetime_start) DO UPDATE
SET
  open_price = excluded.open_price,
  datetime_end = excluded.datetime_end,
  close_price = excluded.close_price,   
  high_price = excluded.high_price,   
  low_price = excluded.low_price,   
  volume = excluded.volume,
  updated_at = NOW()    
"""
  
    values_list = []
    wasQuote = {};
    for i, row in enumerate(quotes):
      key = row['ticker'] + str(row['datetime_start']);

      if key in wasQuote:
        continue

      wasQuote[key] = 1
      
      values = (row['ticker'], str(row['datetime_start']),  str(row['datetime_end']),
                row['open_price'],
                row['close_price'],
                row['high_price'],   
                row['low_price'],
                row['volume']
                )
      values_list.append(values)

    cursor = self.conn.cursor(cursor_factory=pg_extras.DictCursor)
    pg_extras.execute_values(cursor, sql, values_list)
    self.conn.commit()
    cursor.close()

  # Метод возвращает все external_id для заданного analyst_company_id
  def getExternalIdsForAnalystCompany(self, analystCompanyId ):
    analystCompanyId = int(analystCompanyId)
    cursor = self.conn.cursor(cursor_factory=pg_extras.DictCursor)
    cursor.execute(f"SELECT distinct(external_id) as external_id  FROM stocks_forecasts WHERE analyst_company_id = {analystCompanyId}")
    externalIds = cursor.fetchall()
    cursor.close()

    result = {}
    if type(externalIds) is list:
      for extId in externalIds:
        result[extId['external_id']] = extId['external_id']
    
    return result    
  
  #Метод добавляет запись в parser_log
  def insertIntoParserLog(self, data):
    sql = """
INSERT INTO parser_log(
    analyst_company_id,
    start_time
)
VALUES (%s, %s)
RETURNING id
"""  
    cursor = self.conn.cursor()
    cursor.execute(sql, [data['analyst_company_id'], str(data['start_time'])])
    log_id = cursor.fetchone()[0]    
    self.conn.commit()
    cursor.close()    

    return log_id
  

  # Метод обновляет запись в parser_log
  def updateParserLog(self, data):

    sql = """
INSERT INTO parser_log(
   id,
   analyst_company_id,
   start_time,
   end_time,
   forecast_datetime,   
   external_id
)
VALUES (%s, %s, %s, %s, %s, %s)
  ON CONFLICT (id) DO UPDATE
SET
  analyst_company_id = excluded.analyst_company_id,
  start_time = excluded.start_time,
  end_time = excluded.end_time,
  forecast_datetime = excluded.forecast_datetime,   
  external_id = excluded.external_id,
  updated_at = NOW()    
"""

    cursor = self.conn.cursor()
    cursor.execute(sql, [data['id'], data['analyst_company_id'], str(data['start_time']), str(data['end_time']), 
                         data['forecast_datetime'], data['external_id']])
    self.conn.commit()
    cursor.close()    


  # Метод возвращает последний удачно спарсенный прогноз
  def getLastParsedForecast(self, analystCompanyId):
    analystCompanyId = int(analystCompanyId)
    cursor = self.conn.cursor(cursor_factory=pg_extras.DictCursor)
    cursor.execute(f"SELECT id, start_time, end_time, forecast_datetime FROM parser_log WHERE analyst_company_id = {analystCompanyId} and forecast_datetime is not NULL ORDER BY id DESC LIMIT 1")
    column_names = [desc[0] for desc in cursor.description]
    result = cursor.fetchone()
    cursor.close()

    if result:
      result = dict(zip(column_names, result))
    else:
      #видимо, это первый запуск. Поставим заведомо старую дату forecast_datetime
      result = {'forecast_datetime' : datetime.strptime('1970-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")}

    return result
  
  #метод получает финальные данные по прогнозу
  def getForecastFinalData(self, forecast):
    
    if not forecast['price_end']:
      return None
    
    startDate = forecast['start_date']
    endDate = forecast['end_date']
    ticker = forecast['ticker']

    startDateStr = str(startDate)
    endDateStr = str(endDate) + ' 12:00:00'
    
    sql = f"""
WITH maxv AS
(
   SELECT datetime_end, high_price, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY high_price DESC) AS rn
   FROM 
     stocks_quotes sq 
   WHERE 
     ticker='{ticker}' 
     AND datetime_end >='{startDateStr}' AND datetime_end <= '{endDateStr}'
),

minv AS 
(
   SELECT datetime_end, low_price, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY low_price ASC) AS rn
   FROM 
     stocks_quotes sq 
   WHERE 
     ticker='{ticker}' 
     AND datetime_end >='{startDateStr}' AND datetime_end <='{endDateStr}'
)

SELECT 
   maxv.high_price, 
   maxv.datetime_end as maxprice_date,
   minv.low_price, 
   minv.datetime_end as minprice_date
FROM 
  maxv, minv
WHERE 
  minv.rn = 1
  AND maxv.rn = 1
"""
    
    cursor = self.conn.cursor(cursor_factory=pg_extras.DictCursor)
    cursor.execute(sql)
    column_names = [desc[0] for desc in cursor.description]
    result = cursor.fetchone()
    cursor.close()
 
    if result:
      result = dict(zip(column_names, result))
    else:
      return None
    
   
    forecast['price_max_real'] = result['high_price']
    forecast['price_max_date'] = result['maxprice_date']
    forecast['price_min_real'] = result['low_price']
    forecast['price_min_date'] = result['minprice_date']
    forecast['end_real_price_delta'] = round(100 * (forecast['price_end'] - forecast['price_start'])/forecast['price_start'], 3)
    forecast['price_max_delta'] = round(100 * (forecast['price_max_real'] - forecast['price_start'])/forecast['price_start'], 3)
    forecast['price_min_delta'] = round(100 * (forecast['price_min_real'] - forecast['price_start'])/forecast['price_start'], 3)

#0 - прогноз не рассчитан, 1 - прогноз сбылся, 2 - прогноз не сбылся, 3 - цена осталась той же самой
    
    if forecast['end_real_price_delta'] == 0:
      forecast['forecast_result'] = 3
    elif forecast['forecast_direction'] == 1 and forecast['end_real_price_delta'] > 0:
      forecast['forecast_result'] = 1
    elif forecast['forecast_direction'] == 1 and forecast['end_real_price_delta'] < 0:
      forecast['forecast_result'] = 2
    elif forecast['forecast_direction'] == 2 and forecast['end_real_price_delta'] < 0:
      forecast['forecast_result'] = 1
    elif forecast['forecast_direction'] == 2 and forecast['end_real_price_delta'] > 0:
      forecast['forecast_result'] = 2

    return forecast