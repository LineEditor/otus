from tinkoff.invest import CandleInterval, Client, RequestError
from tinkoff.invest.utils import now
from tinkoff.invest.schemas import InstrumentType, InstrumentStatus, AssetType 
import pycurl
import certifi
import requests
from io import BytesIO
import subprocess
import json
from datetime import date, datetime, timedelta
import time

class TInvestAPI:
  API_TOKEN = 'ТОКЕН Т-ИНВЕСТИЦИЙ'
  apiCallCnt = 0
  stocks = {}

  def __init__(self):
    self.today = date.today()

  #Получаем и сохраняем справочник всех акций
  def getShares(self):
    stocksList = []

    with Client(self.API_TOKEN) as client:
      r = client.instruments.get_assets()
      for asset in (r.assets):
        if asset.type != AssetType.ASSET_TYPE_SECURITY:
          continue

        stockName = asset.name
        for inst in asset.instruments:
          if inst.instrument_kind == InstrumentType.INSTRUMENT_TYPE_SHARE:
             stocksList.append({'ticker' : inst.ticker, 'figi' : inst.figi, 'stock_name' : stockName, 'class_code' : inst.class_code})

    return stocksList
    
  #Получаем и сохраняем справочник всех акций
  def loadAllStocks(self):
    stocksList = []

    with Client(self.API_TOKEN) as client:
      r = client.instruments.shares(instrument_status = InstrumentStatus.INSTRUMENT_STATUS_ALL)
      
      assets = r.instruments
      wasStock = {}  
      for s in assets:
          currency = s.nominal.currency
          if not currency:
            currency = 'usd'

          if s.ticker in wasStock:
            continue
          
          wasStock[s.ticker] = 1

          stocksList.append({'ticker' : s.ticker, 'figi' : s.figi, 
                             'stock_name' : s.name, 
                             'country_id' : s.country_of_risk, 'currency' : currency,
                             'lot' : s.lot, 'class_code' : s.class_code, 'sector' : s.sector})

    return stocksList
      
  #Получает свечи через системный вызов CURL
  def getCandleListExternalCURL(self, fcData, interval = "CANDLE_INTERVAL_HOUR"):

    endDate = fcData['end_date']
    fromD = fcData['date_inserted'].strftime("%Y-%m-%dT%H:%M:%S.000000000Z")
    
    curt = datetime.now()

    if endDate:
      if endDate >= self.today:
        toD = curt.strftime("%Y-%m-%dT%H:%M:%S.000000000Z")      
      else:
        toD = str(endDate) + 'T12:00:00.000000000Z'
    else:
      toD = curt.strftime("%Y-%m-%dT%H:%M:%S.000000000Z") 

    figi = self.stocks[fcData['ticker']]['figi']    
    
    data = {
     "figi": figi,
     "from": fromD,
     "to": toD,
     "interval": interval,
     "candleSourceType": "CANDLE_SOURCE_EXCHANGE"      
    }

    datastr = json.dumps(data)
    self.apiCallCnt += 1
    result = subprocess.run(['curl', 
                             '-X',
                             'POST',
                             'https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest.api.contract.v1.MarketDataService/GetCandles',
                             '-H',
                             'accept: application/json',
                             '-H',
                             'Authorization: Bearer ' + self.API_TOKEN,
                             '-H',
                             'Content-Type: application/json',
                             '-d',
                             datastr
                             ],
                             capture_output=True, text=True)

    #Надо уложиться в лимити 150 запросов в минуту
    if self.apiCallCnt >= 150:
      self.apiCallCnt = 0
      time.sleep(50)

    return json.loads(result.stdout)

  # получаем все цены по прогнозу
  def getPricesForForecast(self, fcData, interval = "CANDLE_INTERVAL_HOUR"):
    candles = self.getCandleListExternalCURL(fcData, interval)
    
    if not candles or 'candles' not in candles:
      return None

    candleList = candles['candles']  

    if interval == "CANDLE_INTERVAL_HOUR": 
      curDelta = timedelta(hours=1)
    elif interval == "CANDLE_INTERVAL_10_MIN": 
      curDelta = timedelta(minutes=10)

    prices = []
    for candle in candleList:
      datetime_start = datetime.strptime(candle['time'], "%Y-%m-%dT%H:%M:%SZ")

      datetime_end = datetime_start + curDelta
      prices.append({'ticker' : fcData['ticker'], 
                     'datetime_start' : datetime_start, 
                     'datetime_end' : datetime_end, 
                     'open_price' : self.formatPrice(candle['open']), 
                     'close_price' : self.formatPrice(candle['close']), 
                     'high_price' : self.formatPrice(candle['high']), 
                     'low_price' : self.formatPrice(candle['low']),
                     'volume' : candle['volume']})

    return prices  

  #Формирует цену из целой и дробной части
  def formatPrice(self, price):
    #price['units'] - целая часть
    #price['nano'] - дробная часть (целое число из 9 знаков, миллиардная доля единицы)
    return int(price['units']) + float(int(price['nano']) / 1000000000)
  
  #Получает свечи через request.post
  #Не используется
  def getCandleListHTTP(self, fcData):
    fromD = fcData['date_inserted'].strftime("%Y-%m-%dT%H:%M:%S.000000000Z")
    toD = str(fcData['end_date']) + 'T12:00:00.000000000Z'
    figi = self.stocks[fcData['ticker']]['figi']
    
    data = {"figi": figi,
            "from": fromD,
            "to": toD,
            "interval": "CANDLE_INTERVAL_HOUR",
            "candleSourceType": "CANDLE_SOURCE_EXCHANGE"
    }   

    headers = {'accept' : 'application/json',
      'Authorization': 'Bearer ' + self.API_TOKEN,
      'Content-Type': 'application/json',
    }

    response = requests.post('https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest.api.contract.v1.MarketDataService/GetCandles', 
                          headers=headers, 
                          data=data)
    
    return response.json


  #Получает свечи через pycurl
  #не используется
  def getCandleListCURL(self, fcData):
    fromD = fcData['date_inserted'].strftime("%Y-%m-%dT%H:%M:%S.000000000Z")
    toD = str(fcData['end_date']) + 'T12:00:00.000000000Z'
    figi = self.stocks[fcData['ticker']]['figi']

    headers = ['accept : application/json', 
               'Authorization: Bearer ' + self.API_TOKEN,
               'Content-Type: application/json',
    ]
     
    data = {
     "figi": figi,
     "from": fromD,
     "to": toD,
     "interval": "CANDLE_INTERVAL_HOUR",
     "candleSourceType": "CANDLE_SOURCE_EXCHANGE"      
    }


    buffer = BytesIO()
    curl = pycurl.Curl()
    curl.reset()
    curl.setopt(pycurl.URL, 'https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest.api.contract.v1.MarketDataService/GetCandles')
    curl.setopt(pycurl.HTTPHEADER, headers)
    curl.setopt(pycurl.POSTFIELDS, json.dumps(data))
    curl.setopt(pycurl.WRITEDATA, buffer)
    curl.setopt(curl.CAINFO, certifi.where())
    curl.perform()
    curl.close()

    response = buffer.getvalue().decode("utf-8")
    return
  
  #Получить свечи через RPC
  #Не используется. Выдаёт ошибку
  def getCandleListRPC(self, fcData):
    insertedDate = fcData['date_inserted']
    endDate = str(fcData['end_date']) + ' 12:00'
    figi = self.stocks[fcData['ticker']]['figi']
    endDateTm = datetime.strptime(endDate, "%Y-%m-%d %H:%M")

    with Client(self.API_TOKEN) as client:
      try:
        candleList = client.get_all_candles(
          figi = figi,
          from_ = insertedDate,
          to = endDateTm,
          interval=CandleInterval.CANDLE_INTERVAL_HOUR,
        )
      except RequestError as err:
        candleList = []
        print(err) 

    if not candleList:
      return None
    
    return candleList    
  
  def setStocks(self, stcks):
    self.stocks = stcks
    return