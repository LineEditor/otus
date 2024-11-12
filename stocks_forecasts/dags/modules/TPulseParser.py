import sys
import json
import requests
import re
from ForecastDb import ForecastDb
from TInvestAPI import TInvestAPI
from datetime import date, datetime, timedelta
import time
import logging as _log 

class TPulseParser:
  ANALYST_T_INVEST = 1
  CURRENCY_RUB = 1
  CURRENCY_USD = 2
  
  urlTTrading = 'https://www.tbank.ru/invest/social/profile/T-Trading/' 
  apiURL = 'https://www.tbank.ru/api/common/v1/' 
  sessionId = ''
  wuid = ''
  trackingId = ''
  indicators = []
  stocks = []
  externalIds = {}
  today = None
  client = None
  curLogId = None
  lastParsedForecast = None
  topForecast = None

  headers = {'accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
           'accept-encoding' : 'gzip, deflate, br',
           'accept-language' : 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
           'sec-ch-ua' : '"Chromium";v="106", "Atom";v="26", "Not;A=Brand";v="99"',
           'sec-ch-ua-mobile' : '?0',
           'sec-ch-ua-platform' : '"Windows"',
           'sec-fetch-dest' : 'document',
           'sec-fetch-mode' : 'navigate',
           'sec-fetch-site' : 'none',
           'sec-fetch-user' : '?1',
           'upgrade-insecure-requests' : '1',
           'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Atom/26.0.0.0 Safari/537.36'
           }

  def __init__(self):
    self.fDb = ForecastDb()
    self.today   = datetime.strptime(str(date.today()) + ' 23:59:59', "%Y-%m-%d %H:%M:%S")
    self.today00 = date.today()
    self.client = TInvestAPI()
    st = self.client.loadAllStocks()
    self.stocks = self.fDb.addStocks(st)
    self.client.setStocks(self.stocks)
    self.indicators = self.fDb.getIndicators() 

    return

  def run(self):
    self.startLogging()
    self.startParse()
    self.getPostsWithForecasts()
    self.endLogging()


  def startLogging(self):
    self.lastParsedForecast = self.fDb.getLastParsedForecast(self.ANALYST_T_INVEST)
    _log.info("lastParsedForecast:")
    _log.info(self.lastParsedForecast)

    self.topForecast = {'analyst_company_id' : self.ANALYST_T_INVEST, 'start_time' : datetime.now(), 
                        'forecast_datetime' : None, 'external_id' : None}    
    self.topForecast['id'] = self.fDb.insertIntoParserLog(self.topForecast)

  def endLogging(self):
    self.topForecast['end_time'] = datetime.now()
    self.fDb.updateParserLog(self.topForecast)

  def getPostsWithForecasts(self):
    page = 0
    cursor = ''

    while True:

      # получаем очередную порцию прогнозов. Сдвиг происходит с помощью параметра cursor
      postURL = 'https://www.tbank.ru/api/invest-gw/social/v1/profile/994d30c7-39bf-4009-985f-91845334f50b/post?limit=30&sessionId=' + self.sessionId
      postURL += '&appName=invest&appVersion=1.473.0&origin=web&platform=web'
        
      if cursor:
        postURL += '&cursor=' + cursor

      response = requests.get(postURL, headers=self.headers, cookies=self.allCookiesJar)
        
      data = response.json()

      if 'payload' not in data:
        break

      if 'items' not in data['payload']:
        break
 
      items = data['payload']['items']

      if len(items) == 0:
        break

      if 'nextCursor' in data['payload']:
        cursor = str(data['payload']['nextCursor'])
      else:  
        cursor = ''  

      #json_object = json.dumps(data, indent=4, ensure_ascii=False)
      #Writing to sample.json
      #with open("files/sample"+ str(page) +".json", "w") as outfile:
      #   outfile.write(json_object)

      forecasts = self.parseForecasts(items)

      if not forecasts:
        _log.info("NO forecasts. LastForecastDate: " + str(self.lastParsedForecast['forecast_datetime']))
        break

      self.indicators = self.getIndicatorsFromForecasts(forecasts)
      forecasts, quotes = self.getAdditionalDataForForecasts(forecasts)

      self.fDb.saveForecasts(forecasts)
      self.fDb.addQuotes(quotes)

      page += 1
      pingURL = self.apiURL + 'ping?appName=invest&appVersion=1.473.0&origin=web%2Cib5%2Cplatform&sessionid=' + self.sessionId + '&wuid=' + self.wuid
      response = requests.get(pingURL, headers=self.headers, cookies=self.allCookiesJar)
      self.allCookiesJar = requests.cookies.RequestsCookieJar() 
      time.sleep(1)

      _log.info(f"page={page},  cursor={cursor}\n") 

      if cursor == '':
        break  

  # Парсим порцию прогнозов
  def parseForecasts(self, items):
    forecasts = {}
    
    # Перебираем прогозы
    n = 0


    for item in items:
      n += 1
      forecastDate = item['inserted']   
      externalId  = item['id']
      dateParts = forecastDate.split(".") #Отсекаем всё что после точки, т.е. микросекунды и часовой пояс
      forecastDate = dateParts[0]

      fDate = datetime.strptime(forecastDate, "%Y-%m-%dT%H:%M:%S")
      _log.info(f"n={n}, dateInserted={fDate}, lastParsedForecast = " + str(self.lastParsedForecast['forecast_datetime']))

      # Не обрабатываем прогнозы, которые старше, чем дата последнего запуска парсера
      if fDate <= self.lastParsedForecast['forecast_datetime']:
        break

      if self.topForecast['forecast_datetime'] is None:
        self.topForecast['forecast_datetime'] = fDate
        self.topForecast['external_id'] = externalId

      content = item['content']['text']

      direction = -1
      lines = content.split("\n")
      lineCnt = len(lines)
      curLineNum = 0
     
      while True:
        if curLineNum >= lineCnt:
          break

        line = lines[curLineNum]
          
        if line.find('Бумага') > -1:
          currForecast = self.getForecast(lines[curLineNum:curLineNum + 5])
          if (currForecast == None):
            curLineNum += 5
            continue

          currForecast['date_inserted'] = fDate
          currForecast['start_date'] = fDate.date()
          currForecast['end_date'] = fDate.date() + timedelta(days=currForecast['term'])
          currForecast['external_id'] = externalId;

          key = str(fDate.date()) + '-' + currForecast['ticker']

          if key not in forecasts:
            forecasts[key] = currForecast

        curLineNum += 1  
    
    return forecasts

  #Получаем дополнительную инфу по прогнозам (цены)
  def getAdditionalDataForForecasts(self, forecasts):

    unsetKeys  = []
    _log.info("START getAdditionalDataForForecasts")
    allQuotes = []
    for key, fcData in forecasts.items():
      ticker = fcData['ticker']
      
      if ticker not in self.stocks:
        #Внештатная ситуация - тикер, которого нет в справочнике
        _log.info(f"{ticker} NOT IN STOCKS!")
        unsetKeys.append(key)
        continue

      indicator = fcData['indicator']

      price_end = None
      price_start = None
      quotes = self.client.getPricesForForecast(fcData)

      forecasts[key]['price_forecast'] = None

      if quotes:
        price_start = quotes[0]['open_price'] 

        #Если прогноз на падение, то price_delta_forecast - отрицательный, и price_forecast будет меньше начальной
        forecasts[key]['price_forecast'] = price_start * (1 + fcData['price_delta_forecast'] / 100)
        if fcData['end_date'] <= self.today00:
          price_end = quotes[-1]['open_price'] 

      forecasts[key]['indicator_id'] = self.indicators[indicator]['id']
      forecasts[key]['price_start'] = price_start
      forecasts[key]['price_end'] = price_end
      forecasts[key]['end_real_price'] = None
      forecasts[key]['price_max_real'] = None
      forecasts[key]['price_max_date'] = None
      forecasts[key]['price_min_real'] = None
      forecasts[key]['price_min_date'] = None
      forecasts[key]['end_real_price_delta'] = None
      forecasts[key]['price_max_delta'] = None
      forecasts[key]['price_min_delta'] = None
      forecasts[key]['forecast_result'] = None

      if quotes:
        allQuotes += quotes

    for unK in unsetKeys:
      del forecasts[unK]

    _log.info("END getAdditionalDataForForecasts")
    return forecasts, allQuotes
  
  #парсим прогноз по конкретной бумаге
  def getForecast(self, fcData):
    
    result = {'analyst_company_id' : self.ANALYST_T_INVEST, 'company' : '', 'ticker' : '',  'term' : 0, 'direction' : 0, 'success_procent' : 0, 'indicator' : ''}
    bumaga    = fcData[0]
    potential = fcData[1]
    term      = fcData[2]
    success_procent = fcData[3]
    indicator = fcData[4]

    m = re.match(r".*Бумага (?P<company>.*)\.*\{\$(?P<ticker>.*)\}.*", bumaga)
    if m == None:
      return None

    stDict = m.groupdict()

    result['company'] = stDict['company']
    result['ticker'] = stDict['ticker']
    
    if potential.find('Потенциал роста') > -1:
      m = re.match(r".*роста (.*)%", potential)
      if m == None:
        return None
      
      result['price_delta_forecast'] = float(m.group(1).strip())
      direction = 1
    else:
      m = re.match(r".*падения (.*)%", potential)            
      if m == None:
        _log.info(f"NONE: {potential}")        
        return None

      result['price_delta_forecast'] = -1 * float(m.group(1).strip())
      direction = 2      

    result['price_delta_forecastdelta'] = m.group(1)

    m = re.match(r".*В (.*)%.*", success_procent)
    result['success_procent'] = int(m.group(1))

    m = re.match(r".*Срок (\d+) ", term)
    result['term'] = int(m.group(1))
 
    m = re.match(r".*индикатора (.*)", indicator)
    result['indicator'] = m.group(1).strip()

    result['forecast_direction'] = direction

    return result

  def startParse(self):
    response = requests.get(self.urlTTrading, headers=self.headers)
    response.encoding = 'utf-8'

    if response.status_code == 200:
      self.allCookiesJar = requests.cookies.RequestsCookieJar()
      self.wuid = response.cookies['__P__wuid']

      sessionURL = self.apiURL + 'session?appName=invest&appVersion=1.473.0&origin=web%2Cib5%2Cplatform'  
      response = requests.get(sessionURL, headers=self.headers, cookies=self.allCookiesJar)
      data = response.json()
      
      self.sessionId  = data['payload']
      self.trackingId = data['trackingId'] 

      pingURL = self.apiURL + 'ping?appName=invest&appVersion=1.473.0&origin=web%2Cib5%2Cplatform&sessionid=' + self.sessionId + '&wuid=' + self.wuid
      response = requests.get(pingURL, headers=self.headers, cookies=self.allCookiesJar)

      self.allCookiesJar = requests.cookies.RequestsCookieJar() 
    else:
      _log.info(f"Не удалось скачать страницу. Код ответа: {response.status_code}")
      raise Exception(f"Не удалось скачать страницу. Код ответа: {response.status_code}")

  # получаем индикаторы из прогнозов, и сохраняем в БД те, которые новые
  def getIndicatorsFromForecasts(self, forecasts):

    indicatorsList = []
    wasIndicator = {}
    for key, fcData in forecasts.items():
      indicator = fcData['indicator']

      if indicator in self.indicators or indicator in wasIndicator:
        continue 

      wasIndicator[indicator] = 1
      indicatorsList.append({'indicator_name' : indicator, 'success_proc' : fcData['success_procent']}) 

    return self.fDb.addIndicators(indicatorsList) 

