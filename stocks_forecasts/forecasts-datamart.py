from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col

from pyspark.sql.window import Window
import pandas as pd   
from dotenv import dotenv_values
from sqlalchemy import create_engine
import psycopg2
import psycopg2.extras as pg_extras
from pyspark.sql.types import  (StructType, StructField, DateType, BooleanType, DoubleType, IntegerType, StringType, TimestampType)

## Метод расчёта результативности long-прогнозов
@F.udf(returnType=StringType())
def calcResultLong(start_date, end_date, price_start, price_delta_forecast, price_end, price_max_real, price_max_date, price_min_real, price_min_date):
    price_delta_forecast = float(price_delta_forecast)
    price_start = float(price_start)
    price_end = float(price_end)
    price_max_real = float(price_max_real)
    price_min_real = float(price_min_real)   

    resStr = ''
    result1 = 0
    day_yield1 = 0
    day_yield2 = 0
    day_yield3 = 0

    # Вариант 1. Продаём, если прирост цены составил хотя бы 50% от price_delta_forecast
    # Если цена опустилась от начальной price_start до уровня price_start (1 - 0.5 * price_delta_forecast/ 100) - фиксируем убыток и продаём
    trgCoef = 0.5

    stopPrice   = price_start * (1 - trgCoef * price_delta_forecast/100)
    targetPrice = price_start * (1 + trgCoef * price_delta_forecast/100)

    # Стоп-цена - больше мин. цены (т.е. была достигнута), и дата достижения мин. цены меньше даты макс цены.
    # или же targetPrice вообще никогда не была достигнута 
    # значит продажа по стоп-лоссу
    if (stopPrice >= price_min_real) and ((price_min_date <= price_max_date) or (price_max_real < targetPrice)):
      deltaDays = (price_min_date.date() - start_date).days
      if deltaDays == 0:
        deltaDays = 1

      day_yield1 = -trgCoef*price_delta_forecast / deltaDays  
      result1 = -1

    #Стоп цена меньше мин. цены (не была достигнута), но при этом targetPrice тоже не была достигнута, 
    # то есть результат считаем по цене в конце периода. Если она меньше начальной, то это отрицательный результат
    elif (stopPrice < price_min_real) and (price_max_real < targetPrice) and  (price_end <= price_start):
      deltaDays = (end_date - start_date).days
      if deltaDays == 0:
        deltaDays = 1

      day_yield1 = (100*(price_end - price_start)/price_start)/deltaDays 
      result1 = -2
    
    # targetPrice был достигнут, и дата достижения targetPrice меньше даты достижения мин. цены, или же 
    # стоп-цена никогда не была достигнута. 
    elif (price_max_real > targetPrice) and ((price_max_date <= price_min_date) or (stopPrice < price_min_real)):
      deltaDays = (price_max_date.date() - start_date).days
      if deltaDays == 0:
        deltaDays = 1

      day_yield1 = trgCoef*price_delta_forecast / deltaDays  
      result1 = 1

    # targetPrice не был достигнут, при этом стоп-цена тоже не была достигнута, 
    # то есть результат считаем по цене в конце периода. Если она больше начальной, то это положительный результат
    elif (price_max_real < targetPrice) and (stopPrice < price_min_real) and  (price_end >= price_start):
      deltaDays = (end_date - start_date).days
      if deltaDays == 0:
        deltaDays = 1

      day_yield1 = (100*(price_end - price_start)/price_start)/deltaDays 
      result1 = 2


    # Вариант 2. Продаём, если прирост цены составил хотя бы 90% от price_delta_forecast
    # не ставим стоп-лосс, если цена не достигла прироста 90% от price_delta_forecast, фиксируем результат в конце периода

    trgCoef = 0.9

    targetPrice = price_start * (1 + trgCoef * price_delta_forecast/100)

    # targetPrice был достигнут
    if price_max_real > targetPrice:
      deltaDays = (price_max_date.date() - start_date).days
      if deltaDays == 0:
        deltaDays = 1

      day_yield2 = trgCoef*price_delta_forecast / deltaDays  
      result2 = 1
    else:
      deltaDays = (end_date - start_date).days
      if deltaDays == 0:
        deltaDays = 1

      day_yield2 = (100*(price_end - price_start)/price_start)/deltaDays  
      if day_yield2 < 0:
        result2 = -1
      else:
        result2 = 2

    # Вариант 3. Продаём, если прирост цены составил хотя бы 100% от price_delta_forecast
    # не ставим стоп-лосс, если цена не достигла прироста 100% от price_delta_forecast, фиксируем результат в конце периода

    trgCoef = 1

    targetPrice = price_start * (1 + trgCoef * price_delta_forecast/100)

    # targetPrice был достигнут
    if price_max_real > targetPrice:
      deltaDays = (price_max_date.date() - start_date).days
      if deltaDays == 0:
        deltaDays = 1

      day_yield3 = trgCoef*price_delta_forecast / deltaDays  
      result3 = 1
    else:
      deltaDays = (end_date - start_date).days
      if deltaDays == 0:
        deltaDays = 1

      day_yield3 = (100*(price_end - price_start)/price_start)/deltaDays  
      if day_yield3 < 0:
        result3 = -1
      else:
        result3 = 2

    resStr = f"{result1};{day_yield1};{result2};{day_yield2};{result3};{day_yield3}"

    return resStr
        
## Метод расчёта результативности short-прогнозов        
@F.udf(returnType=StringType())  
def calcResultShort(start_date, end_date, price_start, price_delta_forecast, price_end, price_max_real, price_max_date, price_min_real, price_min_date):
    price_delta_forecast = float(price_delta_forecast)
    price_start = float(price_start)
    price_end = float(price_end)
    price_max_real = float(price_max_real)
    price_min_real = float(price_min_real)   

    resStr = ''
    result1 = 0
    day_yield1 = 0
    day_yield2 = 0
    day_yield3 = 0

    # price_delta_forecast - отрицателен, т.к. это прогноз на падение цены.
    # Соответственно, price_start * (1 - 0.5 * price_delta_forecast/100) - это прирост цены, плохой для нас вариант

    # Вариант 1. Продаём, если падение цены составил хотя бы 50% от price_delta_forecast
    # Если цена выросла от начальной price_start до уровня price_start * (1 - 0.5 * price_delta_forecast/100) - фиксируем убыток и продаём
    trgCoef = 0.5

    stopPrice   = price_start * (1 - trgCoef * price_delta_forecast/100)
    targetPrice = price_start * (1 + trgCoef * price_delta_forecast/100)

    # Стоп-цена - меньше макс цены (т.е. была достигнута), и дата достижения макс. цены меньше даты мин цены.
    # или же targetPrice вообще никогда не была достигнута 
    # значит продажа по стоп-лоссу
    if (stopPrice <= price_max_real) and ((price_max_date <= price_min_date) or (price_min_real > targetPrice)):
      deltaDays = (price_max_date.date() - start_date).days
      if deltaDays == 0:
        deltaDays = 1

      day_yield1 = trgCoef*price_delta_forecast / deltaDays  
      result1 = -1

    #Стоп цена больше макс. цены (не была достигнута), но при этом targetPrice тоже не была достигнута, 
    #то есть результат считаем по цене в конце периода. Если она больше начальной, то это отрицательный результат
    elif (stopPrice > price_max_real) and (price_min_real > targetPrice) and  (price_end >= price_start):
      deltaDays = (end_date - start_date).days
      if deltaDays == 0:
        deltaDays = 1

      day_yield1 = (100*(price_end - price_start)/price_start)/deltaDays 
      result1 = -2
    
    # targetPrice был достигнут, и дата достижения targetPrice меньше даты достижения макс. цены, или же 
    # стоп-цена никогда не была достигнута 
    elif (price_min_real < targetPrice) and ((price_min_date < price_max_date) or (stopPrice > price_max_real)):
      deltaDays = (price_min_date.date() - start_date).days
      if deltaDays == 0:
        deltaDays = 1

      day_yield1 = -trgCoef*price_delta_forecast / deltaDays  
      result1 = 1

    # targetPrice не был достигнут, при этом стоп-цена тоже не была достигнута, 
    # то есть результат считаем по цене в конце периода. Если она меньше начальной, то это положительный результат
    elif (price_min_real > targetPrice) and (stopPrice > price_max_real) and  (price_end <= price_start):
      deltaDays = (end_date - start_date).days
      if deltaDays == 0:
        deltaDays = 1

      day_yield1 = (100*(price_start - price_end)/price_start)/deltaDays 
      result1 = 2

    # Вариант 2. Продаём, если падение цены составило хотя бы 90% от price_delta_forecast
    # не ставим стоп-лосс, если цена не достигла прироста 90% от price_delta_forecast, фиксируем результат в конце периода

    trgCoef = 0.9
    
    #Напоминание: price_delta_forecast у нас отрицательный для шортов
    targetPrice = price_start * (1 + trgCoef * price_delta_forecast/100)

    # targetPrice был достигнут
    if price_min_real < targetPrice:
      deltaDays = (price_min_date.date() - start_date).days
      if deltaDays == 0:
        deltaDays = 1

      day_yield2 = -trgCoef*price_delta_forecast / deltaDays  
      result2 = 1
    else:
      deltaDays = (end_date - start_date).days
      if deltaDays == 0:
        deltaDays = 1

      day_yield2 = (100*(price_end - price_start)/price_start)/deltaDays  

      if day_yield2 > 0:
        result2 = -1
      else:
        result2 = 2

      #В шортах инвертируем day_yield2, т.к. нам надо, чтобы процентный доход в день был положительным
      #если это положительный результат (а в случае шортов падение цены - положтельный результат)
      day_yield2 = -1*day_yield2

    # Вариант 3. Продаём, если падение цены составило хотя бы 100% от price_delta_forecast
    # не ставим стоп-лосс, если цена не достигла прироста 100% от price_delta_forecast, фиксируем результат в конце периода

    trgCoef = 1
    
    #Напоминание: price_delta_forecast у нас отрицательный для шортов
    targetPrice = price_start * (1 + trgCoef * price_delta_forecast/100)

    # targetPrice был достигнут
    if price_min_real < targetPrice:
      deltaDays = (price_min_date.date() - start_date).days
      if deltaDays == 0:
        deltaDays = 1

      day_yield3 = -trgCoef*price_delta_forecast / deltaDays  
      result3 = 1
    else:
      deltaDays = (end_date - start_date).days
      if deltaDays == 0:
        deltaDays = 1

      day_yield3 = (100*(price_end - price_start)/price_start)/deltaDays  

      if day_yield3 > 0:
        result3 = -1
      else:
        result3 = 2

      #В шортах инвертируем day_yield2, т.к. нам надо, чтобы процентный доход в день был положительным
      #если это положительный результат (а в случае шортов падение цены - положтельный результат)
      day_yield3 = -1*day_yield3

    resStr = f"{result1};{day_yield1};{result2};{day_yield2};{result3};{day_yield3}"

    return resStr

# Метод получает метрики
def getMetrics(longFc, shortFc):
  
  r = {}

  r['longForecasts'] = longFc.count()
  r['shortForecasts'] = shortFc.count()

  r['minDate'] = longFc.agg({"start_date": "min"}).head()[0]
  r['maxDate'] = longFc.agg({"start_date": "max"}).head()[0]

  r['longSuccess50'] = longFc.filter(longFc['res50'] >=1).count()
  r['longSuccess50Proc'] = longFc.agg({"proc50": "avg"}).head()[0]

  r['longSuccess90'] = longFc.filter(longFc['res90'] >=1).count()
  r['longSuccess90Proc'] = longFc.agg({"proc90": "avg"}).head()[0]

  r['longSuccess100'] = longFc.filter(longFc['res100'] >=1).count()
  r['longSuccess100Proc'] = longFc.agg({"proc100": "avg"}).head()[0]

  r['shortSuccess50'] = shortFc.filter(shortFc['res50'] >=1).count()
  r['shortSuccess50Proc'] = shortFc.agg({"proc50": "avg"}).head()[0]
 
  r['shortSuccess90'] = shortFc.filter(shortFc['res90'] >=1).count()
  r['shortSuccess90Proc'] = shortFc.agg({"proc90": "avg"}).head()[0]
 
  r['shortSuccess100'] = shortFc.filter(shortFc['res100'] >=1).count()
  r['shortSuccess100Proc'] = shortFc.agg({"proc100": "avg"}).head()[0]

  return r

### 1. Создаём схему

sk = SparkSession.builder.appName("Forecasts datamart").getOrCreate()

path = '/home/daniil/StocksForecasts/'
dbC = dotenv_values(path + ".env")

engine = create_engine(f"postgresql+psycopg2://{dbC['dbUser']}:{dbC['dbPassw']}@{dbC['dbHost']}:{dbC['dbPort']}/{dbC['dbName']}?client_encoding=utf8", echo=False)
pdf = pd.read_sql('SELECT sf.*, i.indicator_name, s.stock_name, s.country_id, s.sector FROM stocks_forecasts sf INNER JOIN stocks s ON sf.ticker = s.ticker INNER JOIN indicators i ON sf.indicator_id = i.id WHERE forecast_result is not null', engine.connect().connection)

fcDf = sk.createDataFrame(pdf)

#2. сделаем отдельные датафреймов для прогнозов на рост и падение

longFc = fcDf.filter(fcDf['forecast_direction'] == 1)
shortFc = fcDf.filter(fcDf['forecast_direction'] == 2)

#3. Добавим колонку str_result, в которой будут склеены результаты расчёта тремя способами

longFc = longFc.withColumn("str_result", calcResultLong(
    col("start_date"), 
    col("end_date"), 
    col("price_start"), 
    col("price_delta_forecast"), 
    col("price_end"), 
    col("price_max_real"), 
    col("price_max_date"), 
    col("price_min_real"), 
    col("price_min_date")
))

shortFc = shortFc.withColumn("str_result", calcResultShort(
    col("start_date"), 
    col("end_date"), 
    col("price_start"), 
    col("price_delta_forecast"), 
    col("price_end"), 
    col("price_max_real"), 
    col("price_max_date"), 
    col("price_min_real"), 
    col("price_min_date")
))

#4. Вытащим из колонки str_result полученные результаты в отдельные колонки 
new_cols = ['res50', 'proc50', 'res90', 'proc90', 'res100', 'proc100']
for i, c in enumerate(new_cols):
    longFc = longFc.withColumn(c, F.split('str_result', ';')[i])


for i, c in enumerate(new_cols):
    shortFc = shortFc.withColumn(c, F.split('str_result', ';')[i])    


#5. А теперь собственно подсчёты результативности прогнозов

stat = []

StatSchema = StructType([StructField("Параметр", StringType(), True),
                            StructField("Значение", StringType(), True)])


totalForecasts = fcDf.count()
stat.append(['Всего прогнозов', totalForecasts])

ruForecasts = fcDf.filter(fcDf['country_id'] == 'RU').count()
foreignForecasts = fcDf.filter(fcDf['country_id'] != 'RU').count()


m = getMetrics(longFc, shortFc)

stat.append(['RU компании', ruForecasts])
stat.append(['Не RU компании', foreignForecasts])
stat.append(['ЗА ВСЁ ВРЕМЯ', ''])

stat.append(['мин дата', str(m['minDate'])])
stat.append(['макс дата', str(m['maxDate'])])
stat.append(['Прогнозы на рост', m['longForecasts']])
stat.append(['Прогнозы на падение', m['shortForecasts']])

stat.append(['long успех 50%', round(100*m['longSuccess50']/m['longForecasts'],1)])

stat.append(['среднедн доход 50%', round(m['longSuccess50Proc'], 3)])

stat.append(['long успех 90%', round(100*m['longSuccess90']/m['longForecasts'],1)])
stat.append(['среднедн доход 90%', round(m['longSuccess90Proc'], 3)])

stat.append(['long успех 100%', round(100*m['longSuccess100']/m['longForecasts'],1)])
stat.append(['среднедн доход 100%', round(m['longSuccess100Proc'], 3)])    

stat.append(['short успех 50%', round(100*m['shortSuccess50']/m['shortForecasts'],1)])          
stat.append(['среднедн доход 50%', round(m['shortSuccess50Proc'], 3)])

stat.append(['short успех 90%', round(100*m['shortSuccess90']/m['shortForecasts'],1)])
stat.append(['short доход 90%', round(m['shortSuccess90Proc'], 3)])

stat.append(['short успех 100%', round(100*m['shortSuccess100']/m['shortForecasts'],1)])
stat.append(['short доход 100%', round(m['shortSuccess100Proc'], 3)])


long2022 = longFc.filter((longFc['start_date'] >= '2022-01-01') & (longFc['start_date'] <= '2022-12-31'))
short2022 = shortFc.filter((shortFc['start_date'] >= '2022-01-01') & (shortFc['start_date'] <= '2022-12-31'))

m = getMetrics(long2022, short2022)

stat.append(['', ''])
stat.append(['ЗА 2022 г', ''])

stat.append(['мин дата', str(m['minDate'])])
stat.append(['макс дата', str(m['maxDate'])])
stat.append(['Прогнозы на рост', m['longForecasts']])
stat.append(['Прогнозы на падение', m['shortForecasts']])

stat.append(['long успех 50%', round(100*m['longSuccess50']/m['longForecasts'],1)])

stat.append(['среднедн доход 50%', round(m['longSuccess50Proc'], 3)])

stat.append(['long успех 90%', round(100*m['longSuccess90']/m['longForecasts'],1)])
stat.append(['среднедн доход 90%', round(m['longSuccess90Proc'], 3)])

stat.append(['long успех 100%', round(100*m['longSuccess100']/m['longForecasts'],1)])
stat.append(['среднедн доход 100%', round(m['longSuccess100Proc'], 3)])    

stat.append(['short успех 50%', round(100*m['shortSuccess50']/m['shortForecasts'],1)])          
stat.append(['среднедн доход 50%', round(m['shortSuccess50Proc'], 3)])

stat.append(['short успех 90%', round(100*m['shortSuccess90']/m['shortForecasts'],1)])
stat.append(['short доход 90%', round(m['shortSuccess90Proc'], 3)])

stat.append(['short успех 100%', round(100*m['shortSuccess100']/m['shortForecasts'],1)])
stat.append(['short доход 100%', round(m['shortSuccess100Proc'], 3)])


long2023 = longFc.filter((longFc['start_date'] >= '2023-01-01') & (longFc['start_date'] <= '2023-12-31'))
short2023 = shortFc.filter((shortFc['start_date'] >= '2023-01-01') & (shortFc['start_date'] <= '2023-12-31'))

m = getMetrics(long2023, short2023)

stat.append(['', ''])
stat.append(['ЗА 2023 г', ''])

stat.append(['мин дата', str(m['minDate'])])
stat.append(['макс дата', str(m['maxDate'])])
stat.append(['Прогнозы на рост', m['longForecasts']])
stat.append(['Прогнозы на падение', m['shortForecasts']])

stat.append(['long успех 50%', round(100*m['longSuccess50']/m['longForecasts'],1)])

stat.append(['среднедн доход 50%', round(m['longSuccess50Proc'], 3)])

stat.append(['long успех 90%', round(100*m['longSuccess90']/m['longForecasts'],1)])
stat.append(['среднедн доход 90%', round(m['longSuccess90Proc'], 3)])

stat.append(['long успех 100%', round(100*m['longSuccess100']/m['longForecasts'],1)])
stat.append(['среднедн доход 100%', round(m['longSuccess100Proc'], 3)])    

stat.append(['short успех 50%', round(100*m['shortSuccess50']/m['shortForecasts'],1)])          
stat.append(['среднедн доход 50%', round(m['shortSuccess50Proc'], 3)])

stat.append(['short успех 90%', round(100*m['shortSuccess90']/m['shortForecasts'],1)])
stat.append(['short доход 90%', round(m['shortSuccess90Proc'], 3)])

stat.append(['short успех 100%', round(100*m['shortSuccess100']/m['shortForecasts'],1)])
stat.append(['short доход 100%', round(m['shortSuccess100Proc'], 3)])


long2024_1 = longFc.filter((longFc['start_date'] >= '2024-01-01') & (longFc['start_date'] <= '2024-05-20'))
short2024_1 = shortFc.filter((shortFc['start_date'] >= '2024-01-01') & (shortFc['start_date'] <= '2024-05-20'))

m = getMetrics(long2024_1, short2024_1)

stat.append(['', ''])
stat.append(['ЗА 2024 г до 20 мая', ''])

stat.append(['мин дата', str(m['minDate'])])
stat.append(['макс дата', str(m['maxDate'])])
stat.append(['Прогнозы на рост', m['longForecasts']])
stat.append(['Прогнозы на падение', m['shortForecasts']])

stat.append(['long успех 50%', round(100*m['longSuccess50']/m['longForecasts'],1)])

stat.append(['среднедн доход 50%', round(m['longSuccess50Proc'], 3)])

stat.append(['long успех 90%', round(100*m['longSuccess90']/m['longForecasts'],1)])
stat.append(['среднедн доход 90%', round(m['longSuccess90Proc'], 3)])

stat.append(['long успех 100%', round(100*m['longSuccess100']/m['longForecasts'],1)])
stat.append(['среднедн доход 100%', round(m['longSuccess100Proc'], 3)])    

stat.append(['short успех 50%', round(100*m['shortSuccess50']/m['shortForecasts'],1)])          
stat.append(['среднедн доход 50%', round(m['shortSuccess50Proc'], 3)])

stat.append(['short успех 90%', round(100*m['shortSuccess90']/m['shortForecasts'],1)])
stat.append(['short доход 90%', round(m['shortSuccess90Proc'], 3)])

stat.append(['short успех 100%', round(100*m['shortSuccess100']/m['shortForecasts'],1)])
stat.append(['short доход 100%', round(m['shortSuccess100Proc'], 3)])

long2024_2 = longFc.filter((longFc['start_date'] > '2024-05-20') & (longFc['start_date'] <= '2024-12-31'))
short2024_2 = shortFc.filter((shortFc['start_date'] > '2024-05-20') & (shortFc['start_date'] <= '2024-12-31'))

m = getMetrics(long2024_2, short2024_2)

stat.append(['', ''])
stat.append(['ЗА 2024 г после 20 мая', ''])

stat.append(['мин дата', str(m['minDate'])])
stat.append(['макс дата', str(m['maxDate'])])
stat.append(['Прогнозы на рост', m['longForecasts']])
stat.append(['Прогнозы на падение', m['shortForecasts']])

stat.append(['long успех 50%', round(100*m['longSuccess50']/m['longForecasts'],1)])

stat.append(['среднедн доход 50%', round(m['longSuccess50Proc'], 3)])

stat.append(['long успех 90%', round(100*m['longSuccess90']/m['longForecasts'],1)])
stat.append(['среднедн доход 90%', round(m['longSuccess90Proc'], 3)])

stat.append(['long успех 100%', round(100*m['longSuccess100']/m['longForecasts'],1)])
stat.append(['среднедн доход 100%', round(m['longSuccess100Proc'], 3)])    

stat.append(['short успех 50%', round(100*m['shortSuccess50']/m['shortForecasts'],1)])          
stat.append(['среднедн доход 50%', round(m['shortSuccess50Proc'], 3)])

stat.append(['short успех 90%', round(100*m['shortSuccess90']/m['shortForecasts'],1)])
stat.append(['short доход 90%', round(m['shortSuccess90Proc'], 3)])

stat.append(['short успех 100%', round(100*m['shortSuccess100']/m['shortForecasts'],1)])
stat.append(['short доход 100%', round(m['shortSuccess100Proc'], 3)])

statDF = sk.createDataFrame(data=stat, schema = StatSchema)

# Выводим результат

statDF.show(200)