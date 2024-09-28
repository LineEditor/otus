from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd   

import sys

if len(sys.argv) < 3:
   print("Укажите путь до файлов с исходными данными и путь для результирующего файла")
   exit(1)


inputDir = sys.argv[1]
outputDir = sys.argv[2]

from pyspark.sql.types import  (StructType, StructField, DateType, BooleanType, DoubleType, IntegerType, StringType, TimestampType)


### 1. Создаём схему

sk = SparkSession.builder.appName("BostoCrimeAnalysis").getOrCreate()

CrimeSchema = StructType    ([StructField("INCIDENT_NUMBER", StringType(), True),
                            StructField("OFFENSE_CODE", StringType(), True),
                            StructField("OFFENSE_CODE_GROUP", StringType(), True ),
                            StructField("OFFENSE_DESCRIPTION", StringType(), True ),
                            StructField("DISTRICT", StringType(), True),
                            StructField("REPORTING_AREA", StringType(), True),
                            StructField("SHOOTING", StringType(), True),
                            StructField("OCCURRED_ON_DATE", DateType(), True ),
                            StructField("YEAR", IntegerType(), True),
                            StructField("MONTH", IntegerType(), True),
                            StructField("DAY_OF_WEEK", StringType(), True),
                            StructField("HOUR", IntegerType(), True),
                            StructField("UCR_PART", IntegerType(), True),
                            StructField("STREET", StringType(), True),
                            StructField("Lat", DoubleType(), True),
                            StructField("Long", DoubleType(), True),
                            StructField("Location", StringType(), True )
                            ])

#Создаём два датафрейма - с данными о преступлениях и со справочником кодов преступлений
crimeDf = sk.read.csv(inputDir + '/crime.csv',header=True,schema=CrimeSchema)

CodesSchema = StructType([StructField("CODE", IntegerType(), True),
                            StructField("NAME", StringType(), True)])

offenceCodes = sk.read.csv(inputDir + '/offense_codes.csv',header=True,schema=CodesSchema)

#Добавляем колонку crime_type, которая содержит ту часть NAME, которая до тире
split_col = F.split(offenceCodes['NAME'],  r"\s+\-\s+")
offenceCodes = offenceCodes.dropDuplicates(["CODE"])
offenceCodes = offenceCodes.withColumn("crime_type", split_col.getItem(0))

# очищаем данные
crimeDf = crimeDf.fillna("N", ["SHOOTING"])
crimeDf = crimeDf.filter(crimeDf.DISTRICT.isNotNull())

#Добавляем колонку с годом преступления
crimeDf = crimeDf.withColumn("YEAR", F.year("OCCURRED_ON_DATE"))

#присоединяем описание кодов
crimeDf = crimeDf.join(offenceCodes, crimeDf.OFFENSE_CODE == offenceCodes.CODE, "inner")

#сформируем датафрейм, в котором подсчитаем количество преступлений в каждом районе за каждый месяц конкретного года, а так же усредненную широту и долготу преступлений
dfCrDistrictMonth = crimeDf.groupBy("DISTRICT", "YEAR", "MONTH").agg(F.count('INCIDENT_NUMBER').alias('month_crime_count'), F.avg('Lat').alias('Lat'), F.avg('Long').alias('Long'))

# посчитаем медианное значение преступлений на районе в месяце, суммарное количество преступлений на районе, усреднённую широту и долготу места преступления
dfCrDistrictMonth = dfCrDistrictMonth.groupBy("DISTRICT").agg(F.percentile_approx('month_crime_count', 0.5).alias('crimes_monthly'), 
                                                              F.sum('month_crime_count').alias('crimes_total'),
                                                              F.avg('Lat').alias('Lat'),
                                                              F.avg('Long').alias('Long'),
                                                              )

#Сделаем датафрейм, сгруппированный по району и типу преступления. При этом оставим только топ-3 преступлений

dfByDistrictAndCrime = crimeDf.groupBy("DISTRICT", "crime_type").count().withColumnRenamed("count","district_crime_type_count")

window = Window.partitionBy(dfByDistrictAndCrime['DISTRICT']).orderBy(dfByDistrictAndCrime['district_crime_type_count'].desc())
dfByDistrictAndCrime = dfByDistrictAndCrime.select('*', F.rank().over(window).alias('rank')).filter(F.col('rank') <= 3) 
  
# теперь для каждого района склеим типы преступлений
dfTopCrimes = dfByDistrictAndCrime.groupby("DISTRICT").agg(F.concat_ws(", ", F.collect_list(dfByDistrictAndCrime.crime_type)).alias("frequent_crime_types"))

#Склеим датафрейм с топ-3 преступлений с датафреймом с остальной статистикой по району
datamart = dfTopCrimes.join(dfCrDistrictMonth, dfTopCrimes.DISTRICT == dfCrDistrictMonth.DISTRICT, "inner").drop(dfTopCrimes.DISTRICT)
datamart = datamart.select("DISTRICT", "crimes_total", "crimes_monthly", "frequent_crime_types", "Lat", "Long").withColumnRenamed("DISTRICT", "district");

#datamart.show(500)

datamart.write.mode('overwrite').parquet(outputDir + "/boston-crime.parquet")