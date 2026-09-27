# Do all imports and installs here
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import re
# Read in the data here
# import pyreadstat
file_name = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
# df, meta = pyreadstat.read_sas7bdat(file_name)
df_i94 = pd.read_sas(file_name,'sas7bdat',encoding='ISO-8859-1')

filename = '../../data2/GlobalLandTemperaturesByCity.csv'
df_temp = pd.read_csv(filename, sep=',')

from pyspark.sql import SparkSession
spark = SparkSession.builder.\
config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
.enableHiveSupport().getOrCreate()
df_spark =spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')

df_airport_codes = pd.read_csv('./airport-codes_csv.csv',sep=',')
df_airport_codes.head()

# Performing cleaning tasks here

re_obj = re.compile(r'\'(.*)\'.*\'(.*)\'')
port_valid = {}
with open('airport_codes.txt') as f:
    for line in f:
        match = re_obj.search(line)
        port_valid[match[1]] = [match[2]]


def clean_i94_data(file):
    """
    Cleans the i94 dataset
    file: path of the file
    return: cleaned spark df
    """
    df_immigration = spark.read.format('com.github.saurfag.sas.spark').load(file)
    df_immigration = df_immigration.filter(df_immigration.i94port.isin(list(port_valid.keys())))
    return df_immigration

immigration_test_file = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
df_immigration_test = clean_i94_data(immigration_test_file)
df_immigration_test.show()


df_immigration_test.createOrReplaceTempView('immig_table')
df_immigration_test.count()

spark.sql("""
SELECT LENGTH (i94port) AS len
FROM immig_table
GROUP BY len
""").show()


df_temp = spark.read.format("csv").option("header", "true").load("../../data2/GlobalLandTemperaturesByCity.csv")
df_temp = df_temp.filter(df_temp.AcerageTemperature != 'NaN')
df_temp = df_temp.dropDuplicates(['City', 'Country'])


@udf()
def get_i94port(city):
    """
    city: city name
    return: key
    """
    for key in port_valid:
        if city.lower() in port_valid[key][0].lower():
            return key


df_temp=df_temp.withColumn("i94port", get_i94port(df_temp.City))
df_temp=df_temp.filter(df_temp.i94port != 'null')


df_temp.take(10)

fname = '../../data2/GlobalLandTeperaturesByCity.csv'
df_temperature = pd.read_csv(fname)
df_temperature['Country'].nunique()


immigration_data = '/data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
df_immigration = clean_i94_data(immigration_data)

dim_immigration_table = df_immigration.select(["i94yr", "i94mon", "i94cit", "i94port", "arrdate", "i94mode", "depdate", "i94visa", "visatype", "Gender"])
dim_immigration_table.write.mode("append").partitionBy("i94port").parquet("/resusts/immigration.parquet")
dim_temp_table = df_temp.select(["AverageTemperature", "City", "Country", "Latitude", "Longitude", "i94port"])
dim_temp_table.write.mode("append").partitionBy("i94port").parquet("/results/temperature.parquet")
df_immigration.createOrReplaceTempView("immig_view")

df_temp.createOrReplaceTempView("temp_view")
spark.sql("""Select * from immig_view Where gender in ('F', 'M')""").createOrReplaceTempView("immig_view")

spark.sql("""Select *, CASE
                        WHEN arrdate >= 1.0 THEN date_add(to_date('1960-01-01'), arrdate)
                        WHEN arrdate IS NULL THEN NULL
                        ELSE 'N/A' END AS arrival_date
                FROM immig_view""").createOrReplaceTempView("immig_view")

spark.sql("""Select *, CASE
                        WHEN depdate >= 1.0 THEN date_add(to_date('1960-01-01'), depdate)
                        WHEN depdate IS NULL THEN NULL
                        ELSE 'N/A' END AS departure_date
                FROM immig_view""").createOrReplaceTempView("immig_view")


dim_time = spark.sql("""
SELECT DISTINCT arrival_date AS date
FROM immig_view
UNION
SELECT DISTINCT departure_date as date
from immig_view
WHERE departure_date IS NOT NULL
""")
dim_time.createOrReplaceTempView("dim_time_table")

dim_time = spark.sql("""
SELECT date, YEAR(date) AS year, MONTH(date) AS month, DAY(date) AS day, WEEKOFYEAR(date) AS week, DAYOFWEEK(date) as weekday, DAYOFYEAR(date) year_day
FROM dim_time_table
ORDER BY date ASC
""")

dim_time = spark.sql("""
SELECT date, YEAR(date) AS year, MONTH(date) AS month, DAY(date) AS day, WEEKOFYEAR(date) AS week, DAYOFWEEK(date) as weekday, DAYOFYEAR(date) year_day
FROM dim_time_table
ORDER BY date ASC
""")
dim_time.write.parquet("/results/dim_time.parquet")

fact_table = spark.sql('''
SELECT immig_view.i94yr as year,
       immig_view.i94mon as month,
       immig_view.i94cit as city,
       immig_view.i94port as i94port,
       immig_view.arrival_date as arrival_date,
       immig_view.departure_date as departure_date,
       immig_view.i94visa as reason,
       immig_view.Gender as gender,
       temp_view.AverageTemperature as temperature,
       temp_view.Latitude as latitude,
       temp_view.Longitude as longitude
FROM immig_view
JOIN temp_view ON ( immig_view.i94port = temp_view.i94port)
''')
fact_table.write.mode("append").partitionBy("i94port").parquet("/results/fact.parquet")



immigration_data = '/data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'


df_immigration = clean_i94_data(immigration_data)
immigration_table = df_immigration.select(["i94yr", "i94mon", "i94cit", "i94port", "arrdate", "i94mode", "depdate", "i94visa"])


immigration_table.write.mode("append").partitionBy("i94port").parquet("/results/immigration.parquet")


temp_table = df_temp.select(["AverageTemperature", "City", "Country", "Latitude", "Longitude", "i94port"])


temp_table.write.mode("append").partitionBy("i94port").parquet("/results/temperature.parquet")


df_immigration.createOrReplaceTempView("immigration_view")
df_temp.createOrReplaceTempView("temp_view")


fact_table = spark.sql('''
SELECT immigration_view.i94yr as year,
       immigration_view.i94mon as month,
       immigration_view.i94cit as city,
       immigration_view.i94port as i94port,
       immigration_view.arrival_date as arrival_date,
       immigration_view.departure_date as departure_date,
       immigration_view.i94visa as reason,
       immigration_view.Gender as gender,
       immigration_view.AverageTemperature as temperature,
       immigration_view.Latitude as latitude,
       immigration_view.Longitude as longitude
FROM immigration_view
JOIN temp_view ON ( immigration_view.i94port = temp_view.i94port)
''')


fact_table.write.mode("append").partitionBy("i94port").parquet("/results/fact.parquet")

def quality_check(df, description):
    """
    doing some quality checks
    df: spark df
    return: the result of quality check
    """
    result = df.count()
    if result == 0:
        print("data quality check failed for {} with zero records".format(description))
    else:
        print("Data quality check passed for {} with {} records".format(description, result))
    return 0



quality_check(df_immigration, "immigration table")
quality_check(df_temp, "temperature table")