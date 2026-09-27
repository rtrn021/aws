
from pyspark.sql import SparkSession
print(SparkSession)



spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("/Users/rtrn/PycharmProjects/aws/data/csv/pokemon.csv")
df.printSchema()
df.show()


# import findspark
# findspark.init()
#
# import pyspark
#
# from pyspark.sql import SparkSession
# from pyspark import SparkContext
# sc = SparkContext()
# spark = SparkSession(sc)