from pyspark.sql import SparkSession
from pyspark.sql.functions import count_distinct,avg,stddev

spark = SparkSession.builder.appName('aggs').getOrCreate()
df = spark.read.csv('sales_info.csv',inferSchema=True,header=True)

dd=df.groupBy('Company').sum('Sales')
dd.show()


dd=df.agg({'sales':'sum'})
dd.show()


df.select(avg('Sales').alias('Average')).show()

