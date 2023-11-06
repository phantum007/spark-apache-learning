import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import \
    (dayofmonth, hour,dayofyear, month,year,weekofyear, format_number, date_format)

spark = SparkSession.builder.appName('dates').getOrCreate()
df = spark.read.csv('appl_stock.csv',inferSchema=True,header=True)

df.select(year(df['date']),dayofmonth(df['date'])).show()

newdf = df.withColumn("Year",year(df['date']) ) 

res= newdf.groupBy("Year").mean().select(["Year","avg(Close)"])
res=res.withColumnRenamed("avg(Close)", 'average closing price')
res.select(['Year',format_number('average closing price',2).alias('average closing price')]).show()
