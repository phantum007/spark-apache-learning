import pyspark
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('ops').getOrCreate()
df = spark.read.csv('appl_stock.csv',inferSchema=True,header=True)
# df.printSchema()
# df.show()
# df.show(3)
# df.head(3)[0]


df.filter((df['close']<200) & (df['open']>200) ).show()
res = df.filter((df['close']<200) & (df['open']>200) ).collect()
print(res[0].asDict()['Open'])