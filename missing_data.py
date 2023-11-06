from pyspark.sql import SparkSession
from pyspark.sql.functions import count_distinct,avg,stddev, mean

spark = SparkSession.builder.appName('missing_data').getOrCreate()
df = spark.read.csv('ContainsNull.csv',inferSchema=True,header=True)
df.show()
df1=df
df2=df
df3=df
df4=df
df5=df

ddf1=df
ddf2=df
ddf3=df
ddf4=df
ddf5=df

df1.na.drop().show()
df2.na.drop(thresh=2).show()
df3.na.drop(how='all').show()
df4.na.drop(how='any').show()
df5.na.drop(subset=['sales']).show()


ddf1.na.fill(0).show()
ddf2.na.fill('No Name').show()
ddf2.na.fill('No Name', subset=['name']).show()

mean_value = df.select(mean(df['sales'])).collect()
mean_sales = mean_value[0][0]

ddf2=ddf2.na.fill('rabhat', subset=['name'])
ddf2.na.fill(mean_sales, subset=['sales']).show()

