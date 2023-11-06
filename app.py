import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField,StringType, 
                                IntegerType, StructType)

spark = SparkSession.builder.appName('Basics').getOrCreate()
df = spark.read.json('people.json')

# df.show()
df.printSchema()
# df.columns
# df.describe()
# df.describe().show()

data_schema = [StructField('age',IntegerType(),True),
               StructField('name',StringType(),True)]

final_struc = StructType(fields=data_schema)

df1 = spark.read.json('people.json',schema=final_struc)

df1.printSchema()
print(type(df1.select('age')))
df1.show()

print(type(df1['age']))
df1[['age']].show()


rr = df.head(2)[0]
print(type(rr))
print(rr)


d1 = df.select(['age','name'])
print(type(d1))
print(d1)
d1.show()



d2 = df.withColumn('new column name', df['age']*2)
print(type(d2))
d2.show()


d2.createOrReplaceTempView('people')
res = spark.sql('SELECT * FROM people')
res.show()


res = spark.sql('SELECT * FROM people where age == 30')
res.show()

# df.createOrReplaceTempView('ppl')