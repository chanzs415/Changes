from pyspark.sql import SparkSession

# create SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

df = spark.read.csv("hdfs://namenode:9000/data/data/*.csv", header = True)
lessNames = ["tempmax", "tempmin"]

dfNew = df[lessNames]
dfNew.show(10)
newDF = dfNew.write.csv('/usr/local/output3', mode='overwrite', header=True)


