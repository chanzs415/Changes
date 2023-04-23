import sys
from pyspark.sql.functions import *
from pyspark.ml.feature import Imputer, StringIndexer, IndexToString, OneHotEncoder
from pyspark.sql import SparkSession

if len(sys.argv) < 2:
    print("Please provide the input file path as a command-line argument.")
    sys.exit(1)

# Get the input file path from command-line argument
input_file_path = sys.argv[1]

# create SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()
# Read in csv file
df = spark.read.format("csv").option("header", "true").load(input_file_path)
#df = spark.read.format("csv").option("header", "true").load("hdfs://namenode:9000/data/combined_csv.csv")

# Check for duplicates
duplicates = df.groupBy(*df.columns).count().filter(col("count") > 1)

if duplicates.count() > 0:
    print(f"There are {duplicates.count()} duplicates in the DataFrame.")
else:
    print("There are no duplicates in the DataFrame.")

# Check for missing values
df1 = df.select([count(when(col(c).isNull() | (col(c) == ""), c)).alias(c) for c in df.columns])
df1.show()

# Convert the "preciptype" column from String to index
indexer = StringIndexer(inputCol="preciptype", outputCol="preciptype_idx", handleInvalid="skip")

from pyspark.ml.feature import Imputer, StringIndexer

# Convert the "preciptype" column from String to index
indexer = StringIndexer(inputCol="preciptype", outputCol="preciptype_idx", handleInvalid="skip")
df2 = indexer.fit(df).transform(df)

# Cast columns to Double
df3 = df2.withColumn("uvindex_double", col("uvindex").cast("double")) \
         .withColumn("severerisk_double", col("severerisk").cast("double")) \
         .withColumn("windgust_double", col("windgust").cast("double")) \
         .withColumn("solarradiation_double", col("solarradiation").cast("double")) \
         .withColumn("solarenergy_double", col("solarenergy").cast("double"))

# Impute missing values for categorical variables
imputer_cate = Imputer(inputCols=["uvindex_double", "severerisk_double", "preciptype_idx"],
                       outputCols=["uvindex_imp", "severerisk_imp", "preciptype_imp"],
                       strategy="median")
df_cate = imputer_cate.fit(df3).transform(df3)

# Impute missing values for continuous variables
imputer_cont = Imputer(inputCols=["windgust_double", "solarradiation_double", "solarenergy_double"],
                       outputCols=["windgust_imp", "solarradiation_imp", "solarenergy_imp"],
                       strategy="mean")
df_cont = imputer_cont.fit(df_cate).transform(df_cate)

# Drop unnecessary columns and rename columns
df_new = df_cont.drop("preciptype", "uvindex", "severerisk", "windgust", "solarradiation", "solarenergy", 
                       "uvindex_double", "severerisk_double", "preciptype_idx", "windgust_double", 
                       "solarradiation_double", "solarenergy_double")
df_clean = df_new.withColumnRenamed("uvindex_imp", "uvindex") \
                 .withColumnRenamed("severerisk_imp", "severerisk") \
                 .withColumnRenamed("preciptype_imp", "preciptype") \
                 .withColumnRenamed("windgust_imp", "windgust") \
                 .withColumnRenamed("solarradiation_imp", "solarradiation") \
                 .withColumnRenamed("solarenergy_imp", "solarenergy")

# Write the cleaned dataframe to csv
df_clean.write.mode('overwrite').csv('/usr/local/output', header=True)

spark.stop()

