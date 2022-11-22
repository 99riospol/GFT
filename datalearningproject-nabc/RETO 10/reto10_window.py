import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Create SparkSession for kafka
spark_kafka = SparkSession \
        .builder \
        .appName("test") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .config("spark.jars.packages",  "org.apache.spark:spark-sql-kafka-0-10_2.11:4.2.5") \
        .getOrCreate()

# Create stream for kafka topic
kafka_df = spark_kafka.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "docker_test-kafka-1:29092") \
    .option("failOnDataLoss", "false") \
    .option("subscribe", "simpsons-quotes") \
    .option("startingOffsets", "earliest") \
    .load()

# Print kafka schema
kafka_df.printSchema()

# Save message value decoded as stream in new dataframe
string_df = kafka_df.select(col("timestamp"),expr("CAST(value AS string)"))

# Print string_df schema
string_df.printSchema()

# Save string dataframe in new dataframe with timestamp and DB schema
schema = StructType([StructField('quote', StringType()), \
                     StructField('character',StringType()), \
                     StructField('image',StringType()), \
                     StructField('characterDirection',StringType())])

timestamp_df = string_df.withColumn("data",from_json(col("value"), schema)).select("timestamp","data.*")

# Print timestamp_df schema
timestamp_df.printSchema()

# Create dataframe that counts the number of times a character shows up in 1 minute window
windowed_df = timestamp_df \
                .withWatermark("timestamp", "1 minute") \
                .groupBy(window(timestamp_df.timestamp, "1 minute", "1 minute"), timestamp_df.character) \
                .count()

# Writes to postgres and mongodb
def write_window_to_postgres_and_mongo(df, batch_id):
        mode="append"
        url_postgres_character_count = "jdbc:postgresql://docker_test-postgres-1:5432/simpsons"
        uri_mongo_character_count = "mongodb://root:1234@mongo:27017/simpsons.window_character_count?authSource=admin"
        properties = {"user": "root", "password": "1234", "driver": "org.postgresql.Driver"}
        
        # Insert into PostgreSQL
        df_postgres = df.withColumn("window_start", df["window"]["start"]) \
                        .withColumn("window_end", df["window"]["end"]) \
                        .select("window_start", "window_end", "character", "count")
        df_postgres.write.jdbc(url=url_postgres_character_count, table="public.window_character_count", mode=mode, properties=properties)

        # Insert into MongoDB
        df.write\
                .format('com.mongodb.spark.sql.DefaultSource')\
                .mode('append')\
                .option("spark.mongodb.output.uri", uri_mongo_character_count)\
                .save()

# Read from windowed_df
windowed_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .foreachBatch(write_window_to_postgres_and_mongo) \
    .start() \
    .awaitTermination()