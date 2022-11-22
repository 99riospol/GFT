import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

window = 0

spark_kafka = SparkSession \
        .builder \
        .appName("test") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .config("spark.jars.packages",  "org.apache.spark:spark-sql-kafka-0-10_2.11:4.2.5") \
        .getOrCreate()

kafka_df = spark_kafka.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "docker_test-kafka-1:29092") \
    .option("failOnDataLoss", "false") \
    .option("subscribe", "simpsons-quotes") \
    .option("startingOffsets", "earliest") \
    .load()

# Save message value decoded as stream in new dataframe
string_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Save string dataframe in new dataframe with DB schema

schema = StructType([StructField('quote', StringType()), \
                     StructField('character',StringType()), \
                     StructField('image',StringType()), \
                     StructField('characterDirection',StringType())])

json_df = string_df.withColumn("data",from_json(col("value"), schema)).select("data.*")

# Write to postgres and mongodb from topic
def write_to_postgres_and_mongo(df, batch_id):
        global window
        mode="append"
        url_postgres_simpsons = "jdbc:postgresql://docker_test-postgres-1:5432/simpsons"
        url_postgres_character_count = "jdbc:postgresql://docker_test-postgres-1:5432/simpsons"
        uri_mongo_quotes = "mongodb://root:1234@mongo:27017/simpsons.quotes?authSource=admin"
        uri_mongo_character_count = "mongodb://root:1234@mongo:27017/simpsons.character_count?authSource=admin"
        properties = {"user": "root", "password": "1234", "driver": "org.postgresql.Driver"}
        # Count quotes
        num_quotes_df = (df.withColumn("count", count(df["quote"]).over(Window.partitionBy("character"))).withColumn("window",lit(window))).select('window','character','count').distinct()
        # Insert into PostgreSQL
        df.write.jdbc(url=url_postgres_simpsons, table="public.simpsons", mode=mode, properties=properties)
        num_quotes_df.write.jdbc(url=url_postgres_character_count, table="public.character_count", mode=mode, properties=properties)
        # Insert into MongoDB
        df.write\
        .format('com.mongodb.spark.sql.DefaultSource')\
        .mode('append')\
        .option("spark.mongodb.output.uri", uri_mongo_quotes)\
        .save()
        num_quotes_df.write\
        .format('com.mongodb.spark.sql.DefaultSource')\
        .mode('append')\
        .option("spark.mongodb.output.uri", uri_mongo_character_count)\
        .save()
        # Increment window
        window+=1

json_df.writeStream \
    .format("console") \
    .foreachBatch(write_to_postgres_and_mongo) \
    .trigger(processingTime="1 minute") \
    .start() \
    .awaitTermination()
