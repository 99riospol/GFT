# Reto 10 - NiFi y Kafka y Spark Job y BD

En este reto se pedía procesar la cantidad de veces que se repite el mismo personaje durante 1 minuto utilizando un job en Spark.

Para ello, se lanzará Spark por medio de contenedores de docker y se definirá un script de Python con pyspark que leerá del topic "simpsons-quotes" de Kafka y almacenará, por un lado, la información de los mensajes en la tabla "simpsons" de PostgreSQL y la colección "quotes" de MongoDB, y, por otro, el número de veces que aparece un personaje cada minuto junto con su nombre y la ventana en la que se contó en la tabla y colección "character_count" de PostgreSQL y MongoDB, respectivamente.

## Lanzar Spark con docker-compose

Para este reto, se partió del archivo [docker-compose.yml](docker-compose.yml) de retos anteriores, añadiendo las siguientes líneas para lanzar Spark:

```yaml
  spark:
    image: docker.io/bitnami/spark:3.3
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    ports:
      - 8082:8080
      - 7070:7070
      - 4040:4040
    volumes:
      - ./python:/opt/bitnami/spark/scripts

  spark-worker:
    image: docker.io/bitnami/spark:3.3
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark:7077
      - SPARK_WORKER_MEMORY=1G
      - SPARK_WORKER_CORES=1
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
```

Como se puede ver, se lanzan dos contenedores a partir de la imagen [docker.io/bitnami/spark:3.3](https://registry.hub.docker.com/r/bitnami/spark):

- **spark**: contenedor que se lanzará en modo "master". Expone sus puerto 8080, mapeándolo al 8082 de la máquina local, y 7070 y 4040, este último para poder acceder a Spark UI. Además, se crea un volumen que mapea el directorio "python" local con el directorio "/opt/bitnami/spark/scripts" del contenedor, donde se alamacenará el script de python utilizado para resolver el ejercicio.
- **spark-worker**: contenedor que se lanza en modo "worker", con 'spark' como su "master".

Una vez definidos en el fichero docker-compose.yml, los contenedores se lanzar con la siguiente instrucción (desde el directorio en el que se encuentre el fichero):

```
docker-compose up -d
```

Tras ello, se podrán ver los contenedores creados en Docker Desktop:

![Captura de Docker Desktop](reto10_1.png)

Y se podrá acceder a la interfaz de Spark en http://localhost:8082:

![Captura de Interfaz de Spark](reto10_2.png)

A continuación, se presentan los pasos para dos soluciones distintas al reto: una primera sin el uso de Window Operations de Spark Structured Streaming y otra segunda que sí hace uso de ellas.

## Solución SIN Window Operations de Spark Structured Streaming

### 1 - Creación de tablas y colecciones

Para almacenar la información, se crearon las siguiente tablas en PostgreSQL:

#### Tabla 'simpsons'

Almacena la información de los mensajes: quote, character, image y characterDirection.

![Captura de tabla 'simpsons'](reto10_3.png)

#### Tabla 'character_count'

Almacena el número de veces que aparece un personaje en cada ventana de 1 minuto: window, character y count.

![Captura de tabla 'character_count'](reto10_4.png)

Las colecciones equivalentes en MongoDB, 'quotes' y 'character_count', se crean al almacena información en ellas:

![Captura colecciones MongoDB](reto10_5.png)

### 2 - Script de Python

Tras lanzar el contenedor con Spark y crear las tablas y colecciones, el reto se resolvió por medio del siguiente script de python con pyspark de nombre [reto10.py](reto10.py):

```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

window = 0

# Create SparkSession for Kafka
spark_kafka = SparkSession \
        .builder \
        .appName("test") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .config("spark.jars.packages",  "org.apache.spark:spark-sql-kafka-0-10_2.11:4.2.5") \
        .getOrCreate()

# Read from Kafka topic
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

# Read from topic every minute
json_df.writeStream \
    .format("console") \
    .foreachBatch(write_to_postgres_and_mongo) \
    .trigger(processingTime="1 minute") \
    .start() \
    .awaitTermination()

```

### 3 - Ejecución

Para probar el correcto funcionamiento del script, se utilizó el siguiente pipeline de NiFi:

![Captura pipeline NiFi](reto10_6.png)

Este es el mismo utilizado en el [Reto 4](../RETO%204/README.md), cambiando el valor de scheduling del procesador GetHTTP de 30 a 5 segundos para que realice más consultas a la API durante el minuto de la ventana:

![Captura procesador GetFile](reto10_7.png)

Para que la ejecución del script funcionara, se tuvieron que añadir los siguientes [jars](./jars/) commons-pool2-2.11.1.jar, postgresql-42.2.25.jar, spark-sql-kafka-0-10_2.12-3.3.0.jar, kafka-clients-3.3.0.jar, spark-token-provider-kafka-0-10_2.12-3.3.0.jar, mongo-spark-connector_2.12-3.0.1.jar, mongodb-driver-3.12.11.jar, mongodb-driver-core-3.12.11.jar y bson-3.12.11.jar al directorio /opt/bitnami/spark/jars del contenedor, por medio de la siguiente instrucción:

```
docker cp [nombre_jar] [nombre_contenedor]:/opt/bitnami/spark/jars
```

Una vez puesto en marcha el pipeline y copiados los jars necesarios, se puede lanzar el script de python desde el contenedor de spark con la siguiente instrucción:

```
./bin/spark-submit ./scripts/reto10.py
```

Tras lo cual se podrán ver los jobs en Spark UI en http://localhost:4040:

![Captura Spark UI](reto10_8.png)

Y la información resultante en las tablas de PostgreSQL y colecciones de MongoDB:

#### Tabla 'simpsons'
![Captura tabla 'simpsons' PostgreSQL](reto10_9.png)

#### Tabla 'character_count'
![Captura tabla 'character_count' PostgreSQL](reto10_10.png)

#### Colección 'quotes'
![Captura colección 'quotes' MongoDB](reto10_11.png)

#### Colección 'character_count'
![Captura colección 'character_count' MongoDB (1)](reto10_12.png)
![Captura colección 'character_count' MongoDB (2)](reto10_13.png)

## Solución CON Window Operations de Spark Structured Streaming

### 1 - Creación de tablas y colecciones

Para almacenar la información, se creó la siguiente tabla en PostgreSQL y colección en MongoDB:

#### Tabla 'window_character_count'

Almacena el número de veces que aparece un personaje en cada ventana de 1 minuto: window_start, window_end, character y count.

![Captura de tabla 'window_character_count'](reto10_14.png)

#### Colección 'window_character_count'

Equivalente a la tabla del mismo nombre para PostgreSQL, por lo que almacenará documentos con la misma información.

![Captura colección 'window_character_count'](reto10_15.png)

### 2 - Script de Python

Tras lanzar el contenedor con Spark y crear la tabla y colección, el reto se resolvió por medio del siguiente script de python con pyspark de nombre [reto10_window.py](reto10_window.py):

```python
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
```

### 3 - Ejecución

En este caso, se partió de los mensajes ya existentes en el topic de Kafka tras la ejecución de la solución anterior.

Además, no fue necesario añadir ningún jar extra a [los ya añadidos en la anterior solución](./jars/).

Se puede lanzar el script con la siguiente instrucción:

```
./bin/spark-submit ./scripts/reto10_window.py
```

Tras ejecutar el script, se podrán ver los jobs en Spark UI en http://localhost:4040:

![Captura Spark UI](reto10_18.png)

Y la información resultante en las tablas de PostgreSQL y colecciones de MongoDB:

#### Tabla 'window_character_count'
![Captura tabla 'window_character_count' PostgreSQL](reto10_16.png)

#### Colección 'window_character_count'
![Captura colección 'character_count' MongoDB](reto10_17.png)