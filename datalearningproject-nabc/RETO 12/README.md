# RETO 12 - NiFi y Kafka y Flink Job y BD

En este reto se pide leer los mensajes del topic "simpsons-quotes" de Kafka, procesarlos en tiempo real mediante un job de Flink, filtrando por los personajes Homer Simpson, Nelson Muntz y Troy McClure e incorporando la fecha de procesamiento, y almacenarlos en una base de datos PostgreSQL y otra MongoDB.

## 1 - Lanzar Flink con docker-compose

Para este reto, se ha utilizado la misma infraestructura lanzada en el [Reto 11](../RETO%2011/docker-compose.yml), que incluía dos contenedores a partir de la imagen **flink:latest**, uno como jobmanager y otro como taskmanager.

Los contenedores se lanzar con la siguiente instrucción desde el directorio en el que se encuentre el fichero:

```
docker-compose up -d
```

Tras ello, se podrán ver los contenedores creados en Docker Desktop:

![Captura de Docker Desktop](reto12_0.png)

Y se podrá acceder a la interfaz de Flink en http://localhost:8083:

![Captura de Interfaz de Flink](../RETO%2011/reto11_1.png)

## 2 - Creación de tablas y colecciones

Para almacenar la información, se creó la siguiente tabla 'timestamp_simpsons' en PostgreSQL, que almacena el tiempo de procesado y la información de los mensajes: timestamp, quote, character, image y character_direction.

![Captura de tabla 'timestamp_simpsons'](reto12_1.png)

Además, también se crea la colección equivalente en MongoDB, 'timestamp_simpsons', donde se almacenará la misma información:

![Captura colección 'timestamp_simpsons'](reto12_2.png)

## 3 - Aplicación Java con Maven

Para implementar la solución al reto, se creó una aplicación Java con Maven, la aplicación [reto12](./reto12).

Para su correcto funcionamiento, se incluyeron las siguientes dependencias en el archivo [pom.xml](./reto12/pom.xml), teniendo en cuenta las versiones de Scala y de Flink del contenedor (en este caso 2.12 y 1.15.2, respectivamente):

```xml
    <dependencies>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-core</artifactId>
            <version>1.15.1</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-kafka_2.12</artifactId>
            <version>1.14.6</version>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.apache.flink/flink-streaming-java -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java_2.12</artifactId>
            <version>1.14.6</version>
            <scope>compile</scope>
        </dependency>

        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>2.0.3</version>
        </dependency>

        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>2.0.3</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-jdbc</artifactId>
            <version>1.15.2</version>
            <scope>compile</scope>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.postgresql/postgresql -->
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <version>42.5.0</version>
            <scope>compile</scope>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.apache.flink/flink-table-api-java-bridge -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-api-java-bridge</artifactId>
            <version>1.15.2</version>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.apache.flink/flink-table-api-java -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-api-java</artifactId>
            <version>1.15.2</version>
        </dependency>

        <!-- https://mvnrepository.com/artifact/com.google.code.gson/gson -->
        <dependency>
            <groupId>com.google.code.gson</groupId>
            <artifactId>gson</artifactId>
            <version>2.9.1</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-java</artifactId>
            <version>1.15.2</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients</artifactId>
            <version>1.15.2</version>
            <scope>test</scope>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.mongodb/mongo-java-driver -->
        <dependency>
            <groupId>org.mongodb</groupId>
            <artifactId>mongo-java-driver</artifactId>
            <version>3.8.1</version>
        </dependency>

        <dependency>
            <groupId>org.mongodb</groupId>
            <artifactId>bson</artifactId>
            <version>3.8.1</version>
        </dependency>
        <dependency>
            <groupId>org.mongodb</groupId>
            <artifactId>mongodb-driver-core</artifactId>
            <version>3.8.1</version>
        </dependency>

    </dependencies>
```

En la implementación de la aplicación, los mensajes del topic 'simpsons_quotes' de Kafka se deserializaron convirtiéndolos en un POJO de nombre [Message](./reto12/src/main/java/org/example/Message.java) con un parámetro por cada dato que se guardará en las tablas de la base de datos:

```java
package org.example;

import java.io.Serializable;
import java.sql.Timestamp;

public class Message implements Serializable {
    private String quote;
    private String character;
    private String image;
    private String characterDirection;
    private Timestamp timestamp;

    public Message() {
    }

    public String getQuote() {
        return quote;
    }

    public void setQuote(String quote) {
        this.quote = quote;
    }

    public String getCharacter() {
        return character;
    }

    public void setCharacter(String character) {
        this.character = character;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getCharacterDirection() {
        return characterDirection;
    }

    public void setCharacterDirection(String characterDirection) {
        this.characterDirection = characterDirection;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }
}

```

Además, para poder deserializarlo a este objeto Message se creó la clase [MessageDeserializationSchema](./reto12/src/main/java/org/example/MessageDeserializationSchema.java), que implementa la interfaz DeserializationSchema:

```java
package org.example;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;

public class MessageDeserializationSchema implements DeserializationSchema<Message> {

    private static final Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();

    private static final long serialVersionUID = 1L;

    @Override
    public Message deserialize(byte[] kafkaMessage) {
        String line = new String(kafkaMessage, StandardCharsets.UTF_8);
        Message msg = gson.fromJson(line,Message.class);
        msg.setTimestamp(new Timestamp(System.currentTimeMillis ()));
        return msg;
    }

    @Override
    public boolean isEndOfStream(Message nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Message> getProducedType() {
        return TypeInformation.of(Message.class);
    }
}
```

Una vez definidas las dependencias, el POJO y el deserializador, la aplicación está compuesta por la clase Java [ReadKafkaWriteDB.java](./reto12/src/main/java/org/example/ReadKafkaWriteDB.java) con el siguiente contenido:

```java
package org.example;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.Document;

public class ReadKafkaWriteDB {

    public static void writeToMongo(Document document) {
        String uri = "mongodb://root:1234@mongo:27017/simpsons.timestamp_simpsons?authSource=admin";
        MongoClient mongoClient = MongoClients.create(uri);
        MongoDatabase db = mongoClient.getDatabase("simpsons");
        MongoCollection<Document> coll = db.getCollection("timestamp_simpsons");
        coll.insertOne(document);
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<Message> source = KafkaSource
                .<Message>builder()
                .setBootstrapServers("docker_test-kafka-1:29092")
                .setTopics("simpsons-quotes")
                .setValueOnlyDeserializer(new MessageDeserializationSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .filter((Message message) -> (message.getCharacter().equals("Homer Simpson") || message.getCharacter().equals("Nelson Muntz") || message.getCharacter().equals("Troy McClure")))
                .addSink(
                        JdbcSink.sink(
                                "insert into public.timestamp_simpsons (timestamp, quote, character, image, character_direction) values (?, ?, ?, ?, ?)",
                                (statement, message) -> {
                                    statement.setTimestamp(1, message.getTimestamp());
                                    statement.setString(2, message.getQuote());
                                    statement.setString(3, message.getCharacter());
                                    statement.setString(4, message.getImage());
                                    statement.setString(5, message.getCharacterDirection());
                                },
                                JdbcExecutionOptions.builder()
                                        .withBatchSize(1000)
                                        .withBatchIntervalMs(200)
                                        .withMaxRetries(5)
                                        .build(),
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl("jdbc:postgresql://docker_test-postgres-1:5432/simpsons")
                                        .withDriverName("org.postgresql.Driver")
                                        .withUsername("root")
                                        .withPassword("1234")
                                        .build()
                        ));

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .filter((Message message) -> (message.getCharacter().equals("Homer Simpson") || message.getCharacter().equals("Nelson Muntz") || message.getCharacter().equals("Troy McClure")))
                .map(new MapFunction<Message, Void>() {
                    private static final long serialVersionUID = -6867736771747690202L;

                    @Override
                    public Void map(Message value) {
                        Document doc = new Document("timestamp",value.getTimestamp()).append("quote",value.getQuote()).append("character",value.getCharacter()).append("image",value.getImage()).append("character_direction",value.getCharacterDirection());
                        writeToMongo(doc);
                        return null;
                    }
                });


        env.execute();
    }

}
```

En este código, se establece una fuente de datos de Kafka, KafkaSource, indicando el broker y el nombre del topic del que se va a leer y que se empezará a leer desde el primer mensaje (earliest offset) y deserializando el valor de los mensajes a objetos Message con el MessageDeserializationSchema definido previamente. 

Después, se lee del KafkaSource utilizando un StreamExecutionEnvironment, se filtra por el personaje y se  y se escribe a la base de datos PostgreSQL con un **JdbcSink**.

Además, se vuelve a leer del KafkaSource y a filtrar por el personaje, tras lo cual se se utiliza la función map para, para cada mensaje, crear un documento que se almacenará en la colección timestamp_simpsons de MongoDB con la función **writeToMongo**.

Por último, se ejecuta el StreamExecutionEnvironment.

## 4 - Ejecución del job desde Flink

Tras generar el jar de la aplicación en IntelliJ IDEA, es posible ejecutar los jobs de Flink mediante los mismos pasos explicados en el [Reto 11](../RETO%2011/README.md):

![Captura de los jobs ejecutándose en Flink](reto12_3.png)

## 5 - Resultado

Una vez lanzados los jobs de Flink, será posible ver la información tanto en PostgreSQL como en MongoDB:

#### Tabla 'timestamp_simpsons'
![Captura de la tabla 'timestamp_simpsons' con la información](reto12_5.png)

#### Colección 'timestamp_simpsons'
![Captura de la colección 'timestamp_simpsons' con la información](reto12_4.png)
