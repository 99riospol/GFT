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

        /*String uri = "mongodb://root:1234@mongo:27017/simpsons.timestamp_simpsons?authSource=admin";
        MongoClient mongoClient = MongoClients.create(uri);
        MongoDatabase db = mongoClient.getDatabase("simpsons");
        MongoCollection<Document> coll = db.getCollection("timestamp_simpsons");*/

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
                        //coll.insertOne(doc);
                        return null;
                    }
                });


        env.execute();
    }

}