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