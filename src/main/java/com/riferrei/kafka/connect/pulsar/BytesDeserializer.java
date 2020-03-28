package com.riferrei.kafka.connect.pulsar;

import java.util.Collections;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.pulsar.client.api.Message;

public class BytesDeserializer implements Deserializer<byte[]> {

    @Override
    public SourceRecord deserialize(Message<byte[]> message,
        TopicNamingStrategy topicNamingStrategy) {
        String topic = TopicNameUtil.getTopic(
            message.getTopicName(), topicNamingStrategy);
        String offset = message.getMessageId().toString();
        return new SourceRecord(
            Collections.singletonMap("topic", topic),
            Collections.singletonMap("offset", offset),
            topic, Schema.BYTES_SCHEMA, message.getKeyBytes(),
            Schema.BYTES_SCHEMA, message.getData());
    }

}
