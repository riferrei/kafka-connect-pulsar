package com.riferrei.kafka.connect.pulsar;

import com.google.protobuf.Any;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;

public class ProtoBufDeserializer implements Deserializer<Any> {

    private String generatedClass;
    private String messageClass;
    private PulsarAdmin pulsarAdmin;
    private TopicNamingStrategy topicNamingStrategy;

    public ProtoBufDeserializer(String generatedClass,
        String messageClass, PulsarAdmin pulsarAdmin,
        TopicNamingStrategy topicNamingStrategy) {
    }

    @Override
    public SourceRecord deserialize(Message<Any> message) {
        return null;
    }

}
