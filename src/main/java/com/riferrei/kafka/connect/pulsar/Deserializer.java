package com.riferrei.kafka.connect.pulsar;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.pulsar.client.api.Message;

public interface Deserializer<T> {

    SourceRecord deserialize(Message<T> message,
        TopicNamingStrategy topicNamingStrategy);

}
