package com.riferrei.kafka.connect.pulsar;

import java.io.IOException;

import org.junit.ClassRule;
import org.testcontainers.containers.PulsarContainer;

public abstract class AbstractBasicTest {

    protected static final String SERVICE_URL_VALUE = "pulsar://localhost:6650";
    protected static final String SERVICE_HTTP_URL_VALUE = "http://localhost:8080";
    protected static final String TOPIC_PATTERN_VALUE = "persistent://public/default/.*";

    protected static final String[] TOPIC = {
        "topic-1", "topic-2", "topic-3",
        "topic-4", "topic-5", "topic-6"
    };

    protected String fullyQualifiedTopic(String topic) {
        return String.format("public-default-%s", topic);
    }

    protected String listToString(String... topics) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < topics.length; i++) {
            if (i == topics.length - 1) {
                sb.append(topics[i]);
            } else {
                sb.append(topics[i]).append(", ");
            }
        }
        return sb.toString();
    }

    @ClassRule
    public static PulsarContainer pulsar =
        new PulsarContainer(PropertiesUtil.getPulsarVersion());

    protected String getServiceUrl() {
        return pulsar.getPulsarBrokerUrl();
    }

    protected String getServiceHttpUrl() {
        return pulsar.getHttpServiceUrl();
    }

    protected void produceMessages(String topic, int numMessages)
        throws UnsupportedOperationException, IOException, InterruptedException {
        String message = PulsarSourceTask.class.getSimpleName();
        pulsar.execInContainer("bin/pulsar-client", "produce", topic,
            "--messages", message, "--num-produce", String.valueOf(numMessages));
    }

    protected void createPartitionedTopic(String topic, int partitions)
        throws UnsupportedOperationException, IOException, InterruptedException {
        pulsar.execInContainer("bin/pulsar-admin", "topics",
            "create-partitioned-topic", topic, "--partitions",
            String.valueOf(partitions));
    }
    
}
