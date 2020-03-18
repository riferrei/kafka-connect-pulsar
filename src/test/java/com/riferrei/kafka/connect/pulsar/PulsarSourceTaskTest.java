package com.riferrei.kafka.connect.pulsar;

import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.PulsarContainer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static com.riferrei.kafka.connect.pulsar.PulsarSourceConnectorConfig.*;

public class PulsarSourceTaskTest extends AbstractBasicTest {

    @Rule
    public PulsarContainer pulsar = new PulsarContainer(pulsarVersion);

    private void produceMessages(String topic, int numMessages)
        throws UnsupportedOperationException, IOException, InterruptedException {
        String message = PulsarSourceTask.class.getSimpleName();
        pulsar.execInContainer("bin/pulsar-client", "produce", topic,
        "--messages", message, "--num-produce", String.valueOf(numMessages));
    }

    @Test
    public void taskVersionShouldMatch() {
        String version = VersionUtil.getVersion();
        assertEquals(version, new PulsarSourceTask().version());
    }

    @Test
    public void checkConnectionLifeCycleMgmt() {
        assertDoesNotThrow(() -> {
            Map<String, String> props = new HashMap<>();
            props.put(SERVICE_URL_CONFIG, pulsar.getPulsarBrokerUrl());
            props.put(SERVICE_HTTP_URL_CONFIG, pulsar.getHttpServiceUrl());
            props.put(TOPIC_PATTERN, topicPatternValue);
            PulsarSourceTask task = new PulsarSourceTask();
            try {
                task.start(props);
            } finally {
                task.stop();
            }
        });
    }

    @Test
    public void shouldReadEarlierMessages() {
        final int numMsgs = 5;
        assertDoesNotThrow(() -> {
            produceMessages(topic[0], numMsgs);
        });
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, pulsar.getPulsarBrokerUrl());
        props.put(SERVICE_HTTP_URL_CONFIG, pulsar.getHttpServiceUrl());
        props.put(SUBSCRIPTION_INITIAL_POSITION_CONFIG, SubscriptionInitialPosition.Earliest.name());
        props.put(BATCH_MAX_NUM_MESSAGES_CONFIG, String.valueOf(numMsgs));
        props.put(TOPIC_NAMES, topic[0]);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(props);
                assertEquals(numMsgs, task.poll().size());
            } finally {
                task.stop();
            }
        });
    }

    @Test
    public void readingOnlyLatestMessages() {
        final int numMsgs = 5;
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, pulsar.getPulsarBrokerUrl());
        props.put(SERVICE_HTTP_URL_CONFIG, pulsar.getHttpServiceUrl());
        props.put(BATCH_MAX_NUM_MESSAGES_CONFIG, String.valueOf(numMsgs));
        props.put(TOPIC_NAMES, topic[1]);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(props);
                produceMessages(topic[1], numMsgs);
                assertEquals(numMsgs, task.poll().size());
            } finally {
                task.stop();
            }
        });
    }
    
}
