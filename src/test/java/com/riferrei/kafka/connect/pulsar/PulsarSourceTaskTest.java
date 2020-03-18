package com.riferrei.kafka.connect.pulsar;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.PulsarContainer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
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

    private void createPartitionedTopic(String topic, int partitions)
        throws UnsupportedOperationException, IOException, InterruptedException {
        pulsar.execInContainer("bin/pulsar-admin", "topics",
            "create-partitioned-topic", topic, "--partitions",
            String.valueOf(partitions));
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
        props.put(BATCH_MAX_NUM_MESSAGES_CONFIG, String.valueOf(numMsgs));
        props.put(SUBSCRIPTION_INITIAL_POSITION_CONFIG, SubscriptionInitialPosition.Earliest.name());
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
    public void shouldReadOnlyLatestMessages() {
        final int numMsgs = 5;
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, pulsar.getPulsarBrokerUrl());
        props.put(SERVICE_HTTP_URL_CONFIG, pulsar.getHttpServiceUrl());
        props.put(BATCH_MAX_NUM_MESSAGES_CONFIG, String.valueOf(numMsgs));
        props.put(SUBSCRIPTION_INITIAL_POSITION_CONFIG, SubscriptionInitialPosition.Latest.name());
        props.put(TOPIC_NAMES, topic[0]);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(props);
                produceMessages(topic[0], numMsgs);
                assertEquals(numMsgs, task.poll().size());
            } finally {
                task.stop();
            }
        });
    }

    @Test
    public void ensureTopicNameIsNameOnly() {
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, pulsar.getPulsarBrokerUrl());
        props.put(SERVICE_HTTP_URL_CONFIG, pulsar.getHttpServiceUrl());
        props.put(TOPIC_NAMING_STRATEGY_CONFIG, TopicNamingStrategyOptions.NameOnly.name());
        props.put(TOPIC_NAMES, topic[0]);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(props);
                produceMessages(topic[0], 1);
                List<SourceRecord> records = task.poll();
                assertEquals(topic[0], records.get(0).topic());
            } finally {
                task.stop();
            }
        });
    }

    @Test
    public void ensureTopicNameIsFullyQualified() {
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, pulsar.getPulsarBrokerUrl());
        props.put(SERVICE_HTTP_URL_CONFIG, pulsar.getHttpServiceUrl());
        props.put(TOPIC_NAMING_STRATEGY_CONFIG, TopicNamingStrategyOptions.FullyQualified.name());
        props.put(TOPIC_NAMES, topic[0]);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(props);
                produceMessages(topic[0], 1);
                List<SourceRecord> records = task.poll();
                assertEquals(fullyQualifiedTopic(topic[0]),
                    records.get(0).topic());
            } finally {
                task.stop();
            }
        });
    }

    @Test
    public void checkReadFromMultipleTopicsUsingNames() {
        final int numMsgsPerTopic = 2;
        final int numMsgsTotal = 6;
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, pulsar.getPulsarBrokerUrl());
        props.put(SERVICE_HTTP_URL_CONFIG, pulsar.getHttpServiceUrl());
        props.put(BATCH_MAX_NUM_MESSAGES_CONFIG, String.valueOf(numMsgsTotal));
        props.put(TOPIC_NAMES, listToString(topic[0], topic[1], topic[2]));
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(props);
                produceMessages(topic[0], numMsgsPerTopic);
                produceMessages(topic[1], numMsgsPerTopic);
                produceMessages(topic[2], numMsgsPerTopic);
                assertEquals(numMsgsTotal, task.poll().size());
            } finally {
                task.stop();
            }
        });
    }

    @Test
    public void partitionedTopicShouldCreateOneTopic() {
        final String topic = "customPartitionedTopic";
        final int partitions = 4;
        final int numMsgs = 4;
        assertDoesNotThrow(() -> {
            createPartitionedTopic(topic, partitions);
        });
        Map<String, String> props = new HashMap<>();
        props.put(SERVICE_URL_CONFIG, pulsar.getPulsarBrokerUrl());
        props.put(SERVICE_HTTP_URL_CONFIG, pulsar.getHttpServiceUrl());
        props.put(BATCH_MAX_NUM_MESSAGES_CONFIG, String.valueOf(numMsgs));
        props.put(TOPIC_NAMES, topic);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(props);
                produceMessages(topic, numMsgs);
                List<SourceRecord> records = task.poll();
                assertEquals(numMsgs, records.size());
                for (SourceRecord record : records) {
                    assertEquals(topic, record.topic());
                }
            } finally {
                task.stop();
            }
        });
    }
    
}
