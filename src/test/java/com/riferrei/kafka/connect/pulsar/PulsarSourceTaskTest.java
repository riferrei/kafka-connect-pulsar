/**

    Copyright © 2020 Ricardo Ferreira (riferrei@riferrei.com)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

*/

package com.riferrei.kafka.connect.pulsar;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static com.riferrei.kafka.connect.pulsar.PulsarSourceConnectorConfig.*;

public class PulsarSourceTaskTest extends AbstractBasicTest {

    @Test
    public void taskVersionShouldMatch() {
        String version = PropertiesUtil.getConnectorVersion();
        assertEquals(version, new PulsarSourceTask().version());
    }

    @Test
    public void shouldReadEarlierMessages() {
        final int numMsgs = 5;
        assertDoesNotThrow(() -> {
            produceBytesBasedMessages(topic, numMsgs);
        });
        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(SERVICE_URL_CONFIG, getServiceUrl());
        connectorProps.put(SERVICE_HTTP_URL_CONFIG, getServiceHttpUrl());
        connectorProps.put(BATCH_MAX_NUM_MESSAGES_CONFIG, String.valueOf(numMsgs));
        connectorProps.put(SUBSCRIPTION_INITIAL_POSITION_CONFIG,
            SubscriptionInitialPosition.Earliest.name());
        connectorProps.put(TOPIC_WHITELIST_CONFIG, topic);
        Map<String, String> taskProps = getTaskProps(connectorProps);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(taskProps);
                List<SourceRecord> records = task.poll();
                assertEquals(numMsgs, records.size());
            } finally {
                task.stop();
            }
        });
    }

    @Test
    public void shouldReadLatestMessages() {
        final int numMsgs = 5;
        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(SERVICE_URL_CONFIG, getServiceUrl());
        connectorProps.put(SERVICE_HTTP_URL_CONFIG, getServiceHttpUrl());
        connectorProps.put(BATCH_MAX_NUM_MESSAGES_CONFIG, String.valueOf(numMsgs));
        connectorProps.put(SUBSCRIPTION_INITIAL_POSITION_CONFIG,
            SubscriptionInitialPosition.Latest.name());
        connectorProps.put(TOPIC_WHITELIST_CONFIG, topic);
        Map<String, String> taskProps = getTaskProps(connectorProps);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(taskProps);
                produceBytesBasedMessages(topic, numMsgs);
                List<SourceRecord> records = task.poll();
                assertEquals(numMsgs, records.size());
            } finally {
                task.stop();
            }
        });
    }

    @Test
    public void ensureTopicNameIsNameOnly() {
        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(SERVICE_URL_CONFIG, getServiceUrl());
        connectorProps.put(SERVICE_HTTP_URL_CONFIG, getServiceHttpUrl());
        connectorProps.put(TOPIC_NAMING_STRATEGY_CONFIG,
            TopicNamingStrategy.NameOnly.name());
        connectorProps.put(TOPIC_WHITELIST_CONFIG, topic);
        Map<String, String> taskProps = getTaskProps(connectorProps);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(taskProps);
                produceBytesBasedMessages(topic, 1);
                List<SourceRecord> records = task.poll();
                assertEquals(topic, records.get(0).topic());
            } finally {
                task.stop();
            }
        });
    }

    @Test
    public void ensureTopicNameIsFullyQualified() {
        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(SERVICE_URL_CONFIG, getServiceUrl());
        connectorProps.put(SERVICE_HTTP_URL_CONFIG, getServiceHttpUrl());
        connectorProps.put(TOPIC_NAMING_STRATEGY_CONFIG,
            TopicNamingStrategy.FullyQualified.name());
        connectorProps.put(TOPIC_WHITELIST_CONFIG, topic);
        Map<String, String> taskProps = getTaskProps(connectorProps);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(taskProps);
                produceBytesBasedMessages(topic, 1);
                List<SourceRecord> records = task.poll();
                assertEquals(fullyQualifiedTopic(topic),
                    records.get(0).topic());
            } finally {
                task.stop();
            }
        });
    }

    @Test
    public void checkMultipleTopicsUsingOnlyWhitelist() {
        final int numMsgsPerTopic = 5;
        final int numMsgsTotal = 15;
        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(SERVICE_URL_CONFIG, getServiceUrl());
        connectorProps.put(SERVICE_HTTP_URL_CONFIG, getServiceHttpUrl());
        connectorProps.put(BATCH_MAX_NUM_MESSAGES_CONFIG, String.valueOf(numMsgsTotal));
        connectorProps.put(TOPIC_WHITELIST_CONFIG, listToString(REUSABLE_TOPICS[0],
            REUSABLE_TOPICS[1], REUSABLE_TOPICS[2]));
        Map<String, String> taskProps = getTaskProps(connectorProps);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(taskProps);
                produceBytesBasedMessages(REUSABLE_TOPICS[0], numMsgsPerTopic);
                produceBytesBasedMessages(REUSABLE_TOPICS[1], numMsgsPerTopic);
                produceBytesBasedMessages(REUSABLE_TOPICS[2], numMsgsPerTopic);
                List<SourceRecord> records = task.poll();
                assertEquals(numMsgsTotal, records.size());
            } finally {
                task.stop();
            }
        });
    }

    @Test
    public void checkMultipleTopicsUsingOnlyRegex() {
        final int numMsgsPerTopic = 5;
        final int numMsgsTotal = 15;
        assertDoesNotThrow(() -> {
            createNonPartitionedTopic(REUSABLE_TOPICS[0]);
            createNonPartitionedTopic(REUSABLE_TOPICS[1]);
            createNonPartitionedTopic(REUSABLE_TOPICS[2]);
        });
        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(SERVICE_URL_CONFIG, getServiceUrl());
        connectorProps.put(SERVICE_HTTP_URL_CONFIG, getServiceHttpUrl());
        connectorProps.put(BATCH_MAX_NUM_MESSAGES_CONFIG, String.valueOf(numMsgsTotal));
        connectorProps.put(TOPIC_REGEX_CONFIG, TOPIC_REGEX_VALUE);
        Map<String, String> taskProps = getTaskProps(connectorProps);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(taskProps);
                produceBytesBasedMessages(REUSABLE_TOPICS[0], numMsgsPerTopic);
                produceBytesBasedMessages(REUSABLE_TOPICS[1], numMsgsPerTopic);
                produceBytesBasedMessages(REUSABLE_TOPICS[2], numMsgsPerTopic);
                List<SourceRecord> records = task.poll();
                assertEquals(numMsgsTotal, records.size());
            } finally {
                task.stop();
            }
        });
    }

    @Test
    public void checkMultipleTopicsUsingWhitelistAndRegex() {
        final int numMsgsPerTopic = 5;
        final int numMsgsTotal = 30;
        assertDoesNotThrow(() -> {
            createNonPartitionedTopic(REUSABLE_TOPICS[0]);
            createNonPartitionedTopic(REUSABLE_TOPICS[1]);
            createNonPartitionedTopic(REUSABLE_TOPICS[2]);
            createNonPartitionedTopic(REUSABLE_TOPICS[3]);
            createNonPartitionedTopic(REUSABLE_TOPICS[4]);
            createNonPartitionedTopic(REUSABLE_TOPICS[5]);
        });
        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(SERVICE_URL_CONFIG, getServiceUrl());
        connectorProps.put(SERVICE_HTTP_URL_CONFIG, getServiceHttpUrl());
        connectorProps.put(BATCH_MAX_NUM_MESSAGES_CONFIG, String.valueOf(numMsgsTotal));
        connectorProps.put(TOPIC_WHITELIST_CONFIG, listToString(REUSABLE_TOPICS[0],
            REUSABLE_TOPICS[1], REUSABLE_TOPICS[2]));
        connectorProps.put(TOPIC_REGEX_CONFIG, TOPIC_REGEX_VALUE);
        Map<String, String> taskProps = getTaskProps(connectorProps);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(taskProps);
                produceBytesBasedMessages(REUSABLE_TOPICS[0], numMsgsPerTopic);
                produceBytesBasedMessages(REUSABLE_TOPICS[1], numMsgsPerTopic);
                produceBytesBasedMessages(REUSABLE_TOPICS[2], numMsgsPerTopic);
                produceBytesBasedMessages(REUSABLE_TOPICS[3], numMsgsPerTopic);
                produceBytesBasedMessages(REUSABLE_TOPICS[4], numMsgsPerTopic);
                produceBytesBasedMessages(REUSABLE_TOPICS[5], numMsgsPerTopic);
                List<SourceRecord> records = task.poll();
                assertEquals(numMsgsTotal, records.size());
            } finally {
                task.stop();
            }
        });
    }

    @Test
    public void partitionedTopicShouldCreateOneTopic() {
        final int partitions = 4;
        final int numMsgs = 4;
        assertDoesNotThrow(() -> {
            createPartitionedTopic(topic, partitions);
        });
        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(SERVICE_URL_CONFIG, getServiceUrl());
        connectorProps.put(SERVICE_HTTP_URL_CONFIG, getServiceHttpUrl());
        connectorProps.put(BATCH_MAX_NUM_MESSAGES_CONFIG, String.valueOf(numMsgs));
        connectorProps.put(TOPIC_WHITELIST_CONFIG, topic);
        Map<String, String> taskProps = getTaskProps(connectorProps);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(taskProps);
                produceBytesBasedMessages(topic, numMsgs);
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

    @Test
    public void checkShouldCreateStructFromJSON() {
        final int numMsgs = 1;
        assertDoesNotThrow(() -> {
            produceJSONBasedMessages(topic, numMsgs);
        });
        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(SERVICE_URL_CONFIG, getServiceUrl());
        connectorProps.put(SERVICE_HTTP_URL_CONFIG, getServiceHttpUrl());
        connectorProps.put(BATCH_MAX_NUM_MESSAGES_CONFIG, String.valueOf(numMsgs));
        connectorProps.put(TOPIC_WHITELIST_CONFIG, topic);
        connectorProps.put(SCHEMA_DESERIALIZATION_ENABLED_CONFIG, String.valueOf(true));
        Map<String, String> taskProps = getTaskProps(connectorProps);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(taskProps);
                produceJSONBasedMessages(topic, numMsgs);
                List<SourceRecord> records = task.poll();
                assertEquals(numMsgs, records.size());
                SourceRecord record = records.get(0);
                Schema schema = record.valueSchema();
                assertEquals(schema.type(), Type.STRUCT);
                assertTrue(record.value() instanceof Struct);
            } finally {
                task.stop();
            }
        });
    }

    @Test
    public void checkShouldCreateStructFromAvro() {
        final int numMsgs = 1;
        assertDoesNotThrow(() -> {
            produceAvroBasedMessages(topic, numMsgs);
        });
        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(SERVICE_URL_CONFIG, getServiceUrl());
        connectorProps.put(SERVICE_HTTP_URL_CONFIG, getServiceHttpUrl());
        connectorProps.put(BATCH_MAX_NUM_MESSAGES_CONFIG, String.valueOf(numMsgs));
        connectorProps.put(TOPIC_WHITELIST_CONFIG, topic);
        connectorProps.put(SCHEMA_DESERIALIZATION_ENABLED_CONFIG, String.valueOf(true));
        Map<String, String> taskProps = getTaskProps(connectorProps);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(taskProps);
                produceAvroBasedMessages(topic, numMsgs);
                List<SourceRecord> records = task.poll();
                assertEquals(numMsgs, records.size());
                SourceRecord record = records.get(0);
                Schema schema = record.valueSchema();
                assertEquals(schema.type(), Type.STRUCT);
                assertTrue(record.value() instanceof Struct);
            } finally {
                task.stop();
            }
        });
    }

    @Test
    public void checkShouldCreateStructFromAvroGen() {
        final int numMsgs = 1;
        assertDoesNotThrow(() -> {
            produceAvroGenBasedMessages(topic, numMsgs);
        });
        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(SERVICE_URL_CONFIG, getServiceUrl());
        connectorProps.put(SERVICE_HTTP_URL_CONFIG, getServiceHttpUrl());
        connectorProps.put(BATCH_MAX_NUM_MESSAGES_CONFIG, String.valueOf(numMsgs));
        connectorProps.put(TOPIC_WHITELIST_CONFIG, topic);
        connectorProps.put(SCHEMA_DESERIALIZATION_ENABLED_CONFIG, String.valueOf(true));
        Map<String, String> taskProps = getTaskProps(connectorProps);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(taskProps);
                produceAvroGenBasedMessages(topic, numMsgs);
                List<SourceRecord> records = task.poll();
                assertEquals(numMsgs, records.size());
                SourceRecord record = records.get(0);
                Schema schema = record.valueSchema();
                assertEquals(schema.type(), Type.STRUCT);
                assertTrue(record.value() instanceof Struct);
            } finally {
                task.stop();
            }
        });
    }

    @Test
    public void checkShouldCreateStructFromProtoBufGen() {
        final int numMsgs = 1;
        assertDoesNotThrow(() -> {
            produceProtoBufBasedMessages(topic, numMsgs);
        });
        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(SERVICE_URL_CONFIG, getServiceUrl());
        connectorProps.put(SERVICE_HTTP_URL_CONFIG, getServiceHttpUrl());
        connectorProps.put(BATCH_MAX_NUM_MESSAGES_CONFIG, String.valueOf(numMsgs));
        connectorProps.put(TOPIC_WHITELIST_CONFIG, topic);
        connectorProps.put(SCHEMA_DESERIALIZATION_ENABLED_CONFIG, String.valueOf(true));
        connectorProps.put(PROTOBUF_JAVA_GENERATED_CLASS_CONFIG,
            ProtoBufGenComplexType.class.getName());
        connectorProps.put(PROTOBUF_JAVA_MESSAGE_CLASS_CONFIG,
            ProtoBufGenComplexType.ProtoBufComplexType.class.getName());
        Map<String, String> taskProps = getTaskProps(connectorProps);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(taskProps);
                produceProtoBufBasedMessages(topic, numMsgs);
                List<SourceRecord> records = task.poll();
                //assertEquals(numMsgs, records.size());
                //SourceRecord record = records.get(0);
                //Schema schema = record.valueSchema();
                //assertEquals(schema.type(), Type.BYTES);
                //assertTrue(record.value() instanceof byte[]);
            } finally {
                task.stop();
            }
        });
    }

    private Map<String, String> getTaskProps(Map<String, String> connectorProps) {
        PulsarSourceConnector connector = new PulsarSourceConnector();
        connector.start(connectorProps);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        return taskConfigs.get(0);
    }
    
}
