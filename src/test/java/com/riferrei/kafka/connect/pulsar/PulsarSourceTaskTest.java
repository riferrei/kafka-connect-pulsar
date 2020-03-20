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

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
            produceMessages(TOPIC[0], numMsgs);
        });
        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(SERVICE_URL_CONFIG, getServiceUrl());
        connectorProps.put(SERVICE_HTTP_URL_CONFIG, getServiceHttpUrl());
        connectorProps.put(BATCH_MAX_NUM_MESSAGES_CONFIG, String.valueOf(numMsgs));
        connectorProps.put(SUBSCRIPTION_INITIAL_POSITION_CONFIG,
            SubscriptionInitialPosition.Earliest.name());
        connectorProps.put(TOPIC_WHITELIST_CONFIG, TOPIC[0]);
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
        connectorProps.put(TOPIC_WHITELIST_CONFIG, TOPIC[0]);
        Map<String, String> taskProps = getTaskProps(connectorProps);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(taskProps);
                produceMessages(TOPIC[0], numMsgs);
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
            TopicNamingStrategyOptions.NameOnly.name());
        connectorProps.put(TOPIC_WHITELIST_CONFIG, TOPIC[0]);
        Map<String, String> taskProps = getTaskProps(connectorProps);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(taskProps);
                produceMessages(TOPIC[0], 1);
                List<SourceRecord> records = task.poll();
                assertEquals(TOPIC[0], records.get(0).topic());
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
            TopicNamingStrategyOptions.FullyQualified.name());
        connectorProps.put(TOPIC_WHITELIST_CONFIG, TOPIC[0]);
        Map<String, String> taskProps = getTaskProps(connectorProps);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(taskProps);
                produceMessages(TOPIC[0], 1);
                List<SourceRecord> records = task.poll();
                assertEquals(fullyQualifiedTopic(TOPIC[0]), records.get(0).topic());
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
        connectorProps.put(TOPIC_WHITELIST_CONFIG, listToString(TOPIC[0], TOPIC[1], TOPIC[2]));
        Map<String, String> taskProps = getTaskProps(connectorProps);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(taskProps);
                produceMessages(TOPIC[0], numMsgsPerTopic);
                produceMessages(TOPIC[1], numMsgsPerTopic);
                produceMessages(TOPIC[2], numMsgsPerTopic);
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
            produceMessages(TOPIC[0], numMsgsPerTopic);
            produceMessages(TOPIC[1], numMsgsPerTopic);
            produceMessages(TOPIC[2], numMsgsPerTopic);
        });
        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(SERVICE_URL_CONFIG, getServiceUrl());
        connectorProps.put(SERVICE_HTTP_URL_CONFIG, getServiceHttpUrl());
        connectorProps.put(BATCH_MAX_NUM_MESSAGES_CONFIG, String.valueOf(numMsgsTotal));
        connectorProps.put(SUBSCRIPTION_INITIAL_POSITION_CONFIG,
            SubscriptionInitialPosition.Earliest.name());
        connectorProps.put(TOPIC_REGEX_CONFIG, TOPIC_REGEX_VALUE);
        Map<String, String> taskProps = getTaskProps(connectorProps);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(taskProps);
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
        final int numMsgsTotal = 25;
        assertDoesNotThrow(() -> {
            produceMessages(TOPIC[0], numMsgsPerTopic);
            produceMessages(TOPIC[1], numMsgsPerTopic);
            produceMessages(TOPIC[2], numMsgsPerTopic);
            produceMessages(TOPIC[3], numMsgsPerTopic);
            produceMessages(TOPIC[4], numMsgsPerTopic);
            produceMessages(TOPIC[5], numMsgsPerTopic);
        });
        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(SERVICE_URL_CONFIG, getServiceUrl());
        connectorProps.put(SERVICE_HTTP_URL_CONFIG, getServiceHttpUrl());
        connectorProps.put(BATCH_MAX_NUM_MESSAGES_CONFIG, String.valueOf(numMsgsTotal));
        connectorProps.put(SUBSCRIPTION_INITIAL_POSITION_CONFIG,
            SubscriptionInitialPosition.Earliest.name());
        connectorProps.put(TOPIC_WHITELIST_CONFIG, listToString(TOPIC[0], TOPIC[1], TOPIC[2]));
        connectorProps.put(TOPIC_REGEX_CONFIG, TOPIC_REGEX_VALUE);
        Map<String, String> taskProps = getTaskProps(connectorProps);
        PulsarSourceTask task = new PulsarSourceTask();
        assertDoesNotThrow(() -> {
            try {
                task.start(taskProps);
                List<SourceRecord> records = task.poll();
                assertEquals(numMsgsTotal, records.size());
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

    private Map<String, String> getTaskProps(Map<String, String> connectorProps) {
        PulsarSourceConnector connector = new PulsarSourceConnector();
        connector.start(connectorProps);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        return taskConfigs.get(0);
    }
    
}
