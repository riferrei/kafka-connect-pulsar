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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.riferrei.kafka.connect.pulsar.PulsarSourceConnectorConfig.*;

public class PulsarSourceTask extends SourceTask {

    private static Logger log = LoggerFactory.getLogger(PulsarSourceTask.class);

    private List<String> partitionedTopics;
    private PulsarSourceConnectorConfig config;
    private PulsarClient client;
    private Consumer<byte[]> consumer;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        partitionedTopics = getPartitionedTopics(properties);
        config = new PulsarSourceConnectorConfig(properties);
        String serviceUrl = config.getString(PulsarSourceConnectorConfig.SERVICE_URL_CONFIG);
        String subscriptionName = config.getString(PulsarSourceConnectorConfig.SUBSCRIPTION_NAME_CONFIG);
        subscriptionName = subscriptionName == null ? UUID.randomUUID().toString() : subscriptionName;
        int batchMaxNumMessages = config.getInt(PulsarSourceConnectorConfig.BATCH_MAX_NUM_MESSAGES_CONFIG);
        int batchMaxNumBytes = config.getInt(PulsarSourceConnectorConfig.BATCH_MAX_NUM_BYTES_CONFIG);
        int batchTimeout = config.getInt(PulsarSourceConnectorConfig.BATCH_TIMEOUT_CONFIG);
        ConsumerBuilder<byte[]> builder = null;
        try {
            client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .loadConf(clientConfig())
                .build();
            builder = client.newConsumer()
                .subscriptionName(subscriptionName)
                .loadConf(consumerConfig())
                .batchReceivePolicy(BatchReceivePolicy.builder()
                    .timeout(batchTimeout, TimeUnit.MILLISECONDS)
                    .maxNumMessages(batchMaxNumMessages)
                    .maxNumBytes(batchMaxNumBytes)
                    .build());
            if (properties.containsKey(TOPIC_PATTERN)) {
                builder.topicsPattern(properties.get(TOPIC_PATTERN));
            } else if (properties.containsKey(TOPIC_NAMES)) {
                builder.topics(getTopicNames(properties));
            }
            consumer = builder.subscribe();
        } catch (PulsarClientException pce) {
            if (log.isErrorEnabled()) {
                log.error("Exception thrown while creating consumer: ", pce);
            }
        }
    }

    private List<String> getPartitionedTopics(Map<String, String> properties) {
        List<String> partitionedTopics = new ArrayList<>();
        List<String> topicNames = getTopicNames(properties);
        String serviceHttpUrl = properties.get(SERVICE_HTTP_URL_CONFIG);
        PulsarAdmin pulsarAdmin = null;
        try {
            pulsarAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(serviceHttpUrl)
                .build();
            Topics topics = pulsarAdmin.topics();
            for (String topicName : topicNames) {
                if (topics.getPartitionedTopicMetadata(topicName).partitions > 0) {
                    partitionedTopics.add(topicName);
                }
            }
        } catch (PulsarClientException | PulsarAdminException pe) {
            if (log.isErrorEnabled()) {
                log.error("Exception thrown during getPartitionedTopics(): ", pe);
            }
        } finally {
            pulsarAdmin.close();
        }
        return partitionedTopics;
    }

    private List<String> getTopicNames(Map<String, String> properties) {
        String topicNames = properties.get(PulsarSourceConnectorConfig.TOPIC_NAMES);
        String[] topicList = topicNames.split(",");
        List<String> topics = new ArrayList<>(topicList.length);
        for (String topic : topicList) {
            topics.add(topic.trim());
        }
        return topics;
    }

    private Map<String, Object> clientConfig() {
        Map<String, Object> clientConfig = new HashMap<>();
        clientConfig.put("authPluginClassName", config.getString(AUTH_PLUGIN_CLASS_NAME_CONFIG));
        clientConfig.put("authParams", config.getString(AUTH_PARAMS_CONFIG));
        clientConfig.put("operationTimeoutMs", config.getLong(OPERATION_TIMEOUT_MS_CONFIG));
        clientConfig.put("statsIntervalSeconds", config.getLong(STATS_INTERVAL_SECONDS_CONFIG));
        clientConfig.put("numIoThreads", config.getInt(NUM_IO_THREADS_CONFIG));
        clientConfig.put("numListenerThreads", config.getInt(NUM_LISTENER_THREADS_CONFIG));
        clientConfig.put("useTcpNoDelay", config.getBoolean(USE_TCP_NODELAY_CONFIG));
        clientConfig.put("useTls", config.getBoolean(USE_TLS_CONFIG));
        clientConfig.put("tlsTrustCertsFilePath", config.getString(TLS_TRUST_CERTS_FILE_PATH_CONFIG));
        clientConfig.put("tlsAllowInsecureConnection", config.getBoolean(TLS_ALLOW_INSECURE_CONNECTION_CONFIG));
        clientConfig.put("tlsHostnameVerificationEnable", config.getBoolean(TLS_HOSTNAME_VERIFICATION_ENABLED_CONFIG));
        clientConfig.put("concurrentLookupRequest", config.getInt(CONCURRENT_LOOKUP_REQUEST_CONFIG));
        clientConfig.put("maxLookupRequest", config.getInt(MAX_LOOKUP_REQUEST_CONFIG));
        clientConfig.put("maxNumberOfRejectedRequestPerConnection", config.getInt(MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION_CONFIG));
        clientConfig.put("keepAliveIntervalSeconds", config.getInt(KEEP_ALIVE_INTERVAL_SECONDS_CONFIG));
        clientConfig.put("connectionTimeoutMs", config.getInt(CONNECTION_TIMEOUT_MS_CONFIG));
        clientConfig.put("requestTimeoutMs", config.getInt(REQUEST_TIMEOUT_MS_CONFIG));
        clientConfig.put("initialBackoffIntervalNanos", TimeUnit.MILLISECONDS.toNanos(config.getLong(INITIAL_BACKOFF_INTERVAL_NANOS_CONFIG)));
        clientConfig.put("maxBackoffIntervalNanos", TimeUnit.MILLISECONDS.toNanos(config.getLong(MAX_BACKOFF_INTERVAL_NANOS_CONFIG)));
        return clientConfig;
    }

    private Map<String, Object> consumerConfig() {
        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put("subscriptionType", config.getString(SUBSCRIPTION_TYPE_CONFIG));
        consumerConfig.put("receiverQueueSize", config.getInt(RECEIVER_QUEUE_SIZE_CONFIG));
        consumerConfig.put("acknowledgementsGroupTimeMicros", TimeUnit.MILLISECONDS.toMicros(config.getLong(ACKNOWLEDMENTS_GROUP_TIME_MICROS_CONFIG)));
        consumerConfig.put("negativeAckRedeliveryDelayMicros", TimeUnit.MILLISECONDS.toMicros(config.getLong(NEGATIVE_ACK_REDELIVERY_DELAY_MICROS_CONFIG)));
        consumerConfig.put("maxTotalReceiverQueueSizeAcrossPartitions", config.getInt(MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS_CONFIG));
        consumerConfig.put("consumerName", config.getString(CONSUMER_NAME_CONFIG));
        consumerConfig.put("ackTimeoutMillis", config.getLong(ACK_TIMEOUT_MILLIS_CONFIG));
        consumerConfig.put("tickDurationMillis", config.getLong(TICK_DURATION_MILLIS_CONFIG));
        consumerConfig.put("priorityLevel", config.getInt(PRIORITY_LEVEL_CONFIG));
        consumerConfig.put("cryptoFailureAction", config.getString(CRYPTO_FAILURE_ACTION_CONFIG));
        consumerConfig.put("readCompacted", config.getBoolean(READ_COMPACTED_CONFIG));
        consumerConfig.put("subscriptionInitialPosition", config.getString(SUBSCRIPTION_INITIAL_POSITION_CONFIG));
        consumerConfig.put("patternAutoDiscoveryPeriod", config.getInt(PATTERN_AUTO_DISCOVERY_PERIOD_CONFIG));
        consumerConfig.put("regexSubscriptionMode", config.getString(REGEX_SUBSCRIPTION_MODE_CONFIG));
        consumerConfig.put("autoUpdatePartitions", config.getBoolean(AUTO_UPDATE_PARTITIONS_CONFIG));
        consumerConfig.put("replicateSubscriptionState", config.getBoolean(REPLICATE_SUBSCRIPTION_STATE_CONFIG));
        return consumerConfig;
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        Messages<byte[]> messages = null;
        try {
            messages = consumer.batchReceive();
            if (messages != null && messages.size() > 0) {
                for (Message<byte[]> message : messages) {
                    records.add(createSourceRecord(message));
                }
            }
            consumer.acknowledge(messages);
        } catch (PulsarClientException pce) {
            consumer.negativeAcknowledge(messages);
            if (log.isErrorEnabled()) {
                log.error("Exception thrown during poll(): ", pce);
            }
        }
        return records;
    }

    private SourceRecord createSourceRecord(Message<byte[]> message) {
        String topicName = getTopicName(message.getTopicName());
        byte[] recordKey = message.getKeyBytes();
        byte[] recordValue = message.getData();
        byte[] messageId = message.getMessageId().toByteArray();
        String producerName = message.getProducerName();
        Map<String, String> sourcePartition = Collections
            .singletonMap("producer", producerName);
        Map<String, byte[]> sourceOffset = Collections
            .singletonMap("messageId", messageId);
        return new SourceRecord(
            sourcePartition, sourceOffset, topicName,
            null, recordKey, null, recordValue);
    }

    private String getTopicName(String topicName) {
        // If the topic is partitioned then we need to
        // remove the '-partition-n' suffix to avoid
        // ending up with multiple topics in the Kafka
        // side that ultimately represent only 1 topic.
        for (String partitionedTopic : partitionedTopics) {
            if (topicName.contains(partitionedTopic)
                && topicName.contains("-partition-")) {
                topicName = topicName.replaceAll("-partition-\\d+", "");
                break;
            }
        }
        URI topic = null;
        try {
            topic = new URI(topicName);
        } catch (Exception ex) {
            if (log.isErrorEnabled()) {
                log.error("Exception thrown while parsing the topic: ", ex);
            }
            return null;
        }
        String tnsValue = config.getString(TOPIC_NAMING_STRATEGY_CONFIG);
        TopicNamingStrategyOptions tns = TopicNamingStrategyOptions.valueOf(tnsValue);
        if (tns.equals(TopicNamingStrategyOptions.NameOnly)) {
            String[] topicNameParts = topic.getPath().split("/");
            topicName = topicNameParts[topicNameParts.length - 1];
        } else if (tns.equals(TopicNamingStrategyOptions.FullyQualified)) {
            StringBuilder fullyQualifiedTopic = new StringBuilder();
            if (topic.getHost() != null && topic.getHost().length() > 0) {
                fullyQualifiedTopic.append(topic.getHost());
            }
            fullyQualifiedTopic.append(topic.getPath().replaceAll("/", "-"));
            topicName = fullyQualifiedTopic.toString();
        }
        return topicName;
    }

    @Override
    public void stop() {
        if (consumer != null) {
            consumer.closeAsync();
        }
        if (client != null) {
            client.closeAsync();
        }
    }

}
