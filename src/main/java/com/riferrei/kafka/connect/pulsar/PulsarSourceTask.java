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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaInfoWithVersion;
import com.google.protobuf.Any;

import static com.riferrei.kafka.connect.pulsar.PulsarSourceConnectorConfig.*;

public class PulsarSourceTask extends SourceTask {

    private static Logger log = LoggerFactory.getLogger(PulsarSourceTask.class);

    private PulsarSourceConnectorConfig config;
    private PulsarAdmin pulsarAdmin;
    private PulsarClient pulsarClient;

    private List<Consumer<byte[]>> bytesBasedConsumers = new ArrayList<>();
    private List<Consumer<GenericRecord>> recordBasedConsumers = new ArrayList<>();
    private List<Consumer<Any>> protoBufBasedConsumers = new ArrayList<>();

    private Deserializer<byte[]> bytesDeserializer;
    private Deserializer<GenericRecord> recordDeserializer;
    private Deserializer<Any> protoBufDeserializer;

    @Override
    public String version() {
        return PropertiesUtil.getConnectorVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        config = new PulsarSourceConnectorConfig(properties);
        String serviceHttpUrl = config.getString(SERVICE_HTTP_URL_CONFIG);
        String authPluginClassName = config.getString(AUTH_PLUGIN_CLASS_NAME_CONFIG);
        String authParams = config.getString(AUTH_PARAMS_CONFIG);
        String tlsTrustCertsFilePath = config.getString(TLS_TRUST_CERTS_FILE_PATH_CONFIG);
        boolean tlsAllowInsecureConnection = config.getBoolean(TLS_ALLOW_INSECURE_CONNECTION_CONFIG);
        boolean tlsHostnameVerificationEnabled = config.getBoolean(TLS_HOSTNAME_VERIFICATION_ENABLED_CONFIG);
        int connectionTimeout = config.getInt(CONNECTION_TIMEOUT_MS_CONFIG);
        int requestTimeout = config.getInt(REQUEST_TIMEOUT_MS_CONFIG);
        String serviceUrl = config.getString(SERVICE_URL_CONFIG);
        try {
            pulsarAdmin = PulsarAdmin.builder()
                .authentication(authPluginClassName, authParams)
                .tlsTrustCertsFilePath(tlsTrustCertsFilePath)
                .allowTlsInsecureConnection(tlsAllowInsecureConnection)
                .enableTlsHostnameVerification(tlsHostnameVerificationEnabled)
                .connectionTimeout(connectionTimeout, TimeUnit.MILLISECONDS)
                .requestTimeout(requestTimeout, TimeUnit.MILLISECONDS)
                .serviceHttpUrl(serviceHttpUrl)
                .build();
            pulsarClient = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .loadConf(clientConfig())
                .build();
        } catch (PulsarClientException pce) {
            throw new ConnectException("Error while creating clients");
        }
        List<String> topics = getTopicNames(properties);
        int batchMaxNumMessages = config.getInt(BATCH_MAX_NUM_MESSAGES_CONFIG);
        int batchMaxNumBytes = config.getInt(BATCH_MAX_NUM_BYTES_CONFIG);
        int batchTimeout = config.getInt(BATCH_TIMEOUT_CONFIG);
        boolean deadLetterTopicEnabled = config.getBoolean(DEAD_LETTER_TOPIC_ENABLED_CONFIG);
        int maxRedeliverCount = config.getInt(DEAD_LETTER_TOPIC_MAX_REDELIVER_COUNT_CONFIG);
        boolean messageDeserializationEnabled = config.getBoolean(MESSAGE_DESERIALIZATION_ENABLED_CONFIG);
        createConsumers(topics, batchTimeout, batchMaxNumMessages, batchMaxNumBytes,
            deadLetterTopicEnabled, maxRedeliverCount, messageDeserializationEnabled);
    }

    private List<String> getTopicNames(Map<String, String> properties) {
        String topicNames = properties.get(TOPIC_NAMES);
        String[] topicList = topicNames.split(",");
        List<String> topics = new ArrayList<>(topicList.length);
        for (String topic : topicList) {
            topics.add(topic.trim());
        }
        return topics;
    }

    private String subscriptionName(String topic) {
        return String.format("connect-task-%s", topic);
    }

    private String deadLetterTopic(String topic) {
        return String.format("connect-task-%s-DLQ", topic);
    }

    private void createConsumers(List<String> topics, int batchTimeout,
        int batchMaxNumMessages, int batchMaxNumBytes, boolean deadLetterTopicEnabled,
        int maxRedeliverCount, boolean messageDeserializationEnabled) {
        for (String topic : topics) {
            if (messageDeserializationEnabled) {
                SchemaInfoWithVersion schemaInfoWithVersion = null;
                try {
                    schemaInfoWithVersion = pulsarAdmin.schemas()
                        .getSchemaInfoWithVersion(topic);
                } catch (PulsarAdminException pae) {
                    // Ignore any exceptions thrown
                }
                if (schemaInfoWithVersion != null) {
                    SchemaInfo schemaInfo = schemaInfoWithVersion.getSchemaInfo();
                    long schemaVersion = schemaInfoWithVersion.getVersion();
                    SchemaRegistry.createSchema(topic, schemaInfo, schemaVersion);
                    switch (schemaInfo.getType()) {
                        case JSON: case AVRO:
                            recordBasedConsumers.add(createRecordBasedConsumer(topic,
                                batchTimeout, batchMaxNumMessages, batchMaxNumBytes,
                                deadLetterTopicEnabled, maxRedeliverCount));
                            break;
                        case PROTOBUF:
                            protoBufBasedConsumers.add(createProtoBufBasedConsumer(topic,
                                batchTimeout, batchMaxNumMessages, batchMaxNumBytes,
                                deadLetterTopicEnabled, maxRedeliverCount));
                            break;
                        default:
                            bytesBasedConsumers.add(createBytesBasedConsumer(topic,
                                batchTimeout, batchMaxNumMessages, batchMaxNumBytes,
                                deadLetterTopicEnabled, maxRedeliverCount));
                            break;
                    }
                } else {
                    bytesBasedConsumers.add(createBytesBasedConsumer(topic,
                        batchTimeout, batchMaxNumMessages, batchMaxNumBytes,
                        deadLetterTopicEnabled, maxRedeliverCount));
                }
            } else {
                bytesBasedConsumers.add(createBytesBasedConsumer(topic,
                    batchTimeout, batchMaxNumMessages, batchMaxNumBytes,
                    deadLetterTopicEnabled, maxRedeliverCount));
            }
        }
    }

    private Consumer<byte[]> createBytesBasedConsumer(String topic,
        int batchTimeout, int batchMaxNumMessages, int batchMaxNumBytes,
        boolean deadLetterTopicEnabled, int maxRedeliverCount) {
        if (bytesDeserializer == null) {
            String tns = config.getString(TOPIC_NAMING_STRATEGY_CONFIG);
            TopicNamingStrategy topicNamingStrategy = TopicNamingStrategy.valueOf(tns);
            bytesDeserializer = new BytesDeserializer(topicNamingStrategy);
        }
        Consumer<byte[]> consumer = null;
        ConsumerBuilder<byte[]> builder = pulsarClient.newConsumer(
            org.apache.pulsar.client.api.Schema.BYTES)
                .loadConf(consumerConfig())
                .subscriptionName(subscriptionName(topic))
                .topic(topic)
                .batchReceivePolicy(BatchReceivePolicy.builder()
                    .timeout(batchTimeout, TimeUnit.MILLISECONDS)
                    .maxNumMessages(batchMaxNumMessages)
                    .maxNumBytes(batchMaxNumBytes)
                    .build());
        if (deadLetterTopicEnabled) {
            builder.deadLetterPolicy(DeadLetterPolicy.builder()
                .deadLetterTopic(deadLetterTopic(topic))
                .maxRedeliverCount(maxRedeliverCount)
                .build());
        }
        try {
            consumer = builder.subscribe();
        } catch (PulsarClientException pce) {
            throw new ConnectException(String.format(
                "Error creating consumer for topic '%s'",
                topic), pce);
        }
        return consumer;
    }

    private Consumer<GenericRecord> createRecordBasedConsumer(String topic,
        int batchTimeout, int batchMaxNumMessages, int batchMaxNumBytes,
        boolean deadLetterTopicEnabled, int maxRedeliverCount) {
        if (recordDeserializer == null) {
            String tns = config.getString(TOPIC_NAMING_STRATEGY_CONFIG);
            TopicNamingStrategy topicNamingStrategy = TopicNamingStrategy.valueOf(tns);
            recordDeserializer = new RecordDeserializer(pulsarAdmin, topicNamingStrategy);
        }
        Consumer<GenericRecord> consumer = null;
        ConsumerBuilder<GenericRecord> builder = pulsarClient.newConsumer(
            org.apache.pulsar.client.api.Schema.AUTO_CONSUME())
                .loadConf(consumerConfig())
                .subscriptionName(subscriptionName(topic))
                .topic(topic)
                .batchReceivePolicy(BatchReceivePolicy.builder()
                    .timeout(batchTimeout, TimeUnit.MILLISECONDS)
                    .maxNumMessages(batchMaxNumMessages)
                    .maxNumBytes(batchMaxNumBytes)
                    .build());
        if (deadLetterTopicEnabled) {
            builder.deadLetterPolicy(DeadLetterPolicy.builder()
                .deadLetterTopic(deadLetterTopic(topic))
                .maxRedeliverCount(maxRedeliverCount)
                .build());
        }
        try {
            consumer = builder.subscribe();
        } catch (PulsarClientException pce) {
            throw new ConnectException(String.format(
                "Error creating consumer for topic '%s'",
                topic), pce);
        }
        return consumer;
    }

    private Consumer<Any> createProtoBufBasedConsumer(String topic,
        int batchTimeout, int batchMaxNumMessages, int batchMaxNumBytes,
        boolean deadLetterTopicEnabled, int maxRedeliverCount) {
        if (protoBufDeserializer == null) {
            String tns = config.getString(TOPIC_NAMING_STRATEGY_CONFIG);
            TopicNamingStrategy topicNamingStrategy = TopicNamingStrategy.valueOf(tns);
            String messageClass = config.getString(PROTOBUF_JAVA_MESSAGE_CLASS_CONFIG);
            if (messageClass == null) {
                throw new ConnectException(String.format("The property '%s' is "
                    + "enabled and it was detected that the topic '%s' has its "
                    + "schema based on Protocol Buffers. Thus, the property '%s' "
                    + "also need to be enabled in the connector configuration.",
                    MESSAGE_DESERIALIZATION_ENABLED_CONFIG, topic,
                    PROTOBUF_JAVA_MESSAGE_CLASS_CONFIG));
            }
            protoBufDeserializer = new ProtoBufDeserializer(pulsarAdmin,
                topicNamingStrategy, messageClass);
        }
        Consumer<Any> consumer = null;
        ConsumerBuilder<Any> builder = pulsarClient.newConsumer(
            org.apache.pulsar.client.api.Schema.PROTOBUF(Any.class))
                .loadConf(consumerConfig())
                .subscriptionName(subscriptionName(topic))
                .topic(topic)
                .batchReceivePolicy(BatchReceivePolicy.builder()
                    .timeout(batchTimeout, TimeUnit.MILLISECONDS)
                    .maxNumMessages(batchMaxNumMessages)
                    .maxNumBytes(batchMaxNumBytes)
                    .build());
        if (deadLetterTopicEnabled) {
            builder.deadLetterPolicy(DeadLetterPolicy.builder()
                .deadLetterTopic(deadLetterTopic(topic))
                .maxRedeliverCount(maxRedeliverCount)
                .build());
        }
        try {
            consumer = builder.subscribe();
        } catch (PulsarClientException pce) {
            throw new ConnectException(String.format(
                "Error creating consumer for topic '%s'",
                topic), pce);
        }
        return consumer;
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
        consumerConfig.put("subscriptionType", SubscriptionType.Exclusive.name());
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
        consumerConfig.put("regexSubscriptionMode", config.getString(REGEX_SUBSCRIPTION_MODE_CONFIG));
        consumerConfig.put("autoUpdatePartitions", config.getBoolean(AUTO_UPDATE_PARTITIONS_CONFIG));
        consumerConfig.put("replicateSubscriptionState", config.getBoolean(REPLICATE_SUBSCRIPTION_STATE_CONFIG));
        return consumerConfig;
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        for (Consumer<byte[]> consumer : bytesBasedConsumers) {
            Messages<byte[]> messages = null;
            try {
                messages = consumer.batchReceive();
            } catch (Exception ex) {
                log.warn("Error while polling messages", ex);
            }
            if (messages != null && messages.size() > 0) {
                for (Message<byte[]> message : messages) {
                    try {
                        SourceRecord record = fromBytes(message);
                        if (record != null) {
                            records.add(record);
                        }
                        consumer.acknowledge(message);
                    } catch (Exception ex) {
                        consumer.negativeAcknowledge(message);
                    }
                }
            }
        }
        for (Consumer<GenericRecord> consumer : recordBasedConsumers) {
            Messages<GenericRecord> messages = null;
            try {
                messages = consumer.batchReceive();
            } catch (Exception ex) {
                log.warn("Error while polling messages", ex);
            }
            if (messages != null && messages.size() > 0) {
                for (Message<GenericRecord> message : messages) {
                    try {
                        SourceRecord record = fromRecord(message);
                        if (record != null) {
                            records.add(record);
                        }
                        consumer.acknowledge(message);
                    } catch (Exception ex) {
                        consumer.negativeAcknowledge(message);
                    }
                }
            }
        }
        for (Consumer<Any> consumer : protoBufBasedConsumers) {
            Messages<Any> messages = null;
            try {
                messages = consumer.batchReceive();
            } catch (Exception ex) {
                log.warn("Error while polling messages", ex);
            }
            if (messages != null && messages.size() > 0) {
                for (Message<Any> message : messages) {
                    try {
                        SourceRecord record = fromProtoBuf(message);
                        if (record != null) {
                            records.add(record);
                        }
                    } catch (Exception ex) {
                        consumer.negativeAcknowledge(message);
                    }
                }
            }
        }
        return records;
    }

    private SourceRecord fromBytes(Message<byte[]> message) {
        return bytesDeserializer.deserialize(message);
    }

    private SourceRecord fromRecord(Message<GenericRecord> message) {
        return recordDeserializer.deserialize(message);
    }

    private SourceRecord fromProtoBuf(Message<Any> message) {
        return protoBufDeserializer.deserialize(message);
    }

    @Override
    public void stop() {
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }
        for (Consumer<byte[]> consumer : bytesBasedConsumers) {
            consumer.closeAsync().exceptionally((ex) -> {
                if (log.isErrorEnabled()) {
                    log.error("Error closing a consumer", ex);
                }
                return null;
            });
        }
        for (Consumer<GenericRecord> consumer : recordBasedConsumers) {
            consumer.closeAsync().exceptionally((ex) -> {
                if (log.isErrorEnabled()) {
                    log.error("Error closing a consumer", ex);
                }
                return null;
            });
        }
        for (Consumer<Any> consumer : protoBufBasedConsumers) {
            consumer.closeAsync().exceptionally((ex) -> {
                if (log.isErrorEnabled()) {
                    log.error("Error closing a consumer", ex);
                }
                return null;
            });
        }
        if (pulsarClient != null) {
            pulsarClient.closeAsync().exceptionally((ex) -> {
                if (log.isErrorEnabled()) {
                    log.error("Error closing a client", ex);
                }
                return null;
            });
        }
    }

}
