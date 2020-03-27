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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.avro.Schema.Type;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
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
import org.apache.pulsar.client.impl.schema.SchemaUtils;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecordUtil;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaInfoWithVersion;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericData;

import static com.riferrei.kafka.connect.pulsar.PulsarSourceConnectorConfig.*;

public class PulsarSourceTask extends SourceTask {

    private static Logger log = LoggerFactory.getLogger(PulsarSourceTask.class);

    private PulsarSourceConnectorConfig config;
    private PulsarAdmin pulsarAdmin;
    private PulsarClient pulsarClient;
    private List<Consumer<byte[]>> bytesBasedConsumers;
    private List<Consumer<GenericRecord>> structBasedConsumers;
    private Map<String, Schema> schemas;

    @Override
    public String version() {
        return PropertiesUtil.getConnectorVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        config = new PulsarSourceConnectorConfig(properties);
        String serviceHttpUrl = config.getString(SERVICE_HTTP_URL_CONFIG);
        String serviceUrl = config.getString(SERVICE_URL_CONFIG);
        try {
            pulsarAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(serviceHttpUrl)
                .build();
            pulsarClient = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .loadConf(clientConfig())
                .build();
        } catch (PulsarClientException pce) {
            if (log.isErrorEnabled()) {
                log.error("Error while creating clients: ", pce);
            }
        }
        List<String> topics = getTopicNames(properties);
        int batchMaxNumMessages = config.getInt(BATCH_MAX_NUM_MESSAGES_CONFIG);
        int batchMaxNumBytes = config.getInt(BATCH_MAX_NUM_BYTES_CONFIG);
        int batchTimeout = config.getInt(BATCH_TIMEOUT_CONFIG);
        boolean deadLetterTopicEnabled = config.getBoolean(DEAD_LETTER_TOPIC_ENABLED_CONFIG);
        int maxRedeliverCount = config.getInt(DEAD_LETTER_TOPIC_MAX_REDELIVER_COUNT_CONFIG);
        boolean schemaDeserializationEnabled = config.getBoolean(SCHEMA_DESERIALIZATION_ENABLED_CONFIG);
        createConsumers(topics, batchTimeout, batchMaxNumMessages, batchMaxNumBytes,
            deadLetterTopicEnabled, maxRedeliverCount, schemaDeserializationEnabled);
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

    private String schemaKey(String topic, long schemaVersion) {
        return String.format("%s/%d", topic, schemaVersion);
    }

    private String subscriptionName(String topic) {
        return String.format("connect-task-%s", topic);
    }

    private String deadLetterTopic(String topic) {
        return String.format("connect-task-%s-DLQ", topic);
    }

    private void createConsumers(List<String> topics, int batchTimeout,
        int batchMaxNumMessages, int batchMaxNumBytes, boolean deadLetterTopicEnabled,
        int maxRedeliverCount, boolean schemaDeserializationEnabled) {
        bytesBasedConsumers = new ArrayList<>();
        structBasedConsumers = new ArrayList<>();
        schemas = new HashMap<>();
        for (String topic : topics) {
            if (schemaDeserializationEnabled) {
                SchemaInfoWithVersion schemaInfoWithVersion = null;
                try {
                    schemaInfoWithVersion = pulsarAdmin.schemas()
                        .getSchemaInfoWithVersion(topic);
                } catch (PulsarAdminException pae) {
                    // Ignore any exceptions thrown
                }
                if (schemaInfoWithVersion != null) {
                    SchemaInfo schemaInfo = schemaInfoWithVersion.getSchemaInfo();
                    if (schemaInfo.getType().equals(SchemaType.JSON)
                        || schemaInfo.getType().equals(SchemaType.AVRO)) {
                        long schemaVersion = schemaInfoWithVersion.getVersion();
                        String schemaKey = schemaKey(topic, schemaVersion);
                        Schema schema = createConnectSchema(schemaInfo);
                        schemas.put(schemaKey, schema);
                        try {
                            structBasedConsumers.add(createStructBasedConsumer(topic,
                                batchTimeout, batchMaxNumMessages, batchMaxNumBytes,
                                deadLetterTopicEnabled, maxRedeliverCount));
                        } catch (PulsarClientException pce) {
                            if (log.isErrorEnabled()) {
                                log.error("Error while creating a consumer for topic '%s': ", topic);
                                log.error("Error: ", pce);
                            }
                        }
                    } else {
                        try {
                            bytesBasedConsumers.add(createBytesBasedConsumer(topic,
                                batchTimeout, batchMaxNumMessages, batchMaxNumBytes,
                                deadLetterTopicEnabled, maxRedeliverCount));
                        } catch (PulsarClientException pce) {
                            if (log.isErrorEnabled()) {
                                log.error("Error while creating a consumer for topic '%s': ", topic);
                                log.error("Error: ", pce);
                            }
                        }
                    }
                } else {
                    try {
                        bytesBasedConsumers.add(createBytesBasedConsumer(topic,
                            batchTimeout, batchMaxNumMessages, batchMaxNumBytes,
                            deadLetterTopicEnabled, maxRedeliverCount));
                    } catch (PulsarClientException pce) {
                        if (log.isErrorEnabled()) {
                            log.error("Error while creating a consumer for topic '%s': ", topic);
                            log.error("Error: ", pce);
                        }
                    }
                }
            } else {
                try {
                    bytesBasedConsumers.add(createBytesBasedConsumer(topic,
                        batchTimeout, batchMaxNumMessages, batchMaxNumBytes,
                        deadLetterTopicEnabled, maxRedeliverCount));
                } catch (PulsarClientException pce) {
                    if (log.isErrorEnabled()) {
                        log.error("Error while creating a consumer for topic '%s': ", topic);
                        log.error("Error: ", pce);
                    }
                }
            }
        }
    }

    private Consumer<GenericRecord> createStructBasedConsumer(String topic,
        int batchTimeout, int batchMaxNumMessages, int batchMaxNumBytes,
        boolean deadLetterTopicEnabled, int maxRedeliverCount) throws PulsarClientException {
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
        return builder.subscribe();
    }

    private Consumer<byte[]> createBytesBasedConsumer(String topic,
        int batchTimeout, int batchMaxNumMessages, int batchMaxNumBytes,
        boolean deadLetterTopicEnabled, int maxRedeliverCount) throws PulsarClientException {
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
        return builder.subscribe();
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
                log.warn("Error while polling for messages", ex);
            }
            if (messages != null && messages.size() > 0) {
                for (Message<byte[]> message : messages) {
                    try {
                        SourceRecord record = createBytesBasedRecord(message);
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
        for (Consumer<GenericRecord> consumer : structBasedConsumers) {
            Messages<GenericRecord> messages = null;
            try {
                messages = consumer.batchReceive();
            } catch (Exception ex) {
                log.warn("Error while polling for messages", ex);
            }
            if (messages != null && messages.size() > 0) {
                for (Message<GenericRecord> message : messages) {
                    try {
                        SourceRecord record = createStructBasedRecord(message);
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
        return records;
    }

    private SourceRecord createBytesBasedRecord(Message<byte[]> message) {
        String topic = getTopicName(message.getTopicName());
        String offset = message.getMessageId().toString();
        return new SourceRecord(
            Collections.singletonMap(sourcePartition, topic),
            Collections.singletonMap(sourceOffset, offset),
            topic, Schema.BYTES_SCHEMA, message.getKeyBytes(),
            Schema.BYTES_SCHEMA, message.getData());
    }

    private SourceRecord createStructBasedRecord(Message<GenericRecord> message) {
        String topic = getTopicName(message.getTopicName());
        String offset = message.getMessageId().toString();
        byte[] schemaVersionBytes = message.getSchemaVersion();
        String schemaVersionStr = SchemaUtils.getStringSchemaVersion(schemaVersionBytes);
        long schemaVersion = Long.parseLong(schemaVersionStr);
        String schemaKey = schemaKey(topic, schemaVersion);
        Schema valueSchema = schemas.get(schemaKey);
        if (valueSchema == null) {
            SchemaInfo schemaInfo = null;
            try {
                schemaInfo = pulsarAdmin.schemas().getSchemaInfo(topic, schemaVersion);
            } catch (PulsarAdminException pae) {
                log.warn("A struct-based message was received for the "
                    + "topic '%s' containing the specific schema version "
                    + "%d but the schema could not be found. This should "
                    + "not be possible!", topic, schemaVersion);
                return null;
            }
            valueSchema = createConnectSchema(schemaInfo);
            schemas.put(schemaKey, valueSchema);
        }
        GenericRecord record = message.getValue();
        Struct value = buildStruct(record, valueSchema);
        return new SourceRecord(
            Collections.singletonMap(sourcePartition, topic),
            Collections.singletonMap(sourceOffset, offset),
            topic, Schema.BYTES_SCHEMA, message.getKeyBytes(),
            value.schema(), value);
    }

    @SuppressWarnings("unchecked")
    private Struct buildStruct(GenericRecord record, Schema schema) {
        Struct struct = new Struct(schema);
        List<org.apache.kafka.connect.data.Field> fields = schema.fields();
        for (org.apache.kafka.connect.data.Field field : fields) {
            Object fieldValue = record.getField(field.name());
            if (fieldValue != null) {
                if (fieldValue instanceof GenericRecord) {
                    GenericRecord fieldRecord = (GenericRecord) fieldValue;
                    if (field.schema().type().equals(Schema.Type.STRUCT)) {
                        fieldValue = buildStruct(fieldRecord, field.schema());
                    } else if (field.schema().type().equals(Schema.Type.MAP)) {
                        fieldValue = GenericJsonRecordUtil.getMap(fieldRecord);
                    } else if (field.schema().type().equals(Schema.Type.ARRAY)) {
                        Schema valueSchema = field.schema().valueSchema();
                        fieldValue = GenericJsonRecordUtil.getArray(fieldRecord, valueSchema);
                    }
                } else {
                    switch (field.schema().type()) {
                        case BYTES:
                            if (fieldValue instanceof String) {
                                fieldValue = ((String) fieldValue).getBytes();
                            }
                            break;
                        case ARRAY:
                            if (fieldValue instanceof GenericData.Array) {
                                // This is a Avro serialized array and thus
                                // we need to obtain its content based on the
                                // schema set for the value.
                                Schema valueSchema = field.schema().valueSchema();
                                switch (valueSchema.type()) {
                                    case INT8:
                                        GenericData.Array<Byte> byteArray =
                                            (GenericData.Array<Byte>) fieldValue;
                                        List<Byte> byteList = new ArrayList<>(byteArray.size());
                                        byteList.addAll(byteArray);
                                        fieldValue = byteList;
                                        break;
                                    case INT16:
                                        GenericData.Array<Short> shortArray =
                                            (GenericData.Array<Short>) fieldValue;
                                        List<Short> shortList = new ArrayList<>(shortArray.size());
                                        shortList.addAll(shortArray);
                                        fieldValue = shortList;
                                        break;
                                    case INT32:
                                        GenericData.Array<Integer> intArray =
                                            (GenericData.Array<Integer>) fieldValue;
                                        List<Integer> intList = new ArrayList<>(intArray.size());
                                        intList.addAll(intArray);
                                        fieldValue = intList;
                                        break;
                                    case INT64:
                                        GenericData.Array<Long> longArray =
                                            (GenericData.Array<Long>) fieldValue;
                                        List<Long> longList = new ArrayList<>(longArray.size());
                                        longList.addAll(longArray);
                                        fieldValue = longList;
                                        break;
                                    case FLOAT32:
                                        GenericData.Array<Float> floatArray =
                                            (GenericData.Array<Float>) fieldValue;
                                        List<Float> floatList = new ArrayList<>(floatArray.size());
                                        floatList.addAll(floatArray);
                                        fieldValue = floatList;
                                        break;
                                    case FLOAT64:
                                        GenericData.Array<Double> doubleArray =
                                            (GenericData.Array<Double>) fieldValue;
                                        List<Double> doubleList = new ArrayList<>(doubleArray.size());
                                        doubleList.addAll(doubleArray);
                                        fieldValue = doubleList;
                                        break;
                                    case BOOLEAN:
                                        GenericData.Array<Boolean> boolArray =
                                            (GenericData.Array<Boolean>) fieldValue;
                                        List<Boolean> boolList = new ArrayList<>(boolArray.size());
                                        boolList.addAll(boolArray);
                                        fieldValue = boolList;
                                        break;
                                    case STRING: default:
                                        GenericData.Array<Object> strArray =
                                            (GenericData.Array<Object>) fieldValue;
                                        List<String> strList = new ArrayList<>(strArray.size());
                                        // Deep copy the content of the array to avoid any
                                        // issues with strings that are specific to Avro.
                                        for (Object value : strArray) {
                                            strList.add(value.toString());
                                        }
                                        fieldValue = strList;
                                        break;
                                }
                            }
                            break;
                        case MAP:
                            if (fieldValue instanceof Map) {
                                Schema keyToSchema = field.schema().keySchema();
                                Schema valueToSchema = field.schema().valueSchema();
                                Map<Object, Object> original = (Map<Object, Object>) fieldValue;
                                Map<Object, Object> newMap = new HashMap<>(original.size());
                                Set<Object> keySet = original.keySet();
                                for (Object key : keySet) {
                                    Object value = original.get(key);
                                    Schema keyFromSchema = Values.inferSchema(key);
                                    if (keyFromSchema == null) {
                                        // Unlike to happen but Avro may have
                                        // some data type wrappers that would
                                        // make this detection a bit hard.
                                        keyFromSchema = Schema.STRING_SCHEMA;
                                        key = key.toString();
                                    }
                                    Schema valueFromSchema = Values.inferSchema(value);
                                    key = convert(keyFromSchema, keyToSchema, key);
                                    value = convert(valueFromSchema, valueToSchema, value);
                                    newMap.put(key, value);
                                }
                                fieldValue = newMap;
                            }
                            break;
                        case STRING:
                            if (fieldValue instanceof GenericData.EnumSymbol) {
                                fieldValue = fieldValue.toString();
                            } else {
                                fieldValue = Values.convertToString(field.schema(), fieldValue);
                            }
                            break;
                        default:
                            fieldValue = convert(null, field.schema(), fieldValue);
                            break;
                    }
                }
                struct.put(field.name(), fieldValue);
            }
        }
        return struct;
    }

    private Object convert(Schema fromSchema, Schema toSchema, Object value) {
        switch (toSchema.type()) {
            case INT8:
                value = Values.convertToByte(fromSchema, value);
                break;
            case INT16:
                value = Values.convertToShort(fromSchema, value);
                break;
            case INT32:
                value = Values.convertToInteger(fromSchema, value);
                break;
            case INT64:
                value = Values.convertToLong(fromSchema, value);
                break;
            case FLOAT32:
                value = Values.convertToFloat(fromSchema, value);
                break;
            case FLOAT64:
                value = Values.convertToDouble(fromSchema, value);
                break;
            case BOOLEAN:
                value = Values.convertToBoolean(fromSchema, value);
                break;
            case STRING:
                value = Values.convertToString(fromSchema, value);
                break;
            default:
                break;
        }
        return value;
    }

    private String getTopicName(String topicName) {
        TopicName tName = TopicName.get(topicName);
        if (tName.isPartitioned()) {
            topicName = tName.getPartitionedTopicName();
        }
        URI topic = null;
        try {
            topic = new URI(topicName);
        } catch (Exception ex) {
            if (log.isErrorEnabled()) {
                log.error("Error while parsing the topic %s", topicName);
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

    private Schema createConnectSchema(SchemaInfo schemaInfo) {
        return buildSchema(schemaInfo.getSchemaDefinition());
    }

    private Schema buildSchema(String schemaDefinition) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        org.apache.avro.Schema parsedSchema = new Parser().parse(schemaDefinition);
        schemaBuilder.name(parsedSchema.getName());
        List<Field> schemaFields = parsedSchema.getFields();
        for (Field field : schemaFields) {
            if (field.schema().getType().equals(Type.UNION)) {
                List<org.apache.avro.Schema> types = field.schema().getTypes();
                for (org.apache.avro.Schema typeOption : types) {
                    Type type = typeOption.getType();
                    if (!type.equals(Type.NULL)) {
                        if (type.equals(Type.RECORD)) {
                            String fieldSchemaDef = typeOption.toString();
                            Schema fieldSchema = buildSchema(fieldSchemaDef);
                            if (field.hasDefaultValue()) {
                                schemaBuilder.field(field.name(), fieldSchema)
                                    .optional().defaultValue(null);
                            } else {
                                schemaBuilder.field(field.name(), fieldSchema);
                            }
                        } else if (type.equals(Type.ARRAY)) {
                            org.apache.avro.Schema itemType = typeOption.getElementType();
                            if (itemType.getType().equals(org.apache.avro.Schema.Type.RECORD)) {
                                // Maps in Avro are defined as arrays of records therefore
                                // we need to capture its details here in the arrays section
                                org.apache.avro.Schema kType = itemType.getField(key).schema();
                                Schema keySchema = typeMapping.get(kType.getType());
                                org.apache.avro.Schema vType = itemType.getField(value).schema();
                                Schema valueSchema = typeMapping.get(vType.getType());
                                if (field.hasDefaultValue()) {
                                    schemaBuilder.field(field.name(), SchemaBuilder.map(
                                        keySchema, valueSchema)
                                            .optional()
                                            .defaultValue(null)
                                            .build());
                                } else {
                                    schemaBuilder.field(field.name(), SchemaBuilder.map(
                                        keySchema, valueSchema).build());
                                }
                            } else {
                                Schema valueSchema = typeMapping.get(itemType.getType());
                                if (field.hasDefaultValue()) {
                                    schemaBuilder.field(field.name(), SchemaBuilder.array(
                                        valueSchema)
                                            .optional()
                                            .defaultValue(null)
                                            .build());
                                } else {
                                    schemaBuilder.field(field.name(), SchemaBuilder.array(
                                        valueSchema).build());
                                }
                            }
                        } else if (type.equals(Type.MAP)) {
                            org.apache.avro.Schema valueType = typeOption.getValueType();
                            Schema valueSchema = typeMapping.get(valueType.getType());
                            if (field.hasDefaultValue()) {
                                schemaBuilder.field(field.name(), SchemaBuilder.map(
                                    Schema.STRING_SCHEMA, valueSchema)
                                        .optional()
                                        .defaultValue(null)
                                        .build());
                            } else {
                                schemaBuilder.field(field.name(), SchemaBuilder.map(
                                    Schema.STRING_SCHEMA, valueSchema).build());
                            }
                        } else {
                            Schema fieldSchema = typeMapping.get(type);
                            if (field.hasDefaultValue()) {
                                Object value = defaultValues.get(fieldSchema);
                                schemaBuilder.field(field.name(), fieldSchema)
                                    .optional().defaultValue(value);
                            } else {
                                schemaBuilder.field(field.name(), fieldSchema);
                            }
                        }
                        break;
                    }
                }
            } else {
                Type type = field.schema().getType();
                Schema fieldSchema = typeMapping.get(type);
                if (field.hasDefaultValue()) {
                    Object value = defaultValues.get(fieldSchema);
                    schemaBuilder.field(field.name(), fieldSchema)
                        .optional().defaultValue(value);
                } else {
                    schemaBuilder.field(field.name(), fieldSchema);
                }
            }
        }
        return schemaBuilder.build();
    }

    @Override
    public void stop() {
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }
        for (Consumer<byte[]> consumer : bytesBasedConsumers) {
            consumer.closeAsync().exceptionally((ex) -> {
                if (log.isErrorEnabled()) {
                    log.error("Error while closing a consumer", ex);
                }
                return null;
            });
        }
        for (Consumer<GenericRecord> consumer : structBasedConsumers) {
            consumer.closeAsync().exceptionally((ex) -> {
                if (log.isErrorEnabled()) {
                    log.error("Error while closing a consumer", ex);
                }
                return null;
            });
        }
        if (pulsarClient != null) {
            pulsarClient.closeAsync().exceptionally((ex) -> {
                if (log.isErrorEnabled()) {
                    log.error("Error while closing a client", ex);
                }
                return null;
            });
        }
    }

    private static String sourcePartition = "topic";
    private static String sourceOffset = "offset";
    private static String key = "key";
    private static String value = "value";

    private static Map<org.apache.avro.Schema.Type, Schema> typeMapping = new HashMap<>();
    private static Map<Schema, Object> defaultValues = new HashMap<>();

    static {

        typeMapping.put(org.apache.avro.Schema.Type.STRING, Schema.STRING_SCHEMA);
        typeMapping.put(org.apache.avro.Schema.Type.BOOLEAN, Schema.BOOLEAN_SCHEMA);
        typeMapping.put(org.apache.avro.Schema.Type.BYTES, Schema.BYTES_SCHEMA);
        typeMapping.put(org.apache.avro.Schema.Type.INT, Schema.INT32_SCHEMA);
        typeMapping.put(org.apache.avro.Schema.Type.LONG, Schema.INT64_SCHEMA);
        typeMapping.put(org.apache.avro.Schema.Type.FLOAT, Schema.FLOAT32_SCHEMA);
        typeMapping.put(org.apache.avro.Schema.Type.DOUBLE, Schema.FLOAT64_SCHEMA);
        typeMapping.put(org.apache.avro.Schema.Type.ENUM, Schema.STRING_SCHEMA);

        defaultValues.put(Schema.STRING_SCHEMA, null);
        defaultValues.put(Schema.BOOLEAN_SCHEMA, false);
        defaultValues.put(Schema.BYTES_SCHEMA, null);
        defaultValues.put(Schema.INT32_SCHEMA, 0);
        defaultValues.put(Schema.INT64_SCHEMA, 0L);
        defaultValues.put(Schema.FLOAT32_SCHEMA, 0.0F);
        defaultValues.put(Schema.FLOAT64_SCHEMA, 0.0D);

    }

}
