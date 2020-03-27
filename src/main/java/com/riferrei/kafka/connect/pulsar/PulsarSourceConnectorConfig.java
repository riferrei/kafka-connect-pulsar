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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;

public class PulsarSourceConnectorConfig extends AbstractConfig {

    public PulsarSourceConnectorConfig(final Map<?, ?> originalProps) {
        super(CONFIG_DEF, originalProps);
    }

    // Basic Options
    public static final String SERVICE_URL_CONFIG = "service.url";
    private static final String SERVICE_URL_DOC = "URL for the Pulsar cluster service";

    public static final String SERVICE_HTTP_URL_CONFIG = "service.http.url";
    private static final String SERVICE_HTTP_URL_DOC = "URL for the Pulsar admin service";

    public static final String TOPIC_WHITELIST_CONFIG = "topic.whitelist";
    private static final String TOPIC_WHITELIST_DOC = "List of allowed topics to read from";
    private static final String TOPIC_WHITELIST_DEFAULT = null;

    public static final String TOPIC_REGEX_CONFIG = "topic.regex";
    private static final String TOPIC_REGEX_DOC = "Regex of allowed topics to read from";
    private static final String TOPIC_REGEX_DEFAULT = null;

    public static final String TOPIC_POLL_INTERVAL_MS_CONFIG = "topic.poll.interval.ms";
    private static final String TOPIC_POLL_INTERVAL_MS_DOC = "How often to poll Pulsar for topics matching topic.regex";
    private static final long TOPIC_POLL_INTERVAL_MS_DEFAULT = 300000;

    public static final String TOPIC_BLACKLIST_CONFIG = "topic.blacklist";
    private static final String TOPIC_BLACKLIST_DOC = "List of topics to exclude from read";
    private static final String TOPIC_BLACKLIST_DEFAULT = null;

    public static final String DEAD_LETTER_TOPIC_ENABLED_CONFIG = "dead.letter.topic.enabled";
    private static final String DEAD_LETTER_TOPIC_ENABLED_DOC = "If enabled, it configures a dead letter topic for each consumer";
    private static final boolean DEAD_LETTER_TOPIC_ENABLED_DEFAULT = false;

    public static final String DEAD_LETTER_TOPIC_MAX_REDELIVER_COUNT_CONFIG = "dead.letter.topic.max.redeliver.count";
    private static final String DEAD_LETTER_TOPIC_MAX_REDELIVER_COUNT_DOC = "Number of redeliver attempts for failed messages";
    private static final int DEAD_LETTER_TOPIC_MAX_REDELIVER_COUNT_DEFAULT = 5;

    public static final String SCHEMA_DESERIALIZATION_ENABLED_CONFIG = "schema.deserialization.enabled";
    private static final String SCHEMA_DESERIALIZATION_ENABLED_DOC = "If enabled, messages serialized with JSON or Avro will be mapped to structs";
    private static final boolean SCHEMA_DESERIALIZATION_ENABLED_DEFAULT = false;

    public static final String BATCH_MAX_NUM_MESSAGES_CONFIG = "batch.max.num.messages";
    private static final String BATCH_MAX_NUM_MESSAGES_DOC = "Maximum number of messages per batch";
    private static final int BATCH_MAX_NUM_MESSAGES_DEFAULT = -1;

    public static final String BATCH_MAX_NUM_BYTES_CONFIG = "batch.max.num.bytes";
    private static final String BATCH_MAX_NUM_BYTES_DOC = "Maximum number of bytes per batch";
    private static final int BATCH_MAX_NUM_BYTES_DEFAULT = 20480;

    public static final String BATCH_TIMEOUT_CONFIG = "batch.timeout";
    private static final String BATCH_TIMEOUT_DOC = "Timeout criteria per batch";
    private static final int BATCH_TIMEOUT_DEFAULT = 100;

    public enum TopicNamingStrategyOptions {
        NameOnly, FullyQualified
    };
    public static final String TOPIC_NAMING_STRATEGY_CONFIG = "topic.naming.strategy";
    private static final String TOPIC_NAMING_STRATEGY_DOC = "Topic naming strategy for the Kafka topic";
    private static final String TOPIC_NAMING_STRATEGY_DEFAULT = TopicNamingStrategyOptions.NameOnly.name();

    // Client Options
    public static final String AUTH_PLUGIN_CLASS_NAME_CONFIG = "auth.plugin.class.name";
    private static final String AUTH_PLUGIN_CLASS_NAME_DOC = "Name of the authentication plugin";
    private static final String AUTH_PLUGIN_CLASS_NAME_DEFAULT = "";

    public static final String AUTH_PARAMS_CONFIG = "auth.params";
    private static final String AUTH_PARAMS_DOC = "String represents parameters for the authentication plugin";
    private static final String AUTH_PARAMS_DEFAULT = "";

    public static final String OPERATION_TIMEOUT_MS_CONFIG = "operation.timeout.ms";
    private static final String OPERATION_TIMEOUT_MS_DOC = "Operation timeout";
    private static final long OPERATION_TIMEOUT_MS_DEFAULT = 30000;

    public static final String STATS_INTERVAL_SECONDS_CONFIG = "stats.interval.seconds";
    private static final String STATS_INTERVAL_SECONDS_DOC = "Interval between each stats info";
    private static final long STATS_INTERVAL_SECONDS_DEFAULT = 60;

    public static final String NUM_IO_THREADS_CONFIG = "num.io.threads";
    private static final String NUM_IO_THREADS_DOC = "The number of threads used for handling connections to brokers";
    private static final int NUM_IO_THREADS_DEFAULT = 1;

    public static final String NUM_LISTENER_THREADS_CONFIG = "num.listener.threads";
    private static final String NUM_LISTENER_THREADS_DOC = "The number of threads used for handling message listeners";
    private static final int NUM_LISTENER_THREADS_DEFAULT = 1;

    public static final String USE_TCP_NODELAY_CONFIG = "use.tcp.nodelay";
    private static final String USE_TCP_NODELAY_DOC = "Whether to use TCP no-delay flag on the connection to disable Nagle algorithm";
    private static final boolean USE_TCP_NODELAY_DEFAULT = true;

    public static final String USE_TLS_CONFIG = "use.tls";
    private static final String USE_TLS_DOC = "Whether to use TLS encryption on the connection";
    private static final boolean USE_TLS_DEFAULT = false;

    public static final String TLS_TRUST_CERTS_FILE_PATH_CONFIG = "tls.trust.certs.file.path";
    private static final String TLS_TRUST_CERTS_FILE_PATH_DOC = "Path to the trusted TLS certificate file";
    private static final String TLS_TRUST_CERTS_FILE_PATH_DEFAULT = "";

    public static final String TLS_ALLOW_INSECURE_CONNECTION_CONFIG = "tls.allow.insecure.connection";
    private static final String TLS_ALLOW_INSECURE_CONNECTION_DOC = "Whether the Pulsar client accepts untrusted TLS certificate from broker";
    private static final boolean TLS_ALLOW_INSECURE_CONNECTION_DEFAULT = false;

    public static final String TLS_HOSTNAME_VERIFICATION_ENABLED_CONFIG = "tls.hostname.verification.enabled";
    private static final String TLS_HOSTNAME_VERIFICATION_ENABLED_DOC = "Whether to enable TLS hostname verification";
    private static final boolean TLS_HOSTNAME_VERIFICATION_ENABLED_DEFAULT = false;

    public static final String CONCURRENT_LOOKUP_REQUEST_CONFIG = "concurrent.lookup.request";
    private static final String CONCURRENT_LOOKUP_REQUEST_DOC = "The number of concurrent lookup requests allowed to send on each broker connection to prevent overload on broker";
    private static final int CONCURRENT_LOOKUP_REQUEST_DEFAULT = 5000;

    public static final String MAX_LOOKUP_REQUEST_CONFIG = "max.lookup.request";
    private static final String MAX_LOOKUP_REQUEST_DOC = "The maximum number of lookup requests allowed on each broker connection to prevent overload on broker";
    private static final int MAX_LOOKUP_REQUEST_DEFAULT = 50000;

    public static final String MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION_CONFIG = "max.number.rejected.request.perconnection";
    private static final String MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION_DOC = "The maximum number of rejected requests of a broker in a certain time frame (30 seconds) after the current connection is closed and the client creates a new connection to connect to a different broker";
    private static final int MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION_DEFAULT = 50;

    public static final String KEEP_ALIVE_INTERVAL_SECONDS_CONFIG = "keep.alive.interval.seconds";
    private static final String KEEP_ALIVE_INTERVAL_SECONDS_DOC = "Seconds of keeping alive interval for each client broker connection";
    private static final int KEEP_ALIVE_INTERVAL_SECONDS_DEFAULT = 30;

    public static final String CONNECTION_TIMEOUT_MS_CONFIG = "connection.timeout.ms";
    private static final String CONNECTION_TIMEOUT_MS_DOC = "Duration of waiting for a connection to a broker to be established";
    private static final int CONNECTION_TIMEOUT_MS_DEFAULT = 10000;

    public static final String REQUEST_TIMEOUT_MS_CONFIG = "request.timeout.ms";
    private static final String REQUEST_TIMEOUT_MS_DOC = "Maximum duration for completing a request";
    private static final int REQUEST_TIMEOUT_MS_DEFAULT = 60000;

    public static final String INITIAL_BACKOFF_INTERVAL_NANOS_CONFIG = "initial.backoff.interval.nanos";
    private static final String INITIAL_BACKOFF_INTERVAL_NANOS_DOC = "Default duration for a backoff interval";
    private static final long INITIAL_BACKOFF_INTERVAL_NANOS_DEFAULT = TimeUnit.MILLISECONDS.toNanos(100);

    public static final String MAX_BACKOFF_INTERVAL_NANOS_CONFIG = "max.backoff.interval.nanos";
    private static final String MAX_BACKOFF_INTERVAL_NANOS_DOC = "Maximum duration for a backoff interval";
    private static final long MAX_BACKOFF_INTERVAL_NANOS_DEFAULT = TimeUnit.MILLISECONDS.toNanos(30);

    // Consumer Options
    public static final String RECEIVER_QUEUE_SIZE_CONFIG = "receiver.queue.size";
    private static final String RECEIVER_QUEUE_SIZE_DOC = "Size of a consumer's receiver queue";
    private static final int RECEIVER_QUEUE_SIZE_DEFAULT = 1000;

    public static final String ACKNOWLEDMENTS_GROUP_TIME_MICROS_CONFIG = "acknowledgements.group.time.micros";
    private static final String ACKNOWLEDMENTS_GROUP_TIME_MICROS_DOC = "Group a consumer acknowledgment for a specified time";
    private static final long ACKNOWLEDMENTS_GROUP_TIME_MICROS_DEFAULT = TimeUnit.MILLISECONDS.toMicros(100);

    public static final String NEGATIVE_ACK_REDELIVERY_DELAY_MICROS_CONFIG = "negative.ack.redelivery.delay.micros";
    private static final String NEGATIVE_ACK_REDELIVERY_DELAY_MICROS_DOC = "Delay to wait before redelivering messages that failed to be processed";
    private static final long NEGATIVE_ACK_REDELIVERY_DELAY_MICROS_DEFAULT = TimeUnit.MILLISECONDS.toMicros(1);

    public static final String MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS_CONFIG = "max.total.receiver.queue.size.across.partitions";
    private static final String MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS_DOC = "The max total receiver queue size across partitions";
    private static final int MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS_DEFAULT = 50000;

    public static final String CONSUMER_NAME_CONFIG = "consumer.name";
    private static final String CONSUMER_NAME_DOC = "Consumer name";
    private static final String CONSUMER_NAME_DEFAULT = null;

    public static final String ACK_TIMEOUT_MILLIS_CONFIG = "ack.timeout.millis";
    private static final String ACK_TIMEOUT_MILLIS_DOC = "Timeout of unacked messages";
    private static final long ACK_TIMEOUT_MILLIS_DEFAULT = 0;

    public static final String TICK_DURATION_MILLIS_CONFIG = "tick.duration.millis";
    private static final String TICK_DURATION_MILLIS_DOC = "Granularity of the ack-timeout redelivery";
    private static final long TICK_DURATION_MILLIS_DEFAULT = 1000;

    public static final String PRIORITY_LEVEL_CONFIG = "priority.level";
    private static final String PRIORITY_LEVEL_DOC = "Priority level for a consumer to which a broker gives more priority while dispatching messages in the shared subscription mode";
    private static final int PRIORITY_LEVEL_DEFAULT = 0;

    public static final String CRYPTO_FAILURE_ACTION_CONFIG = "crypto.failure.action";
    private static final String CRYPTO_FAILURE_ACTION_DOC = "Consumer should take action when it receives a message that can not be decrypted";
    private static final String CRYPTO_FAILURE_ACTION_DEFAULT = ConsumerCryptoFailureAction.FAIL.name();

    public static final String READ_COMPACTED_CONFIG = "read.compacted";
    private static final String READ_COMPACTED_DOC = "If enabled, a consumer reads messages from a compacted topic rather than reading a full message backlog of a topic";
    private static final boolean READ_COMPACTED_DEFAULT = false;

    public static final String SUBSCRIPTION_INITIAL_POSITION_CONFIG = "subscription.initial.position";
    private static final String SUBSCRIPTION_INITIAL_POSITION_DOC = "Initial position at which to set cursor when subscribing to a topic at first time";
    private static final String SUBSCRIPTION_INITIAL_POSITION_DEFAULT = SubscriptionInitialPosition.Latest.name();

    public static final String REGEX_SUBSCRIPTION_MODE_CONFIG = "regex.subscription.mode";
    private static final String REGEX_SUBSCRIPTION_MODE_DOC = "When subscribing to a topic using a regular expression, you can pick a certain type of topics";
    private static final String REGEX_SUBSCRIPTION_MODE_DEFAULT = RegexSubscriptionMode.PersistentOnly.name();

    public static final String AUTO_UPDATE_PARTITIONS_CONFIG = "auto.update.partitions";
    private static final String AUTO_UPDATE_PARTITIONS_DOC = "If enabled, a consumer subscribes to partition increasement automatically";
    private static final boolean AUTO_UPDATE_PARTITIONS_DEFAULT = true;

    public static final String REPLICATE_SUBSCRIPTION_STATE_CONFIG = "replicate.subscription.state";
    private static final String REPLICATE_SUBSCRIPTION_STATE_DOC = "If enabled, a subscription state is replicated to geo-replicated clusters";
    private static final boolean REPLICATE_SUBSCRIPTION_STATE_DEFAULT = false;

    // Non-Options
    public static final String TOPIC_NAMES = "topic.names";
    public static final ConfigDef CONFIG_DEF = createConfigDef();

    private static ConfigDef createConfigDef() {
        ConfigDef configDef = new ConfigDef();
        addBasicOptions(configDef);
        addClientOptions(configDef);
        addConsumerOptions(configDef);
        return configDef;
    }

    private static void addBasicOptions(final ConfigDef configDef) {
        configDef.define(
            SERVICE_URL_CONFIG,
            Type.STRING,
            Importance.HIGH,
            SERVICE_URL_DOC)
        .define(
            SERVICE_HTTP_URL_CONFIG,
            Type.STRING,
            Importance.HIGH,
            SERVICE_HTTP_URL_DOC)
        .define(
            TOPIC_WHITELIST_CONFIG,
            Type.LIST,
            TOPIC_WHITELIST_DEFAULT,
            Importance.HIGH,
            TOPIC_WHITELIST_DOC)
        .define(
            TOPIC_REGEX_CONFIG,
            Type.STRING,
            TOPIC_REGEX_DEFAULT,
            Importance.HIGH,
            TOPIC_REGEX_DOC)
        .define(
            TOPIC_POLL_INTERVAL_MS_CONFIG,
            Type.LONG,
            TOPIC_POLL_INTERVAL_MS_DEFAULT,
            Importance.HIGH,
            TOPIC_POLL_INTERVAL_MS_DOC)
        .define(
            TOPIC_BLACKLIST_CONFIG,
            Type.LIST,
            TOPIC_BLACKLIST_DEFAULT,
            Importance.HIGH,
            TOPIC_BLACKLIST_DOC)
        .define(
            DEAD_LETTER_TOPIC_ENABLED_CONFIG,
            Type.BOOLEAN,
            DEAD_LETTER_TOPIC_ENABLED_DEFAULT,
            Importance.HIGH,
            DEAD_LETTER_TOPIC_ENABLED_DOC)
        .define(
            DEAD_LETTER_TOPIC_MAX_REDELIVER_COUNT_CONFIG,
            Type.INT,
            DEAD_LETTER_TOPIC_MAX_REDELIVER_COUNT_DEFAULT,
            Importance.HIGH,
            DEAD_LETTER_TOPIC_MAX_REDELIVER_COUNT_DOC)
        .define(
            SCHEMA_DESERIALIZATION_ENABLED_CONFIG,
            Type.BOOLEAN,
            SCHEMA_DESERIALIZATION_ENABLED_DEFAULT,
            Importance.HIGH,
            SCHEMA_DESERIALIZATION_ENABLED_DOC)
        .define(
            BATCH_MAX_NUM_MESSAGES_CONFIG,
            Type.INT,
            BATCH_MAX_NUM_MESSAGES_DEFAULT,
            Importance.HIGH,
            BATCH_MAX_NUM_MESSAGES_DOC)
        .define(
            BATCH_MAX_NUM_BYTES_CONFIG,
            Type.INT,
            BATCH_MAX_NUM_BYTES_DEFAULT,
            Importance.HIGH,
            BATCH_MAX_NUM_BYTES_DOC)
        .define(
            BATCH_TIMEOUT_CONFIG,
            Type.INT,
            BATCH_TIMEOUT_DEFAULT,
            Importance.HIGH,
            BATCH_TIMEOUT_DOC)
        .define(
            TOPIC_NAMING_STRATEGY_CONFIG,
            Type.STRING,
            TOPIC_NAMING_STRATEGY_DEFAULT,
            Importance.HIGH,
            TOPIC_NAMING_STRATEGY_DOC
        );
    }

    private static void addClientOptions(final ConfigDef configDef) {
        configDef.define(
            AUTH_PLUGIN_CLASS_NAME_CONFIG,
            Type.STRING,
            AUTH_PLUGIN_CLASS_NAME_DEFAULT,
            Importance.LOW,
            AUTH_PLUGIN_CLASS_NAME_DOC)
        .define(
            AUTH_PARAMS_CONFIG,
            Type.STRING,
            AUTH_PARAMS_DEFAULT,
            Importance.LOW,
            AUTH_PARAMS_DOC)
        .define(
            OPERATION_TIMEOUT_MS_CONFIG,
            Type.LONG,
            OPERATION_TIMEOUT_MS_DEFAULT,
            Importance.LOW,
            OPERATION_TIMEOUT_MS_DOC)
        .define(
            STATS_INTERVAL_SECONDS_CONFIG,
            Type.LONG,
            STATS_INTERVAL_SECONDS_DEFAULT,
            Importance.LOW,
            STATS_INTERVAL_SECONDS_DOC)
        .define(
            NUM_IO_THREADS_CONFIG,
            Type.INT,
            NUM_IO_THREADS_DEFAULT,
            Importance.LOW,
            NUM_IO_THREADS_DOC)
        .define(
            NUM_LISTENER_THREADS_CONFIG,
            Type.INT,
            NUM_LISTENER_THREADS_DEFAULT,
            Importance.LOW,
            NUM_LISTENER_THREADS_DOC)
        .define(
            USE_TCP_NODELAY_CONFIG,
            Type.BOOLEAN,
            USE_TCP_NODELAY_DEFAULT,
            Importance.LOW,
            USE_TCP_NODELAY_DOC)
        .define(
            USE_TLS_CONFIG,
            Type.BOOLEAN,
            USE_TLS_DEFAULT,
            Importance.LOW,
            USE_TLS_DOC)
        .define(
            TLS_TRUST_CERTS_FILE_PATH_CONFIG,
            Type.STRING,
            TLS_TRUST_CERTS_FILE_PATH_DEFAULT,
            Importance.LOW,
            TLS_TRUST_CERTS_FILE_PATH_DOC)
        .define(
            TLS_ALLOW_INSECURE_CONNECTION_CONFIG,
            Type.BOOLEAN,
            TLS_ALLOW_INSECURE_CONNECTION_DEFAULT,
            Importance.LOW,
            TLS_ALLOW_INSECURE_CONNECTION_DOC)
        .define(
            TLS_HOSTNAME_VERIFICATION_ENABLED_CONFIG,
            Type.BOOLEAN,
            TLS_HOSTNAME_VERIFICATION_ENABLED_DEFAULT,
            Importance.LOW,
            TLS_HOSTNAME_VERIFICATION_ENABLED_DOC)
        .define(
            CONCURRENT_LOOKUP_REQUEST_CONFIG,
            Type.INT,
            CONCURRENT_LOOKUP_REQUEST_DEFAULT,
            Importance.LOW,
            CONCURRENT_LOOKUP_REQUEST_DOC)
        .define(
            MAX_LOOKUP_REQUEST_CONFIG,
            Type.INT,
            MAX_LOOKUP_REQUEST_DEFAULT,
            Importance.LOW,
            MAX_LOOKUP_REQUEST_DOC)
        .define(
            MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION_CONFIG,
            Type.INT,
            MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION_DEFAULT,
            Importance.LOW,
            MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION_DOC)
        .define(
            KEEP_ALIVE_INTERVAL_SECONDS_CONFIG,
            Type.INT,
            KEEP_ALIVE_INTERVAL_SECONDS_DEFAULT,
            Importance.LOW,
            KEEP_ALIVE_INTERVAL_SECONDS_DOC)
        .define(
            CONNECTION_TIMEOUT_MS_CONFIG,
            Type.INT,
            CONNECTION_TIMEOUT_MS_DEFAULT,
            Importance.LOW,
            CONNECTION_TIMEOUT_MS_DOC)
        .define(
            REQUEST_TIMEOUT_MS_CONFIG,
            Type.INT,
            REQUEST_TIMEOUT_MS_DEFAULT,
            Importance.LOW,
            REQUEST_TIMEOUT_MS_DOC)
        .define(
            INITIAL_BACKOFF_INTERVAL_NANOS_CONFIG,
            Type.LONG,
            INITIAL_BACKOFF_INTERVAL_NANOS_DEFAULT,
            Importance.LOW,
            INITIAL_BACKOFF_INTERVAL_NANOS_DOC)
        .define(
            MAX_BACKOFF_INTERVAL_NANOS_CONFIG,
            Type.LONG,
            MAX_BACKOFF_INTERVAL_NANOS_DEFAULT,
            Importance.LOW,
            MAX_BACKOFF_INTERVAL_NANOS_DOC
        );
    }

    private static void addConsumerOptions(final ConfigDef configDef) {
        configDef.define(
            RECEIVER_QUEUE_SIZE_CONFIG,
            Type.INT,
            RECEIVER_QUEUE_SIZE_DEFAULT,
            Importance.LOW,
            RECEIVER_QUEUE_SIZE_DOC)
        .define(
            ACKNOWLEDMENTS_GROUP_TIME_MICROS_CONFIG,
            Type.LONG,
            ACKNOWLEDMENTS_GROUP_TIME_MICROS_DEFAULT,
            Importance.LOW,
            ACKNOWLEDMENTS_GROUP_TIME_MICROS_DOC)
        .define(
            NEGATIVE_ACK_REDELIVERY_DELAY_MICROS_CONFIG,
            Type.LONG,
            NEGATIVE_ACK_REDELIVERY_DELAY_MICROS_DEFAULT,
            Importance.LOW,
            NEGATIVE_ACK_REDELIVERY_DELAY_MICROS_DOC)
        .define(
            MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS_CONFIG,
            Type.INT,
            MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS_DEFAULT,
            Importance.LOW,
            MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS_DOC)
        .define(
            CONSUMER_NAME_CONFIG,
            Type.STRING,
            CONSUMER_NAME_DEFAULT,
            Importance.LOW,
            CONSUMER_NAME_DOC)
        .define(
            ACK_TIMEOUT_MILLIS_CONFIG,
            Type.LONG,
            ACK_TIMEOUT_MILLIS_DEFAULT,
            Importance.LOW,
            ACK_TIMEOUT_MILLIS_DOC)
        .define(
            TICK_DURATION_MILLIS_CONFIG,
            Type.LONG,
            TICK_DURATION_MILLIS_DEFAULT,
            Importance.LOW,
            TICK_DURATION_MILLIS_DOC)
        .define(
            PRIORITY_LEVEL_CONFIG,
            Type.INT,
            PRIORITY_LEVEL_DEFAULT,
            Importance.LOW,
            PRIORITY_LEVEL_DOC)
        .define(
            CRYPTO_FAILURE_ACTION_CONFIG,
            Type.STRING,
            CRYPTO_FAILURE_ACTION_DEFAULT,
            Importance.LOW,
            CRYPTO_FAILURE_ACTION_DOC)
        .define(
            READ_COMPACTED_CONFIG,
            Type.BOOLEAN,
            READ_COMPACTED_DEFAULT,
            Importance.LOW,
            READ_COMPACTED_DOC)
        .define(
            SUBSCRIPTION_INITIAL_POSITION_CONFIG,
            Type.STRING,
            SUBSCRIPTION_INITIAL_POSITION_DEFAULT,
            Importance.LOW,
            SUBSCRIPTION_INITIAL_POSITION_DOC)
        .define(
            REGEX_SUBSCRIPTION_MODE_CONFIG,
            Type.STRING,
            REGEX_SUBSCRIPTION_MODE_DEFAULT,
            Importance.LOW,
            REGEX_SUBSCRIPTION_MODE_DOC)
        .define(
            AUTO_UPDATE_PARTITIONS_CONFIG,
            Type.BOOLEAN,
            AUTO_UPDATE_PARTITIONS_DEFAULT,
            Importance.LOW,
            AUTO_UPDATE_PARTITIONS_DOC)
        .define(
            REPLICATE_SUBSCRIPTION_STATE_CONFIG,
            Type.BOOLEAN,
            REPLICATE_SUBSCRIPTION_STATE_DEFAULT,
            Importance.LOW,
            REPLICATE_SUBSCRIPTION_STATE_DOC
        );
    }

}
