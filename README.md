# Kafka Connect Pulsar Connector

This repository contains an implementation of a source connector for [Apache Pulsar](https://pulsar.apache.org).
This connector allows data from Pulsar topics to be automatically copied to Kafka topics using [Kafka Connect](https://kafka.apache.org/documentation/#connect).
By using Kafka Connect to transfer data between these two tecnologies, you can ensure a higher degree of fault-tolerance, scalability, and security that would be hard to achieve with ad-hoc implementations.

## Building the connector

The first thing you need to do to start using this connector is building it. In order to do that, you need to install the following dependencies:

- [Java 1.8+](https://openjdk.java.net/)
- [Apache Maven](https://maven.apache.org/)

After installing these dependencies, execute the following command:

```bash
mvn clean install
```

Keep in mind that this command also force the tests to be executed. Some of the tests rely on the [TestContainers](https://www.testcontainers.org/) framework and therefore -- you will need to have a functional Docker installation in your machine.
If this is not the case or you just want the connector then execute the command with the parameter `-DskipTests`.

## Trying the connector

After building the connector you can try it by using the Docker-based installation from this repository.
Follow the instructions below to start an environment with Pulsar, Zookeeper, Kafka, and Connect to quickly experiment with the connector.

[![asciicast](https://asciinema.org/a/311541.svg)](https://asciinema.org/a/311541)

### 1 - Starting the environment

Start the environment with the following command:

```bash
docker-compose up
```

Wait until all containers are up so you can start the testing.

### 2 - Sending data to Pulsar

Open a terminal to execute the following command:

```bash
docker exec pulsar bin/pulsar-client produce msgs --messages "first five messages" --num-produce 5
```

This will produce five messages to a topic named `msgs`.

### 3 - Install the connector

Open a terminal to execute the following command:

```bash
curl -X POST -H "Content-Type:application/json" -d @examples/basic-example.json http://localhost:8083/connectors
```

This will request the deployment of a new connector named `basic-example` that aims to read all messages stored in the topic `msgs`.
The connector will purposely read all messages from that Pulsar topic from the beginning, so any messages produced so far will be copied to Kafka.

### 4 - Check the data in Kafka

Open a terminal to execute the following command:

```bash
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic msgs --from-beginning
```

After executing this command you should see five records containing 'first five messages' as payload.
This command doesn't halt automatically and it will keep waiting for new records to arrive until you stop it manually.
For now just leave it running so we can verify the streaming behavior.

### 5 - Verifying data streaming

Now that the connector has been deployed and it is working as expected, you should be able to see instant streaming of data from Pulsar to Kafka.
In order to verify this behavior, send another set of messages to the `msgs` topic in Pulsar:

```bash
docker exec pulsar bin/pulsar-client produce msgs --messages "next five messages" --num-produce 5
```

At this point you should have ten records stored in Kafka, which means that the connector is working as expected.

## Configuration reference

Table below contains all the configurations that are specific to the connector.
Though the table says that the properties `topic.whitelist` and `topic.regex` are mandatory, only one of them is in fact mandatory.
However, they can be used together to mix up a static and dynamic method about which topics should be read from.

| Configuration | Description | Mandatory? | Default | Possible Values
| ----------- | ----------- | ------------- | ------------- | ------------- |
| service.url | URL for the Pulsar cluster service | Yes | N/A | N/A |
| service.http.url | URL for the Pulsar admin service | Yes | N/A | N/A |
| topic.whitelist | List of allowed topics to read from | Yes | N/A | N/A |
| topic.regex | Regex of allowed topics to read from | Yes | N/A | N/A |
| topic.poll.interval.ms | How often to poll Pulsar for topics matching topic.regex | No | 300000 | N/A || topic.blacklist | List of topics to exclude from read | No | N/A | N/A |
| dead.letter.topic.enabled | If enabled, it will send failed messages to a topic named 'connect-task-${topicName}-DLQ' | No | false | N/A |
| dead.letter.topic.max.redeliver.count | Number of redeliver attempts before sending to the DLQ | No | 5 | N/A |
| schema.deserialization.enabled | If enabled, messages serialized with JSON or Avro will be transformed into struct-based records | No | false | N/A |
| batch.max.num.messages | Maximum number of messages to wait for each batch | No | 10 | N/A |
| batch.max.num.bytes | Maximum number of bytes to wait for each batch | No | 1024 | N/A |
| batch.timeout | Timeout criteria for each batch | No | 1000 | N/A |
| topic.naming.strategy | Dictates how the topic name from Pulsar to Kafka should be mapped | No | NameOnly | [NameOnly, FullyQualified] |

Table below contains all the configurations that are specific to Pulsar, notably client and consumer configurations.
This is not meant to be a complete list of configurations, and the ones shown below are the ones that make sense to have in the context of the connector.

| Configuration | Description | Mandatory? | Default | Possible Values
| ----------- | ----------- | ------------- | ------------- | ------------- |
| auth.plugin.class.name | Name of the authentication plugin | No | N/A | N/A |
| auth.params | String represents parameters for the authentication plugin | No | N/A | N/A |
| operation.timeout.ms | Operation timeout | No | 30000 | N/A |
| stats.interval.seconds | Interval between each stats info | No | 60 | N/A |
| num.io.threads | The number of threads used for handling connections to brokers | No | 1 | N/A
| num.listener.threads | The number of threads used for handling message listeners | No | 1 | N/A |
| use.tcp.nodelay | Whether to use TCP no-delay flag on the connection to disable Nagle algorithm | No | true | N/A |
| use.tls | Whether to use TLS encryption on the connection | No | false | N/A |
| tls.trust.certs.file.path | Path to the trusted TLS certificate file | No | N/A | N/A |
| tls.allow.insecure.connection | Whether the Pulsar client accepts untrusted TLS certificate from broker | No | false | N/A |
| tls.hostname.verification.enabled | Whether to enable TLS hostname verification | No | false | N/A |
| concurrent.lookup.request | The number of concurrent lookup requests allowed to send on each broker connection to prevent overload on broker | No | 5000 | N/A |
| max.lookup.request | The maximum number of lookup requests allowed on each broker connection to prevent overload on broker | No | 50000 | N/A |
| max.number.rejected.request.perconnection | The maximum number of rejected requests of a broker in a certain time frame (30 seconds) after the current connection is closed and the client creates a new connection to connect to a different broker | No | 50 | N/A |
| keep.alive.interval.seconds | Seconds of keeping alive interval for each client broker connection | No | 30 | N/A |
| connection.timeout.ms | Duration of waiting for a connection to a broker to be established | No | 10000 | N/A |
| request.timeout.ms | Maximum duration for completing a request | No | 60000 | N/A |
| initial.backoff.interval.nanos | Default duration for a backoff interval | No | 100 | N/A |
| max.backoff.interval.nanos | Maximum duration for a backoff interval | No | 30 | N/A |
| receiver.queue.size | Size of a consumer's receiver queue | No | 1000 | N/A |
| acknowledgements.group.time.micros | Group a consumer acknowledgment for a specified time | No | 100 | N/A |
| negative.ack.redelivery.delay.micros | Delay to wait before redelivering messages that failed to be processed | No | 1 | N/A |
| max.total.receiver.queue.size.across.partitions | The max total receiver queue size across partitions | No | 50000 | N/A |
| consumer.name | Consumer name | No | N/A | N/A |
| ack.timeout.millis | Timeout of unacked messages | No | 0 | N/A |
| tick.duration.millis | Subscription type | No | 1000 | N/A |
| priority.level | Priority level for a consumer to which a broker gives more priority while dispatching messages in the shared subscription mode | No | 0 | N/A |
| crypto.failure.action | Consumer should take action when it receives a message that can not be decrypted | No | FAIL | [FAIL, DISCARD, CONSUME] |
| read.compacted | If enabled, a consumer reads messages from a compacted topic rather than reading a full message backlog of a topic | No | false | N/A |
| subscription.initial.position | Initial position at which to set cursor when subscribing to a topic at first time | No | Latest | [Latest, Earliest] |
| regex.subscription.mode | When subscribing to a topic using a regular expression, you can pick a certain type of topics | No | PersistentOnly | [PersistentOnly, NonPersistentOnly, AllTopics] |
| auto.update.partitions | If enabled, a consumer subscribes to partition increasement automatically | No | true | N/A |
| replicate.subscription.state | If enabled, a subscription state is replicated to geo-replicated clusters | No | false | N/A |

# License

This project is licensed under the [Apache 2.0 License](./LICENSE).
