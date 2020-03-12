# Kafka Connect Pulsar Connector

This repository contains an implementation of a source connector for [Apache Pulsar](https://pulsar.apache.org).
This connector allows data from Pulsar topics to be automatically copied to Kafka topics using Kafka Connect.
By using Kafka Connect to transfer data between these two tecnologies, you can ensure a higher degree of fault-tolerance, scalability, and security that would be hard to achieve with ad-hoc implementations.

## Getting started

The very first thing you need to do to start using this connector is build it. In order to do that, you need to install the following dependencies:

- [Java 1.8+](https://openjdk.java.net/)
- [Apache Maven](https://maven.apache.org/)

Once you have that installed, go ahead and execute the folllowing command to generate the connector:

```bash
mvn clean install
```

Once the build finishes, a new directory called `connector` will be created with the connector JAR file inside it.

## Testing the connector

In order to validate if the connector is working as expected, as well as to validate the behavior of possible configurations, [examples](/examples) of the connector have been created for testing purposes.
The fastest way to test the connector without having to worry in installing Pulsar and Kafka is using Docker.
This repository contains a [docker-compose.yml](docker-compose.yml) file that can be used to quickly spin up Pulsar, Zookeeper, Kafka, and Kafka Connect with a single-command.
Go ahead and execute the following command:

```bash
docker-compose up
```

Wait until all containers are up so you can start the testing.

### 1 - Sending messages to Pulsar

Open a terminal to execute the following command:

```bash
docker exec pulsar bin/pulsar-client produce msgs --messages "first five messages" --num-produce 5
```

This will produce five messages to a topic names `msgs`.

### 2 - Install the connector

Open a terminal to execute the following command:

```bash
curl -X POST -H "Content-Type:application/json" -d @examples/basic-example.json http://localhost:8083/connectors
```

This will request the deployment of a new connector named `basic-example` that aims to read all messages stored in the topic `msgs`.
THe connector will purposely read all messages from the Pulsar topic from the beginning, so any messages produced so far will be copied to Kafka.
When that happens, Kafka will create a topic with the same name.

### 3 - Check the messages in Kafka

Open a terminal to execute the following command:

```bash
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic msgs --from-beginning
```

After this command finished you should see five records containing 'first five messages' as payload.
This command don't necessarily finish since it will keep waiting for new records to arrive.
For now just leave it running.
Then, using another terminal send another five new messages using the following command:

```bash
docker exec pulsar bin/pulsar-client produce msgs --messages "next five messages" --num-produce 5
```

At this point you should have ten records being displayed, which means that the connector is working as expected.

## Configuration reference

This connector is highly customizable, and the table below explains the different knobs available.

| Configuration | Description | Mandatory? | Default | Possible Values
| ----------- | ----------- | ------------- | ------------- | ------------- |
| service.url | Service URL for the Pulsar service | Yes | N/A | N/A |
| topic.whitelist | List of allowed topics to read from | Yes | N/A | N/A |
| topic.blacklist | List of topics to exclude from read | No | N/A | N/A |
| subscription.name | The name of the consumer subscription | No | A random UUID if not specified | N/A |
| batch.max.num.messages | Maximum number of messages per batch | No | 10 | N/A |
| batch.max.num.bytes | Maximum number of bytes per batch | No | 1024 | N/A |
| batch.timeout | Timeout criteria per batch | No | 1000 | N/A |
| topic.naming.strategy | Topic naming strategy for the Kafka topic | No | NameOnly | [NameOnly, FullyQualified] |
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
| subscription.type | Subscription type | No | Exclusive | N/A |
| receiver.queue.size | Size of a consumer's receiver queue | No | 1000 | N/A |
| acknowledgements.group.time.micros | Group a consumer acknowledgment for a specified time | No | 100 | N/A |
| negative.ack.redelivery.delay.micros | Delay to wait before redelivering messages that failed to be processed | No | 1 | N/A |
| max.total.receiver.queue.size.across.partitions | The max total receiver queue size across partitions | No | 50000 | N/A |
| consumer.name | Consumer name | No | N/A | N/A |
| ack.timeout.millis | Timeout of unacked messages | No | 0 | N/A |
| tick.duration.millis | Subscription type | No | Exclusive | N/A |
| priority.level | Priority level for a consumer to which a broker gives more priority while dispatching messages in the shared subscription mode | No | 0 | N/A |
| crypto.failure.action | Consumer should take action when it receives a message that can not be decrypted | No | FAIL | N/A |
| read.compacted | If enabled, a consumer reads messages from a compacted topic rather than reading a full message backlog of a topic | No | false | N/A |
| subscription.initial.position | Initial position at which to set cursor when subscribing to a topic at first time | No | Latest | N/A |
| auto.update.partitions | If enabled, a consumer subscribes to partition increasement automatically | No | true | N/A |
| replicate.subscription.state | If enabled, a subscription state is replicated to geo-replicated clusters | No | false | N/A |

# License

This project is licensed under the [GNU General Public License](LICENSE).
