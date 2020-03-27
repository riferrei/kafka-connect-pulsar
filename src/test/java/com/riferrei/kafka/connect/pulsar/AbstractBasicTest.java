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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;

import org.testcontainers.containers.PulsarContainer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import com.google.protobuf.ByteString;
import com.riferrei.kafka.connect.pulsar.ProtoBufGenComplexType.ProtoBufComplexType;
import com.riferrei.kafka.connect.pulsar.ProtoBufGenComplexType.ProtoBufComplexType.ProtoBufInnerType;
import com.riferrei.kafka.connect.pulsar.ProtoBufGenComplexType.ProtoBufComplexType.ProtoBufInnerType.ProtoBufMultipleOptions;

public abstract class AbstractBasicTest {

    @ClassRule
    public static PulsarContainer pulsar =
        new PulsarContainer(PropertiesUtil.getPulsarVersion());

    private static PulsarAdmin pulsarAdmin;
    private static PulsarClient pulsarClient;

    @BeforeClass
    public static void setup() {
        try {
            pulsarAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsar.getHttpServiceUrl())
                .build();
            pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsar.getPulsarBrokerUrl())
                .build();
        } catch (PulsarClientException pce) {
            pce.printStackTrace();
        }
    }

    @Before
    public void deleteReusableTopics() {
        for (String topic : REUSABLE_TOPICS) {
            try {
                pulsarAdmin.topics().delete(topic);
            } catch (PulsarAdminException pae) {
            }
        }
        topic = UUID.randomUUID().toString();
    }

    @AfterAll
    public static void tearDown() {
        pulsarAdmin.close();
        pulsarClient.closeAsync();
    }

    protected static final String SERVICE_URL_VALUE = "pulsar://localhost:6650";
    protected static final String SERVICE_HTTP_URL_VALUE = "http://localhost:8080";
    protected static final String TOPIC_REGEX_VALUE = "persistent://public/default/topic-.*";
    protected static final Random RANDOM = new Random(System.currentTimeMillis());

    protected static final String[] REUSABLE_TOPICS = {
        "topic-1", "topic-2", "topic-3",
        "topic-4", "topic-5", "topic-6" };

    protected String topic;

    protected enum MessageType {
        JSON, AVRO, BYTES
    }

    protected String fullyQualifiedTopic(String topic) {
        return String.format("public-default-%s", topic);
    }

    protected String listToString(String... topics) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < topics.length; i++) {
            if (i == topics.length - 1) {
                sb.append(topics[i]);
            } else {
                sb.append(topics[i]).append(", ");
            }
        }
        return sb.toString();
    }

    protected String getServiceUrl() {
        return pulsar.getPulsarBrokerUrl();
    }

    protected String getServiceHttpUrl() {
        return pulsar.getHttpServiceUrl();
    }

    protected void produceBytesBasedMessages(String topic, int numMessages)
        throws PulsarClientException {
        final String message = UUID.randomUUID().toString();
        Producer<byte[]> producer = null;
        try {
            producer = pulsarClient.newProducer()
            .topic(topic)
            .create();
            for (int i = 0; i < numMessages; i++) {
                producer.send(message.getBytes());
            }
        } finally {
            if (producer != null) {
                producer.closeAsync();
            }
        }
    }

    protected void produceJSONBasedMessages(String topic, int numMessages)
        throws PulsarClientException {
        Producer<JSONComplexType> producer = null;
        try {
            producer = pulsarClient.newProducer(
                Schema.JSON(JSONComplexType.class))
                    .topic(topic)
                    .create();
            for (int i = 0; i < numMessages; i++) {
                producer.send(createJSONComplexType());
            }
        } finally {
            if (producer != null) {
                producer.closeAsync();
            }
        }
    }

    protected void produceAvroBasedMessages(String topic, int numMessages)
        throws PulsarClientException {
        Producer<AvroComplexType> producer = null;
        try {
            producer = pulsarClient.newProducer(
                Schema.AVRO(AvroComplexType.class))
                    .topic(topic)
                    .create();
            for (int i = 0; i < numMessages; i++) {
                producer.send(createAvroComplexType());
            }
        } finally {
            if (producer != null) {
                producer.closeAsync();
            }
        }
    }

    protected void produceAvroGenBasedMessages(String topic, int numMessages)
        throws PulsarClientException {
        Producer<AvroGenComplexType> producer = null;
        try {
            producer = pulsarClient.newProducer(
                Schema.AVRO(AvroGenComplexType.class))
                    .topic(topic)
                    .create();
            for (int i = 0; i < numMessages; i++) {
                producer.send(createAvroGenComplexType());
            }
        } finally {
            if (producer != null) {
                producer.closeAsync();
            }
        }
    }

    protected void produceProtoBufBasedMessages(String topic, int numMessages)
        throws PulsarClientException {
        Producer<ProtoBufComplexType> producer = null;
        try {
            producer = pulsarClient.newProducer(
                Schema.PROTOBUF(ProtoBufComplexType.class))
                    .topic(topic)
                    .create();
            for (int i = 0; i < numMessages; i++) {
                producer.send(createProtoBufGenComplexType());
            }
        } finally {
            if (producer != null) {
                producer.closeAsync();
            }
        }
    }

    protected JSONComplexType createJSONComplexType() {

        JSONComplexType complexType = new JSONComplexType();
        complexType.setStringField(String.valueOf(RANDOM.nextInt()));
        complexType.setBooleanField(RANDOM.nextBoolean());
        byte[] bytes = new byte[1024];
        RANDOM.nextBytes(bytes);
        complexType.setBytesField(bytes);
        complexType.setIntField(RANDOM.nextInt());
        complexType.setLongField(RANDOM.nextLong());
        complexType.setFloatField(RANDOM.nextFloat());
        complexType.setDoubleField(RANDOM.nextDouble());

        Map<String, Double> mapField = new HashMap<>(1);
        mapField.put(String.valueOf(RANDOM.nextInt()), RANDOM.nextDouble());
        complexType.setMapField(mapField);

        JSONInnerType innerField = new JSONInnerType();
        innerField.setDoubleField(RANDOM.nextDouble());
        String[] arrayField = new String[]{
            String.valueOf(RANDOM.nextInt())
        };
        innerField.setArrayField(arrayField);
        innerField.setEnumField(MultipleOptions.ThirdOption);
        complexType.setInnerField(innerField);

        return complexType;

    }

    protected AvroComplexType createAvroComplexType() {

        AvroComplexType complexType = new AvroComplexType();
        complexType.setStringField(String.valueOf(RANDOM.nextInt()));
        complexType.setBooleanField(RANDOM.nextBoolean());
        byte[] bytes = new byte[1024];
        RANDOM.nextBytes(bytes);
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        complexType.setBytesField(byteBuffer);
        complexType.setIntField(RANDOM.nextInt());
        complexType.setLongField(RANDOM.nextLong());
        complexType.setFloatField(RANDOM.nextFloat());
        complexType.setDoubleField(RANDOM.nextDouble());

        Map<String, Double> mapField = new HashMap<>(1);
        mapField.put(String.valueOf(RANDOM.nextInt()), RANDOM.nextDouble());
        complexType.setMapField(mapField);

        AvroInnerType innerField = new AvroInnerType();
        innerField.setDoubleField(RANDOM.nextDouble());
        innerField.setArrayField(Arrays.asList(String.valueOf(RANDOM.nextInt())));
        innerField.setEnumField(MultipleOptions.ThirdOption);
        complexType.setInnerField(innerField);

        return complexType;

    }

    protected AvroGenComplexType createAvroGenComplexType() {

        AvroGenComplexType.Builder complexType =
            AvroGenComplexType.newBuilder();

        complexType.setStringField(String.valueOf(RANDOM.nextInt()));
        complexType.setBooleanField(RANDOM.nextBoolean());
        byte[] bytes = new byte[1024];
        RANDOM.nextBytes(bytes);
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        complexType.setBytesField(byteBuffer);
        complexType.setIntField(RANDOM.nextInt());
        complexType.setLongField(RANDOM.nextLong());
        complexType.setFloatField(RANDOM.nextFloat());
        complexType.setDoubleField(RANDOM.nextDouble());

        /* Apparently there is a bug in Pulsar's client code for
           Avro which complains about the map values set into the
           records. If set, then it throws the following exception:
           
            org.apache.pulsar.shade.org.apache.avro.UnresolvedUnionException: Not in union
                ["null",{"type":"array","items":{"type":"record","name":"PairCharSequenceDouble",
                "namespace":"org.apache.pulsar.shade.org.apache.avro.reflect",
                "fields":[{"name":"key","type":"string"},{"name":"value","type":"double"}]},
                "java-class":"java.util.Map"}]

            For this reason, the code below has been commented out
            to allow the tests to pass. This should be revisited as
            new versions of Pulsar are relesead. */

        // Map<CharSequence, Double> mapField = new HashMap<>(1);
        // mapField.put(String.valueOf(RANDOM.nextInt()), RANDOM.nextDouble());
        // complexType.setMapField(mapField);

        complexType.setInnerFieldBuilder(AvroGenInnerType.newBuilder()
            .setDoubleField(RANDOM.nextDouble())
            .setArrayField(Arrays.asList(String.valueOf(RANDOM.nextInt())))
            .setEnumField(AvroGenMultipleOptions.ThirdOption));

        return complexType.build();

    }

    protected ProtoBufComplexType createProtoBufGenComplexType() {

        ProtoBufComplexType.Builder complexType = ProtoBufComplexType.newBuilder();
        complexType.setStringField(String.valueOf(RANDOM.nextInt()));
        complexType.setBooleanField(RANDOM.nextBoolean());
        byte[] bytes = new byte[1024];
        RANDOM.nextBytes(bytes);
        complexType.setBytesField(ByteString.copyFrom(bytes));
        complexType.setIntField(RANDOM.nextInt());
        complexType.setLongField(RANDOM.nextLong());
        complexType.setFloatField(RANDOM.nextFloat());
        complexType.setDoubleField(RANDOM.nextDouble());

        complexType.putMapField(
            String.valueOf(RANDOM.nextInt()),
            RANDOM.nextDouble());

        complexType.setInnerField(ProtoBufInnerType.newBuilder()
            .setDoubleField(RANDOM.nextDouble())
            .addArrayField(String.valueOf(RANDOM.nextInt()))
            .setEnumField(ProtoBufMultipleOptions.THIRD_OPTION)
            .build());

        return complexType.build();

    }

    protected void createNonPartitionedTopic(String topic)
        throws PulsarAdminException {
        pulsarAdmin.topics().createNonPartitionedTopic(topic);
    }

    protected void createPartitionedTopic(String topic, int partitions)
        throws PulsarAdminException {
        pulsarAdmin.topics().createPartitionedTopic(topic, partitions);
    }

}
