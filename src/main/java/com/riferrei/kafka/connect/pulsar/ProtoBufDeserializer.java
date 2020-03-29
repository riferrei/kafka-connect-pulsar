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
import java.util.Set;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.schema.SchemaUtils;
import org.apache.pulsar.common.schema.SchemaInfo;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.MapEntry;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class ProtoBufDeserializer implements Deserializer<Any> {

    private static Logger log = LoggerFactory.getLogger(ProtoBufDeserializer.class);

    private PulsarAdmin pulsarAdmin;
    private TopicNamingStrategy topicNamingStrategy;
    private Map<String, Method> cache = new HashMap<>();
    private String messageClass;

    public ProtoBufDeserializer(PulsarAdmin pulsarAdmin,
        TopicNamingStrategy topicNamingStrategy, String messageClass) {
        this.pulsarAdmin = pulsarAdmin;
        this.topicNamingStrategy = topicNamingStrategy;
        this.messageClass = messageClass;
    }

    @Override
    public SourceRecord deserialize(Message<Any> message) {
        String topic = message.getTopicName();
        String offset = message.getMessageId().toString();
        byte[] schemaVersionBytes = message.getSchemaVersion();
        String schemaVersionStr = SchemaUtils.getStringSchemaVersion(schemaVersionBytes);
        long schemaVersion = Long.parseLong(schemaVersionStr);
        Schema valueSchema = SchemaRegistry.getSchema(topic, schemaVersion);
        if (valueSchema == null) {
            SchemaInfo schemaInfo = null;
            try {
                schemaInfo = pulsarAdmin.schemas().getSchemaInfo(topic, schemaVersion);
            } catch (PulsarAdminException pae) {
                log.warn("A record-based message was received for the "
                    + "topic '%s' containing the specific schema version "
                    + "%d but the schema could not be found. This should "
                    + "not be possible!", topic, schemaVersion);
                return null;
            }
            valueSchema = SchemaRegistry.createSchema(topic, schemaInfo, schemaVersion);
        }
        byte[] messageBytes = message.getData();
        Struct value = buildStruct(messageClass, messageBytes, valueSchema);
        topic = TopicNameUtil.getTopic(topic, topicNamingStrategy);
        return new SourceRecord(
            Collections.singletonMap(SOURCE_PARTITION, topic),
            Collections.singletonMap(SOURCE_OFFSET, offset),
            topic, Schema.BYTES_SCHEMA, message.getKeyBytes(),
            value.schema(), value);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Struct buildStruct(String messageClass, byte[] messageBytes, Schema schema) {
        Struct struct = new Struct(schema);
        Object messageObject = createMessageObject(messageClass, messageBytes);
        if (messageObject != null) {
            GeneratedMessageV3 message = (GeneratedMessageV3) messageObject;
            Map<FieldDescriptor, Object> fieldValueMap = message.getAllFields();
            Set<FieldDescriptor> fieldDescriptors = fieldValueMap.keySet();
            for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
                String fieldName = fieldDescriptor.getName();
                Field fieldReference = schema.field(fieldName);
                if (fieldReference != null) {
                    Schema fieldSchema = fieldReference.schema();
                    Object fieldValue = message.getField(fieldDescriptor);
                    if (fieldValue != null) {
                        switch (fieldSchema.type()) {
                            case BYTES:
                                ByteString byteString = (ByteString) fieldValue;
                                fieldValue = byteString.toByteArray();
                                break;
                            case ARRAY:
                                break;
                            case MAP:
                                Schema keyToSchema = fieldSchema.keySchema();
                                Schema valueToSchema = fieldSchema.valueSchema();
                                Collection original = (Collection) fieldValue;
                                Map<Object, Object> newMap = new HashMap<>(original.size());
                                original.forEach(entry -> {
                                    MapEntry mapEntry = (MapEntry) entry;
                                    Schema keyFromSchema = Values.inferSchema(mapEntry.getKey());
                                    Schema valueFromSchema = Values.inferSchema(mapEntry.getValue());
                                    Object key = convert(keyFromSchema, keyToSchema, mapEntry.getKey());
                                    Object value = convert(valueFromSchema, valueToSchema, mapEntry.getValue());
                                    newMap.put(key, value);
                                });
                                fieldValue = newMap;
                                break;
                            case STRUCT:
                                GeneratedMessageV3 innerFieldAsMessage = (GeneratedMessageV3) fieldValue;
                                String innerMessageClass = innerFieldAsMessage.getClass().getName();
                                byte[] innerMessageBytes = innerFieldAsMessage.toByteArray();
                                fieldValue = buildStruct(innerMessageClass, innerMessageBytes, fieldSchema);
                                break;
                            default:
                                if (fieldValue instanceof Descriptors.EnumValueDescriptor) {
                                    fieldValue = fieldValue.toString();
                                }
                                break;
                        }
                        struct.put(fieldName, fieldValue);
                    } else {
                        if (!fieldSchema.isOptional()) {
                            throw new DataException(String.format("Field '%s' "
                            + "is required but no value was set.", fieldName));
                        }
                    }
                } else {
                    log.warn("The field '%s' was contained in a message "
                        + "but with no schema associated.", fieldName);
                }
            }
        }
        return struct;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Object createMessageObject(String messageClass, byte[] messageBytes) {
        final Class[] paramTypes = new Class[]{byte[].class};
        Object messageObject = null;
        try {
            Method parseFromMethod = cache.get(messageClass);
            if (parseFromMethod == null) {
                Class messageClazz = Class.forName(messageClass);
                parseFromMethod = messageClazz.getDeclaredMethod(
                    "parseFrom", paramTypes);
                cache.put(messageClass, parseFromMethod);
            }
            messageObject = parseFromMethod.invoke(null, messageBytes);
        } catch (ClassNotFoundException cnfe) {
            throw new ConnectException(String.format(
                "The message class '%s' was not found",
                messageClass), cnfe);
        } catch (Exception ex) {
            throw new ConnectException("Error creating an object "
                + "out of the bytes from the serialized message", ex);
        }
        return messageObject;
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

}
