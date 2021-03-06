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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.SchemaUtils;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecordUtil;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericData;

public class RecordDeserializer implements Deserializer<GenericRecord> {

    private static Logger log = LoggerFactory.getLogger(RecordDeserializer.class);

    private PulsarAdmin pulsarAdmin;
    private TopicNamingStrategy topicNamingStrategy;

    public RecordDeserializer(PulsarAdmin pulsarAdmin,
        TopicNamingStrategy topicNamingStrategy) {
        this.pulsarAdmin = pulsarAdmin;
        this.topicNamingStrategy = topicNamingStrategy;
    }

    @Override
    public SourceRecord deserialize(Message<GenericRecord> message) {
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
            GenericRecord record = message.getValue();
            Struct value = buildStruct(record, valueSchema);
            topic = TopicNameUtil.getTopic(topic, topicNamingStrategy);
            return new SourceRecord(
                Collections.singletonMap(SOURCE_PARTITION, topic),
                Collections.singletonMap(SOURCE_OFFSET, offset),
                topic, Schema.BYTES_SCHEMA, message.getKeyBytes(),
                value.schema(), value);
    }

    @SuppressWarnings("unchecked")
    private Struct buildStruct(GenericRecord record, Schema schema) {
        Struct struct = new Struct(schema);
        List<org.apache.kafka.connect.data.Field> fields = schema.fields();
        for (org.apache.kafka.connect.data.Field field : fields) {
            Schema fieldSchema = field.schema();
            Object fieldValue = record.getField(field.name());
            if (fieldValue != null) {
                switch (fieldSchema.type()) {
                    case BYTES:
                        if (fieldValue instanceof String) {
                            fieldValue = ((String) fieldValue).getBytes();
                        }
                        break;
                    case ARRAY:
                        if (fieldValue instanceof GenericRecord) {
                            GenericRecord fieldRecord = (GenericRecord) fieldValue;
                            Schema valueSchema = field.schema().valueSchema();
                            fieldValue = GenericJsonRecordUtil.getArray(
                                fieldRecord, valueSchema);
                        }
                        if (fieldValue instanceof GenericData.Array) {
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
                                    for (Object value : strArray) {
                                        strList.add(value.toString());
                                    }
                                    fieldValue = strList;
                                    break;
                            }
                        }
                        break;
                    case MAP:
                        if (fieldValue instanceof GenericRecord) {
                            GenericRecord fieldRecord = (GenericRecord) fieldValue;
                            fieldValue = GenericJsonRecordUtil.getMap(fieldRecord);
                        }
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
                    case STRUCT:
                        if (fieldValue instanceof GenericRecord) {
                            GenericRecord fieldRecord = (GenericRecord) fieldValue;
                            fieldValue = buildStruct(fieldRecord, field.schema());
                        }
                        break;
                    default:
                        if (fieldValue instanceof GenericData.EnumSymbol) {
                            fieldValue = fieldValue.toString();
                        }
                        fieldValue = convert(null, field.schema(), fieldValue);
                        break;
                }
                struct.put(field.name(), fieldValue);
            } else {
                if (!field.schema().isOptional()) {
                    throw new DataException(String.format("Field '%s' "
                    + "is required but no value was set.", field.name()));
                }
            }
        }
        return struct;
    }

    private Object convert(Schema fromSchema, Schema toSchema, Object value) {
        switch (toSchema.type()) {
            case INT8:
                if (!(value instanceof Byte)) {
                    value = Values.convertToByte(fromSchema, value);
                }
                break;
            case INT16:
                if (!(value instanceof Short)) {
                    value = Values.convertToShort(fromSchema, value);
                }
                break;
            case INT32:
                if (!(value instanceof Integer)) {
                    value = Values.convertToInteger(fromSchema, value);
                }
                break;
            case INT64:
                if (!(value instanceof Long)) {
                    value = Values.convertToLong(fromSchema, value);
                }
                break;
            case FLOAT32:
                if (!(value instanceof Float)) {
                    value = Values.convertToFloat(fromSchema, value);
                }
                break;
            case FLOAT64:
                if (!(value instanceof Double)) {
                    value = Values.convertToDouble(fromSchema, value);
                }
                break;
            case BOOLEAN:
                if (!(value instanceof Boolean)) {
                    value = Values.convertToBoolean(fromSchema, value);
                }
                break;
            case STRING:
                if (!(value instanceof String)) {
                    value = Values.convertToString(fromSchema, value);
                }
                break;
            default:
                break;
        }
        return value;
    }

}
