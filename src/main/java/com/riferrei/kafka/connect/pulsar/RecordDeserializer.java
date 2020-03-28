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

    public RecordDeserializer(PulsarAdmin pulsarAdmin) {
        this.pulsarAdmin = pulsarAdmin;
    }

    @Override
    public SourceRecord deserialize(Message<GenericRecord> message,
        TopicNamingStrategy topicNamingStrategy) {
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
                Collections.singletonMap("topic", topic),
                Collections.singletonMap("offset", offset),
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

}
