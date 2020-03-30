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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.avro.Schema.Type;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;

public final class SchemaRegistry {

    private static Map<String, Schema> schemas = new HashMap<>();

    public static Schema createSchema(String topic,
        SchemaInfo schemaInfo, long schemaVersion) {
        Schema schema = buildSchema(schemaInfo.getSchemaDefinition());
        schemas.put(schemaKey(topic, schemaVersion), schema);
        return schema;
    }

    public static Schema getSchema(String topic, long schemaVersion) {
        return schemas.get(schemaKey(topic, schemaVersion));
    }

    private static String schemaKey(String topic, long schemaVersion) {
        return String.format("%s/%d", topic, schemaVersion);
    }

    private static Schema buildSchema(String schemaDefinition) {
        final Parser schemaParser = new Parser();
        final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        org.apache.avro.Schema parsed = schemaParser.parse(schemaDefinition);
        schemaBuilder.name(parsed.getName());
        List<Field> schemaFields = parsed.getFields();
        for (Field field : schemaFields) {
            org.apache.avro.Schema fieldSchemaRef = field.schema();
            switch (fieldSchemaRef.getType()) {
                case UNION:
                    List<org.apache.avro.Schema> unionTypes = fieldSchemaRef.getTypes();
                    for (org.apache.avro.Schema typeOption : unionTypes) {
                        Type type = typeOption.getType();
                        if (!type.equals(Type.NULL)) {
                            Schema fieldSchema = null;
                            Schema valueSchema = null;
                            switch (type) {
                                case RECORD:
                                    String fieldSchemaDef = typeOption.toString();
                                    fieldSchema = buildSchema(fieldSchemaDef);
                                    if (field.hasDefaultValue()) {
                                        schemaBuilder.field(field.name(), fieldSchema)
                                            .optional().defaultValue(null).build();
                                    } else {
                                        schemaBuilder.field(field.name(), fieldSchema);
                                    }
                                    break;
                                case MAP:
                                    org.apache.avro.Schema valueType = typeOption.getValueType();
                                    valueSchema = typeMapping.get(valueType.getType());
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
                                    break;
                                case ARRAY:
                                    org.apache.avro.Schema itemType = typeOption.getElementType();
                                    if (itemType.getType().equals(org.apache.avro.Schema.Type.RECORD)) {
                                        org.apache.avro.Schema kType = itemType.getField(key).schema();
                                        Schema keySchema = typeMapping.get(kType.getType());
                                        org.apache.avro.Schema vType = itemType.getField(value).schema();
                                        valueSchema = typeMapping.get(vType.getType());
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
                                        valueSchema = typeMapping.get(itemType.getType());
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
                                    break;
                                default:
                                    fieldSchema = typeMapping.get(type);
                                    if (field.hasDefaultValue()) {
                                        Object value = defaultValues.get(fieldSchema);
                                        schemaBuilder.field(field.name(), SchemaBuilder.type(
                                            fieldSchema.type()).optional().defaultValue(value)
                                            .build());
                                    } else {
                                        schemaBuilder.field(field.name(), fieldSchema);
                                    }
                                    break;
                            }
                            break;
                        }
                    }
                    break;
                case ARRAY:
                    org.apache.avro.Schema itemType = fieldSchemaRef.getElementType();
                    switch (itemType.getType()) {
                        case RECORD:
                            org.apache.avro.Schema keyType = itemType.getField(key).schema();
                            org.apache.avro.Schema valueType = itemType.getField(value).schema();
                            Schema keySchema = typeMapping.get(keyType.getType());
                            Schema valueSchema = typeMapping.get(valueType.getType());
                            if (field.hasDefaultValue()) {
                                schemaBuilder.field(field.name(), SchemaBuilder.map(
                                    keySchema, valueSchema)
                                        .optional()
                                        .defaultValue(null)
                                        .build());
                            } else {
                                schemaBuilder.field(field.name(), SchemaBuilder.map(
                                    keySchema, valueSchema)
                                        .build());
                            }
                            break;
                        default:
                            Schema arrayItemSchema = typeMapping.get(itemType.getType());
                            if (field.hasDefaultValue()) {
                                schemaBuilder.field(field.name(), SchemaBuilder.array(
                                    arrayItemSchema)
                                        .optional()
                                        .defaultValue(null)
                                        .build());
                            } else {
                                schemaBuilder.field(field.name(), SchemaBuilder.array(
                                    arrayItemSchema)
                                        .build());
                            }
                            break;
                    }
                    break;
                default:
                    Type fieldType = fieldSchemaRef.getType();
                    Schema fieldSchema = typeMapping.get(fieldType);
                    if (field.hasDefaultValue()) {
                        Object value = defaultValues.get(fieldSchema);
                        schemaBuilder.field(field.name(), SchemaBuilder.type(
                            fieldSchema.type()).optional().defaultValue(value)
                            .build());
                    } else {
                        schemaBuilder.field(field.name(), fieldSchema);
                    }
                    break;
            }
        }
        return schemaBuilder.build();
    }

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

    private SchemaRegistry() {
    }

}
