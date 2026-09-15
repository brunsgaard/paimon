/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.action.cdc.format.protobuf;

import org.apache.paimon.schema.ColumnIdentityMarker;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.TextNode;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Converts a protobuf {@link Descriptor} to Paimon fields, and a protobuf {@link Message} to the
 * string values the CDC sink casts with {@code TypeUtils.castFromCdcValueString}.
 *
 * <p>Type mapping:
 *
 * <ul>
 *   <li>int32, sint32, sfixed32 to INT
 *   <li>uint32, fixed32, int64, sint64, sfixed64 to BIGINT
 *   <li>uint64, fixed64 to DECIMAL(20, 0)
 *   <li>float to FLOAT, double to DOUBLE, bool to BOOLEAN, string to STRING
 *   <li>bytes to STRING holding base64
 *   <li>enum to STRING holding the value name
 *   <li>google.protobuf.Timestamp to TIMESTAMP(6) in UTC
 *   <li>google.protobuf wrapper types to the wrapped scalar
 *   <li>other messages to ROW, repeated fields to ARRAY, map fields to MAP
 *   <li>a message that contains itself to STRING holding JSON, at the recursive occurrence
 * </ul>
 */
public class ProtobufSchemaConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC);

    private static final String TIMESTAMP = "google.protobuf.Timestamp";
    private static final String WRAPPER_PREFIX = "google.protobuf.";
    private static final String WRAPPER_SUFFIX = "Value";
    private static final String WRAPPER_FIELD = "value";

    /** Scheme of the identity marker appended to every column description. */
    public static final String IDENTITY_SCHEME = "proto";

    private final boolean readDefaultValues;

    /** Comment index for the descriptor being converted. Rebuilt on every {@link #toFields}. */
    private transient ProtobufComments comments;

    public ProtobufSchemaConverter(boolean readDefaultValues) {
        this.readDefaultValues = readDefaultValues;
    }

    // ---------------------------------------------------------------- schema

    public List<DataField> toFields(Descriptor descriptor) {
        comments = new ProtobufComments();
        Set<String> visiting = new HashSet<>();
        visiting.add(descriptor.getFullName());
        List<DataField> fields = new ArrayList<>();
        int id = 0;
        for (FieldDescriptor field : descriptor.getFields()) {
            fields.add(
                    new DataField(id++, field.getName(), toType(field, visiting), describe(field)));
        }
        return fields;
    }

    /**
     * The column description carries the field's documentation comment and a {@code [proto:N]}
     * identity marker. The marker is what lets the sink recognise a renamed field.
     */
    private String describe(FieldDescriptor field) {
        return ColumnIdentityMarker.withIdentity(
                comments.commentOf(field), IDENTITY_SCHEME + ":" + field.getNumber());
    }

    private DataType toType(FieldDescriptor field, Set<String> visiting) {
        if (field.isMapField()) {
            Descriptor entry = field.getMessageType();
            return DataTypes.MAP(
                    toSingularType(entry.findFieldByName("key"), visiting),
                    toSingularType(entry.findFieldByName("value"), visiting));
        }
        DataType type = toSingularType(field, visiting);
        return field.isRepeated() ? DataTypes.ARRAY(type) : type;
    }

    private DataType toSingularType(FieldDescriptor field, Set<String> visiting) {
        switch (field.getType()) {
            case INT32:
            case SINT32:
            case SFIXED32:
                return DataTypes.INT();
            case UINT32:
            case FIXED32:
            case INT64:
            case SINT64:
            case SFIXED64:
                return DataTypes.BIGINT();
            case UINT64:
            case FIXED64:
                return DataTypes.DECIMAL(20, 0);
            case FLOAT:
                return DataTypes.FLOAT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case BOOL:
                return DataTypes.BOOLEAN();
            case STRING:
            case BYTES:
            case ENUM:
                return DataTypes.STRING();
            case MESSAGE:
            case GROUP:
                return toMessageType(field.getMessageType(), visiting);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported protobuf type "
                                + field.getType()
                                + " for "
                                + field.getFullName());
        }
    }

    private DataType toMessageType(Descriptor message, Set<String> visiting) {
        String name = message.getFullName();
        if (TIMESTAMP.equals(name)) {
            return DataTypes.TIMESTAMP(6);
        }
        FieldDescriptor wrapped = wrappedField(message);
        if (wrapped != null) {
            return toSingularType(wrapped, visiting);
        }
        if (!visiting.add(name)) {
            // The message contains itself. A ROW cannot be infinitely deep, so keep the
            // recursive occurrence as JSON text.
            return DataTypes.STRING();
        }
        try {
            RowType.Builder builder = RowType.builder();
            for (FieldDescriptor field : message.getFields()) {
                builder.field(field.getName(), toType(field, visiting), describe(field));
            }
            return builder.build();
        } finally {
            visiting.remove(name);
        }
    }

    /** Returns the single {@code value} field of a google.protobuf wrapper, else null. */
    private static FieldDescriptor wrappedField(Descriptor message) {
        String name = message.getFullName();
        if (!name.startsWith(WRAPPER_PREFIX)
                || !name.endsWith(WRAPPER_SUFFIX)
                || message.getFields().size() != 1) {
            return null;
        }
        FieldDescriptor field = message.getFields().get(0);
        return WRAPPER_FIELD.equals(field.getName()) && !field.isRepeated() ? field : null;
    }

    // ---------------------------------------------------------------- values

    /** Field name to CDC string value. Fields that are NULL are left out. */
    public Map<String, String> toValues(Message message) {
        Map<String, String> values = new LinkedHashMap<>();
        for (FieldDescriptor field : message.getDescriptorForType().getFields()) {
            Object value = valueOf(message, field);
            if (value != null) {
                values.put(field.getName(), asString(value));
            }
        }
        return values;
    }

    /** Returns a String for scalars, a JsonNode for complex values, or null. */
    private Object valueOf(Message message, FieldDescriptor field) {
        if (field.isMapField()) {
            List<?> entries = (List<?>) message.getField(field);
            if (entries.isEmpty() && !readDefaultValues) {
                return null;
            }
            ObjectNode node = OBJECT_MAPPER.createObjectNode();
            for (Object o : entries) {
                Message entry = (Message) o;
                Descriptor entryType = entry.getDescriptorForType();
                FieldDescriptor key = entryType.findFieldByName("key");
                FieldDescriptor value = entryType.findFieldByName("value");
                node.set(
                        scalarToString(key, entry.getField(key)),
                        toJsonNode(value, entry.getField(value)));
            }
            return node;
        }
        if (field.isRepeated()) {
            List<?> items = (List<?>) message.getField(field);
            if (items.isEmpty() && !readDefaultValues) {
                return null;
            }
            ArrayNode node = OBJECT_MAPPER.createArrayNode();
            for (Object item : items) {
                node.add(toJsonNode(field, item));
            }
            return node;
        }
        if (!message.hasField(field)) {
            if (!readDefaultValues || field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                return null;
            }
            return scalarToString(field, field.getDefaultValue());
        }
        return singularValue(field, message.getField(field));
    }

    private Object singularValue(FieldDescriptor field, Object value) {
        if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
            return scalarToString(field, value);
        }
        Message message = (Message) value;
        Descriptor type = message.getDescriptorForType();
        if (TIMESTAMP.equals(type.getFullName())) {
            return formatTimestamp(message);
        }
        FieldDescriptor wrapped = wrappedField(type);
        if (wrapped != null) {
            return scalarToString(wrapped, message.getField(wrapped));
        }
        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        for (FieldDescriptor nested : type.getFields()) {
            Object nestedValue = valueOf(message, nested);
            if (nestedValue != null) {
                node.set(nested.getName(), toJsonNode(nestedValue));
            }
        }
        return node;
    }

    private JsonNode toJsonNode(FieldDescriptor field, Object value) {
        return toJsonNode(singularValue(field, value));
    }

    private static JsonNode toJsonNode(Object value) {
        return value instanceof JsonNode ? (JsonNode) value : TextNode.valueOf((String) value);
    }

    private static String asString(Object value) {
        if (value instanceof JsonNode) {
            try {
                return OBJECT_MAPPER.writeValueAsString(value);
            } catch (JsonProcessingException e) {
                throw new IllegalStateException("Failed to serialize nested protobuf value", e);
            }
        }
        return (String) value;
    }

    private static String scalarToString(FieldDescriptor field, Object value) {
        switch (field.getType()) {
            case BYTES:
                return Base64.getEncoder().encodeToString(((ByteString) value).toByteArray());
            case ENUM:
                return ((EnumValueDescriptor) value).getName();
            case UINT32:
            case FIXED32:
                return Integer.toUnsignedString((Integer) value);
            case UINT64:
            case FIXED64:
                return Long.toUnsignedString((Long) value);
            default:
                return String.valueOf(value);
        }
    }

    private static String formatTimestamp(Message timestamp) {
        Descriptor type = timestamp.getDescriptorForType();
        long seconds = (Long) timestamp.getField(type.findFieldByName("seconds"));
        int nanos = (Integer) timestamp.getField(type.findFieldByName("nanos"));
        return TIMESTAMP_FORMATTER.format(Instant.ofEpochSecond(seconds, nanos));
    }
}
