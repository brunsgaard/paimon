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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.BooleanNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.DoubleNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.NullNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.TextNode;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
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
 *   <li>google.protobuf.Timestamp to TIMESTAMP_LTZ(6)
 *   <li>google.protobuf.Duration to DECIMAL(20, 9) seconds
 *   <li>google.protobuf.Struct, Value and ListValue to STRING holding JSON
 *   <li>google.protobuf.FieldMask to STRING holding the comma separated paths
 *   <li>google.protobuf.Empty to BOOLEAN, true when set
 *   <li>google.protobuf wrapper types to the wrapped scalar
 *   <li>other messages to ROW, or to one column per leaf when flattening is on
 *   <li>repeated fields to ARRAY, map fields to MAP
 *   <li>a message that contains itself to STRING holding JSON, at the recursive occurrence
 * </ul>
 *
 * <p>With flattening on, a singular message field {@code a} with leaf {@code b} becomes the column
 * {@code a_b} with identity {@code proto:A.B}, A and B being the field numbers. Repeated and map
 * fields keep their ROW element type.
 */
public class ProtobufSchemaConverter implements Serializable {

    private static final long serialVersionUID = 2L;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private static final String TIMESTAMP = "google.protobuf.Timestamp";
    private static final String DURATION = "google.protobuf.Duration";
    private static final String STRUCT = "google.protobuf.Struct";
    private static final String VALUE = "google.protobuf.Value";
    private static final String LIST_VALUE = "google.protobuf.ListValue";
    private static final String FIELD_MASK = "google.protobuf.FieldMask";
    private static final String EMPTY = "google.protobuf.Empty";
    private static final String WRAPPER_PREFIX = "google.protobuf.";
    private static final String WRAPPER_SUFFIX = "Value";
    private static final String WRAPPER_FIELD = "value";

    /** Scheme of the identity marker appended to every column description. */
    public static final String IDENTITY_SCHEME = "proto";

    private static final String FLATTEN_SEPARATOR = "_";
    private static final String IDENTITY_SEPARATOR = ".";

    private final boolean readDefaultValues;
    private final boolean flattenNestedMessages;

    /** Comment index for the descriptor being converted. Rebuilt on every {@link #toFields}. */
    private transient ProtobufComments comments;

    public ProtobufSchemaConverter(boolean readDefaultValues) {
        this(readDefaultValues, false);
    }

    public ProtobufSchemaConverter(boolean readDefaultValues, boolean flattenNestedMessages) {
        this.readDefaultValues = readDefaultValues;
        this.flattenNestedMessages = flattenNestedMessages;
    }

    // ---------------------------------------------------------------- schema

    public List<DataField> toFields(Descriptor descriptor) {
        comments = new ProtobufComments();
        Set<String> visiting = new HashSet<>();
        visiting.add(descriptor.getFullName());
        List<DataField> fields = new ArrayList<>();
        collectFields(descriptor, "", "", visiting, fields);
        return fields;
    }

    private void collectFields(
            Descriptor message,
            String namePrefix,
            String identityPrefix,
            Set<String> visiting,
            List<DataField> out) {
        for (FieldDescriptor field : message.getFields()) {
            String name = namePrefix + field.getName();
            String identity = identityPrefix + field.getNumber();
            if (flattens(field, visiting)) {
                Descriptor nested = field.getMessageType();
                visiting.add(nested.getFullName());
                collectFields(
                        nested,
                        name + FLATTEN_SEPARATOR,
                        identity + IDENTITY_SEPARATOR,
                        visiting,
                        out);
                visiting.remove(nested.getFullName());
                continue;
            }
            for (DataField existing : out) {
                if (existing.name().equals(name)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Field %s maps to column '%s', which another field already uses. "
                                            + "Rename one of them or turn off '%s'.",
                                    field.getFullName(),
                                    name,
                                    ProtobufOptions.FLATTEN_NESTED_MESSAGES.key()));
                }
            }
            out.add(
                    new DataField(
                            out.size(), name, toType(field, visiting), describe(field, identity)));
        }
    }

    /**
     * A singular message field is flattened into its leaves unless it maps to a scalar (well-known
     * types) or the message is already on the path, which would never end.
     */
    private boolean flattens(FieldDescriptor field, Set<String> visiting) {
        if (!flattenNestedMessages
                || field.isRepeated()
                || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
            return false;
        }
        Descriptor nested = field.getMessageType();
        return wellKnownType(nested, visiting) == null && !visiting.contains(nested.getFullName());
    }

    /**
     * The column description carries the field's documentation comment and a {@code [proto:N]}
     * identity marker. The marker is what lets the sink recognise a renamed field.
     */
    private String describe(FieldDescriptor field, String identity) {
        return ColumnIdentityMarker.withIdentity(
                comments.commentOf(field), IDENTITY_SCHEME + ":" + identity);
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
        DataType wellKnown = wellKnownType(message, visiting);
        if (wellKnown != null) {
            return wellKnown;
        }
        String name = message.getFullName();
        if (!visiting.add(name)) {
            // The message contains itself. A ROW cannot be infinitely deep, so keep the
            // recursive occurrence as JSON text.
            return DataTypes.STRING();
        }
        try {
            RowType.Builder builder = RowType.builder();
            for (FieldDescriptor field : message.getFields()) {
                builder.field(
                        field.getName(),
                        toType(field, visiting),
                        describe(field, String.valueOf(field.getNumber())));
            }
            return builder.build();
        } finally {
            visiting.remove(name);
        }
    }

    /** The Paimon type of a google.protobuf well-known message, or null for any other message. */
    private DataType wellKnownType(Descriptor message, Set<String> visiting) {
        switch (message.getFullName()) {
            case TIMESTAMP:
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6);
            case DURATION:
                return DataTypes.DECIMAL(20, 9);
            case STRUCT:
            case VALUE:
            case LIST_VALUE:
            case FIELD_MASK:
                return DataTypes.STRING();
            case EMPTY:
                return DataTypes.BOOLEAN();
            default:
                FieldDescriptor wrapped = wrappedField(message);
                return wrapped == null ? null : toSingularType(wrapped, visiting);
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

    /** Column name to CDC string value. Fields that are NULL are left out. */
    public Map<String, String> toValues(Message message) {
        Map<String, String> values = new LinkedHashMap<>();
        Set<String> visiting = new HashSet<>();
        visiting.add(message.getDescriptorForType().getFullName());
        collectValues(message, "", visiting, values);
        return values;
    }

    private void collectValues(
            Message message, String namePrefix, Set<String> visiting, Map<String, String> out) {
        for (FieldDescriptor field : message.getDescriptorForType().getFields()) {
            String name = namePrefix + field.getName();
            if (flattens(field, visiting)) {
                if (message.hasField(field)) {
                    Message nested = (Message) message.getField(field);
                    String nestedName = nested.getDescriptorForType().getFullName();
                    visiting.add(nestedName);
                    collectValues(nested, name + FLATTEN_SEPARATOR, visiting, out);
                    visiting.remove(nestedName);
                }
                continue;
            }
            Object value = valueOf(message, field);
            if (value != null) {
                out.put(name, asString(value));
            }
        }
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
        Object wellKnown = wellKnownValue(message);
        if (wellKnown != null) {
            return wellKnown;
        }
        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        for (FieldDescriptor nested : message.getDescriptorForType().getFields()) {
            Object nestedValue = valueOf(message, nested);
            if (nestedValue != null) {
                node.set(nested.getName(), toJsonNode(nestedValue));
            }
        }
        return node;
    }

    /** The value of a google.protobuf well-known message, or null for any other message. */
    private Object wellKnownValue(Message message) {
        Descriptor type = message.getDescriptorForType();
        switch (type.getFullName()) {
            case TIMESTAMP:
                return formatTimestamp(message);
            case DURATION:
                return formatDuration(message);
            case STRUCT:
                return structToJson(message);
            case VALUE:
                return valueToJson(message);
            case LIST_VALUE:
                return listValueToJson(message);
            case FIELD_MASK:
                return String.join(
                        ",",
                        ((List<?>) message.getField(type.findFieldByName("paths")))
                                .stream().map(Object::toString).toArray(String[]::new));
            case EMPTY:
                return "true";
            default:
                FieldDescriptor wrapped = wrappedField(type);
                return wrapped == null ? null : scalarToString(wrapped, message.getField(wrapped));
        }
    }

    private JsonNode structToJson(Message struct) {
        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        FieldDescriptor fields = struct.getDescriptorForType().findFieldByName("fields");
        for (Object o : (List<?>) struct.getField(fields)) {
            Message entry = (Message) o;
            Descriptor entryType = entry.getDescriptorForType();
            node.set(
                    (String) entry.getField(entryType.findFieldByName("key")),
                    valueToJson((Message) entry.getField(entryType.findFieldByName("value"))));
        }
        return node;
    }

    private JsonNode valueToJson(Message value) {
        Descriptor type = value.getDescriptorForType();
        FieldDescriptor kind = value.getOneofFieldDescriptor(type.getOneofs().get(0));
        if (kind == null) {
            return NullNode.getInstance();
        }
        Object v = value.getField(kind);
        switch (kind.getName()) {
            case "number_value":
                return DoubleNode.valueOf((Double) v);
            case "string_value":
                return TextNode.valueOf((String) v);
            case "bool_value":
                return BooleanNode.valueOf((Boolean) v);
            case "struct_value":
                return structToJson((Message) v);
            case "list_value":
                return listValueToJson((Message) v);
            default:
                return NullNode.getInstance();
        }
    }

    private JsonNode listValueToJson(Message list) {
        ArrayNode node = OBJECT_MAPPER.createArrayNode();
        FieldDescriptor values = list.getDescriptorForType().findFieldByName("values");
        for (Object o : (List<?>) list.getField(values)) {
            node.add(valueToJson((Message) o));
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

    /**
     * The sink parses a TIMESTAMP_LTZ string as local time in the JVM's zone, so the instant is
     * formatted in that same zone and round-trips.
     */
    private static String formatTimestamp(Message timestamp) {
        Descriptor type = timestamp.getDescriptorForType();
        long seconds = (Long) timestamp.getField(type.findFieldByName("seconds"));
        int nanos = (Integer) timestamp.getField(type.findFieldByName("nanos"));
        return TIMESTAMP_FORMATTER.format(
                Instant.ofEpochSecond(seconds, nanos).atZone(ZoneId.systemDefault()));
    }

    private static String formatDuration(Message duration) {
        Descriptor type = duration.getDescriptorForType();
        long seconds = (Long) duration.getField(type.findFieldByName("seconds"));
        int nanos = (Integer) duration.getField(type.findFieldByName("nanos"));
        return BigDecimal.valueOf(seconds).add(BigDecimal.valueOf(nanos, 9)).toPlainString();
    }
}
