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

import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.InstantiationUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A decoded protobuf message. The deserialization schema produces it and {@link
 * ProtobufRecordParser} consumes it. Values are already in the string form that the CDC sink
 * expects, and the schema describes the message type the values were decoded with, so the record is
 * self-describing.
 *
 * <p>The schema travels as Java-serialized bytes. Flink falls back to Kryo for this record, and
 * Kryo cannot rebuild the unmodifiable lists inside Paimon's {@code RowType}. Records from one
 * descriptor share one byte array, so a consumer can cache the parsed fields by comparing it.
 */
public class ProtobufSourceRecord implements Serializable {

    private static final long serialVersionUID = 2L;

    private final byte[] schema;
    private final Map<String, String> values;

    private transient List<DataField> fields;

    public ProtobufSourceRecord(List<DataField> fields, Map<String, String> values) {
        this(serialize(fields), values);
        this.fields = new ArrayList<>(fields);
    }

    public ProtobufSourceRecord(byte[] schema, Map<String, String> values) {
        this.schema = schema;
        this.values = new LinkedHashMap<>(values);
    }

    public static byte[] serialize(List<DataField> fields) {
        try {
            return InstantiationUtil.serializeObject(new ArrayList<>(fields));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize protobuf schema", e);
        }
    }

    public static List<DataField> deserialize(byte[] schema) {
        try {
            return InstantiationUtil.deserializeObject(
                    schema, ProtobufSourceRecord.class.getClassLoader());
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException("Failed to deserialize protobuf schema", e);
        }
    }

    /** The serialized schema. Identical bytes for every record decoded with one descriptor. */
    public byte[] schemaBytes() {
        return schema;
    }

    public List<DataField> fields() {
        if (fields == null) {
            fields = deserialize(schema);
        }
        return fields;
    }

    /** Field name to CDC string value. Fields that are NULL are absent. */
    public Map<String, String> values() {
        return values;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ProtobufSourceRecord)) {
            return false;
        }
        ProtobufSourceRecord that = (ProtobufSourceRecord) o;
        return Arrays.equals(schema, that.schema) && values.equals(that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(schema), values);
    }

    @Override
    public String toString() {
        return values.toString();
    }
}
