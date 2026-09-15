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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A decoded protobuf message. The deserialization schema produces it and {@link
 * ProtobufRecordParser} consumes it. Values are already in the string form that the CDC sink
 * expects, and fields describe the message type the values were decoded with, so the record is
 * self-describing and plain Java serializable.
 */
public class ProtobufSourceRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<DataField> fields;
    private final Map<String, String> values;

    public ProtobufSourceRecord(List<DataField> fields, Map<String, String> values) {
        // Plain collections keep the record friendly to Flink's Kryo fallback serializer.
        this.fields = new ArrayList<>(fields);
        this.values = new LinkedHashMap<>(values);
    }

    public List<DataField> fields() {
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
        return fields.equals(that.fields) && values.equals(that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields, values);
    }

    @Override
    public String toString() {
        return values.toString();
    }
}
