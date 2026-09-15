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
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.EVENT;
import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.NODE;
import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.descriptor;
import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.descriptorSet;
import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.fullEvent;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ProtobufSchemaConverter}. */
public class ProtobufSchemaConverterTest {

    private static final FileDescriptorSet SET = descriptorSet(false);
    private static final Descriptor EVENT_TYPE = descriptor(SET, EVENT);

    @Test
    public void testTypes() {
        Map<String, DataType> types =
                typesOf(new ProtobufSchemaConverter(false).toFields(EVENT_TYPE));

        assertThat(types.keySet())
                .containsExactly(
                        "id",
                        "name",
                        "score",
                        "active",
                        "payload",
                        "level",
                        "address",
                        "tags",
                        "counts",
                        "ts",
                        "big",
                        "maybe",
                        "addresses");
        assertThat(types.get("id")).isEqualTo(DataTypes.BIGINT());
        assertThat(types.get("name")).isEqualTo(DataTypes.STRING());
        assertThat(types.get("score")).isEqualTo(DataTypes.DOUBLE());
        assertThat(types.get("active")).isEqualTo(DataTypes.BOOLEAN());
        assertThat(types.get("payload")).isEqualTo(DataTypes.STRING());
        assertThat(types.get("level")).isEqualTo(DataTypes.STRING());
        assertThat(types.get("tags")).isEqualTo(DataTypes.ARRAY(DataTypes.STRING()));
        assertThat(types.get("counts"))
                .isEqualTo(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
        assertThat(types.get("ts")).isEqualTo(DataTypes.TIMESTAMP(6));
        assertThat(types.get("big")).isEqualTo(DataTypes.DECIMAL(20, 0));
        assertThat(types.get("maybe")).isEqualTo(DataTypes.INT());

        RowType address = (RowType) types.get("address");
        assertThat(address.getFieldNames()).containsExactly("city", "zip");
        assertThat(address.getFieldTypes()).containsExactly(DataTypes.STRING(), DataTypes.STRING());
        assertThat(types.get("addresses")).isEqualTo(DataTypes.ARRAY(address));
    }

    @Test
    public void testValues() {
        Map<String, String> values =
                new ProtobufSchemaConverter(false).toValues(fullEvent(EVENT_TYPE));

        assertThat(values)
                .containsEntry("id", "42")
                .containsEntry("name", "alice")
                .containsEntry("score", "9.5")
                .containsEntry("active", "true")
                .containsEntry("payload", "aGk=")
                .containsEntry("level", "HIGH")
                .containsEntry("address", "{\"city\":\"Copenhagen\",\"zip\":\"2100\"}")
                .containsEntry("tags", "[\"a\",\"b\"]")
                .containsEntry("counts", "{\"x\":\"1\"}")
                .containsEntry("ts", "2023-11-14 22:13:20.123456")
                .containsEntry("big", "18446744073709551615")
                .containsEntry("maybe", "7")
                .containsEntry(
                        "addresses",
                        "[{\"city\":\"Copenhagen\",\"zip\":\"2100\"},{\"city\":\"Aarhus\",\"zip\":\"8000\"}]");
    }

    @Test
    public void testUnsetFieldsAreNull() {
        DynamicMessage onlyId =
                DynamicMessage.newBuilder(EVENT_TYPE)
                        .setField(EVENT_TYPE.findFieldByName("id"), 1L)
                        .build();

        Map<String, String> values = new ProtobufSchemaConverter(false).toValues(onlyId);

        assertThat(values).containsOnlyKeys("id");
    }

    @Test
    public void testReadDefaultValues() {
        DynamicMessage onlyId =
                DynamicMessage.newBuilder(EVENT_TYPE)
                        .setField(EVENT_TYPE.findFieldByName("id"), 1L)
                        .build();

        Map<String, String> values = new ProtobufSchemaConverter(true).toValues(onlyId);

        assertThat(values)
                .containsEntry("id", "1")
                .containsEntry("name", "")
                .containsEntry("score", "0.0")
                .containsEntry("active", "false")
                .containsEntry("payload", "")
                .containsEntry("level", "LOW")
                .containsEntry("tags", "[]")
                .containsEntry("counts", "{}")
                .containsEntry("big", "0")
                .containsEntry("maybe", "0")
                .containsEntry("addresses", "[]");
        // A message field has no default value. Unset stays NULL.
        assertThat(values).doesNotContainKeys("address", "ts");
    }

    @Test
    public void testRecursiveMessageFallsBackToJsonString() {
        Descriptor node = descriptor(SET, NODE);
        Map<String, DataType> types = typesOf(new ProtobufSchemaConverter(false).toFields(node));

        assertThat(types.get("name")).isEqualTo(DataTypes.STRING());
        assertThat(types.get("child")).isEqualTo(DataTypes.STRING());

        DynamicMessage kid =
                DynamicMessage.newBuilder(node)
                        .setField(node.findFieldByName("name"), "kid")
                        .build();
        DynamicMessage root =
                DynamicMessage.newBuilder(node)
                        .setField(node.findFieldByName("name"), "root")
                        .setField(node.findFieldByName("child"), kid)
                        .build();

        Map<String, String> values = new ProtobufSchemaConverter(false).toValues(root);
        assertThat(values)
                .containsEntry("name", "root")
                .containsEntry("child", "{\"name\":\"kid\"}");
    }

    @Test
    public void testAddedFieldsAppearInSchemaAndValues() {
        Descriptor v2 = descriptor(descriptorSet(true), EVENT);
        ProtobufSchemaConverter converter = new ProtobufSchemaConverter(false);

        Map<String, DataType> types = typesOf(converter.toFields(v2));
        assertThat(types).containsKey("region");
        assertThat(((RowType) types.get("address")).getFieldNames())
                .containsExactly("city", "zip", "street");

        // A message written with the old descriptor still decodes. The new field is NULL.
        byte[] oldBytes = fullEvent(EVENT_TYPE).toByteArray();
        DynamicMessage decoded;
        try {
            decoded = DynamicMessage.parseFrom(v2, oldBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        assertThat(converter.toValues(decoded)).doesNotContainKey("region").containsKey("id");
    }

    private static Map<String, DataType> typesOf(List<DataField> fields) {
        return fields.stream()
                .collect(
                        Collectors.toMap(
                                DataField::name,
                                DataField::type,
                                (a, b) -> a,
                                java.util.LinkedHashMap::new));
    }
}
