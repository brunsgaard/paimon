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
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.EVENT;
import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.NODE;
import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.WELL_KNOWN;
import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.descriptor;
import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.descriptorSet;
import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.fullEvent;
import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.wellKnown;
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
        assertThat(types.get("ts")).isEqualTo(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6));
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
                .containsEntry("ts", localTimestamp(1700000000L, 123456789))
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

    @Test
    public void testDescriptionsCarryCommentAndFieldNumber() {
        List<DataField> fields = new ProtobufSchemaConverter(false).toFields(EVENT_TYPE);
        Map<String, String> descriptions =
                fields.stream().collect(Collectors.toMap(DataField::name, DataField::description));

        // no comment in the proto: just the identity marker
        assertThat(descriptions.get("id")).isEqualTo("[proto:1]");
        // a documented field keeps its comment ahead of the marker
        assertThat(descriptions.get("address"))
                .isEqualTo(TestProtobufDescriptors.EVENT_ADDRESS_COMMENT + " [proto:7]");
        assertThat(descriptions.get("big")).isEqualTo("[proto:11]");

        // nested ROW fields are marked too, with their own numbers and comments
        RowType address =
                (RowType)
                        fields.stream()
                                .filter(f -> f.name().equals("address"))
                                .findFirst()
                                .get()
                                .type();
        assertThat(address.getFields().get(0).description())
                .isEqualTo(TestProtobufDescriptors.ADDRESS_CITY_COMMENT + " [proto:1]");
        assertThat(address.getFields().get(1).description()).isEqualTo("[proto:2]");

        // ARRAY<ROW> elements share the message type, so the same descriptions
        RowType element =
                (RowType)
                        ((org.apache.paimon.types.ArrayType)
                                        fields.stream()
                                                .filter(f -> f.name().equals("addresses"))
                                                .findFirst()
                                                .get()
                                                .type())
                                .getElementType();
        assertThat(element.getFields().get(0).description())
                .isEqualTo(TestProtobufDescriptors.ADDRESS_CITY_COMMENT + " [proto:1]");

        assertThat(ColumnIdentityMarker.identityOf(descriptions.get("address")))
                .contains("proto:7");
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

    /** What the converter must emit for a TIMESTAMP_LTZ so the sink parses it back in its zone. */
    private static String localTimestamp(long seconds, int nanos) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
                .format(Instant.ofEpochSecond(seconds, nanos).atZone(ZoneId.systemDefault()));
    }

    @Test
    public void testWellKnownTypes() {
        Descriptor type = descriptor(SET, WELL_KNOWN);
        ProtobufSchemaConverter converter = new ProtobufSchemaConverter(false);

        Map<String, DataType> types = typesOf(converter.toFields(type));
        assertThat(types.get("took")).isEqualTo(DataTypes.DECIMAL(20, 9));
        assertThat(types.get("attrs")).isEqualTo(DataTypes.STRING());
        assertThat(types.get("any_value")).isEqualTo(DataTypes.STRING());
        assertThat(types.get("items")).isEqualTo(DataTypes.STRING());
        assertThat(types.get("marker")).isEqualTo(DataTypes.BOOLEAN());
        assertThat(types.get("mask")).isEqualTo(DataTypes.STRING());
        assertThat(types.get("label")).isEqualTo(DataTypes.STRING());

        Map<String, String> values = converter.toValues(wellKnown(type));
        assertThat(values)
                .containsEntry("took", "1.500000000")
                .containsEntry("attrs", "{\"a\":1.0,\"b\":[true,null]}")
                .containsEntry("any_value", "\"x\"")
                .containsEntry("items", "[2.5]")
                .containsEntry("marker", "true")
                .containsEntry("mask", "a.b,c")
                .containsEntry("label", "hello");

        // unset well-known fields are NULL, including the Empty marker
        assertThat(converter.toValues(DynamicMessage.newBuilder(type).build())).isEmpty();
    }

    @Test
    public void testFlattenedFields() {
        ProtobufSchemaConverter converter = new ProtobufSchemaConverter(false, true);
        List<DataField> fields = converter.toFields(EVENT_TYPE);
        Map<String, DataType> types = typesOf(fields);

        // the singular message is gone, its leaves are columns
        assertThat(types.keySet())
                .containsExactly(
                        "id",
                        "name",
                        "score",
                        "active",
                        "payload",
                        "level",
                        "address_city",
                        "address_zip",
                        "tags",
                        "counts",
                        "ts",
                        "big",
                        "maybe",
                        "addresses");
        assertThat(types.get("address_city")).isEqualTo(DataTypes.STRING());
        // identity is the number path, comment is the leaf's own comment
        Map<String, String> descriptions = new LinkedHashMap<>();
        fields.forEach(f -> descriptions.put(f.name(), f.description()));
        assertThat(descriptions.get("address_city"))
                .isEqualTo(TestProtobufDescriptors.ADDRESS_CITY_COMMENT + " [proto:7.1]");
        assertThat(descriptions.get("address_zip")).isEqualTo("[proto:7.2]");
        assertThat(descriptions.get("id")).isEqualTo("[proto:1]");
        // repeated messages keep their ROW element
        assertThat(types.get("addresses")).isInstanceOf(ArrayType.class);
        assertThat(((ArrayType) types.get("addresses")).getElementType())
                .isInstanceOf(RowType.class);
        // ids stay dense
        for (int i = 0; i < fields.size(); i++) {
            assertThat(fields.get(i).id()).isEqualTo(i);
        }

        Map<String, String> values = converter.toValues(fullEvent(EVENT_TYPE));
        assertThat(values)
                .containsEntry("address_city", "Copenhagen")
                .containsEntry("address_zip", "2100")
                .doesNotContainKey("address");

        // an unset message leaves its leaves NULL
        DynamicMessage onlyId =
                DynamicMessage.newBuilder(EVENT_TYPE)
                        .setField(EVENT_TYPE.findFieldByName("id"), 1L)
                        .build();
        assertThat(converter.toValues(onlyId)).containsOnlyKeys("id");
    }

    @Test
    public void testFlatteningStopsAtRecursion() {
        Descriptor node = descriptor(SET, NODE);
        Map<String, DataType> types =
                typesOf(new ProtobufSchemaConverter(false, true).toFields(node));
        assertThat(types.keySet()).containsExactly("name", "child");
        assertThat(types.get("child")).isEqualTo(DataTypes.STRING());
    }
}
