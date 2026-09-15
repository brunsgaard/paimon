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

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.kafka.KafkaProtobufDeserializationSchema;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.BIGINT_UNSIGNED_TO_BIGINT;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_STRING;
import static org.apache.paimon.flink.action.cdc.format.protobuf.ProtobufOptions.DESCRIPTOR_SET_PATH;
import static org.apache.paimon.flink.action.cdc.format.protobuf.ProtobufOptions.MESSAGE_NAME;
import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.EVENT;
import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.descriptor;
import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.descriptorSet;
import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.fullEvent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the Kafka deserialization schema and {@link ProtobufRecordParser} together. */
public class ProtobufRecordParserTest {

    private static final String TOPIC = "events";

    @TempDir Path tempDir;

    private Configuration config;
    private byte[] eventBytes;

    @BeforeEach
    public void before() throws Exception {
        Path file = tempDir.resolve("event.desc");
        Files.write(file, descriptorSet(false).toByteArray());
        config = new Configuration();
        config.set(DESCRIPTOR_SET_PATH, file.toUri().toString());
        config.set(MESSAGE_NAME, EVENT);
        eventBytes = fullEvent(descriptor(descriptorSet(false), EVENT)).toByteArray();
    }

    @Test
    public void testBuildSchema() throws Exception {
        CdcSourceRecord record = deserialize(eventBytes);
        ProtobufRecordParser parser =
                new ProtobufRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());

        Schema schema = parser.buildSchema(record);

        assertThat(schema).isNotNull();
        assertThat(schema.primaryKeys()).isEmpty();
        Map<String, DataType> types = typesOf(schema.fields());
        assertThat(types.get("id")).isEqualTo(DataTypes.BIGINT());
        assertThat(types.get("ts")).isEqualTo(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6));
        assertThat(types.get("big")).isEqualTo(DataTypes.DECIMAL(20, 0));
        assertThat(types.get("tags")).isEqualTo(DataTypes.ARRAY(DataTypes.STRING()));
        assertThat(
                        schema.fields().stream()
                                .filter(f -> f.name().equals("address"))
                                .findFirst()
                                .get()
                                .description())
                .isEqualTo(TestProtobufDescriptors.EVENT_ADDRESS_COMMENT + " [proto:7]");
    }

    @Test
    public void testExtractRecords() throws Exception {
        CdcSourceRecord record = deserialize(eventBytes);
        ProtobufRecordParser parser =
                new ProtobufRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());

        List<RichCdcMultiplexRecord> out = new ArrayList<>();
        parser.flatMap(record, new ListCollector<>(out));

        assertThat(out).hasSize(1);
        RichCdcMultiplexRecord result = out.get(0);
        assertThat(result.databaseName()).isEqualTo(TOPIC);
        assertThat(result.tableName()).isEqualTo(TOPIC);
        assertThat(result.toRichCdcRecord().kind()).isEqualTo(RowKind.INSERT);
        assertThat(result.toRichCdcRecord().toCdcRecord().data())
                .containsEntry("id", "42")
                .containsEntry("name", "alice")
                .containsEntry(
                        "ts",
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
                                .format(
                                        Instant.ofEpochSecond(1700000000L, 123456789)
                                                .atZone(ZoneId.systemDefault())))
                .containsEntry("address", "{\"city\":\"Copenhagen\",\"zip\":\"2100\"}");
    }

    @Test
    public void testKeyAndMetadataArePassedThrough() throws Exception {
        List<CdcSourceRecord> out = new ArrayList<>();
        new KafkaProtobufDeserializationSchema(config)
                .deserialize(
                        new ConsumerRecord<>(TOPIC, 3, 17L, "k1".getBytes(), eventBytes),
                        new ListCollector<>(out));

        assertThat(out).hasSize(1);
        assertThat(out.get(0).getKey()).isEqualTo("k1");
        assertThat(out.get(0).getMetadata("partition")).isEqualTo(3);
        assertThat(out.get(0).getMetadata("offset")).isEqualTo(17L);
    }

    @Test
    public void testTombstoneIsSkipped() throws Exception {
        List<CdcSourceRecord> out = new ArrayList<>();
        new KafkaProtobufDeserializationSchema(config)
                .deserialize(
                        new ConsumerRecord<>(TOPIC, 0, 0L, null, (byte[]) null),
                        new ListCollector<>(out));
        assertThat(out).isEmpty();
    }

    @Test
    public void testTypeMappingToString() throws Exception {
        CdcSourceRecord record = deserialize(eventBytes);
        TypeMapping toString = new TypeMapping(Collections.singleton(TO_STRING));
        Schema schema =
                new ProtobufRecordParser(toString, Collections.emptyList()).buildSchema(record);

        assertThat(typesOf(schema.fields()).values())
                .allMatch(type -> type.equals(DataTypes.STRING()));
    }

    @Test
    public void testTypeMappingUnsignedToBigint() throws Exception {
        CdcSourceRecord record = deserialize(eventBytes);
        TypeMapping mapping = new TypeMapping(Collections.singleton(BIGINT_UNSIGNED_TO_BIGINT));
        Schema schema =
                new ProtobufRecordParser(mapping, Collections.emptyList()).buildSchema(record);

        assertThat(typesOf(schema.fields()).get("big")).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    public void testMissingOptionsAreRejected() {
        Configuration incomplete = new Configuration();
        incomplete.set(MESSAGE_NAME, EVENT);
        assertThatThrownBy(() -> new KafkaProtobufDeserializationSchema(incomplete))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DESCRIPTOR_SET_PATH.key());
    }

    private CdcSourceRecord deserialize(byte[] value) throws Exception {
        List<CdcSourceRecord> out = new ArrayList<>();
        new KafkaProtobufDeserializationSchema(config)
                .deserialize(
                        new ConsumerRecord<>(TOPIC, 0, 0L, null, value), new ListCollector<>(out));
        assertThat(out).hasSize(1);
        return out.get(0);
    }

    private static Map<String, DataType> typesOf(List<DataField> fields) {
        return fields.stream().collect(Collectors.toMap(DataField::name, DataField::type));
    }

    private static class ListCollector<T> implements Collector<T> {
        private final List<T> list;

        ListCollector(List<T> list) {
            this.list = list;
        }

        @Override
        public void collect(T record) {
            list.add(record);
        }

        @Override
        public void close() {}
    }
}
