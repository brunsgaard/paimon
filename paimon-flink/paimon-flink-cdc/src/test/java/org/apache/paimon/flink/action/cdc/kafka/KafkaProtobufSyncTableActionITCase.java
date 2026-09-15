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

package org.apache.paimon.flink.action.cdc.kafka;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors;
import org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.Variant;
import org.apache.paimon.flink.sink.cdc.CdcSchemaEvolutionOptions;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.VALUE_FORMAT;
import static org.apache.paimon.flink.action.cdc.format.protobuf.ProtobufOptions.DESCRIPTOR_SET_PATH;
import static org.apache.paimon.flink.action.cdc.format.protobuf.ProtobufOptions.DESCRIPTOR_SET_REFRESH_INTERVAL;
import static org.apache.paimon.flink.action.cdc.format.protobuf.ProtobufOptions.MESSAGE_NAME;
import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.NESTED;
import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.SIMPLE;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link KafkaSyncTableAction} with the protobuf format. */
public class KafkaProtobufSyncTableActionITCase extends KafkaActionITCaseBase {

    @TempDir Path descriptorDir;

    @Test
    @Timeout(120)
    public void testSchemaEvolution() throws Exception {
        String topic = "protobuf-schema-evolution";
        createTestTopic(topic, 1, 1);

        Path descriptorFile = descriptorDir.resolve("simple.desc");
        FileDescriptorSet v1 = TestProtobufDescriptors.descriptorSet(false);
        Files.write(descriptorFile, v1.toByteArray());
        Descriptor simpleV1 = TestProtobufDescriptors.descriptor(v1, SIMPLE);

        try (KafkaProducer<byte[], byte[]> producer = newProducer()) {
            producer.send(
                            new ProducerRecord<>(
                                    topic,
                                    TestProtobufDescriptors.simple(simpleV1, 1L, "a1", null)))
                    .get();
            producer.send(
                            new ProducerRecord<>(
                                    topic,
                                    TestProtobufDescriptors.simple(simpleV1, 2L, "a2", null)))
                    .get();
        }

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "protobuf");
        kafkaConfig.put(TOPIC.key(), topic);
        kafkaConfig.put(DESCRIPTOR_SET_PATH.key(), descriptorFile.toUri().toString());
        kafkaConfig.put(MESSAGE_NAME.key(), SIMPLE);
        kafkaConfig.put(DESCRIPTOR_SET_REFRESH_INTERVAL.key(), "1s");

        // An append table without a bucket key must not be bucketed.
        Map<String, String> tableConfig = getBasicTableConfig();
        tableConfig.remove("bucket");
        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig).withTableConfig(tableConfig).build();
        runActionWithDefaultEnv(action);

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"id", "name"});
        waitForResult(
                Arrays.asList("+I[1, a1]", "+I[2, a2]"),
                getFileStoreTable(tableName),
                rowType,
                Collections.emptyList());

        // The producer adds a field and publishes the new descriptor set to the same path.
        FileDescriptorSet v2 = TestProtobufDescriptors.descriptorSet(true);
        Files.write(descriptorFile, v2.toByteArray());
        Descriptor simpleV2 = TestProtobufDescriptors.descriptor(v2, SIMPLE);
        // Let the refresh interval pass so the next record is decoded with the new descriptor.
        Thread.sleep(1500);

        try (KafkaProducer<byte[], byte[]> producer = newProducer()) {
            producer.send(
                            new ProducerRecord<>(
                                    topic,
                                    TestProtobufDescriptors.simple(simpleV2, 3L, "a3", "eu")))
                    .get();
        }

        rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"id", "name", "region"});
        waitForResult(
                Arrays.asList("+I[1, a1, NULL]", "+I[2, a2, NULL]", "+I[3, a3, eu]"),
                getFileStoreTable(tableName),
                rowType,
                Collections.emptyList());
    }

    private static KafkaProducer<byte[], byte[]> newProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", getStandardProps().getProperty("bootstrap.servers"));
        props.put("key.serializer", ByteArraySerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    // ------------------------------------------------------------------ helpers

    private Map<String, String> protobufKafkaConfig(
            String topic, Path descriptorFile, String message) {
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "protobuf");
        kafkaConfig.put(TOPIC.key(), topic);
        kafkaConfig.put(DESCRIPTOR_SET_PATH.key(), descriptorFile.toUri().toString());
        kafkaConfig.put(MESSAGE_NAME.key(), message);
        kafkaConfig.put(DESCRIPTOR_SET_REFRESH_INTERVAL.key(), "1s");
        return kafkaConfig;
    }

    /** Append table without buckets, with rename and drop following the source. */
    private Map<String, String> evolvingTableConfig() {
        Map<String, String> tableConfig = getBasicTableConfig();
        tableConfig.remove("bucket");
        tableConfig.put(CdcSchemaEvolutionOptions.RENAME_BY_COMMENT.key(), "true");
        tableConfig.put(CdcSchemaEvolutionOptions.DROP_MISSING_COLUMNS.key(), "true");
        return tableConfig;
    }

    private static void produce(String topic, byte[]... values) throws Exception {
        try (KafkaProducer<byte[], byte[]> producer = newProducer()) {
            for (byte[] value : values) {
                producer.send(new ProducerRecord<>(topic, value)).get();
            }
        }
    }

    private static Descriptor simpleOf(Variant variant) {
        return TestProtobufDescriptors.descriptor(
                TestProtobufDescriptors.descriptorSet(variant), SIMPLE);
    }

    private static void publish(Path descriptorFile, Variant variant) throws Exception {
        Files.write(descriptorFile, TestProtobufDescriptors.descriptorSet(variant).toByteArray());
        // let the refresh interval pass so the next record is decoded with the new descriptor
        Thread.sleep(1500);
    }

    private static List<String> fieldNames(FileStoreTable table) {
        return table.schema().fields().stream()
                .map(DataField::name)
                .collect(java.util.stream.Collectors.toList());
    }

    private static DataField fieldNamed(FileStoreTable table, String name) {
        return table.schema().fields().stream()
                .filter(f -> f.name().equals(name))
                .findFirst()
                .get();
    }

    private static RowType simpleRowType(String secondName) {
        return RowType.of(
                new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                new String[] {"id", secondName});
    }

    // ------------------------------------------------------------------ comments

    @Test
    @Timeout(120)
    public void testCommentsLandOnColumnsAndPropagate() throws Exception {
        String topic = "protobuf-comments";
        createTestTopic(topic, 1, 1);
        Path descriptorFile = descriptorDir.resolve("simple.desc");
        Files.write(
                descriptorFile, TestProtobufDescriptors.descriptorSet(Variant.V1).toByteArray());
        produce(topic, TestProtobufDescriptors.simple(simpleOf(Variant.V1), 1L, "a1", null));

        runActionWithDefaultEnv(
                syncTableActionBuilder(protobufKafkaConfig(topic, descriptorFile, SIMPLE))
                        .withTableConfig(evolvingTableConfig())
                        .build());
        waitForResult(
                Collections.singletonList("+I[1, a1]"),
                getFileStoreTable(tableName),
                simpleRowType("name"),
                Collections.emptyList());

        FileStoreTable table = getFileStoreTable(tableName);
        assertThat(fieldNamed(table, "id").description()).isEqualTo("[proto:1]");
        assertThat(fieldNamed(table, "name").description())
                .isEqualTo(TestProtobufDescriptors.SIMPLE_NAME_COMMENT + " [proto:2]");

        // the producer edits the comment; the column comment follows
        publish(descriptorFile, Variant.V2_COMMENT);
        produce(
                topic,
                TestProtobufDescriptors.simple(simpleOf(Variant.V2_COMMENT), 2L, "a2", null));
        waitForResult(
                Arrays.asList("+I[1, a1]", "+I[2, a2]"),
                getFileStoreTable(tableName),
                simpleRowType("name"),
                Collections.emptyList());
        assertThat(fieldNamed(getFileStoreTable(tableName), "name").description())
                .isEqualTo("Display name, shown to the user. [proto:2]");
    }

    // ------------------------------------------------------------------ rename

    @Test
    @Timeout(120)
    public void testRenameColumn() throws Exception {
        String topic = "protobuf-rename";
        createTestTopic(topic, 1, 1);
        Path descriptorFile = descriptorDir.resolve("simple.desc");
        Files.write(
                descriptorFile, TestProtobufDescriptors.descriptorSet(Variant.V1).toByteArray());
        produce(
                topic,
                TestProtobufDescriptors.simple(simpleOf(Variant.V1), 1L, "a1", null),
                TestProtobufDescriptors.simple(simpleOf(Variant.V1), 2L, "a2", null));

        runActionWithDefaultEnv(
                syncTableActionBuilder(protobufKafkaConfig(topic, descriptorFile, SIMPLE))
                        .withTableConfig(evolvingTableConfig())
                        .build());
        waitForResult(
                Arrays.asList("+I[1, a1]", "+I[2, a2]"),
                getFileStoreTable(tableName),
                simpleRowType("name"),
                Collections.emptyList());

        // name -> title, same field number
        publish(descriptorFile, Variant.V2_RENAMED);
        produce(
                topic,
                TestProtobufDescriptors.simple(simpleOf(Variant.V2_RENAMED), 3L, "a3", null));

        // history is readable under the new name, and no `name` column is left behind
        waitForResult(
                Arrays.asList("+I[1, a1]", "+I[2, a2]", "+I[3, a3]"),
                getFileStoreTable(tableName),
                simpleRowType("title"),
                Collections.emptyList());
        FileStoreTable table = getFileStoreTable(tableName);
        assertThat(fieldNames(table)).containsExactly("id", "title");
        assertThat(fieldNamed(table, "title").description()).endsWith("[proto:2]");
        assertThat(new SchemaManager(table.fileIO(), table.location()).listAllIds())
                .hasSize(2);
    }

    @Test
    @Timeout(120)
    public void testRenameWithoutOptionAddsColumn() throws Exception {
        String topic = "protobuf-rename-off";
        createTestTopic(topic, 1, 1);
        Path descriptorFile = descriptorDir.resolve("simple.desc");
        Files.write(
                descriptorFile, TestProtobufDescriptors.descriptorSet(Variant.V1).toByteArray());
        produce(topic, TestProtobufDescriptors.simple(simpleOf(Variant.V1), 1L, "a1", null));

        Map<String, String> tableConfig = getBasicTableConfig();
        tableConfig.remove("bucket");
        runActionWithDefaultEnv(
                syncTableActionBuilder(protobufKafkaConfig(topic, descriptorFile, SIMPLE))
                        .withTableConfig(tableConfig)
                        .build());
        waitForResult(
                Collections.singletonList("+I[1, a1]"),
                getFileStoreTable(tableName),
                simpleRowType("name"),
                Collections.emptyList());

        publish(descriptorFile, Variant.V2_RENAMED);
        produce(
                topic,
                TestProtobufDescriptors.simple(simpleOf(Variant.V2_RENAMED), 2L, "a2", null));

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"id", "name", "title"});
        // default behaviour: a new column, the old one goes NULL
        waitForResult(
                Arrays.asList("+I[1, a1, NULL]", "+I[2, NULL, a2]"),
                getFileStoreTable(tableName),
                rowType,
                Collections.emptyList());
    }

    // ------------------------------------------------------------------ drop

    @Test
    @Timeout(120)
    public void testDropColumn() throws Exception {
        String topic = "protobuf-drop";
        createTestTopic(topic, 1, 1);
        Path descriptorFile = descriptorDir.resolve("simple.desc");
        Files.write(
                descriptorFile, TestProtobufDescriptors.descriptorSet(Variant.V1).toByteArray());
        produce(topic, TestProtobufDescriptors.simple(simpleOf(Variant.V1), 1L, "a1", null));

        runActionWithDefaultEnv(
                syncTableActionBuilder(protobufKafkaConfig(topic, descriptorFile, SIMPLE))
                        .withTableConfig(evolvingTableConfig())
                        .build());
        waitForResult(
                Collections.singletonList("+I[1, a1]"),
                getFileStoreTable(tableName),
                simpleRowType("name"),
                Collections.emptyList());

        publish(descriptorFile, Variant.V2_DROPPED);
        produce(
                topic,
                TestProtobufDescriptors.simple(simpleOf(Variant.V2_DROPPED), 2L, null, null));

        waitForResult(
                Arrays.asList("+I[1]", "+I[2]"),
                getFileStoreTable(tableName),
                RowType.of(new DataType[] {DataTypes.BIGINT()}, new String[] {"id"}),
                Collections.emptyList());
        FileStoreTable table = getFileStoreTable(tableName);
        assertThat(fieldNames(table)).containsExactly("id");
        assertThat(new SchemaManager(table.fileIO(), table.location()).listAllIds())
                .hasSize(2);
    }

    // ------------------------------------------------------------------ nested

    @Test
    @Timeout(120)
    public void testNestedRenameAndDrop() throws Exception {
        String topic = "protobuf-nested";
        createTestTopic(topic, 1, 1);
        Path descriptorFile = descriptorDir.resolve("nested.desc");
        Files.write(
                descriptorFile, TestProtobufDescriptors.descriptorSet(Variant.V1).toByteArray());
        Descriptor nestedV1 =
                TestProtobufDescriptors.descriptor(
                        TestProtobufDescriptors.descriptorSet(Variant.V1), NESTED);
        produce(topic, TestProtobufDescriptors.nested(nestedV1, 1L, "Copenhagen", "2100"));

        runActionWithDefaultEnv(
                syncTableActionBuilder(protobufKafkaConfig(topic, descriptorFile, NESTED))
                        .withTableConfig(evolvingTableConfig())
                        .build());
        FileStoreTable table = getFileStoreTable(tableName);
        RowType addressV1 = waitForNestedFields(table, "address", "city", "zip");
        assertThat(addressV1.getFields().get(0).description()).endsWith("[proto:1]");
        assertThat(addressV1.getFields().get(1).description()).endsWith("[proto:2]");
        waitForRows(
                table,
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), addressV1},
                        new String[] {"id", "address"}),
                Collections.singletonList("+I[1, +I[Copenhagen, 2100]]"));

        // zip -> postal inside the nested message
        publish(descriptorFile, Variant.V2_RENAMED);
        Descriptor nestedRenamed =
                TestProtobufDescriptors.descriptor(
                        TestProtobufDescriptors.descriptorSet(Variant.V2_RENAMED), NESTED);
        produce(topic, TestProtobufDescriptors.nested(nestedRenamed, 2L, "Aarhus", "8000"));
        RowType addressRenamed = waitForNestedFields(table, "address", "city", "postal");
        // the renamed field keeps its id, so old files still read
        assertThat(addressRenamed.getFields().get(1).id())
                .isEqualTo(addressV1.getFields().get(1).id());
        waitForRows(
                table,
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), addressRenamed},
                        new String[] {"id", "address"}),
                Arrays.asList("+I[1, +I[Copenhagen, 2100]]", "+I[2, +I[Aarhus, 8000]]"));

        // postal removed inside the nested message
        publish(descriptorFile, Variant.V2_DROPPED);
        Descriptor nestedDropped =
                TestProtobufDescriptors.descriptor(
                        TestProtobufDescriptors.descriptorSet(Variant.V2_DROPPED), NESTED);
        produce(topic, TestProtobufDescriptors.nested(nestedDropped, 3L, "Odense", null));
        RowType addressDropped = waitForNestedFields(table, "address", "city");
        waitForRows(
                table,
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), addressDropped},
                        new String[] {"id", "address"}),
                Arrays.asList("+I[1, +I[Copenhagen]]", "+I[2, +I[Aarhus]]", "+I[3, +I[Odense]]"));
    }

    /**
     * Waits until {@code column} is a ROW with exactly the given field names and returns that row
     * type as the table has it, ids and descriptions included, so it can be handed to {@link
     * #waitForResult}.
     */
    private static RowType waitForNestedFields(FileStoreTable table, String column, String... names)
            throws Exception {
        while (true) {
            TableSchema schema = table.copyWithLatestSchema().schema();
            int idx = schema.fieldNames().indexOf(column);
            if (idx >= 0) {
                DataType type = schema.fields().get(idx).type();
                if (type instanceof RowType
                        && ((RowType) type).getFieldNames().equals(Arrays.asList(names))) {
                    return (RowType) type;
                }
            }
            Thread.sleep(500);
        }
    }

    /** Like {@link #waitForResult} but formats nested rows, which the shared helper cannot. */
    private static void waitForRows(FileStoreTable table, RowType rowType, List<String> expected)
            throws Exception {
        List<String> sortedExpected = new ArrayList<>(expected);
        Collections.sort(sortedExpected);
        while (true) {
            FileStoreTable latest = table.copyWithLatestSchema();
            ReadBuilder readBuilder = latest.newReadBuilder();
            TableScan.Plan plan = readBuilder.newScan().plan();
            List<String> actual = new ArrayList<>();
            try (RecordReader<InternalRow> reader =
                    readBuilder
                            .newRead()
                            .createReader(plan == null ? Collections.emptyList() : plan.splits())) {
                reader.forEachRemaining(row -> actual.add(format(row, rowType)));
            }
            Collections.sort(actual);
            if (sortedExpected.equals(actual)) {
                return;
            }
            Thread.sleep(1000);
        }
    }

    private static String format(InternalRow row, RowType type) {
        StringBuilder b = new StringBuilder("+I[");
        for (int i = 0; i < type.getFieldCount(); i++) {
            if (i > 0) {
                b.append(", ");
            }
            DataType fieldType = type.getTypeAt(i);
            if (row.isNullAt(i)) {
                b.append("NULL");
            } else if (fieldType instanceof RowType) {
                RowType nested = (RowType) fieldType;
                b.append(format(row.getRow(i, nested.getFieldCount()), nested));
            } else {
                b.append(InternalRow.createFieldGetter(fieldType, i).getFieldOrNull(row));
            }
        }
        return b.append("]").toString();
    }
}
