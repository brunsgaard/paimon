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

import org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.VALUE_FORMAT;
import static org.apache.paimon.flink.action.cdc.format.protobuf.ProtobufOptions.DESCRIPTOR_SET_PATH;
import static org.apache.paimon.flink.action.cdc.format.protobuf.ProtobufOptions.DESCRIPTOR_SET_REFRESH_INTERVAL;
import static org.apache.paimon.flink.action.cdc.format.protobuf.ProtobufOptions.MESSAGE_NAME;
import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.SIMPLE;

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
}
