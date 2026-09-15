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

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.format.protobuf.ProtobufSourceRecordDecoder;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/**
 * Deserializes protobuf-encoded Kafka record values with a descriptor set. The key, when present,
 * is passed through as a UTF-8 string.
 */
public class KafkaProtobufDeserializationSchema
        implements KafkaRecordDeserializationSchema<CdcSourceRecord> {

    private static final long serialVersionUID = 1L;

    private final ProtobufSourceRecordDecoder decoder;

    public KafkaProtobufDeserializationSchema(Configuration cdcSourceConfig) {
        this.decoder = new ProtobufSourceRecordDecoder(cdcSourceConfig);
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        decoder.open();
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> message, Collector<CdcSourceRecord> out)
            throws IOException {
        if (message.value() == null) {
            // skip tombstone messages
            return;
        }
        String key =
                message.key() == null ? null : new String(message.key(), StandardCharsets.UTF_8);
        out.collect(
                new CdcSourceRecord(
                        message.topic(),
                        key,
                        decoder.decode(message.value()),
                        KafkaActionUtils.extractKafkaMetadata(message)));
    }

    @Override
    public TypeInformation<CdcSourceRecord> getProducedType() {
        return getForClass(CdcSourceRecord.class);
    }
}
