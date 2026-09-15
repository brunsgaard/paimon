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
import org.apache.paimon.flink.action.cdc.format.AbstractDataFormat;
import org.apache.paimon.flink.action.cdc.format.RecordParserFactory;
import org.apache.paimon.flink.action.cdc.kafka.KafkaProtobufDeserializationSchema;
import org.apache.paimon.flink.action.cdc.pulsar.PulsarProtobufDeserializationSchema;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

import java.util.function.Function;

/**
 * Protobuf-encoded records decoded with a {@code FileDescriptorSet}, treated as inserts. Supports
 * the Kafka and Pulsar sources.
 */
public class ProtobufDataFormat extends AbstractDataFormat {

    @Override
    protected RecordParserFactory parser() {
        return ProtobufRecordParser::new;
    }

    @Override
    protected Function<Configuration, KafkaRecordDeserializationSchema<CdcSourceRecord>>
            kafkaDeserializer() {
        return KafkaProtobufDeserializationSchema::new;
    }

    @Override
    protected Function<Configuration, DeserializationSchema<CdcSourceRecord>> pulsarDeserializer() {
        return PulsarProtobufDeserializationSchema::new;
    }

    /** The schema comes from the descriptor, so a missing field means the producer removed it. */
    @Override
    public boolean providesCompleteSchema() {
        return true;
    }
}
