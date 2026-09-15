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

package org.apache.paimon.flink.action.cdc.pulsar;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.format.protobuf.ProtobufSourceRecordDecoder;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/** Deserializes protobuf-encoded Pulsar message values with a descriptor set. */
public class PulsarProtobufDeserializationSchema implements DeserializationSchema<CdcSourceRecord> {

    private static final long serialVersionUID = 1L;

    private final String topic;
    private final ProtobufSourceRecordDecoder decoder;

    public PulsarProtobufDeserializationSchema(Configuration cdcSourceConfig) {
        this.topic = PulsarActionUtils.findOneTopic(cdcSourceConfig);
        this.decoder = new ProtobufSourceRecordDecoder(cdcSourceConfig);
    }

    @Override
    public void open(InitializationContext context) {
        decoder.open();
    }

    @Override
    public CdcSourceRecord deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        return new CdcSourceRecord(topic, null, decoder.decode(message));
    }

    @Override
    public boolean isEndOfStream(CdcSourceRecord nextElement) {
        return false;
    }

    @Override
    public TypeInformation<CdcSourceRecord> getProducedType() {
        return getForClass(CdcSourceRecord.class);
    }
}
