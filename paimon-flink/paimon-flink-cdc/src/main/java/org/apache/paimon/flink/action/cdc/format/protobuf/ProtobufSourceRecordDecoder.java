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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;

import static org.apache.paimon.flink.action.cdc.format.protobuf.ProtobufOptions.DESCRIPTOR_SET_PATH;
import static org.apache.paimon.flink.action.cdc.format.protobuf.ProtobufOptions.DESCRIPTOR_SET_REFRESH_INTERVAL;
import static org.apache.paimon.flink.action.cdc.format.protobuf.ProtobufOptions.MESSAGE_NAME;
import static org.apache.paimon.flink.action.cdc.format.protobuf.ProtobufOptions.READ_DEFAULT_VALUES;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Decodes protobuf bytes into {@link ProtobufSourceRecord}s. Shared by the Kafka and Pulsar
 * deserialization schemas. Holds only configuration until first use, so it is safe to ship to task
 * managers.
 */
public class ProtobufSourceRecordDecoder implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String descriptorSetPath;
    private final String messageName;
    private final Duration refreshInterval;
    private final boolean readDefaultValues;

    private transient ProtobufDescriptorProvider descriptorProvider;
    private transient ProtobufSchemaConverter converter;
    private transient Descriptor fieldsDescriptor;
    private transient List<DataField> fields;

    public ProtobufSourceRecordDecoder(Configuration config) {
        this.descriptorSetPath = required(config, DESCRIPTOR_SET_PATH);
        this.messageName = required(config, MESSAGE_NAME);
        this.refreshInterval = config.get(DESCRIPTOR_SET_REFRESH_INTERVAL);
        this.readDefaultValues = config.get(READ_DEFAULT_VALUES);
    }

    private static String required(Configuration config, ConfigOption<String> option) {
        String value = config.getOptional(option).map(String::trim).orElse("");
        checkArgument(
                !value.isEmpty(),
                String.format("Option '%s' is required for value.format=protobuf.", option.key()));
        return value;
    }

    public void open() {
        descriptorProvider =
                new ProtobufDescriptorProvider(descriptorSetPath, messageName, refreshInterval);
        converter = new ProtobufSchemaConverter(readDefaultValues);
    }

    public ProtobufSourceRecord decode(byte[] bytes) throws IOException {
        if (descriptorProvider == null) {
            open();
        }
        Descriptor descriptor = descriptorProvider.descriptor();
        DynamicMessage message = DynamicMessage.parseFrom(descriptor, bytes);
        return new ProtobufSourceRecord(fieldsFor(descriptor), converter.toValues(message));
    }

    /** Field derivation is cached per descriptor instance, so a reload is what invalidates it. */
    private List<DataField> fieldsFor(Descriptor descriptor) {
        if (descriptor != fieldsDescriptor) {
            fields = converter.toFields(descriptor);
            fieldsDescriptor = descriptor;
        }
        return fields;
    }
}
