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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/** Options for the protobuf data format. */
public class ProtobufOptions {

    public static final ConfigOption<String> DESCRIPTOR_SET_PATH =
            ConfigOptions.key("protobuf.descriptor-set.path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Path to a serialized FileDescriptorSet, as written by "
                                    + "'protoc --descriptor_set_out --include_imports'. Any file system "
                                    + "supported by Flink can be used, for example 'file:///', 's3://' or 'gs://'.");

    public static final ConfigOption<String> MESSAGE_NAME =
            ConfigOptions.key("protobuf.message-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Fully qualified name of the message type that record values are encoded "
                                    + "with, for example 'com.example.events.Purchase'.");

    public static final ConfigOption<Duration> DESCRIPTOR_SET_REFRESH_INTERVAL =
            ConfigOptions.key("protobuf.descriptor-set.refresh-interval")
                    .durationType()
                    .defaultValue(Duration.ZERO)
                    .withDescription(
                            "How often to check the descriptor set for changes. When the file changes, "
                                    + "the new descriptor is used without restarting the job, so fields added "
                                    + "to the message appear as new table columns. Zero disables the check.");

    public static final ConfigOption<Boolean> READ_DEFAULT_VALUES =
            ConfigOptions.key("protobuf.read-default-values")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether fields that are not set in a message produce their protobuf "
                                    + "default value. When false, unset fields produce NULL.");

    public static final ConfigOption<Boolean> FLATTEN_NESTED_MESSAGES =
            ConfigOptions.key("protobuf.flatten-nested-messages")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether singular message fields become one column per leaf, named "
                                    + "'<field>_<leaf>', instead of a ROW column. Repeated and map "
                                    + "fields keep their ROW element type.");

    private ProtobufOptions() {}
}
