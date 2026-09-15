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

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.TimestampProto;

/**
 * Builds descriptor sets for tests without protoc. Two message families exist. {@code test.Event}
 * exercises every type mapping. {@code test.Simple} keeps end-to-end assertions short. Version 2 of
 * each adds fields, which is the producer-side change the format must absorb.
 *
 * <pre>
 * syntax = "proto3";
 * package test;
 * import "google/protobuf/timestamp.proto";
 *
 * enum Level { LOW = 0; HIGH = 1; }
 * message Address { string city = 1; string zip = 2; [v2: string street = 3;] }
 * message Event {
 *   int64 id = 1; string name = 2; double score = 3; bool active = 4; bytes payload = 5;
 *   Level level = 6; Address address = 7; repeated string tags = 8;
 *   map&lt;string, int32&gt; counts = 9; google.protobuf.Timestamp ts = 10; uint64 big = 11;
 *   optional int32 maybe = 12; repeated Address addresses = 13; [v2: string region = 14;]
 * }
 * message Node { string name = 1; Node child = 2; }
 * message Simple { int64 id = 1; string name = 2; [v2: string region = 3;] }
 * </pre>
 */
public class TestProtobufDescriptors {

    public static final String FILE_NAME = "test/event.proto";
    public static final String EVENT = "test.Event";
    public static final String NODE = "test.Node";
    public static final String SIMPLE = "test.Simple";

    public static FileDescriptorSet descriptorSet(boolean v2) {
        return FileDescriptorSet.newBuilder()
                .addFile(TimestampProto.getDescriptor().toProto())
                .addFile(file(v2))
                .build();
    }

    /** Same as {@link #descriptorSet} but without the imported google file. */
    public static FileDescriptorSet descriptorSetWithoutImports(boolean v2) {
        return FileDescriptorSet.newBuilder().addFile(file(v2)).build();
    }

    public static Descriptor descriptor(FileDescriptorSet set, String messageName) {
        try {
            return ProtobufDescriptorProvider.resolve(set, messageName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static FileDescriptorProto file(boolean v2) {
        DescriptorProto.Builder address =
                DescriptorProto.newBuilder()
                        .setName("Address")
                        .addField(scalar("city", 1, Type.TYPE_STRING))
                        .addField(scalar("zip", 2, Type.TYPE_STRING));
        if (v2) {
            address.addField(scalar("street", 3, Type.TYPE_STRING));
        }

        EnumDescriptorProto level =
                EnumDescriptorProto.newBuilder()
                        .setName("Level")
                        .addValue(EnumValueDescriptorProto.newBuilder().setName("LOW").setNumber(0))
                        .addValue(
                                EnumValueDescriptorProto.newBuilder().setName("HIGH").setNumber(1))
                        .build();

        DescriptorProto countsEntry =
                DescriptorProto.newBuilder()
                        .setName("CountsEntry")
                        .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                        .addField(scalar("key", 1, Type.TYPE_STRING))
                        .addField(scalar("value", 2, Type.TYPE_INT32))
                        .build();

        DescriptorProto.Builder event =
                DescriptorProto.newBuilder()
                        .setName("Event")
                        .addNestedType(countsEntry)
                        .addField(scalar("id", 1, Type.TYPE_INT64))
                        .addField(scalar("name", 2, Type.TYPE_STRING))
                        .addField(scalar("score", 3, Type.TYPE_DOUBLE))
                        .addField(scalar("active", 4, Type.TYPE_BOOL))
                        .addField(scalar("payload", 5, Type.TYPE_BYTES))
                        .addField(typed("level", 6, Type.TYPE_ENUM, ".test.Level", false))
                        .addField(typed("address", 7, Type.TYPE_MESSAGE, ".test.Address", false))
                        .addField(
                                scalar("tags", 8, Type.TYPE_STRING)
                                        .toBuilder()
                                        .setLabel(Label.LABEL_REPEATED)
                                        .build())
                        .addField(
                                typed(
                                        "counts",
                                        9,
                                        Type.TYPE_MESSAGE,
                                        ".test.Event.CountsEntry",
                                        true))
                        .addField(
                                typed(
                                        "ts",
                                        10,
                                        Type.TYPE_MESSAGE,
                                        ".google.protobuf.Timestamp",
                                        false))
                        .addField(scalar("big", 11, Type.TYPE_UINT64))
                        .addField(
                                scalar("maybe", 12, Type.TYPE_INT32)
                                        .toBuilder()
                                        .setProto3Optional(true)
                                        .setOneofIndex(0)
                                        .build())
                        .addOneofDecl(OneofDescriptorProto.newBuilder().setName("_maybe"))
                        .addField(typed("addresses", 13, Type.TYPE_MESSAGE, ".test.Address", true));
        if (v2) {
            event.addField(scalar("region", 14, Type.TYPE_STRING));
        }

        DescriptorProto node =
                DescriptorProto.newBuilder()
                        .setName("Node")
                        .addField(scalar("name", 1, Type.TYPE_STRING))
                        .addField(typed("child", 2, Type.TYPE_MESSAGE, ".test.Node", false))
                        .build();

        DescriptorProto.Builder simple =
                DescriptorProto.newBuilder()
                        .setName("Simple")
                        .addField(scalar("id", 1, Type.TYPE_INT64))
                        .addField(scalar("name", 2, Type.TYPE_STRING));
        if (v2) {
            simple.addField(scalar("region", 3, Type.TYPE_STRING));
        }

        return FileDescriptorProto.newBuilder()
                .setName(FILE_NAME)
                .setPackage("test")
                .setSyntax("proto3")
                .addDependency("google/protobuf/timestamp.proto")
                .addEnumType(level)
                .addMessageType(address)
                .addMessageType(event)
                .addMessageType(node)
                .addMessageType(simple)
                .build();
    }

    private static FieldDescriptorProto scalar(String name, int number, Type type) {
        return FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setType(type)
                .setLabel(Label.LABEL_OPTIONAL)
                .setJsonName(name)
                .build();
    }

    private static FieldDescriptorProto typed(
            String name, int number, Type type, String typeName, boolean repeated) {
        return scalar(name, number, type)
                .toBuilder()
                .setTypeName(typeName)
                .setLabel(repeated ? Label.LABEL_REPEATED : Label.LABEL_OPTIONAL)
                .build();
    }

    // ------------------------------------------------------------ sample messages

    /** A fully populated {@code test.Event}. */
    public static DynamicMessage fullEvent(Descriptor event) {
        Descriptor addressType = event.findFieldByName("address").getMessageType();
        Descriptor entryType = event.findFieldByName("counts").getMessageType();
        Descriptor timestampType = event.findFieldByName("ts").getMessageType();

        DynamicMessage address = address(addressType, "Copenhagen", "2100");
        DynamicMessage entry =
                DynamicMessage.newBuilder(entryType)
                        .setField(entryType.findFieldByName("key"), "x")
                        .setField(entryType.findFieldByName("value"), 1)
                        .build();
        // 2023-11-14T22:13:20.123456789Z
        DynamicMessage timestamp =
                DynamicMessage.newBuilder(timestampType)
                        .setField(timestampType.findFieldByName("seconds"), 1700000000L)
                        .setField(timestampType.findFieldByName("nanos"), 123456789)
                        .build();

        return DynamicMessage.newBuilder(event)
                .setField(event.findFieldByName("id"), 42L)
                .setField(event.findFieldByName("name"), "alice")
                .setField(event.findFieldByName("score"), 9.5d)
                .setField(event.findFieldByName("active"), true)
                .setField(event.findFieldByName("payload"), ByteString.copyFromUtf8("hi"))
                .setField(
                        event.findFieldByName("level"),
                        event.findFieldByName("level").getEnumType().findValueByName("HIGH"))
                .setField(event.findFieldByName("address"), address)
                .addRepeatedField(event.findFieldByName("tags"), "a")
                .addRepeatedField(event.findFieldByName("tags"), "b")
                .addRepeatedField(event.findFieldByName("counts"), entry)
                .setField(event.findFieldByName("ts"), timestamp)
                .setField(event.findFieldByName("big"), -1L) // 18446744073709551615 unsigned
                .setField(event.findFieldByName("maybe"), 7)
                .addRepeatedField(event.findFieldByName("addresses"), address)
                .addRepeatedField(
                        event.findFieldByName("addresses"), address(addressType, "Aarhus", "8000"))
                .build();
    }

    public static DynamicMessage address(Descriptor addressType, String city, String zip) {
        return DynamicMessage.newBuilder(addressType)
                .setField(addressType.findFieldByName("city"), city)
                .setField(addressType.findFieldByName("zip"), zip)
                .build();
    }

    /** A {@code test.Simple}; {@code region} is set only when the descriptor has it. */
    public static byte[] simple(Descriptor simple, long id, String name, String region) {
        DynamicMessage.Builder builder =
                DynamicMessage.newBuilder(simple)
                        .setField(simple.findFieldByName("id"), id)
                        .setField(simple.findFieldByName("name"), name);
        FieldDescriptor regionField = simple.findFieldByName("region");
        if (regionField != null && region != null) {
            builder.setField(regionField, region);
        }
        return builder.build().toByteArray();
    }
}
