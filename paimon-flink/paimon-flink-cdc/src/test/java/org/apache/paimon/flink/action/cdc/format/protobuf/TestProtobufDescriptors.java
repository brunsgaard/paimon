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
import com.google.protobuf.DescriptorProtos.SourceCodeInfo;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DurationProto;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.EmptyProto;
import com.google.protobuf.FieldMaskProto;
import com.google.protobuf.StructProto;
import com.google.protobuf.TimestampProto;
import com.google.protobuf.WrappersProto;

import java.util.Arrays;

/**
 * Builds descriptor sets for tests without protoc. {@code test.Event} exercises every type mapping,
 * {@code test.Simple} keeps end-to-end assertions short and {@code test.Nested} carries a
 * message-typed field. {@link Variant} models the producer-side changes the format must absorb.
 *
 * <pre>
 * syntax = "proto3";
 * package test;
 * import "google/protobuf/timestamp.proto";
 *
 * enum Level { LOW = 0; HIGH = 1; }
 * message Address { string city = 1; string zip = 2; [V2_ADDED: string street = 3;] }
 * message Event {
 *   int64 id = 1; string name = 2; double score = 3; bool active = 4; bytes payload = 5;
 *   Level level = 6; Address address = 7; repeated string tags = 8;
 *   map&lt;string, int32&gt; counts = 9; google.protobuf.Timestamp ts = 10; uint64 big = 11;
 *   optional int32 maybe = 12; repeated Address addresses = 13; [V2_ADDED: string region = 14;]
 * }
 * message Node { string name = 1; Node child = 2; }
 * message Simple { int64 id = 1; string name = 2; [V2_ADDED: string region = 3;] }
 * message Nested { int64 id = 1; Address address = 2; }
 * message WellKnown {
 *   google.protobuf.Duration took = 1; google.protobuf.Struct attrs = 2;
 *   google.protobuf.Value any_value = 3; google.protobuf.ListValue items = 4;
 *   google.protobuf.Empty marker = 5; google.protobuf.FieldMask mask = 6;
 *   google.protobuf.StringValue label = 7;
 * }
 * </pre>
 *
 * <p>The file carries {@code source_code_info} with leading comments on {@code Simple.name}, {@code
 * Address.city} and {@code Event.address}, as {@code protoc --include_source_info} writes.
 */
public class TestProtobufDescriptors {

    public static final String FILE_NAME = "test/event.proto";
    public static final String EVENT = "test.Event";
    public static final String NODE = "test.Node";
    public static final String SIMPLE = "test.Simple";
    public static final String NESTED = "test.Nested";
    public static final String WELL_KNOWN = "test.WellKnown";

    public static final String SIMPLE_NAME_COMMENT = "Display name.";
    public static final String ADDRESS_CITY_COMMENT = "City name.";
    public static final String EVENT_ADDRESS_COMMENT = "Where it happened.";

    /** Producer-side changes relative to {@link #V1}. */
    public enum Variant {
        /** The original schema. */
        V1,
        /** Adds {@code Address.street}, {@code Event.region} and {@code Simple.region}. */
        V2_ADDED,
        /**
         * Renames {@code Simple.name} to {@code title} and {@code Address.zip} to {@code postal}.
         */
        V2_RENAMED,
        /** Removes {@code Simple.name} and {@code Address.zip}. */
        V2_DROPPED,
        /** Changes the comment of {@code Simple.name}. */
        V2_COMMENT
    }

    private static final int ADDRESS_INDEX = 0;
    private static final int EVENT_INDEX = 1;
    private static final int SIMPLE_INDEX = 3;

    public static FileDescriptorSet descriptorSet(boolean v2) {
        return descriptorSet(v2 ? Variant.V2_ADDED : Variant.V1);
    }

    public static FileDescriptorSet descriptorSet(Variant variant) {
        return FileDescriptorSet.newBuilder()
                .addFile(TimestampProto.getDescriptor().toProto())
                .addFile(DurationProto.getDescriptor().toProto())
                .addFile(StructProto.getDescriptor().toProto())
                .addFile(EmptyProto.getDescriptor().toProto())
                .addFile(FieldMaskProto.getDescriptor().toProto())
                .addFile(WrappersProto.getDescriptor().toProto())
                .addFile(file(variant))
                .build();
    }

    /** Same as {@link #descriptorSet} but without the imported google file. */
    public static FileDescriptorSet descriptorSetWithoutImports(boolean v2) {
        return FileDescriptorSet.newBuilder()
                .addFile(file(v2 ? Variant.V2_ADDED : Variant.V1))
                .build();
    }

    public static Descriptor descriptor(FileDescriptorSet set, String messageName) {
        try {
            return ProtobufDescriptorProvider.resolve(set, messageName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static FileDescriptorProto file(Variant variant) {
        DescriptorProto.Builder address =
                DescriptorProto.newBuilder()
                        .setName("Address")
                        .addField(scalar("city", 1, Type.TYPE_STRING));
        switch (variant) {
            case V2_RENAMED:
                address.addField(scalar("postal", 2, Type.TYPE_STRING));
                break;
            case V2_DROPPED:
                break;
            default:
                address.addField(scalar("zip", 2, Type.TYPE_STRING));
        }
        if (variant == Variant.V2_ADDED) {
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
        if (variant == Variant.V2_ADDED) {
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
                        .addField(scalar("id", 1, Type.TYPE_INT64));
        switch (variant) {
            case V2_RENAMED:
                simple.addField(scalar("title", 2, Type.TYPE_STRING));
                break;
            case V2_DROPPED:
                break;
            default:
                simple.addField(scalar("name", 2, Type.TYPE_STRING));
        }
        if (variant == Variant.V2_ADDED) {
            simple.addField(scalar("region", 3, Type.TYPE_STRING));
        }

        DescriptorProto nested =
                DescriptorProto.newBuilder()
                        .setName("Nested")
                        .addField(scalar("id", 1, Type.TYPE_INT64))
                        .addField(typed("address", 2, Type.TYPE_MESSAGE, ".test.Address", false))
                        .build();

        DescriptorProto wellKnown =
                DescriptorProto.newBuilder()
                        .setName("WellKnown")
                        .addField(
                                typed(
                                        "took",
                                        1,
                                        Type.TYPE_MESSAGE,
                                        ".google.protobuf.Duration",
                                        false))
                        .addField(
                                typed(
                                        "attrs",
                                        2,
                                        Type.TYPE_MESSAGE,
                                        ".google.protobuf.Struct",
                                        false))
                        .addField(
                                typed(
                                        "any_value",
                                        3,
                                        Type.TYPE_MESSAGE,
                                        ".google.protobuf.Value",
                                        false))
                        .addField(
                                typed(
                                        "items",
                                        4,
                                        Type.TYPE_MESSAGE,
                                        ".google.protobuf.ListValue",
                                        false))
                        .addField(
                                typed(
                                        "marker",
                                        5,
                                        Type.TYPE_MESSAGE,
                                        ".google.protobuf.Empty",
                                        false))
                        .addField(
                                typed(
                                        "mask",
                                        6,
                                        Type.TYPE_MESSAGE,
                                        ".google.protobuf.FieldMask",
                                        false))
                        .addField(
                                typed(
                                        "label",
                                        7,
                                        Type.TYPE_MESSAGE,
                                        ".google.protobuf.StringValue",
                                        false))
                        .build();

        SourceCodeInfo.Builder sourceInfo = SourceCodeInfo.newBuilder();
        if (variant != Variant.V2_DROPPED) {
            // Simple.name / Simple.title is the second field of the fourth message
            sourceInfo.addLocation(
                    location(
                            variant == Variant.V2_COMMENT
                                    ? "Display name, shown to the user."
                                    : SIMPLE_NAME_COMMENT,
                            FileDescriptorProto.MESSAGE_TYPE_FIELD_NUMBER,
                            SIMPLE_INDEX,
                            DescriptorProto.FIELD_FIELD_NUMBER,
                            1));
        }
        sourceInfo.addLocation(
                location(
                        ADDRESS_CITY_COMMENT,
                        FileDescriptorProto.MESSAGE_TYPE_FIELD_NUMBER,
                        ADDRESS_INDEX,
                        DescriptorProto.FIELD_FIELD_NUMBER,
                        0));
        sourceInfo.addLocation(
                location(
                        EVENT_ADDRESS_COMMENT,
                        FileDescriptorProto.MESSAGE_TYPE_FIELD_NUMBER,
                        EVENT_INDEX,
                        DescriptorProto.FIELD_FIELD_NUMBER,
                        6));

        return FileDescriptorProto.newBuilder()
                .setName(FILE_NAME)
                .setPackage("test")
                .setSyntax("proto3")
                .addDependency("google/protobuf/timestamp.proto")
                .addDependency("google/protobuf/duration.proto")
                .addDependency("google/protobuf/struct.proto")
                .addDependency("google/protobuf/empty.proto")
                .addDependency("google/protobuf/field_mask.proto")
                .addDependency("google/protobuf/wrappers.proto")
                .addEnumType(level)
                .addMessageType(address)
                .addMessageType(event)
                .addMessageType(node)
                .addMessageType(simple)
                .addMessageType(nested)
                .addMessageType(wellKnown)
                .setSourceCodeInfo(sourceInfo)
                .build();
    }

    private static SourceCodeInfo.Location location(String leadingComment, Integer... path) {
        return SourceCodeInfo.Location.newBuilder()
                .addAllPath(Arrays.asList(path))
                .setLeadingComments(" " + leadingComment + "\n")
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

    /**
     * An {@code Address}; the second field is {@code zip} or {@code postal} depending on variant.
     */
    public static DynamicMessage address(Descriptor addressType, String city, String second) {
        DynamicMessage.Builder builder =
                DynamicMessage.newBuilder(addressType)
                        .setField(addressType.findFieldByName("city"), city);
        FieldDescriptor secondField = addressType.findFieldByNumber(2);
        if (secondField != null && second != null) {
            builder.setField(secondField, second);
        }
        return builder.build();
    }

    /**
     * A {@code test.Simple}. Field 2 is {@code name} or {@code title} depending on the variant and
     * may be absent; {@code region} is set only when the descriptor has it.
     */
    public static byte[] simple(Descriptor simple, long id, String name, String region) {
        DynamicMessage.Builder builder =
                DynamicMessage.newBuilder(simple).setField(simple.findFieldByNumber(1), id);
        FieldDescriptor second = simple.findFieldByNumber(2);
        if (second != null && name != null) {
            builder.setField(second, name);
        }
        FieldDescriptor regionField = simple.findFieldByNumber(3);
        if (regionField != null && region != null) {
            builder.setField(regionField, region);
        }
        return builder.build().toByteArray();
    }

    /**
     * A {@code test.WellKnown} with every field set: 1.5 s, {@code {"a": 1, "b": [true, null]}},
     * the string value {@code "x"}, the list {@code [2.5]}, an empty marker, mask {@code a.b,c} and
     * label {@code hello}.
     */
    public static DynamicMessage wellKnown(Descriptor wellKnown) {
        Descriptor durationType = wellKnown.findFieldByName("took").getMessageType();
        Descriptor structType = wellKnown.findFieldByName("attrs").getMessageType();
        Descriptor valueType = wellKnown.findFieldByName("any_value").getMessageType();
        Descriptor listType = wellKnown.findFieldByName("items").getMessageType();
        Descriptor emptyType = wellKnown.findFieldByName("marker").getMessageType();
        Descriptor maskType = wellKnown.findFieldByName("mask").getMessageType();
        Descriptor stringValueType = wellKnown.findFieldByName("label").getMessageType();
        Descriptor entryType = structType.findFieldByName("fields").getMessageType();

        DynamicMessage one =
                DynamicMessage.newBuilder(valueType)
                        .setField(valueType.findFieldByName("number_value"), 1d)
                        .build();
        DynamicMessage yes =
                DynamicMessage.newBuilder(valueType)
                        .setField(valueType.findFieldByName("bool_value"), true)
                        .build();
        DynamicMessage nothing =
                DynamicMessage.newBuilder(valueType)
                        .setField(
                                valueType.findFieldByName("null_value"),
                                valueType
                                        .findFieldByName("null_value")
                                        .getEnumType()
                                        .findValueByNumber(0))
                        .build();
        DynamicMessage bList =
                DynamicMessage.newBuilder(listType)
                        .addRepeatedField(listType.findFieldByName("values"), yes)
                        .addRepeatedField(listType.findFieldByName("values"), nothing)
                        .build();
        DynamicMessage b =
                DynamicMessage.newBuilder(valueType)
                        .setField(valueType.findFieldByName("list_value"), bList)
                        .build();
        DynamicMessage struct =
                DynamicMessage.newBuilder(structType)
                        .addRepeatedField(
                                structType.findFieldByName("fields"), entry(entryType, "a", one))
                        .addRepeatedField(
                                structType.findFieldByName("fields"), entry(entryType, "b", b))
                        .build();
        DynamicMessage items =
                DynamicMessage.newBuilder(listType)
                        .addRepeatedField(
                                listType.findFieldByName("values"),
                                DynamicMessage.newBuilder(valueType)
                                        .setField(valueType.findFieldByName("number_value"), 2.5d)
                                        .build())
                        .build();
        return DynamicMessage.newBuilder(wellKnown)
                .setField(
                        wellKnown.findFieldByName("took"),
                        DynamicMessage.newBuilder(durationType)
                                .setField(durationType.findFieldByName("seconds"), 1L)
                                .setField(durationType.findFieldByName("nanos"), 500000000)
                                .build())
                .setField(wellKnown.findFieldByName("attrs"), struct)
                .setField(
                        wellKnown.findFieldByName("any_value"),
                        DynamicMessage.newBuilder(valueType)
                                .setField(valueType.findFieldByName("string_value"), "x")
                                .build())
                .setField(wellKnown.findFieldByName("items"), items)
                .setField(
                        wellKnown.findFieldByName("marker"),
                        DynamicMessage.newBuilder(emptyType).build())
                .setField(
                        wellKnown.findFieldByName("mask"),
                        DynamicMessage.newBuilder(maskType)
                                .addRepeatedField(maskType.findFieldByName("paths"), "a.b")
                                .addRepeatedField(maskType.findFieldByName("paths"), "c")
                                .build())
                .setField(
                        wellKnown.findFieldByName("label"),
                        DynamicMessage.newBuilder(stringValueType)
                                .setField(stringValueType.findFieldByName("value"), "hello")
                                .build())
                .build();
    }

    private static DynamicMessage entry(Descriptor entryType, String key, DynamicMessage value) {
        return DynamicMessage.newBuilder(entryType)
                .setField(entryType.findFieldByName("key"), key)
                .setField(entryType.findFieldByName("value"), value)
                .build();
    }

    /** A {@code test.Nested} with an embedded address. */
    public static byte[] nested(Descriptor nested, long id, String city, String second) {
        Descriptor addressType = nested.findFieldByNumber(2).getMessageType();
        return DynamicMessage.newBuilder(nested)
                .setField(nested.findFieldByNumber(1), id)
                .setField(nested.findFieldByNumber(2), address(addressType, city, second))
                .build()
                .toByteArray();
    }
}
