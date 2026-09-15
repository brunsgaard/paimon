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

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.SourceCodeInfo;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Looks up the documentation comment of a protobuf field from the {@code source_code_info} of its
 * file. Present when the descriptor set was written with {@code protoc --include_source_info};
 * otherwise every lookup returns null.
 */
class ProtobufComments {

    private final Map<FileDescriptor, Map<List<Integer>, String>> byFile = new HashMap<>();

    /** Returns the leading comment of the field, falling back to its trailing comment. */
    @Nullable
    String commentOf(FieldDescriptor field) {
        Map<List<Integer>, String> comments =
                byFile.computeIfAbsent(field.getFile(), ProtobufComments::index);
        if (comments.isEmpty()) {
            return null;
        }
        List<Integer> path = new ArrayList<>(messagePath(field.getContainingType()));
        path.add(DescriptorProtos.DescriptorProto.FIELD_FIELD_NUMBER);
        path.add(field.getIndex());
        return comments.get(path);
    }

    private static Map<List<Integer>, String> index(FileDescriptor file) {
        FileDescriptorProto proto = file.toProto();
        if (!proto.hasSourceCodeInfo()) {
            return Collections.emptyMap();
        }
        Map<List<Integer>, String> comments = new HashMap<>();
        for (SourceCodeInfo.Location location : proto.getSourceCodeInfo().getLocationList()) {
            String comment =
                    !location.getLeadingComments().isEmpty()
                            ? location.getLeadingComments()
                            : location.getTrailingComments();
            if (!comment.isEmpty()) {
                comments.put(location.getPathList(), comment);
            }
        }
        return comments;
    }

    private static List<Integer> messagePath(Descriptor message) {
        List<Integer> path;
        if (message.getContainingType() == null) {
            path = new ArrayList<>();
            path.add(FileDescriptorProto.MESSAGE_TYPE_FIELD_NUMBER);
        } else {
            path = new ArrayList<>(messagePath(message.getContainingType()));
            path.add(DescriptorProtos.DescriptorProto.NESTED_TYPE_FIELD_NUMBER);
        }
        path.add(message.getIndex());
        return path;
    }
}
