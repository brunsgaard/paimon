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

import com.google.protobuf.AnyProto;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DurationProto;
import com.google.protobuf.EmptyProto;
import com.google.protobuf.FieldMaskProto;
import com.google.protobuf.StructProto;
import com.google.protobuf.TimestampProto;
import com.google.protobuf.WrappersProto;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Loads a message {@link Descriptor} from a serialized {@link FileDescriptorSet} and optionally
 * reloads it when the file changes.
 *
 * <p>The file is read through Flink's {@link FileSystem}, so any scheme the Flink cluster supports
 * works. A change is detected from the file status first, so an unchanged file costs one metadata
 * call per check and no download.
 */
public class ProtobufDescriptorProvider {

    private static final Logger LOG = LoggerFactory.getLogger(ProtobufDescriptorProvider.class);

    /** Well-known protos bundled with protobuf-java, used when a descriptor set omits them. */
    private static final Map<String, FileDescriptor> WELL_KNOWN_FILES =
            Stream.of(
                            TimestampProto.getDescriptor(),
                            DurationProto.getDescriptor(),
                            WrappersProto.getDescriptor(),
                            StructProto.getDescriptor(),
                            AnyProto.getDescriptor(),
                            EmptyProto.getDescriptor(),
                            FieldMaskProto.getDescriptor(),
                            DescriptorProtos.getDescriptor())
                    .collect(Collectors.toMap(FileDescriptor::getName, Function.identity()));

    private final Path path;
    private final String messageName;
    private final long refreshIntervalMillis;

    private volatile Descriptor descriptor;
    private long loadedModificationTime = -1;
    private long loadedLength = -1;
    private long lastCheckMillis;

    public ProtobufDescriptorProvider(String path, String messageName, Duration refreshInterval) {
        this.path = new Path(path);
        this.messageName = messageName;
        this.refreshIntervalMillis = refreshInterval.toMillis();
    }

    /** Returns the current descriptor, loading or refreshing it first when needed. */
    public Descriptor descriptor() throws IOException {
        Descriptor current = descriptor;
        if (current == null) {
            synchronized (this) {
                if (descriptor == null) {
                    load();
                }
                return descriptor;
            }
        }
        if (refreshIntervalMillis > 0
                && System.currentTimeMillis() - lastCheckMillis >= refreshIntervalMillis) {
            synchronized (this) {
                if (System.currentTimeMillis() - lastCheckMillis >= refreshIntervalMillis) {
                    reloadIfChanged();
                }
            }
        }
        return descriptor;
    }

    private void reloadIfChanged() throws IOException {
        lastCheckMillis = System.currentTimeMillis();
        FileStatus status = path.getFileSystem().getFileStatus(path);
        if (status.getModificationTime() == loadedModificationTime
                && status.getLen() == loadedLength) {
            return;
        }
        load();
    }

    private void load() throws IOException {
        FileSystem fileSystem = path.getFileSystem();
        FileStatus status = fileSystem.getFileStatus(path);
        byte[] bytes;
        try (FSDataInputStream in = fileSystem.open(path)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) != -1) {
                out.write(buffer, 0, read);
            }
            bytes = out.toByteArray();
        }
        FileDescriptorSet set = FileDescriptorSet.parseFrom(bytes);
        Descriptor resolved;
        try {
            resolved = resolve(set, messageName);
        } catch (DescriptorValidationException e) {
            throw new IOException("Invalid descriptor set at " + path, e);
        }
        descriptor = resolved;
        loadedModificationTime = status.getModificationTime();
        loadedLength = status.getLen();
        lastCheckMillis = System.currentTimeMillis();
        LOG.info(
                "Loaded protobuf descriptor for {} from {} ({} fields)",
                messageName,
                path,
                resolved.getFields().size());
    }

    /**
     * Builds the descriptor for {@code messageName} from a descriptor set, resolving imports
     * between the files it contains. Well-known google protos are supplied from the protobuf
     * runtime when the set does not include them.
     */
    public static Descriptor resolve(FileDescriptorSet set, String messageName)
            throws DescriptorValidationException {
        Map<String, FileDescriptorProto> protos = new HashMap<>();
        for (FileDescriptorProto proto : set.getFileList()) {
            protos.put(proto.getName(), proto);
        }
        Map<String, FileDescriptor> built = new HashMap<>();
        for (String name : protos.keySet()) {
            build(name, protos, built, new HashSet<>());
        }
        for (FileDescriptor file : built.values()) {
            Descriptor found = findMessage(file.getMessageTypes(), messageName);
            if (found != null) {
                return found;
            }
        }
        throw new IllegalArgumentException(
                String.format(
                        "Message '%s' not found in descriptor set. Available messages: %s",
                        messageName, messageNames(built.values())));
    }

    private static FileDescriptor build(
            String name,
            Map<String, FileDescriptorProto> protos,
            Map<String, FileDescriptor> built,
            Set<String> inProgress)
            throws DescriptorValidationException {
        FileDescriptor existing = built.get(name);
        if (existing != null) {
            return existing;
        }
        FileDescriptorProto proto = protos.get(name);
        if (proto == null) {
            FileDescriptor wellKnown = WELL_KNOWN_FILES.get(name);
            if (wellKnown != null) {
                return wellKnown;
            }
            throw new IllegalArgumentException(
                    String.format(
                            "Descriptor set does not contain '%s', which another file imports. "
                                    + "Generate the set with 'protoc --include_imports'.",
                            name));
        }
        if (!inProgress.add(name)) {
            throw new IllegalArgumentException("Cyclic import involving " + name);
        }
        FileDescriptor[] dependencies = new FileDescriptor[proto.getDependencyCount()];
        for (int i = 0; i < dependencies.length; i++) {
            dependencies[i] = build(proto.getDependency(i), protos, built, inProgress);
        }
        FileDescriptor file = FileDescriptor.buildFrom(proto, dependencies);
        built.put(name, file);
        inProgress.remove(name);
        return file;
    }

    private static Descriptor findMessage(List<Descriptor> messages, String fullName) {
        for (Descriptor message : messages) {
            if (message.getFullName().equals(fullName)) {
                return message;
            }
            Descriptor nested = findMessage(message.getNestedTypes(), fullName);
            if (nested != null) {
                return nested;
            }
        }
        return null;
    }

    private static List<String> messageNames(Iterable<FileDescriptor> files) {
        List<String> names = new java.util.ArrayList<>();
        for (FileDescriptor file : files) {
            collectNames(file.getMessageTypes(), names);
        }
        Collections.sort(names);
        return names;
    }

    private static void collectNames(List<Descriptor> messages, List<String> out) {
        for (Descriptor message : messages) {
            out.add(message.getFullName());
            collectNames(message.getNestedTypes(), out);
        }
    }
}
