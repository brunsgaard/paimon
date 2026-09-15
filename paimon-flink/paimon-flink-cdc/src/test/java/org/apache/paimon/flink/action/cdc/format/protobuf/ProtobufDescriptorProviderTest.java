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

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.EVENT;
import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.descriptorSet;
import static org.apache.paimon.flink.action.cdc.format.protobuf.TestProtobufDescriptors.descriptorSetWithoutImports;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ProtobufDescriptorProvider}. */
public class ProtobufDescriptorProviderTest {

    @TempDir Path tempDir;

    @Test
    public void testLoadFromFile() throws Exception {
        Path file = tempDir.resolve("event.desc");
        Files.write(file, descriptorSet(false).toByteArray());

        ProtobufDescriptorProvider provider =
                new ProtobufDescriptorProvider(file.toUri().toString(), EVENT, Duration.ZERO);

        Descriptor descriptor = provider.descriptor();
        assertThat(descriptor.getFullName()).isEqualTo(EVENT);
        assertThat(descriptor.getFields()).hasSize(13);
        // Same instance while nothing changes and refresh is disabled.
        assertThat(provider.descriptor()).isSameAs(descriptor);
    }

    @Test
    public void testReloadWhenFileChanges() throws Exception {
        Path file = tempDir.resolve("event.desc");
        Files.write(file, descriptorSet(false).toByteArray());

        ProtobufDescriptorProvider provider =
                new ProtobufDescriptorProvider(
                        file.toUri().toString(), EVENT, Duration.ofMillis(1));
        Descriptor v1 = provider.descriptor();
        assertThat(v1.getFields()).hasSize(13);

        Thread.sleep(20);
        Files.write(file, descriptorSet(true).toByteArray());
        Thread.sleep(20);

        Descriptor v2 = provider.descriptor();
        assertThat(v2).isNotSameAs(v1);
        assertThat(v2.getFields()).hasSize(14);
        assertThat(v2.findFieldByName("region")).isNotNull();
        assertThat(v2.findFieldByName("address").getMessageType().findFieldByName("street"))
                .isNotNull();
    }

    @Test
    public void testWellKnownImportsComeFromRuntime() throws Exception {
        Descriptor descriptor =
                ProtobufDescriptorProvider.resolve(descriptorSetWithoutImports(false), EVENT);
        assertThat(descriptor.findFieldByName("ts").getMessageType().getFullName())
                .isEqualTo("google.protobuf.Timestamp");
    }

    @Test
    public void testMissingImportIsReported() {
        FileDescriptorSet set =
                FileDescriptorSet.newBuilder()
                        .addFile(
                                FileDescriptorProto.newBuilder()
                                        .setName("a.proto")
                                        .setSyntax("proto3")
                                        .addDependency("b/missing.proto"))
                        .build();

        assertThatThrownBy(() -> ProtobufDescriptorProvider.resolve(set, "whatever"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("b/missing.proto")
                .hasMessageContaining("--include_imports");
    }

    @Test
    public void testUnknownMessageListsAvailableOnes() {
        assertThatThrownBy(
                        () -> ProtobufDescriptorProvider.resolve(descriptorSet(false), "test.Nope"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("test.Nope")
                .hasMessageContaining("test.Event")
                .hasMessageContaining("test.Event.CountsEntry");
    }
}
