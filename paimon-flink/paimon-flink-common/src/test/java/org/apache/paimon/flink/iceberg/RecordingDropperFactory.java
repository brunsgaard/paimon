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

package org.apache.paimon.flink.iceberg;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.iceberg.IcebergMetadataCommitter;
import org.apache.paimon.iceberg.IcebergMetadataCommitterFactory;
import org.apache.paimon.iceberg.IcebergMirrorDropper;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Records the drops it is asked for. Registered with the identifier {@code recording}. While {@link
 * #failing} is set, a drop throws and is not recorded. {@link #exists} is the answer of the
 * existence check; null makes the check throw.
 */
public class RecordingDropperFactory implements IcebergMetadataCommitterFactory {

    static final List<String> drops = new CopyOnWriteArrayList<>();
    static final AtomicBoolean failing = new AtomicBoolean(false);
    static final AtomicReference<Boolean> exists = new AtomicReference<>(true);

    @Override
    public String identifier() {
        return "recording";
    }

    @Override
    public IcebergMetadataCommitter create(FileStoreTable table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IcebergMirrorDropper createDropper(Options options, Identifier table) {
        return new IcebergMirrorDropper() {
            @Override
            public void drop() {
                if (failing.get()) {
                    throw new IllegalStateException("The catalog is not reachable.");
                }
                drops.add(table.getFullName());
            }

            @Override
            public boolean exists() {
                Boolean answer = exists.get();
                if (answer == null) {
                    throw new IllegalStateException("The catalog cannot tell.");
                }
                return answer;
            }
        };
    }
}
