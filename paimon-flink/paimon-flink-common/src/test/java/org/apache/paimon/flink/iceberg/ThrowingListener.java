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
import org.apache.paimon.iceberg.IcebergSyncListener;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fails on request. While {@link #armed} is set, {@code onSynced} throws once. While {@link
 * #linkageError} is set, {@code onDropped} throws a {@link NoClassDefFoundError}. While {@link
 * #openFails} is set, {@code open} throws. {@link #calls} counts the calls after {@code open}.
 */
public class ThrowingListener implements IcebergSyncListener {

    static volatile boolean armed;
    static volatile boolean linkageError;
    static volatile boolean openFails;
    static final AtomicInteger calls = new AtomicInteger();

    @Override
    public void open(Map<String, String> configuration) {
        if (openFails) {
            throw new IllegalStateException("The listener cannot open.");
        }
    }

    @Override
    public void onSynced(
            Identifier table,
            long snapshotId,
            long snapshotTimestampMs,
            String icebergDatabase,
            String icebergTable) {
        calls.incrementAndGet();
        try {
            if (armed) {
                throw new IllegalStateException("The listener fails.");
            }
        } finally {
            armed = false;
        }
    }

    @Override
    public void onDropped(Identifier table, String icebergDatabase, String icebergTable) {
        calls.incrementAndGet();
        if (linkageError) {
            throw new NoClassDefFoundError("Missing.");
        }
    }
}
