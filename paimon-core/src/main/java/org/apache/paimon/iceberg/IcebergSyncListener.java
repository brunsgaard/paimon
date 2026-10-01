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

package org.apache.paimon.iceberg;

import org.apache.paimon.catalog.Identifier;

import java.util.Map;

/**
 * Hears what the {@code iceberg_sync} job did. An implementation is found with {@link
 * java.util.ServiceLoader} through the user code class loader of the job, so it and its
 * dependencies can live outside the Paimon jars.
 *
 * <p>The job calls a listener on the thread that mirrors the table. A call that throws is logged
 * and counted, and never changes the outcome of a sync. A listener that throws from {@link
 * #open(Map)} is not called again.
 */
public interface IcebergSyncListener extends AutoCloseable {

    /**
     * Once, with the job's configuration as key-value pairs; the mirror options are under their own
     * keys.
     */
    void open(Map<String, String> configuration);

    /**
     * After a snapshot of a table reached its mirror. {@code snapshotTimestampMs} is -1 when the
     * job cannot read it. {@code icebergDatabase} and {@code icebergTable} name the Iceberg table.
     */
    void onSynced(
            Identifier table,
            long snapshotId,
            long snapshotTimestampMs,
            String icebergDatabase,
            String icebergTable);

    /** After the mirror of a dropped table was dropped. */
    void onDropped(Identifier table, String icebergDatabase, String icebergTable);

    @Override
    default void close() throws Exception {}
}
