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

import org.apache.paimon.iceberg.IcebergOptions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** One snapshot of one table for {@code write_iceberg_metadata} to mirror. */
public class IcebergSyncTask implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String database;
    public final String table;
    public final long snapshotId;

    /** The snapshot id of a drop task: the table is gone from the catalog, drop its mirror. */
    public static final long DROP = -1L;

    /** The table options that name the Iceberg table of a table. */
    public static final List<String> MIRROR_NAMING_KEYS =
            Collections.unmodifiableList(
                    Arrays.asList(
                            IcebergOptions.METASTORE_DATABASE.key(),
                            IcebergOptions.METASTORE_TABLE.key()));

    /** The values of {@link #MIRROR_NAMING_KEYS} the table had; empty for a sync task. */
    public final Map<String, String> mirrorNaming;

    public IcebergSyncTask(String database, String table, long snapshotId) {
        this(database, table, snapshotId, Collections.emptyMap());
    }

    private IcebergSyncTask(
            String database, String table, long snapshotId, Map<String, String> mirrorNaming) {
        this.database = database;
        this.table = table;
        this.snapshotId = snapshotId;
        this.mirrorNaming = new HashMap<>(mirrorNaming);
    }

    public static IcebergSyncTask drop(String database, String table) {
        return drop(database, table, Collections.emptyMap());
    }

    public static IcebergSyncTask drop(
            String database, String table, Map<String, String> mirrorNaming) {
        return new IcebergSyncTask(database, table, DROP, mirrorNaming);
    }

    public boolean isDrop() {
        return snapshotId == DROP;
    }

    public String fullName() {
        return database + "." + table;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof IcebergSyncTask)) {
            return false;
        }
        IcebergSyncTask that = (IcebergSyncTask) o;
        return snapshotId == that.snapshotId
                && database.equals(that.database)
                && table.equals(that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, table, snapshotId);
    }

    @Override
    public String toString() {
        return fullName() + "@" + snapshotId;
    }
}
