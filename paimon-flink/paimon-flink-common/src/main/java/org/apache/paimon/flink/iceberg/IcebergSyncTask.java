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

import java.io.Serializable;
import java.util.Objects;

/** One snapshot of one table for {@code iceberg_sync} to mirror. */
public class IcebergSyncTask implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String database;
    public final String table;
    public final long snapshotId;

    public IcebergSyncTask(String database, String table, long snapshotId) {
        this.database = database;
        this.table = table;
        this.snapshotId = snapshotId;
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
