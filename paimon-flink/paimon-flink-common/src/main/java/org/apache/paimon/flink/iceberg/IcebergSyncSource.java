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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSource;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.iceberg.IcebergSync;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Discovers the tables that match the patterns and emits one {@link IcebergSyncTask} per snapshot
 * that is not mirrored yet, in snapshot order per table. Bounded in batch mode; in streaming mode
 * it rediscovers the tables and polls for new snapshots every poll interval.
 */
public class IcebergSyncSource extends AbstractNonCoordinatedSource<IcebergSyncTask> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSyncSource.class);

    private final CatalogLoader catalogLoader;
    private final Pattern databasePattern;
    private final Pattern includingPattern;
    @Nullable private final Pattern excludingPattern;
    private final Map<String, String> tableOptions;
    private final boolean isStreaming;
    private final long pollIntervalMillis;

    public IcebergSyncSource(
            CatalogLoader catalogLoader,
            Pattern databasePattern,
            Pattern includingPattern,
            @Nullable Pattern excludingPattern,
            Map<String, String> tableOptions,
            boolean isStreaming,
            Duration pollInterval) {
        this.catalogLoader = catalogLoader;
        this.databasePattern = databasePattern;
        this.includingPattern = includingPattern;
        this.excludingPattern = excludingPattern;
        this.tableOptions = new HashMap<>(tableOptions);
        this.isStreaming = isStreaming;
        this.pollIntervalMillis = pollInterval.toMillis();
    }

    @Override
    public Boundedness getBoundedness() {
        return isStreaming ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<IcebergSyncTask, SimpleSourceSplit> createReader(
            SourceReaderContext context) {
        return new Reader();
    }

    /** The tables whose full name matches the including pattern and not the excluding one. */
    static List<Identifier> matchingTables(
            Catalog catalog,
            Pattern databasePattern,
            Pattern includingPattern,
            @Nullable Pattern excludingPattern)
            throws Catalog.DatabaseNotExistException {
        List<Identifier> matching = new ArrayList<>();
        for (String database : catalog.listDatabases()) {
            if (!databasePattern.matcher(database).matches()) {
                continue;
            }
            for (String table : catalog.listTables(database)) {
                String fullName = database + "." + table;
                if (includingPattern.matcher(fullName).matches()
                        && (excludingPattern == null
                                || !excludingPattern.matcher(fullName).matches())) {
                    matching.add(Identifier.create(database, table));
                }
            }
        }
        return matching;
    }

    private class Reader extends AbstractNonCoordinatedSourceReader<IcebergSyncTask> {

        private final Map<String, Long> lastEmitted = new HashMap<>();
        private final Deque<IcebergSyncTask> queue = new ArrayDeque<>();
        private transient Catalog catalog;
        private boolean passDone;

        @Override
        public void start() {
            catalog = catalogLoader.load();
        }

        @Override
        public InputStatus pollNext(ReaderOutput<IcebergSyncTask> output) throws Exception {
            if (queue.isEmpty()) {
                if (passDone && !isStreaming) {
                    return InputStatus.END_OF_INPUT;
                }
                if (passDone) {
                    Thread.sleep(pollIntervalMillis);
                }
                discover();
                passDone = true;
                if (queue.isEmpty()) {
                    return isStreaming ? InputStatus.NOTHING_AVAILABLE : InputStatus.END_OF_INPUT;
                }
            }
            output.collect(queue.poll());
            return InputStatus.MORE_AVAILABLE;
        }

        private void discover() throws Exception {
            for (Identifier id :
                    matchingTables(catalog, databasePattern, includingPattern, excludingPattern)) {
                Table table;
                try {
                    table = catalog.getTable(id);
                } catch (Catalog.TableNotExistException e) {
                    continue;
                }
                if (!(table instanceof FileStoreTable)) {
                    continue;
                }
                FileStoreTable original = (FileStoreTable) table;
                FileStoreTable mirrored;
                try {
                    IcebergSync.checkNotMirroredByWriters(original);
                    mirrored = IcebergSync.withMirrorDefaults(original, tableOptions);
                } catch (IllegalArgumentException e) {
                    if (!isStreaming) {
                        throw e;
                    }
                    LOG.error("{}", e.getMessage());
                    continue;
                }
                List<Long> pending = IcebergSync.pendingSnapshots(mirrored);
                long from = lastEmitted.getOrDefault(id.getFullName(), -1L);
                if (!pending.isEmpty() && pending.get(0) <= from) {
                    // a pending id at or below the mark means the table was rolled back or
                    // recreated; the mark belongs to the old timeline
                    from = -1;
                }
                for (long snapshotId : pending) {
                    if (snapshotId > from) {
                        queue.add(
                                new IcebergSyncTask(
                                        id.getDatabaseName(), id.getObjectName(), snapshotId));
                        lastEmitted.put(id.getFullName(), snapshotId);
                    }
                }
            }
        }

        @Override
        public void close() throws Exception {
            if (catalog != null) {
                catalog.close();
            }
        }
    }
}
