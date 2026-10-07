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
import org.apache.paimon.utils.SerializableFunction;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    private final List<Map.Entry<String, String>> optionFilters;
    private final Map<String, String> tableOptions;
    private final boolean isStreaming;
    private final long pollIntervalMillis;

    public IcebergSyncSource(
            CatalogLoader catalogLoader,
            Pattern databasePattern,
            Pattern includingPattern,
            @Nullable Pattern excludingPattern,
            List<String> optionFilters,
            Map<String, String> tableOptions,
            boolean isStreaming,
            Duration pollInterval) {
        this.catalogLoader = catalogLoader;
        this.databasePattern = databasePattern;
        this.includingPattern = includingPattern;
        this.excludingPattern = excludingPattern;
        this.optionFilters = new ArrayList<>();
        for (String filter : optionFilters) {
            this.optionFilters.add(parseFilter(filter));
        }
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
        return newReader(context.metricGroup());
    }

    /** For tests: how the latest snapshot id is read for the gauge when nothing is pending. */
    SerializableFunction<FileStoreTable, Long> latestSnapshotReader =
            table -> table.snapshotManager().latestSnapshotId();

    Reader newReader() {
        return newReader(null);
    }

    Reader newReader(@Nullable MetricGroup metricGroup) {
        return new Reader(metricGroup);
    }

    /** Consecutive polls a table must be missing from the catalog before its mirror is dropped. */
    static final int MISSES_BEFORE_DROP = 2;

    /** {@code key=value}; the first {@code =} splits, so a value may hold one. */
    public static Map.Entry<String, String> parseFilter(String filter) {
        int eq = filter.indexOf('=');
        if (eq <= 0 || eq == filter.length() - 1) {
            throw new IllegalArgumentException(
                    "--table_option_filter needs key=value, got '" + filter + "'");
        }
        return new java.util.AbstractMap.SimpleImmutableEntry<>(
                filter.substring(0, eq).trim(), filter.substring(eq + 1).trim());
    }

    /**
     * No filter selects every matching table; otherwise any filter whose key holds its value
     * selects.
     */
    static boolean selected(Map<String, String> options, List<Map.Entry<String, String>> filters) {
        if (filters.isEmpty()) {
            return true;
        }
        for (Map.Entry<String, String> f : filters) {
            if (f.getValue().equals(options.get(f.getKey()))) {
                return true;
            }
        }
        return false;
    }

    /**
     * The tables whose full name matches the including pattern and not the excluding one. When
     * {@code unlisted} is not null, a database whose listing fails is added to it and skipped;
     * otherwise the failure is thrown.
     */
    static List<Identifier> matchingTables(
            Catalog catalog,
            Pattern databasePattern,
            Pattern includingPattern,
            @Nullable Pattern excludingPattern,
            @Nullable Set<String> unlisted)
            throws Exception {
        List<Identifier> matching = new ArrayList<>();
        for (String database : catalog.listDatabases()) {
            if (!databasePattern.matcher(database).matches()) {
                continue;
            }
            List<String> tables;
            try {
                tables = catalog.listTables(database);
            } catch (Exception e) {
                if (unlisted == null) {
                    throw e;
                }
                LOG.warn(
                        "Listing database {} failed; its tables are skipped in this poll: {}",
                        database,
                        e.toString());
                LOG.debug("Database {}: the listing failure.", database, e);
                unlisted.add(database);
                continue;
            }
            for (String table : tables) {
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

    class Reader extends AbstractNonCoordinatedSourceReader<IcebergSyncTask> {

        private final Map<String, Long> lastEmitted = new HashMap<>();
        private final Map<String, String> uuids = new HashMap<>();
        /** The options that name the Iceberg table of each table seen: full name to options. */
        private final Map<String, Map<String, String>> mirrorNamings = new HashMap<>();
        /** Tables seen on an earlier poll and missing since: full name to consecutive misses. */
        private final Map<String, Integer> misses = new HashMap<>();

        private final Deque<IcebergSyncTask> queue = new ArrayDeque<>();
        private transient Catalog catalog;
        private boolean passDone;

        @Nullable private final MetricGroup metricGroup;
        @Nullable private final Counter polls;
        @Nullable private final Counter failedPolls;
        @Nullable private final Counter tableFailures;
        private final Map<String, MetricGroup> tableGroups = new HashMap<>();
        private final Map<String, Long> latestSnapshotId = new ConcurrentHashMap<>();
        private final Map<String, Long> pendingSnapshots = new ConcurrentHashMap<>();

        Reader(@Nullable MetricGroup context) {
            if (context == null) {
                metricGroup = null;
                polls = null;
                failedPolls = null;
                tableFailures = null;
            } else {
                metricGroup = context.addGroup("iceberg_metadata");
                polls = metricGroup.counter("polls");
                failedPolls = metricGroup.counter("failed_polls");
                tableFailures = metricGroup.counter("table_failures");
            }
        }

        /** Sets the gauges of a table; the group stays when the table goes, and reads -1. */
        private void report(String fullName, FileStoreTable table, List<Long> pending) {
            if (metricGroup == null) {
                return;
            }
            Long latest;
            if (!pending.isEmpty()) {
                latest = pending.get(pending.size() - 1);
            } else {
                try {
                    latest = latestSnapshotReader.apply(table);
                } catch (Exception e) {
                    LOG.debug("Table {}: cannot read the latest snapshot id.", fullName, e);
                    latest = latestSnapshotId.get(fullName);
                }
            }
            tableGroups.computeIfAbsent(
                    fullName,
                    name -> {
                        MetricGroup group = metricGroup.addGroup("table", name);
                        group.gauge(
                                "latest_snapshot_id",
                                () -> latestSnapshotId.getOrDefault(name, -1L));
                        group.gauge(
                                "pending_snapshots",
                                () -> pendingSnapshots.getOrDefault(name, -1L));
                        return group;
                    });
            latestSnapshotId.put(fullName, latest == null ? -1L : latest);
            pendingSnapshots.put(fullName, (long) pending.size());
        }

        private void forget(String fullName) {
            latestSnapshotId.remove(fullName);
            pendingSnapshots.remove(fullName);
        }

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
                queue.addAll(discover());
                passDone = true;
                if (queue.isEmpty()) {
                    return isStreaming ? InputStatus.NOTHING_AVAILABLE : InputStatus.END_OF_INPUT;
                }
            }
            output.collect(queue.poll());
            return InputStatus.MORE_AVAILABLE;
        }

        /**
         * One poll. A failed listing in streaming mode is one skipped poll: nothing is emitted and
         * no miss is counted, so a catalog outage never drops a mirror. A database whose listing
         * fails is skipped in the same way, and the other databases go on. In batch mode a failed
         * listing fails.
         */
        List<IcebergSyncTask> discover() throws Exception {
            if (polls != null) {
                polls.inc();
            }
            List<Identifier> matching;
            Set<String> unlisted = new HashSet<>();
            try {
                matching =
                        matchingTables(
                                catalog,
                                databasePattern,
                                includingPattern,
                                excludingPattern,
                                isStreaming ? unlisted : null);
            } catch (Exception e) {
                if (failedPolls != null) {
                    failedPolls.inc();
                }
                if (!isStreaming) {
                    throw e;
                }
                LOG.warn("Listing the catalog failed; this poll is skipped.", e);
                return Collections.emptyList();
            }
            if (failedPolls != null) {
                failedPolls.inc(unlisted.size());
            }
            Set<String> present = new HashSet<>();
            List<List<IcebergSyncTask>> perTable = new ArrayList<>();
            for (Identifier id : matching) {
                present.add(id.getFullName());
                misses.remove(id.getFullName());
                List<IcebergSyncTask> pending = pendingTasks(id);
                if (!pending.isEmpty()) {
                    perTable.add(pending);
                }
            }
            List<IcebergSyncTask> tasks = interleave(perTable);
            for (String gone : new ArrayList<>(lastEmitted.keySet())) {
                if (present.contains(gone)) {
                    continue;
                }
                Identifier id = Identifier.fromString(gone);
                if (unlisted.contains(id.getDatabaseName())) {
                    // a failed listing is no miss
                    continue;
                }
                int n = misses.merge(gone, 1, Integer::sum);
                if (n < MISSES_BEFORE_DROP) {
                    LOG.info(
                            "Table {} is missing from the catalog ({} of {} polls).",
                            gone,
                            n,
                            MISSES_BEFORE_DROP);
                    continue;
                }
                LOG.warn("Table {} is gone from the catalog; its mirror will be dropped.", gone);
                tasks.add(
                        IcebergSyncTask.drop(
                                id.getDatabaseName(),
                                id.getObjectName(),
                                mirrorNamings.getOrDefault(gone, Collections.emptyMap())));
                mirrorNamings.remove(gone);
                lastEmitted.remove(gone);
                uuids.remove(gone);
                misses.remove(gone);
                forget(gone);
            }
            return tasks;
        }

        /** One snapshot per table per round, so a long backlog does not hold the others back. */
        List<IcebergSyncTask> interleave(List<List<IcebergSyncTask>> perTable) {
            List<IcebergSyncTask> out = new ArrayList<>();
            for (int round = 0; ; round++) {
                boolean any = false;
                for (List<IcebergSyncTask> pending : perTable) {
                    if (round < pending.size()) {
                        out.add(pending.get(round));
                        any = true;
                    }
                }
                if (!any) {
                    return out;
                }
            }
        }

        /**
         * The tasks of one table. In streaming mode a table whose reads fail is skipped for this
         * poll and counted: it stays present and keeps its mark, so it is neither dropped nor
         * emitted again from the start. In batch mode the failure fails the job.
         */
        private List<IcebergSyncTask> pendingTasks(Identifier id) throws Exception {
            try {
                return readPendingTasks(id);
            } catch (Catalog.TableNotExistException e) {
                return Collections.emptyList();
            } catch (Exception e) {
                if (!isStreaming) {
                    throw e;
                }
                if (tableFailures != null) {
                    tableFailures.inc();
                }
                LOG.warn("Table {}: skipped in this poll: {}", id.getFullName(), e.toString());
                LOG.debug("Table {}: the failure of this poll.", id.getFullName(), e);
                return Collections.emptyList();
            }
        }

        private List<IcebergSyncTask> readPendingTasks(Identifier id) throws Exception {
            Table table = catalog.getTable(id);
            if (!(table instanceof FileStoreTable)) {
                return Collections.emptyList();
            }
            FileStoreTable original = (FileStoreTable) table;
            if (!selected(original.options(), optionFilters)) {
                // not selected is not gone: the table is forgotten, not dropped
                lastEmitted.remove(id.getFullName());
                uuids.remove(id.getFullName());
                mirrorNamings.remove(id.getFullName());
                misses.remove(id.getFullName());
                forget(id.getFullName());
                return Collections.emptyList();
            }
            FileStoreTable mirrored;
            try {
                IcebergSync.checkNotMirroredByWriters(original);
                mirrored = IcebergSync.withMirrorDefaults(original, tableOptions);
            } catch (IllegalArgumentException e) {
                if (!isStreaming) {
                    throw e;
                }
                LOG.error("{}", e.getMessage());
                return Collections.emptyList();
            }
            Map<String, String> naming = new HashMap<>();
            for (String key : IcebergSyncTask.MIRROR_NAMING_KEYS) {
                String value = mirrored.options().get(key);
                if (value != null) {
                    naming.put(key, value);
                }
            }
            mirrorNamings.put(id.getFullName(), naming);
            List<Long> pending = IcebergSync.pendingSnapshots(mirrored);
            long hint = pending.isEmpty() ? -1 : IcebergSync.lastMirroredSnapshot(mirrored);
            String uuid = original.uuid();
            report(id.getFullName(), original, pending);
            long from = lastEmitted.getOrDefault(id.getFullName(), -1L);
            String knownUuid = uuids.put(id.getFullName(), uuid);
            if (knownUuid != null && !knownUuid.equals(uuid)) {
                // the table was dropped and created again; the mark belongs to the old table
                from = -1;
            }
            if (!pending.isEmpty() && pending.get(0) <= hint) {
                // pendingSnapshots returns an id at or below the hint only after a rollback;
                // the mark belongs to the old timeline
                from = -1;
            }
            List<IcebergSyncTask> tasks = new ArrayList<>();
            for (long snapshotId : pending) {
                if (snapshotId > from) {
                    tasks.add(
                            new IcebergSyncTask(
                                    id.getDatabaseName(), id.getObjectName(), snapshotId));
                    lastEmitted.put(id.getFullName(), snapshotId);
                }
            }
            // a table with nothing to mirror yet is still one we watch for a drop
            lastEmitted.putIfAbsent(id.getFullName(), -1L);
            return tasks;
        }

        @Override
        public void close() throws Exception {
            if (catalog != null) {
                catalog.close();
            }
        }
    }
}
