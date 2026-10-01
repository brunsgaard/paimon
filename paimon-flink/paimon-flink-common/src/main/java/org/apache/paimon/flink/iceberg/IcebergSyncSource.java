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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.UncheckedIOException;
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
        private final Map<String, MetricGroup> tableGroups = new HashMap<>();
        private final Map<String, Long> latestSnapshotId = new ConcurrentHashMap<>();
        private final Map<String, Long> pendingSnapshots = new ConcurrentHashMap<>();

        Reader(@Nullable MetricGroup context) {
            if (context == null) {
                metricGroup = null;
                polls = null;
                failedPolls = null;
            } else {
                metricGroup = context.addGroup("iceberg_metadata");
                polls = metricGroup.counter("polls");
                failedPolls = metricGroup.counter("failed_polls");
            }
        }

        /** Sets the gauges of a table; the group stays when the table goes, and reads -1. */
        private void report(String fullName, @Nullable Long latest, int pending) {
            if (metricGroup == null) {
                return;
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
            pendingSnapshots.put(fullName, (long) pending);
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
         * no miss is counted, so a catalog outage never drops a mirror. In batch mode it fails.
         */
        List<IcebergSyncTask> discover() throws Exception {
            if (polls != null) {
                polls.inc();
            }
            List<Identifier> matching;
            try {
                matching =
                        matchingTables(
                                catalog, databasePattern, includingPattern, excludingPattern);
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
                int n = misses.merge(gone, 1, Integer::sum);
                if (n < MISSES_BEFORE_DROP) {
                    LOG.info(
                            "Table {} is missing from the catalog ({} of {} polls).",
                            gone,
                            n,
                            MISSES_BEFORE_DROP);
                    continue;
                }
                Identifier id = Identifier.fromString(gone);
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

        private List<IcebergSyncTask> pendingTasks(Identifier id) throws Exception {
            Table table;
            try {
                table = catalog.getTable(id);
            } catch (Catalog.TableNotExistException e) {
                return Collections.emptyList();
            }
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
            List<Long> pending;
            long hint;
            try {
                pending = IcebergSync.pendingSnapshots(mirrored);
                hint = pending.isEmpty() ? -1 : IcebergSync.lastMirroredSnapshot(mirrored);
            } catch (IllegalStateException | UncheckedIOException e) {
                if (!isStreaming) {
                    throw e;
                }
                LOG.warn("Table {}: skipped in this poll: {}", id.getFullName(), e.getMessage());
                return Collections.emptyList();
            }
            report(id.getFullName(), original.snapshotManager().latestSnapshotId(), pending.size());
            long from = lastEmitted.getOrDefault(id.getFullName(), -1L);
            String uuid = original.uuid();
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
