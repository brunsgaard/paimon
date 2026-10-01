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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.factories.FactoryException;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.iceberg.IcebergMetadataCommitterFactory;
import org.apache.paimon.iceberg.IcebergMirrorDropper;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.iceberg.IcebergSync;
import org.apache.paimon.iceberg.IcebergSyncListener;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Mirrors the snapshots of the tasks it receives, with one {@link IcebergSync} per table.
 *
 * <p>In a streaming job, a table whose sync or drop throws is put on hold, and the other tables go
 * on. In a batch job the failure fails the job, because a processing-time timer does not fire again
 * in batch execution. A keyed processing-time timer retries a held table after {@link
 * #RETRY_DELAYS_MILLIS}: a held sync runs {@link IcebergSync#syncPending()}, which recomputes the
 * pending snapshots from the hint, so the sync tasks that arrive during the hold are ignored. A
 * held drop is retried as a drop. A drop task replaces a held sync, because the table is gone. A
 * held sync of a table that is gone, and a held drop of a table that exists again, release the
 * hold. The holds are not checkpointed: after a restart the source emits the pending snapshots
 * again, but a held drop is lost.
 *
 * <p>The {@link IcebergSyncListener}s found with {@link ServiceLoader} hear each mirrored snapshot
 * and each dropped mirror on the task thread. A listener call that fails is counted and never
 * changes the outcome of a sync.
 */
public class IcebergSyncOperator extends KeyedProcessFunction<String, IcebergSyncTask, Void> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSyncOperator.class);

    private final CatalogLoader catalogLoader;
    private final Map<String, String> tableOptions;
    private final HashMap<String, String> configuration;
    private final boolean isStreaming;

    private transient Catalog catalog;
    private transient Map<String, Cached> syncs;

    private transient MetricGroup metricGroup;
    private transient Counter synced;
    private transient Counter failureCounter;
    private transient Counter dropped;
    private transient Counter listenerFailures;
    private transient List<IcebergSyncListener> listeners;
    private transient Map<String, MetricGroup> tableGroups;
    private transient Map<String, Long> mirroredId;
    private transient Map<String, Long> mirroredTimestampMs;

    /** An operator for a streaming job. */
    public IcebergSyncOperator(
            CatalogLoader catalogLoader,
            Map<String, String> tableOptions,
            HashMap<String, String> configuration) {
        this(catalogLoader, tableOptions, configuration, true);
    }

    /**
     * In a streaming job a failing table is held and retried. In a batch job the processing-time
     * timers do not fire again, so a sync or a drop that fails fails the job.
     */
    public IcebergSyncOperator(
            CatalogLoader catalogLoader,
            Map<String, String> tableOptions,
            HashMap<String, String> configuration,
            boolean isStreaming) {
        this.catalogLoader = catalogLoader;
        this.tableOptions = new HashMap<>(tableOptions);
        this.configuration = new HashMap<>(configuration);
        this.isStreaming = isStreaming;
    }

    @Override
    public void open(OpenContext openContext) {
        catalog = catalogLoader.load();
        syncs = new HashMap<>();
        onHold = new ConcurrentHashMap<>();
        tableGroups = new HashMap<>();
        mirroredId = new ConcurrentHashMap<>();
        mirroredTimestampMs = new ConcurrentHashMap<>();
        try {
            metricGroup = getRuntimeContext().getMetricGroup().addGroup("iceberg_metadata");
        } catch (IllegalStateException e) {
            metricGroup = new UnregisteredMetricsGroup();
        }
        synced = metricGroup.counter("snapshots_synced");
        failureCounter = metricGroup.counter("sync_failures");
        dropped = metricGroup.counter("mirrors_dropped");
        listenerFailures = metricGroup.counter("listener_failures");
        openListeners();
    }

    /**
     * Finds the listeners through the user code class loader. A listener that cannot load or open
     * is counted and not used.
     */
    private void openListeners() {
        listeners = new ArrayList<>();
        ClassLoader userCode = Thread.currentThread().getContextClassLoader();
        Iterator<IcebergSyncListener> found =
                ServiceLoader.load(
                                IcebergSyncListener.class,
                                userCode == null
                                        ? IcebergSyncOperator.class.getClassLoader()
                                        : userCode)
                        .iterator();
        while (true) {
            IcebergSyncListener listener;
            try {
                if (!found.hasNext()) {
                    return;
                }
                listener = found.next();
            } catch (ServiceConfigurationError | RuntimeException | LinkageError e) {
                listenerFailures.inc();
                LOG.warn("A listener cannot be loaded; the remaining ones are not used.", e);
                return;
            }
            try {
                listener.open(new HashMap<>(configuration));
                listeners.add(listener);
            } catch (Exception | LinkageError e) {
                listenerFailures.inc();
                LOG.warn(
                        "Listener {} failed to open and is not used.",
                        listener.getClass().getName(),
                        e);
            }
        }
    }

    /** Calls each listener; a failure is logged and counted, and the others are still called. */
    private void notifyListeners(Consumer<IcebergSyncListener> call) {
        for (IcebergSyncListener listener : listeners) {
            try {
                call.accept(listener);
            } catch (Exception | LinkageError e) {
                listenerFailures.inc();
                LOG.warn("Listener {} failed.", listener.getClass().getName(), e);
            }
        }
    }

    /** Tells the listeners about a mirrored snapshot. Nothing in here throws. */
    private void notifySynced(
            Identifier id, FileStoreTable table, long snapshotId, long timestampMs) {
        if (listeners.isEmpty()) {
            return;
        }
        try {
            Options options =
                    Options.fromMap(IcebergSync.withMirrorDefaults(table, tableOptions).options());
            String icebergDatabase = IcebergOptions.icebergDatabaseName(options, id);
            String icebergTable = IcebergOptions.icebergTableName(options, id);
            notifyListeners(
                    l -> l.onSynced(id, snapshotId, timestampMs, icebergDatabase, icebergTable));
        } catch (Exception | LinkageError e) {
            listenerFailures.inc();
            LOG.warn("Table {}: the listeners are not told about snapshot {}.", id, snapshotId, e);
        }
    }

    /** Registers the gauges of a table once; a dropped table keeps its group and reads -1. */
    private void registerTable(String fullName) {
        tableGroups.computeIfAbsent(
                fullName,
                name -> {
                    MetricGroup group = metricGroup.addGroup("table", name);
                    group.gauge("mirrored_snapshot_id", () -> mirroredId.getOrDefault(name, -1L));
                    group.gauge(
                            "mirrored_snapshot_timestamp_ms",
                            () -> mirroredTimestampMs.getOrDefault(name, -1L));
                    group.gauge("on_hold", () -> onHold.containsKey(name) ? 1 : 0);
                    return group;
                });
    }

    /** For tests: how the last mirrored snapshot id is read after a retry. */
    SerializableFunction<FileStoreTable, Long> mirroredIdReader =
            table -> IcebergSync.lastMirroredSnapshot(table);

    /**
     * Records the mirrored snapshot of a table. A metric never fails a sync. Returns the snapshot
     * time, or -1 when it cannot be read.
     */
    private long recordMirrored(String fullName, FileStoreTable table, long snapshotId, int count) {
        long timestampMs = -1L;
        try {
            registerTable(fullName);
            synced.inc(count);
            try {
                timestampMs = table.snapshotManager().snapshot(snapshotId).timeMillis();
                mirroredTimestampMs.put(fullName, timestampMs);
            } finally {
                mirroredId.put(fullName, snapshotId);
            }
        } catch (Exception e) {
            LOG.debug("Table {}: no metric for snapshot {}.", fullName, snapshotId, e);
        }
        return timestampMs;
    }

    /**
     * Records the snapshots a successful retry mirrored; the id is the one in the mirror's hint.
     */
    private void recordRetried(Identifier identifier, FileStoreTable table, int count) {
        String fullName = identifier.getFullName();
        synced.inc(count);
        long id = -1L;
        long timestampMs = -1L;
        try {
            id = mirroredIdReader.apply(IcebergSync.withMirrorDefaults(table, tableOptions));
            if (id >= 0) {
                timestampMs = recordMirrored(fullName, table, id, 0);
            }
        } catch (Exception e) {
            LOG.debug("Table {}: no metric for the retry.", fullName, e);
        }
        if (id >= 0) {
            notifySynced(identifier, table, id, timestampMs);
        }
    }

    /** The delays before the retries of a held table; the last one repeats. */
    static final long[] RETRY_DELAYS_MILLIS = {30_000L, 120_000L, 600_000L};

    /** For tests: how a sync is built from the table copy with the mirror options. */
    SerializableFunction<FileStoreTable, IcebergSync> syncFactory = IcebergSync::new;

    /** The tables on hold, by full name. */
    private transient Map<String, Hold> onHold;

    @Override
    public void processElement(IcebergSyncTask task, Context context, Collector<Void> out)
            throws Exception {
        String fullName = task.fullName();
        Hold hold = onHold.get(fullName);
        if (task.isDrop()) {
            if (hold != null) {
                context.timerService().deleteProcessingTimeTimer(hold.retryAt);
                onHold.remove(fullName);
                if (!hold.task.isDrop()) {
                    LOG.info("Table {} is gone; its held sync becomes a drop.", fullName);
                }
            }
            try {
                drop(task);
            } catch (Exception e) {
                if (!isStreaming) {
                    throw e;
                }
                hold(task, 0, context.timerService(), e);
            }
            return;
        }
        if (hold != null) {
            LOG.debug(
                    "Table {} is on hold; snapshot {} waits for the retry.",
                    fullName,
                    task.snapshotId);
            return;
        }
        try {
            syncTask(task);
        } catch (Exception e) {
            evict(fullName);
            if (!isStreaming) {
                throw e;
            }
            hold(task, 0, context.timerService(), e);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<Void> out)
            throws Exception {
        String fullName = context.getCurrentKey();
        Hold hold = onHold.get(fullName);
        if (hold == null || hold.retryAt != timestamp) {
            return;
        }
        IcebergSyncTask task = hold.task;
        try {
            if (task.isDrop()) {
                retryDrop(task, hold.failures);
            } else {
                retrySync(task, hold.failures);
            }
        } catch (Exception e) {
            evict(fullName);
            hold(task, hold.failures, context.timerService(), e);
        }
    }

    private void retrySync(IcebergSyncTask task, int failures) throws Exception {
        FileStoreTable table;
        try {
            table = (FileStoreTable) catalog.getTable(Identifier.create(task.database, task.table));
        } catch (Catalog.TableNotExistException e) {
            onHold.remove(task.fullName());
            LOG.warn("Table {} no longer exists; its held sync is released.", task.fullName());
            return;
        }
        int mirrored = cached(task.fullName(), table).sync.syncPending();
        onHold.remove(task.fullName());
        if (mirrored > 0) {
            recordRetried(Identifier.create(task.database, task.table), table, mirrored);
        }
        LOG.info(
                "Table {} recovered after {} failures; {} snapshots mirrored.",
                task.fullName(),
                failures,
                mirrored);
    }

    private void retryDrop(IcebergSyncTask task, int failures) throws Exception {
        if (exists(task)) {
            onHold.remove(task.fullName());
            LOG.warn(
                    "Table {} exists again; its held drop is released and nothing is dropped.",
                    task.fullName());
            return;
        }
        drop(task);
        onHold.remove(task.fullName());
        LOG.info("Table {}: the mirror was dropped after {} failures.", task.fullName(), failures);
    }

    /** Asks the listing, as the source does; a catalog cache can still hold a dropped table. */
    private boolean exists(IcebergSyncTask task) throws Exception {
        try {
            return catalog.listTables(task.database).contains(task.table);
        } catch (Catalog.DatabaseNotExistException e) {
            return false;
        }
    }

    private void hold(
            IcebergSyncTask task, int failuresBefore, TimerService timers, Exception cause) {
        int failures = failuresBefore + 1;
        long delay = RETRY_DELAYS_MILLIS[Math.min(failures, RETRY_DELAYS_MILLIS.length) - 1];
        long retryAt = timers.currentProcessingTime() + delay;
        try {
            registerTable(task.fullName());
            failureCounter.inc();
        } catch (RuntimeException e) {
            LOG.debug("Table {}: no metric for the failure.", task.fullName(), e);
        }
        onHold.put(task.fullName(), new Hold(task, failures, retryAt));
        timers.registerProcessingTimeTimer(retryAt);
        LOG.warn(
                "Table {}: {} failed ({} in a row); next try in {} ms.",
                task.fullName(),
                task.isDrop() ? "the drop" : "the sync",
                failures,
                delay,
                cause);
    }

    @VisibleForTesting
    int failures(String fullName) {
        Hold hold = onHold.get(fullName);
        return hold == null ? 0 : hold.failures;
    }

    @VisibleForTesting
    public void syncTask(IcebergSyncTask task) throws Exception {
        if (task.isDrop()) {
            drop(task);
            return;
        }
        FileStoreTable table;
        try {
            table = (FileStoreTable) catalog.getTable(Identifier.create(task.database, task.table));
        } catch (Catalog.TableNotExistException e) {
            LOG.warn(
                    "Table {} no longer exists, skipping snapshot {}.",
                    task.fullName(),
                    task.snapshotId);
            return;
        }
        if (cached(task.fullName(), table).sync.sync(task.snapshotId)) {
            long timestampMs = recordMirrored(task.fullName(), table, task.snapshotId, 1);
            notifySynced(
                    Identifier.create(task.database, task.table),
                    table,
                    task.snapshotId,
                    timestampMs);
        }
    }

    private Cached cached(String fullName, FileStoreTable table) throws Exception {
        Cached cached = syncs.get(fullName);
        if (cached == null || cached.schemaId != table.schema().id()) {
            if (cached != null) {
                syncs.remove(fullName);
                cached.sync.close();
            }
            cached =
                    new Cached(
                            table.schema().id(),
                            syncFactory.apply(IcebergSync.withMirrorDefaults(table, tableOptions)));
            syncs.put(fullName, cached);
        }
        return cached;
    }

    /** Closes the sync of a table that failed, so that its retry starts from a new one. */
    private void evict(String fullName) {
        Cached cached = syncs.remove(fullName);
        if (cached != null) {
            try {
                cached.sync.close();
            } catch (Exception e) {
                LOG.warn("Table {}: cannot close the failed sync.", fullName, e);
            }
        }
    }

    /**
     * Drops the mirror of a table that is gone. A drop that fails, for example in a REST outage, is
     * held and retried, but the hold and the reader keep no checkpointed state: a drop still held
     * at a restart is lost, because the table never appears in the listing again.
     */
    private void drop(IcebergSyncTask task) throws Exception {
        Cached cached = syncs.remove(task.fullName());
        if (cached != null) {
            cached.sync.close();
        }
        mirroredId.remove(task.fullName());
        mirroredTimestampMs.remove(task.fullName());
        Map<String, String> merged = new HashMap<>(task.mirrorNaming);
        merged.putAll(tableOptions);
        Options options = Options.fromMap(merged);
        String storage = options.get(IcebergOptions.METADATA_ICEBERG_STORAGE.key());
        IcebergMirrorDropper dropper = null;
        if (storage != null) {
            try {
                dropper =
                        FactoryUtil.discoverFactory(
                                        IcebergSyncOperator.class.getClassLoader(),
                                        IcebergMetadataCommitterFactory.class,
                                        storage.trim().toLowerCase(Locale.ROOT))
                                .createDropper(
                                        options, Identifier.create(task.database, task.table));
            } catch (FactoryException e) {
                LOG.debug("No committer factory for storage {}.", storage, e);
            }
        }
        if (dropper == null) {
            LOG.warn(
                    "Table {} is gone; storage {} keeps no catalog entry, its metadata stays.",
                    task.fullName(),
                    storage);
            return;
        }
        try (IcebergMirrorDropper d = dropper) {
            d.drop();
        }
        dropped.inc();
        if (listeners.isEmpty()) {
            return;
        }
        try {
            Identifier id = Identifier.create(task.database, task.table);
            String icebergDatabase = IcebergOptions.icebergDatabaseName(options, id);
            String icebergTable = IcebergOptions.icebergTableName(options, id);
            notifyListeners(l -> l.onDropped(id, icebergDatabase, icebergTable));
        } catch (Exception | LinkageError e) {
            listenerFailures.inc();
            LOG.warn("Table {}: the listeners are not told about the drop.", task.fullName(), e);
        }
    }

    /** What to retry for a held table: the sync of its pending snapshots, or its drop. */
    private static final class Hold {
        private final IcebergSyncTask task;
        private final int failures;
        private final long retryAt;

        private Hold(IcebergSyncTask task, int failures, long retryAt) {
            this.task = task;
            this.failures = failures;
            this.retryAt = retryAt;
        }
    }

    /** One sync per table, rebuilt when the table's schema (and with it its options) changes. */
    private static final class Cached {
        private final long schemaId;
        private final IcebergSync sync;

        private Cached(long schemaId, IcebergSync sync) {
            this.schemaId = schemaId;
            this.sync = sync;
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (syncs != null) {
                for (Cached cached : syncs.values()) {
                    cached.sync.close();
                }
            }
            if (catalog != null) {
                catalog.close();
            }
        } finally {
            closeListeners();
        }
    }

    private void closeListeners() {
        if (listeners == null) {
            return;
        }
        for (IcebergSyncListener listener : listeners) {
            try {
                listener.close();
            } catch (Exception | LinkageError e) {
                LOG.warn("Listener {} failed to close.", listener.getClass().getName(), e);
            }
        }
    }
}
