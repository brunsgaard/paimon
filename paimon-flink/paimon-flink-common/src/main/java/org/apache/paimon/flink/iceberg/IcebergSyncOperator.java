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
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Mirrors the snapshots of the tasks it receives, with one {@link IcebergSync} per table.
 *
 * <p>A table whose sync or drop throws is put on hold, and the other tables go on. A keyed
 * processing-time timer retries it after {@link #RETRY_DELAYS_MILLIS}: a held sync runs {@link
 * IcebergSync#syncPending()}, which recomputes the pending snapshots from the hint, so the sync
 * tasks that arrive during the hold are ignored. A held drop is retried as a drop. A drop task
 * replaces a held sync, because the table is gone. A held sync of a table that is gone, and a held
 * drop of a table that exists again, release the hold. The holds are not checkpointed: after a
 * restart the source emits the pending snapshots again, but a held drop is lost.
 */
public class IcebergSyncOperator extends KeyedProcessFunction<String, IcebergSyncTask, Void> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSyncOperator.class);

    private final CatalogLoader catalogLoader;
    private final Map<String, String> tableOptions;

    private transient Catalog catalog;
    private transient Map<String, Cached> syncs;

    public IcebergSyncOperator(CatalogLoader catalogLoader, Map<String, String> tableOptions) {
        this.catalogLoader = catalogLoader;
        this.tableOptions = new HashMap<>(tableOptions);
    }

    @Override
    public void open(OpenContext openContext) {
        catalog = catalogLoader.load();
        syncs = new HashMap<>();
        onHold = new HashMap<>();
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
        cached(task.fullName(), table).sync.sync(task.snapshotId);
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
        if (syncs != null) {
            for (Cached cached : syncs.values()) {
                cached.sync.close();
            }
        }
        if (catalog != null) {
            catalog.close();
        }
    }
}
