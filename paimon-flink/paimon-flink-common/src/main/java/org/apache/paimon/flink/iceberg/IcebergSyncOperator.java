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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Mirrors the snapshots of the tasks it receives, with one {@link IcebergSync} per table. */
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
    }

    @Override
    public void processElement(IcebergSyncTask task, Context context, Collector<Void> out)
            throws Exception {
        syncTask(task);
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
        Cached cached = syncs.get(task.fullName());
        if (cached == null || cached.schemaId != table.schema().id()) {
            if (cached != null) {
                cached.sync.close();
            }
            cached =
                    new Cached(
                            table.schema().id(),
                            new IcebergSync(IcebergSync.withMirrorDefaults(table, tableOptions)));
            syncs.put(task.fullName(), cached);
        }
        cached.sync.sync(task.snapshotId);
    }

    /**
     * Drops the mirror of a table that is gone. The reader keeps no checkpointed state, so a drop
     * that fails, for example in a REST outage, is lost after a restart: the table never appears in
     * the listing again.
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
