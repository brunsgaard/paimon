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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Mirrors Paimon snapshots into Iceberg metadata outside the commit path, one snapshot at a time,
 * through the same {@link IcebergCommitCallback} a committing job would run.
 */
public final class IcebergSync implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSync.class);

    private final FileStoreTable table;
    private final IcebergCommitCallback callback;
    private final IndexFileHandler indexFileHandler;

    /** @param table a table copy that carries the {@code metadata.iceberg.*} options. */
    public IcebergSync(FileStoreTable table) {
        IcebergOptions.StorageType storage =
                table.coreOptions().toConfiguration().get(IcebergOptions.METADATA_ICEBERG_STORAGE);
        checkArgument(
                storage != IcebergOptions.StorageType.DISABLED,
                "%s must be set on the table copy given to IcebergSync.",
                IcebergOptions.METADATA_ICEBERG_STORAGE.key());
        this.table = table;
        this.callback = new IcebergCommitCallback(table, "iceberg-sync", true);
        this.indexFileHandler = table.store().newIndexFileHandler();
    }

    /**
     * The table copy the sync works on. A table with deletion vectors needs Iceberg format version
     * 3 for the vectors to be exported; without it the callback exports top-level files only.
     */
    public static FileStoreTable withMirrorDefaults(
            FileStoreTable original, Map<String, String> mirrorOptions) {
        Map<String, String> options = new HashMap<>(mirrorOptions);
        CoreOptions core = original.coreOptions();
        boolean vectors = core.deletionVectorsEnabled() && core.deletionVectorBitmap64();
        String requested = options.get(IcebergOptions.FORMAT_VERSION.key());
        if (vectors && requested == null) {
            options.put(IcebergOptions.FORMAT_VERSION.key(), "3");
        } else if (vectors && "2".equals(requested.trim())) {
            throw new IllegalArgumentException(
                    String.format(
                            "Table %s has %s and %s; its mirror needs %s=3, otherwise the deletion"
                                    + " vectors are not exported.",
                            original.fullName(),
                            CoreOptions.DELETION_VECTORS_ENABLED.key(),
                            CoreOptions.DELETION_VECTOR_BITMAP64.key(),
                            IcebergOptions.FORMAT_VERSION.key()));
        }
        return original.copy(options);
    }

    /** Refuses a table whose own options make its writers commit the mirror. */
    public static void checkNotMirroredByWriters(FileStoreTable original) {
        String storage = original.options().get(IcebergOptions.METADATA_ICEBERG_STORAGE.key());
        checkArgument(
                storage == null
                        || IcebergOptions.StorageType.DISABLED
                                .toString()
                                .equalsIgnoreCase(storage.trim()),
                "Table %s sets %s=%s; its writers already commit Iceberg metadata, so"
                        + " iceberg_sync must not.",
                original.fullName(),
                IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                storage);
    }

    /** The snapshot id in {@code version-hint.text}, or -1 when the table has no mirror yet. */
    public static long lastMirroredSnapshot(FileStoreTable table) {
        Path hint =
                new Path(
                        IcebergCommitCallback.catalogTableMetadataPath(table),
                        IcebergCommitCallback.VERSION_HINT_FILENAME);
        String content;
        try {
            content = table.fileIO().readFileUtf8(hint);
        } catch (FileNotFoundException e) {
            return -1;
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + hint, e);
        }
        try {
            return Long.parseLong(content.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    String.format("%s holds '%s', not a snapshot id.", hint, content.trim()), e);
        }
    }

    /**
     * Snapshot ids to mirror, in order. The first sync of a table mirrors the latest snapshot only.
     * When snapshots between the hint and the earliest kept snapshot expired, the latest snapshot
     * is mirrored on its own and the callback builds it from a scan.
     */
    public static List<Long> pendingSnapshots(FileStoreTable table) {
        SnapshotManager snapshots = table.snapshotManager();
        Long latest = snapshots.latestSnapshotId();
        if (latest == null) {
            return Collections.emptyList();
        }
        long hint = lastMirroredSnapshot(table);
        if (hint < 0) {
            return Collections.singletonList(latest);
        }
        if (hint >= latest) {
            if (mirrorDescribes(table, latest)) {
                return Collections.emptyList();
            }
            LOG.warn(
                    "Table {}: the mirror is at snapshot {} but the table is at {}; a rollback"
                            + " is assumed and {} is mirrored again.",
                    table.fullName(),
                    hint,
                    latest,
                    latest);
            return Collections.singletonList(latest);
        }
        Long earliest = snapshots.earliestSnapshotId();
        if (earliest == null || hint + 1 < earliest) {
            LOG.warn(
                    "Table {}: snapshots {} to {} expired before they were mirrored; syncing {}"
                            + " from a scan.",
                    table.fullName(),
                    hint + 1,
                    earliest == null ? latest : earliest - 1,
                    latest);
            return Collections.singletonList(latest);
        }
        List<Long> pending = new ArrayList<>();
        for (long id = hint + 1; id <= latest; id++) {
            pending.add(id);
        }
        return pending;
    }

    public List<Long> pendingSnapshots() {
        return pendingSnapshots(table);
    }

    /** Whether the mirror's metadata for {@code snapshotId} describes the live Paimon snapshot. */
    private static boolean mirrorDescribes(FileStoreTable table, long snapshotId) {
        Snapshot snapshot = readSnapshot(table, snapshotId);
        if (snapshot == null) {
            return false;
        }
        Path metadataPath =
                new IcebergPathFactory(IcebergCommitCallback.catalogTableMetadataPath(table))
                        .toMetadataPath(snapshotId);
        try {
            if (!table.fileIO().exists(metadataPath)) {
                return false;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + metadataPath, e);
        }
        IcebergMetadata metadata = IcebergMetadata.fromPath(table.fileIO(), metadataPath);
        if (metadata.currentSnapshot() == null) {
            return false;
        }
        String identity =
                metadata.currentSnapshot()
                        .summary()
                        .get(IcebergCommitCallback.SNAPSHOT_SUMMARY_PAIMON_COMMIT_IDENTITY);
        return identity == null || identity.equals(IcebergCommitCallback.commitIdentity(snapshot));
    }

    /** Reads the snapshot file itself; catalog caches can hold a rolled-back timeline. */
    @Nullable
    private static Snapshot readSnapshot(FileStoreTable table, long snapshotId) {
        try {
            return SnapshotManager.tryFromPath(
                    table.fileIO(), table.snapshotManager().snapshotPath(snapshotId));
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    private static void writeHint(FileStoreTable table, long snapshotId) {
        Path hint =
                new Path(
                        IcebergCommitCallback.catalogTableMetadataPath(table),
                        IcebergCommitCallback.VERSION_HINT_FILENAME);
        try {
            table.fileIO().overwriteFileUtf8(hint, String.valueOf(snapshotId));
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot write " + hint, e);
        }
    }

    /**
     * Mirrors one snapshot and returns whether the mirror changed. An id below a hint that still
     * describes the live table is superseded and is skipped; below a hint that does not, the table
     * was rolled back and the id is mirrored again. The head id always reaches the callback, which
     * repairs a hint or a catalog pointer left behind by a crash between its steps. Snapshot 1 is
     * the exception: the callback clears the metadata directory when it is handed the first
     * snapshot, so an already mirrored snapshot 1 only gets its hint repaired.
     */
    public boolean sync(long snapshotId) {
        Snapshot snapshot = readSnapshot(table, snapshotId);
        if (snapshot == null) {
            LOG.warn("Table {}: snapshot {} is gone, skipping.", table.fullName(), snapshotId);
            return false;
        }
        long hint = lastMirroredSnapshot(table);
        if (snapshotId < hint && mirrorDescribes(table, hint)) {
            return false;
        }
        boolean described = mirrorDescribes(table, snapshotId);
        if (snapshotId == Snapshot.FIRST_SNAPSHOT_ID && described) {
            if (hint < snapshotId) {
                writeHint(table, snapshotId);
                return true;
            }
            return false;
        }
        List<ManifestEntry> delta =
                table.store()
                        .newScan()
                        .withKind(ScanMode.DELTA)
                        .withSnapshot(snapshotId)
                        .plan()
                        .files();
        List<IndexManifestEntry> vectors = indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX);
        callback.call(
                new CommitCallback.Context(
                        Collections.emptyList(),
                        delta,
                        vectors,
                        snapshot,
                        snapshot.commitIdentifier()));
        return !(described && hint >= snapshotId);
    }

    /** Mirrors every pending snapshot and returns how many changed the mirror. */
    public int syncPending() {
        int mirrored = 0;
        for (long id : pendingSnapshots()) {
            if (sync(id)) {
                mirrored++;
            }
        }
        return mirrored;
    }

    @Override
    public void close() throws Exception {
        callback.close();
    }
}
