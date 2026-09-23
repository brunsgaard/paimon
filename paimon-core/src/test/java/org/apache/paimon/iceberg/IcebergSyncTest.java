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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergSnapshot;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link IcebergSync}. */
public class IcebergSyncTest {

    private static final RowType ROW_TYPE =
            RowType.of(new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});

    @TempDir java.nio.file.Path tempDir;

    private FileStoreTable stockTable;
    private FileStoreTable mirrorTable;

    @BeforeEach
    public void setUp() throws Exception {
        // append-only: every file is exposed at write time and one commit is one snapshot
        stockTable = createPaimonTable("t", Collections.emptyList(), -1, Collections.emptyMap());
        mirrorTable = stockTable.copy(mirrorOptions());
    }

    @Test
    public void testFirstSyncMirrorsOnlyTheLatestSnapshot() throws Exception {
        write(stockTable, 1, GenericRow.of(1, 10));
        write(stockTable, 2, GenericRow.of(2, 20));

        try (IcebergSync sync = new IcebergSync(mirrorTable)) {
            assertThat(sync.pendingSnapshots()).containsExactly(2L);
            assertThat(sync.syncPending()).isEqualTo(1);
        }

        Path metadataDir = IcebergCommitCallback.catalogTableMetadataPath(mirrorTable);
        assertThat(stockTable.fileIO().exists(new Path(metadataDir, "v1.metadata.json"))).isFalse();
        IcebergMetadata metadata =
                IcebergMetadata.fromPath(
                        stockTable.fileIO(), new Path(metadataDir, "v2.metadata.json"));
        assertThat(metadata.snapshots().stream().map(IcebergSnapshot::snapshotId))
                .containsExactly(2L);
        assertThat(IcebergSync.lastMirroredSnapshot(mirrorTable)).isEqualTo(2L);
        assertThat(getIcebergResult()).containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)");
    }

    @Test
    public void testEverySnapshotSinceTheHintIsReplayedInOrder() throws Exception {
        write(stockTable, 1, GenericRow.of(1, 10));
        try (IcebergSync sync = new IcebergSync(mirrorTable)) {
            sync.syncPending();
            write(stockTable, 2, GenericRow.of(2, 20));
            write(stockTable, 3, GenericRow.of(1, 11));
            write(stockTable, 4, GenericRow.of(3, 30));
            assertThat(sync.pendingSnapshots()).containsExactly(2L, 3L, 4L);
            assertThat(sync.syncPending()).isEqualTo(3);
            assertThat(sync.pendingSnapshots()).isEmpty();
        }

        Path metadataDir = IcebergCommitCallback.catalogTableMetadataPath(mirrorTable);
        IcebergMetadata metadata =
                IcebergMetadata.fromPath(
                        stockTable.fileIO(), new Path(metadataDir, "v4.metadata.json"));
        assertThat(metadata.snapshots().stream().map(IcebergSnapshot::snapshotId))
                .containsExactly(1L, 2L, 3L, 4L);
        assertThat(metadata.currentSnapshot().parentSnapshotId()).isEqualTo(3L);
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder(
                        "Record(1, 10)", "Record(2, 20)", "Record(1, 11)", "Record(3, 30)");
    }

    @Test
    public void testASnapshotBehindTheWritersHeadIsPublished() throws Exception {
        write(stockTable, 1, GenericRow.of(1, 10));
        try (IcebergSync sync = new IcebergSync(mirrorTable)) {
            sync.syncPending();
            write(stockTable, 2, GenericRow.of(2, 20));
            write(stockTable, 3, GenericRow.of(3, 30));

            // the writer is at 3 while the sync publishes 2: the hint and the readers follow
            assertThat(sync.sync(2)).isTrue();
            assertThat(IcebergSync.lastMirroredSnapshot(mirrorTable)).isEqualTo(2L);
            assertThat(getIcebergResult())
                    .containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)");

            assertThat(sync.sync(3)).isTrue();
            assertThat(IcebergSync.lastMirroredSnapshot(mirrorTable)).isEqualTo(3L);
        }
    }

    @Test
    public void testSyncIsIdempotent() throws Exception {
        write(stockTable, 1, GenericRow.of(1, 10));
        write(stockTable, 2, GenericRow.of(2, 20));
        try (IcebergSync sync = new IcebergSync(mirrorTable)) {
            sync.syncPending();
            sync.sync(2L);
            sync.sync(1L);
        }

        Path metadataDir = IcebergCommitCallback.catalogTableMetadataPath(mirrorTable);
        assertThat(stockTable.fileIO().exists(new Path(metadataDir, "v1.metadata.json"))).isFalse();
        assertThat(IcebergSync.lastMirroredSnapshot(mirrorTable)).isEqualTo(2L);
        assertThat(getIcebergResult()).containsExactlyInAnyOrder("Record(1, 10)", "Record(2, 20)");
    }

    @Test
    public void testExpiredGapFallsBackToTheLatestSnapshot() throws Exception {
        write(stockTable, 1, GenericRow.of(1, 10));
        try (IcebergSync sync = new IcebergSync(mirrorTable)) {
            sync.syncPending();
        }
        for (int i = 2; i <= 6; i++) {
            write(stockTable, i, GenericRow.of(i, i * 10));
        }
        stockTable
                .newExpireSnapshots()
                .config(ExpireConfig.builder().snapshotRetainMax(2).snapshotRetainMin(2).build())
                .expire();
        assertThat(stockTable.snapshotManager().earliestSnapshotId()).isEqualTo(5L);

        try (IcebergSync sync = new IcebergSync(mirrorTable)) {
            assertThat(sync.pendingSnapshots()).containsExactly(6L);
            assertThat(sync.syncPending()).isEqualTo(1);
        }

        assertThat(getIcebergResult()).hasSize(6);
        assertThat(IcebergSync.lastMirroredSnapshot(mirrorTable)).isEqualTo(6L);
    }

    @Test
    public void testSyncAfterRollbackReplaysTheLiveTimeline() throws Exception {
        write(stockTable, 1, GenericRow.of(1, 10));
        write(stockTable, 2, GenericRow.of(2, 20));
        write(stockTable, 3, GenericRow.of(3, 30));
        try (IcebergSync sync = new IcebergSync(mirrorTable)) {
            sync.syncPending();
        }

        stockTable.rollbackTo(1);
        write(stockTable, 2, GenericRow.of(4, 40));
        try (IcebergSync sync = new IcebergSync(mirrorTable)) {
            assertThat(sync.syncPending()).isGreaterThanOrEqualTo(1);
        }

        assertThat(getIcebergResult()).containsExactlyInAnyOrder("Record(1, 10)", "Record(4, 40)");
    }

    @Test
    public void testRefusesATableThatMirrorsFromItsWriters() {
        assertThatThrownBy(() -> IcebergSync.checkNotMirroredByWriters(mirrorTable))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(IcebergOptions.METADATA_ICEBERG_STORAGE.key());
        IcebergSync.checkNotMirroredByWriters(stockTable);
    }

    @Test
    public void testRefusesACopyWithoutMirrorOptions() {
        assertThatThrownBy(() -> new IcebergSync(stockTable))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(IcebergOptions.METADATA_ICEBERG_STORAGE.key());
    }

    @Test
    public void testDeletionVectorTableDefaultsToFormatVersion3AndMirrorsVectors()
            throws Exception {
        FileStoreTable pkTable =
                createPaimonTable("pk", Collections.singletonList("k"), 1, deletionVectorOptions());
        FileStoreTable mirrored = IcebergSync.withMirrorDefaults(pkTable, mirrorOptions());
        assertThat(mirrored.coreOptions().toConfiguration().get(IcebergOptions.FORMAT_VERSION))
                .isEqualTo(3);

        String user = UUID.randomUUID().toString();
        try (TableWriteImpl<?> write =
                        pkTable.newWrite(user)
                                .withIOManager(new IOManagerImpl(tempDir.toString()));
                TableCommitImpl commit = pkTable.newCommit(user)) {
            write.write(GenericRow.of(1, 10));
            write.write(GenericRow.of(2, 20));
            write.compact(BinaryRow.EMPTY_ROW, 0, false);
            commit.commit(1, write.prepareCommit(true, 1));
            write.write(GenericRow.of(1, 11));
            write.compact(BinaryRow.EMPTY_ROW, 0, false);
            commit.commit(2, write.prepareCommit(true, 2));
        }
        try (IcebergSync sync = new IcebergSync(mirrored)) {
            sync.syncPending();
        }

        // the Iceberg library on this classpath reads format version 2 only, so the check is
        // on the metadata Paimon wrote: a version 3 table with data files and a deletion vector
        Path metadataDir = IcebergCommitCallback.catalogTableMetadataPath(mirrored);
        long latest = pkTable.snapshotManager().latestSnapshotId();
        IcebergMetadata metadata =
                IcebergMetadata.fromPath(
                        pkTable.fileIO(), new Path(metadataDir, "v" + latest + ".metadata.json"));
        assertThat(metadata.formatVersion()).isEqualTo(3);
        assertThat(Long.parseLong(metadata.currentSnapshot().summary().get("total-data-files")))
                .isGreaterThan(0L);
        assertThat(Long.parseLong(metadata.currentSnapshot().summary().get("total-delete-files")))
                .isGreaterThan(0L);
        assertThat(IcebergSync.lastMirroredSnapshot(mirrored)).isEqualTo(latest);
    }

    @Test
    public void testExplicitFormatVersion2OnADeletionVectorTableIsRefused() throws Exception {
        FileStoreTable pkTable =
                createPaimonTable("pk", Collections.singletonList("k"), 1, deletionVectorOptions());
        Map<String, String> mirror = mirrorOptions();
        mirror.put(IcebergOptions.FORMAT_VERSION.key(), "2");

        assertThatThrownBy(() -> IcebergSync.withMirrorDefaults(pkTable, mirror))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(IcebergOptions.FORMAT_VERSION.key())
                .hasMessageContaining(CoreOptions.DELETION_VECTORS_ENABLED.key());
    }

    @Test
    public void testMetadataWrittenBeforeACrashedHintIsRepairedOnTheNextSync() throws Exception {
        write(stockTable, 1, GenericRow.of(1, 10));
        write(stockTable, 2, GenericRow.of(2, 20));
        write(stockTable, 3, GenericRow.of(3, 30));
        try (IcebergSync sync = new IcebergSync(mirrorTable)) {
            sync.syncPending();
        }
        // the callback writes v3.metadata.json before version-hint.text; a crash in between
        // leaves a matching metadata file behind a stale hint
        Path hint =
                new Path(
                        IcebergCommitCallback.catalogTableMetadataPath(mirrorTable),
                        "version-hint.text");
        stockTable.fileIO().overwriteFileUtf8(hint, "2");

        try (IcebergSync sync = new IcebergSync(mirrorTable)) {
            assertThat(sync.pendingSnapshots()).containsExactly(3L);
            assertThat(sync.sync(3L)).isTrue();
        }
        assertThat(IcebergSync.lastMirroredSnapshot(mirrorTable)).isEqualTo(3L);
    }

    @Test
    public void testSyncAfterRollbackThroughACachingCatalogReplaysTheLiveTimeline()
            throws Exception {
        try (Catalog catalog =
                CatalogFactory.createCatalog(CatalogContext.create(new Path(tempDir.toString())))) {
            FileStoreTable cached =
                    (FileStoreTable) catalog.getTable(Identifier.create("mydb", "t"));
            FileStoreTable mirrored = cached.copy(mirrorOptions());
            write(cached, 1, GenericRow.of(1, 10));
            write(cached, 2, GenericRow.of(2, 20));
            write(cached, 3, GenericRow.of(3, 30));
            try (IcebergSync sync = new IcebergSync(mirrored)) {
                sync.syncPending();
                // keep the pre-rollback snapshot objects hot in the catalog cache
                assertThat(sync.pendingSnapshots()).isEmpty();
            }

            cached.rollbackTo(1);
            write(cached, 2, GenericRow.of(4, 40), GenericRow.of(5, 50));
            try (IcebergSync sync = new IcebergSync(mirrored)) {
                assertThat(sync.syncPending()).isEqualTo(1);
            }
        }
        assertThat(getIcebergResult())
                .containsExactlyInAnyOrder("Record(1, 10)", "Record(4, 40)", "Record(5, 50)");
    }

    @Test
    public void testCorruptHintIsAnErrorNotAFirstSync() throws Exception {
        write(stockTable, 1, GenericRow.of(1, 10));
        write(stockTable, 2, GenericRow.of(2, 20));
        Path metadataDir = IcebergCommitCallback.catalogTableMetadataPath(mirrorTable);
        stockTable.fileIO().mkdirs(metadataDir);
        stockTable.fileIO().overwriteFileUtf8(new Path(metadataDir, "version-hint.text"), "abc");

        assertThatThrownBy(() -> IcebergSync.pendingSnapshots(mirrorTable))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("version-hint.text");
    }

    @Test
    public void testSyncReportsWhetherItMirrored() throws Exception {
        write(stockTable, 1, GenericRow.of(1, 10));
        write(stockTable, 2, GenericRow.of(2, 20));
        try (IcebergSync sync = new IcebergSync(mirrorTable)) {
            assertThat(sync.sync(2L)).isTrue();
            assertThat(sync.sync(2L)).isFalse();
            assertThat(sync.sync(1L)).isFalse();
            write(stockTable, 3, GenericRow.of(3, 30));
            write(stockTable, 4, GenericRow.of(4, 40));
            assertThat(sync.syncPending()).isEqualTo(2);
            assertThat(sync.syncPending()).isEqualTo(0);
        }
    }

    // ------------------------------------------------------------------------------------------

    private static Map<String, String> mirrorOptions() {
        Map<String, String> mirror = new HashMap<>();
        mirror.put(
                IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                IcebergOptions.StorageType.TABLE_LOCATION.toString());
        return mirror;
    }

    private static Map<String, String> deletionVectorOptions() {
        Map<String, String> dv = new HashMap<>();
        dv.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        dv.put(CoreOptions.DELETION_VECTOR_BITMAP64.key(), "true");
        return dv;
    }

    private static void write(FileStoreTable table, long commitIdentifier, GenericRow... rows)
            throws Exception {
        String user = UUID.randomUUID().toString();
        try (TableWriteImpl<?> write = table.newWrite(user);
                TableCommitImpl commit = table.newCommit(user)) {
            for (GenericRow row : rows) {
                write.write(row);
            }
            commit.commit(commitIdentifier, write.prepareCommit(false, commitIdentifier));
        }
    }

    /** A table without any metadata.iceberg option, as a writer would create it. */
    private FileStoreTable createPaimonTable(
            String name,
            List<String> primaryKeys,
            int numBuckets,
            Map<String, String> customOptions)
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());
        Options options = new Options(customOptions);
        options.set(CoreOptions.BUCKET, numBuckets);
        options.set(CoreOptions.FILE_FORMAT, "avro");
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofKibiBytes(32));
        Schema schema =
                new Schema(
                        ROW_TYPE.getFields(),
                        Collections.emptyList(),
                        primaryKeys,
                        options.toMap(),
                        "");
        try (FileSystemCatalog catalog = new FileSystemCatalog(fileIO, path)) {
            catalog.createDatabase("mydb", true);
            Identifier identifier = Identifier.create("mydb", name);
            catalog.createTable(identifier, schema, false);
            return (FileStoreTable) catalog.getTable(identifier);
        }
    }

    private List<String> getIcebergResult() throws Exception {
        HadoopCatalog icebergCatalog = new HadoopCatalog(new Configuration(), tempDir.toString());
        org.apache.iceberg.Table icebergTable =
                icebergCatalog.loadTable(TableIdentifier.of("mydb.db", "t"));
        List<String> actual = new ArrayList<>();
        try (CloseableIterable<Record> result = IcebergGenerics.read(icebergTable).build()) {
            for (Record record : result) {
                actual.add(record.toString());
            }
        }
        return actual;
    }
}
