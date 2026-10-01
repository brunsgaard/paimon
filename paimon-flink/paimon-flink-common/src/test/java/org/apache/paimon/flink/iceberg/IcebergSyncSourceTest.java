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
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.iceberg.IcebergCommitCallback;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.iceberg.IcebergSync;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IcebergSyncSourceTest {

    @TempDir java.nio.file.Path tempDir;

    Catalog catalog;
    Map<String, String> mirrorOptions;

    @BeforeEach
    void setUp() throws Exception {
        Options options = new Options();
        options.set("warehouse", new Path(tempDir.toString(), "warehouse").toString());
        options.set("cache-enabled", "false");
        catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase("db", false);
        mirrorOptions = new HashMap<>();
        mirrorOptions.put(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "table-location");
    }

    void createTable(String name, Map<String, String> tableOptions) throws Exception {
        Schema.Builder schema =
                Schema.newBuilder().column("id", DataTypes.INT()).column("v", DataTypes.STRING());
        tableOptions.forEach(schema::option);
        catalog.createTable(Identifier.create("db", name), schema.build(), false);
    }

    void write(String name) throws Exception {
        FileStoreTable table = (FileStoreTable) catalog.getTable(Identifier.create("db", name));
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(1, BinaryString.fromString("r")));
            commit.commit(write.prepareCommit());
        }
    }

    IcebergSyncSource.Reader reader(Pattern including, List<String> optionFilters) {
        IcebergSyncSource source =
                new IcebergSyncSource(
                        () -> catalog,
                        Pattern.compile("db"),
                        including,
                        null,
                        optionFilters,
                        mirrorOptions,
                        true,
                        Duration.ofSeconds(1));
        IcebergSyncSource.Reader reader = source.newReader();
        reader.start();
        return reader;
    }

    static List<String> names(List<IcebergSyncTask> tasks) {
        return tasks.stream()
                .map(t -> t.fullName() + "@" + (t.isDrop() ? "drop" : String.valueOf(t.snapshotId)))
                .collect(Collectors.toList());
    }

    @Test
    void testOnlyTablesWithAMatchingOptionAreSelected() throws Exception {
        createTable("on", Collections.singletonMap("x.enabled", "true"));
        createTable("off", Collections.singletonMap("x.enabled", "false"));
        createTable("unset", Collections.emptyMap());
        write("on");
        write("off");
        write("unset");
        IcebergSyncSource.Reader reader =
                reader(Pattern.compile("db\\..*"), Collections.singletonList("x.enabled=true"));
        assertThat(names(reader.discover())).containsExactly("db.on@1");
    }

    @Test
    void testAnyOfSeveralFiltersSelects() throws Exception {
        createTable("a", Collections.singletonMap("x.enabled", "true"));
        createTable("b", Collections.singletonMap("y.enabled", "true"));
        write("a");
        write("b");
        IcebergSyncSource.Reader reader =
                reader(
                        Pattern.compile("db\\..*"),
                        java.util.Arrays.asList("x.enabled=true", "y.enabled=true"));
        assertThat(names(reader.discover())).containsExactlyInAnyOrder("db.a@1", "db.b@1");
    }

    @Test
    void testATableThatTurnsTheOptionOffIsNeitherSyncedNorDropped() throws Exception {
        createTable("t", Collections.singletonMap("x.enabled", "true"));
        write("t");
        IcebergSyncSource.Reader reader =
                reader(Pattern.compile("db\\..*"), Collections.singletonList("x.enabled=true"));
        assertThat(names(reader.discover())).containsExactly("db.t@1");
        catalog.alterTable(
                Identifier.create("db", "t"),
                Collections.singletonList(
                        org.apache.paimon.schema.SchemaChange.setOption("x.enabled", "false")),
                false);
        write("t");
        assertThat(reader.discover()).as("not selected").isEmpty();
        assertThat(reader.discover()).as("and not dropped").isEmpty();
        assertThat(reader.discover()).isEmpty();
    }

    @Test
    void testATableWithAnUnreadableHintIsSkippedAndTheOthersGoOn() throws Exception {
        createTable("bad", Collections.emptyMap());
        createTable("good", Collections.emptyMap());
        write("bad");
        write("good");
        FileStoreTable bad = (FileStoreTable) catalog.getTable(Identifier.create("db", "bad"));
        Path hint =
                new Path(
                        IcebergCommitCallback.catalogTableMetadataPath(
                                IcebergSync.withMirrorDefaults(bad, mirrorOptions)),
                        "version-hint.text");
        bad.fileIO().overwriteFileUtf8(hint, "");
        IcebergSyncSource.Reader reader =
                reader(Pattern.compile("db\\..*"), Collections.emptyList());
        long delay = IcebergSync.HINT_READ_DELAY_MILLIS;
        IcebergSync.HINT_READ_DELAY_MILLIS = 0;
        try {
            assertThat(names(reader.discover())).containsExactly("db.good@1");
            assertThat(names(reader.discover())).as("still skipped, not dropped").isEmpty();
        } finally {
            IcebergSync.HINT_READ_DELAY_MILLIS = delay;
        }
        bad.fileIO().delete(hint, false);
        assertThat(names(reader.discover())).containsExactly("db.bad@1");
    }

    @Test
    void testAFailedMetricReadDoesNotFailThePoll() throws Exception {
        createTable("t", Collections.emptyMap());
        write("t");
        FileStoreTable table = (FileStoreTable) catalog.getTable(Identifier.create("db", "t"));
        new IcebergSync(IcebergSync.withMirrorDefaults(table, mirrorOptions)).sync(1);
        IcebergSyncSource source =
                new IcebergSyncSource(
                        () -> catalog,
                        Pattern.compile("db"),
                        Pattern.compile("db\\..*"),
                        null,
                        Collections.emptyList(),
                        mirrorOptions,
                        true,
                        Duration.ofSeconds(1));
        source.latestSnapshotReader =
                t -> {
                    throw new java.io.UncheckedIOException(new java.io.IOException("read"));
                };
        IcebergSyncSource.Reader reader = source.newReader(new UnregisteredMetricsGroup());
        reader.start();
        assertThat(reader.discover()).as("nothing pending, and no failure").isEmpty();
    }

    @Test
    void testAFilterWithoutAnEqualsSignIsRefused() {
        assertThatThrownBy(() -> IcebergSyncSource.parseFilter("nope"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--table_option_filter needs key=value, got 'nope'");
    }

    @Test
    void testTheDropTaskCarriesTheMirrorNamingOfTheTable() throws Exception {
        createTable("t", Collections.singletonMap("metadata.iceberg.table", "other_name"));
        write("t");
        IcebergSyncSource.Reader reader =
                reader(Pattern.compile("db\\..*"), Collections.emptyList());
        reader.discover();
        catalog.dropTable(Identifier.create("db", "t"), false);
        reader.discover();
        List<IcebergSyncTask> tasks = reader.discover();
        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0).mirrorNaming)
                .containsExactly(
                        java.util.Collections.singletonMap("metadata.iceberg.table", "other_name")
                                .entrySet()
                                .iterator()
                                .next());
    }

    @Test
    void testATableIsDroppedOnlyAfterTwoConsecutiveMisses() throws Exception {
        createTable("t", Collections.emptyMap());
        write("t");
        IcebergSyncSource.Reader reader =
                reader(Pattern.compile("db\\..*"), Collections.emptyList());
        assertThat(names(reader.discover())).containsExactly("db.t@1");
        catalog.dropTable(Identifier.create("db", "t"), false);
        assertThat(reader.discover()).as("first miss").isEmpty();
        assertThat(names(reader.discover())).containsExactly("db.t@drop");
        assertThat(reader.discover()).as("dropped once").isEmpty();
    }

    @Test
    void testATableThatComesBackBetweenTheMissesIsNotDropped() throws Exception {
        createTable("t", Collections.emptyMap());
        write("t");
        IcebergSyncSource.Reader reader =
                reader(Pattern.compile("db\\..*"), Collections.emptyList());
        reader.discover();
        catalog.dropTable(Identifier.create("db", "t"), false);
        assertThat(reader.discover()).isEmpty();
        createTable("t", Collections.emptyMap());
        write("t");
        assertThat(names(reader.discover())).containsExactly("db.t@1");
    }

    @Test
    void testAListingFailureSkipsThePollAndDropsNothing() throws Exception {
        createTable("t", Collections.emptyMap());
        write("t");
        FailingListCatalog failing = new FailingListCatalog(catalog);
        IcebergSyncSource source =
                new IcebergSyncSource(
                        () -> failing,
                        Pattern.compile("db"),
                        Pattern.compile("db\\..*"),
                        null,
                        Collections.emptyList(),
                        mirrorOptions,
                        true,
                        Duration.ofSeconds(1));
        IcebergSyncSource.Reader reader = source.newReader();
        reader.start();
        assertThat(names(reader.discover())).containsExactly("db.t@1");
        failing.fail = true;
        assertThat(reader.discover()).as("a failed poll emits nothing").isEmpty();
        assertThat(reader.discover()).as("a second failed poll emits nothing").isEmpty();
        failing.fail = false;
        assertThat(reader.discover()).as("still there, no drop, nothing new").isEmpty();
    }

    @Test
    void testARollbackThatReusesTheSnapshotIdIsEmittedAgain() throws Exception {
        createTable("t", Collections.emptyMap());
        write("t");
        write("t");
        IcebergSyncSource.Reader reader =
                reader(Pattern.compile("db\\..*"), Collections.emptyList());
        assertThat(names(reader.discover())).containsExactly("db.t@2");
        FileStoreTable table = (FileStoreTable) catalog.getTable(Identifier.create("db", "t"));
        try (IcebergSync sync =
                new IcebergSync(IcebergSync.withMirrorDefaults(table, mirrorOptions))) {
            sync.sync(2);
        }
        assertThat(reader.discover()).isEmpty();
        table.rollbackTo(1);
        write("t");
        assertThat(names(reader.discover())).containsExactly("db.t@2");
    }

    @Test
    void testALaggingOperatorIsNotReEmittedEverything() throws Exception {
        createTable("t", Collections.emptyMap());
        write("t");
        IcebergSyncSource.Reader reader =
                reader(Pattern.compile("db\\..*"), Collections.emptyList());
        assertThat(names(reader.discover())).containsExactly("db.t@1");
        FileStoreTable table = (FileStoreTable) catalog.getTable(Identifier.create("db", "t"));
        try (IcebergSync sync =
                new IcebergSync(IcebergSync.withMirrorDefaults(table, mirrorOptions))) {
            sync.sync(1);
        }
        write("t");
        write("t");
        assertThat(names(reader.discover())).containsExactly("db.t@2", "db.t@3");
        assertThat(reader.discover()).isEmpty();
    }

    @Test
    void testTablesAreInterleaved() throws Exception {
        createTable("a", Collections.emptyMap());
        createTable("b", Collections.emptyMap());
        for (int i = 0; i < 3; i++) {
            write("a");
        }
        for (int i = 0; i < 2; i++) {
            write("b");
        }
        // a hint of 0 makes every snapshot from 1 pending; a first sync would take the latest only
        for (String name : new String[] {"a", "b"}) {
            FileStoreTable table = (FileStoreTable) catalog.getTable(Identifier.create("db", name));
            Path hint =
                    new Path(
                            IcebergCommitCallback.catalogTableMetadataPath(
                                    IcebergSync.withMirrorDefaults(table, mirrorOptions)),
                            "version-hint.text");
            table.fileIO().overwriteFileUtf8(hint, "0");
        }
        IcebergSyncSource.Reader reader =
                reader(Pattern.compile("db\\..*"), Collections.emptyList());
        assertThat(names(reader.discover()))
                .containsExactly("db.a@1", "db.b@1", "db.a@2", "db.b@2", "db.a@3");
    }

    @Test
    void testABatchListingFailureFails() throws Exception {
        createTable("t", Collections.emptyMap());
        FailingListCatalog failing = new FailingListCatalog(catalog);
        failing.fail = true;
        IcebergSyncSource source =
                new IcebergSyncSource(
                        () -> failing,
                        Pattern.compile("db"),
                        Pattern.compile("db\\..*"),
                        null,
                        Collections.emptyList(),
                        mirrorOptions,
                        false,
                        Duration.ofSeconds(1));
        IcebergSyncSource.Reader reader = source.newReader();
        reader.start();
        assertThatThrownBy(reader::discover).hasMessageContaining("listing failed");
    }

    IcebergSyncSource.Reader reader(Catalog source, boolean streaming, MetricGroup metrics) {
        IcebergSyncSource syncSource =
                new IcebergSyncSource(
                        () -> source,
                        Pattern.compile("db.*"),
                        Pattern.compile("db.*\\..*"),
                        null,
                        Collections.emptyList(),
                        mirrorOptions,
                        streaming,
                        Duration.ofSeconds(1));
        IcebergSyncSource.Reader reader = syncSource.newReader(metrics);
        reader.start();
        return reader;
    }

    static long counter(RecordingMetricGroup metrics, String name) {
        return metrics.counters.get(name).getCount();
    }

    @Test
    void testATableWhoseGetTableFailsIsSkippedAndTheOthersGoOn() throws Exception {
        createTable("bad", Collections.emptyMap());
        createTable("good", Collections.emptyMap());
        write("bad");
        write("good");
        FailingListCatalog failing = new FailingListCatalog(catalog);
        RecordingMetricGroup metrics = new RecordingMetricGroup();
        IcebergSyncSource.Reader reader = reader(failing, true, metrics);
        assertThat(names(reader.discover())).containsExactlyInAnyOrder("db.bad@1", "db.good@1");
        write("bad");
        write("good");
        failing.failGetTable.add("db.bad");
        assertThat(names(reader.discover())).containsExactly("db.good@2");
        assertThat(names(reader.discover())).as("still skipped, not dropped").isEmpty();
        assertThat(counter(metrics, "table_failures")).isEqualTo(2L);
        failing.failGetTable.clear();
        assertThat(names(reader.discover())).containsExactly("db.bad@2");
    }

    @Test
    void testATableWhoseSnapshotReadFailsIsSkippedAndTheOthersGoOn() throws Exception {
        createTable("bad", Collections.emptyMap());
        createTable("good", Collections.emptyMap());
        write("bad");
        write("good");
        FailingListCatalog failing = new FailingListCatalog(catalog);
        RecordingMetricGroup metrics = new RecordingMetricGroup();
        IcebergSyncSource.Reader reader = reader(failing, true, metrics);
        assertThat(names(reader.discover())).containsExactlyInAnyOrder("db.bad@1", "db.good@1");
        write("bad");
        write("good");
        failing.failSnapshots.add("db.bad");
        assertThat(names(reader.discover())).containsExactly("db.good@2");
        assertThat(names(reader.discover())).as("still skipped, not dropped").isEmpty();
        assertThat(counter(metrics, "table_failures")).isEqualTo(2L);
        failing.failSnapshots.clear();
        assertThat(names(reader.discover())).containsExactly("db.bad@2");
    }

    @Test
    void testABatchTableFailureFails() throws Exception {
        createTable("bad", Collections.emptyMap());
        write("bad");
        FailingListCatalog failing = new FailingListCatalog(catalog);
        failing.failSnapshots.add("db.bad");
        IcebergSyncSource.Reader reader = reader(failing, false, null);
        assertThatThrownBy(reader::discover).hasMessageContaining("latest snapshot id");
    }

    /**
     * Forwards to a catalog. It fails listDatabases, the listing of a database, getTable of a
     * table, or the snapshot reads of a table on demand.
     */
    static final class FailingListCatalog extends DelegateCatalog {
        boolean fail;
        final Set<String> failListing = new HashSet<>();
        final Set<String> failGetTable = new HashSet<>();
        final Set<String> failSnapshots = new HashSet<>();

        FailingListCatalog(Catalog wrapped) {
            super(wrapped);
        }

        @Override
        public CatalogLoader catalogLoader() {
            return wrapped.catalogLoader();
        }

        @Override
        public List<String> listDatabases() {
            if (fail) {
                throw new RuntimeException("listing failed");
            }
            return super.listDatabases();
        }

        @Override
        public List<String> listTables(String databaseName) throws DatabaseNotExistException {
            if (failListing.contains(databaseName)) {
                throw new RuntimeException("listing of " + databaseName + " failed");
            }
            return super.listTables(databaseName);
        }

        @Override
        public Table getTable(Identifier identifier) throws TableNotExistException {
            if (failGetTable.contains(identifier.getFullName())) {
                throw new RuntimeException("getTable failed");
            }
            Table table = super.getTable(identifier);
            if (!failSnapshots.contains(identifier.getFullName())) {
                return table;
            }
            FileStoreTable fileStoreTable = (FileStoreTable) table;
            return FileStoreTableFactory.create(
                    new FailingSnapshotFileIO(),
                    fileStoreTable.location(),
                    fileStoreTable.schema(),
                    fileStoreTable.catalogEnvironment());
        }
    }

    /** Keeps the counters of the source; the per-table groups are not kept. */
    static final class RecordingMetricGroup extends UnregisteredMetricsGroup {
        final Map<String, Counter> counters = new HashMap<>();

        @Override
        public MetricGroup addGroup(String name) {
            return this;
        }

        @Override
        public MetricGroup addGroup(String key, String value) {
            return new UnregisteredMetricsGroup();
        }

        @Override
        public Counter counter(String name) {
            return counters.computeIfAbsent(name, n -> new SimpleCounter());
        }
    }

    /** A local file system on which every read of the snapshot directory fails. */
    static final class FailingSnapshotFileIO extends LocalFileIO {
        private static final long serialVersionUID = 1L;

        private static void check(Path path) throws IOException {
            if (path.toString().contains("/snapshot")) {
                throw new IOException("snapshot read failed: " + path);
            }
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            check(path);
            return super.newInputStream(path);
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            check(path);
            return super.listStatus(path);
        }

        @Override
        public boolean exists(Path path) throws IOException {
            check(path);
            return super.exists(path);
        }
    }
}
