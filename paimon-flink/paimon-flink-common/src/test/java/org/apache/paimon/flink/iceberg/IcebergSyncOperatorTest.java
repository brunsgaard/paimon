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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.utils.TestingMetricUtils;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.iceberg.IcebergSync;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IcebergSyncOperatorTest {

    /** While set, the sync of table {@code bad} fails. */
    private static final AtomicBoolean failing = new AtomicBoolean(true);

    private static final AtomicInteger syncsBuilt = new AtomicInteger();

    @TempDir java.nio.file.Path tempDir;

    private Catalog catalog;
    private Map<String, String> mirrorOptions;
    private IcebergSyncOperator operator;
    private KeyedOneInputStreamOperatorTestHarness<String, IcebergSyncTask, Void> harness;

    private Options catalogOptions() {
        Options catalogOptions = new Options();
        catalogOptions.set("warehouse", new Path(tempDir.toString(), "warehouse").toString());
        catalogOptions.set("cache-enabled", "false");
        return catalogOptions;
    }

    private IcebergSyncOperator newOperator(String storage) {
        return newOperator(storage, true);
    }

    private IcebergSyncOperator newOperator(String storage, boolean isStreaming) {
        Options catalogOptions = catalogOptions();
        mirrorOptions =
                Collections.singletonMap(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), storage);
        return new IcebergSyncOperator(
                () -> CatalogFactory.createCatalog(CatalogContext.create(catalogOptions)),
                mirrorOptions,
                new HashMap<>(Collections.singletonMap("job.key", "job.value")),
                isStreaming);
    }

    private IcebergSyncOperator operatorWithStorage(String storage) {
        IcebergSyncOperator operator = newOperator(storage);
        operator.open(null);
        return operator;
    }

    /** A harness around an operator whose sync of table {@code bad} fails while failing. */
    private void startHarness(String storage) throws Exception {
        startHarness(storage, true);
    }

    private void startHarness(String storage, boolean isStreaming) throws Exception {
        startHarness(newOperator(storage, isStreaming));
    }

    private void startHarness(IcebergSyncOperator newOperator) throws Exception {
        failing.set(true);
        syncsBuilt.set(0);
        catalog = CatalogFactory.createCatalog(CatalogContext.create(catalogOptions()));
        catalog.createDatabase("db", true);
        operator = newOperator;
        operator.syncFactory =
                table -> {
                    syncsBuilt.incrementAndGet();
                    if (table.name().equals("bad") && failing.get()) {
                        throw new IllegalStateException("The mirror cannot be written.");
                    }
                    return new IcebergSync(table);
                };
        harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(operator),
                        IcebergSyncTask::fullName,
                        Types.STRING);
        harness.open();
    }

    @BeforeEach
    void resetListeners() {
        RecordingListener.events.clear();
        RecordingListener.configuration = null;
        RecordingListener.recording = true;
        ThrowingListener.calls.set(0);
    }

    @AfterEach
    void tearDown() throws Exception {
        RecordingListener.recording = false;
        RecordingListener.events.clear();
        RecordingListener.configuration = null;
        ThrowingListener.calls.set(0);
        ThrowingListener.armed = false;
        ThrowingListener.linkageError = false;
        ThrowingListener.openFails = false;
        RecordingDropperFactory.failing.set(false);
        RecordingDropperFactory.exists.set(true);
        if (harness != null) {
            harness.close();
        }
        if (catalog != null) {
            catalog.close();
        }
    }

    private void createTable(String name) throws Exception {
        createTable(name, Collections.emptyMap());
    }

    private void createTable(String name, Map<String, String> options) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .options(options)
                        .build();
        catalog.createTable(Identifier.create("db", name), schema, false);
        write(name);
    }

    private void write(String name) throws Exception {
        FileStoreTable table = (FileStoreTable) catalog.getTable(Identifier.create("db", name));
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(1, BinaryString.fromString("r")));
            commit.commit(write.prepareCommit());
        }
    }

    private FileStoreTable mirrored(String name) throws Exception {
        FileStoreTable table = (FileStoreTable) catalog.getTable(Identifier.create("db", name));
        return IcebergSync.withMirrorDefaults(table, mirrorOptions);
    }

    private static IcebergSyncTask task(String database, String table, long snapshotId) {
        return new IcebergSyncTask(database, table, snapshotId);
    }

    private static long listenerFailures(IcebergSyncOperatorTest test) {
        return TestingMetricUtils.getCounter(test.metrics(), "listener_failures").getCount();
    }

    private static List<String> syncedEvents() {
        return RecordingListener.events.stream()
                .filter(e -> e.startsWith("synced"))
                .collect(Collectors.toList());
    }

    @Test
    void testTheListenerHearsEverySyncAndDrop() throws Exception {
        RecordingListener.events.clear();
        startHarness("table-location");
        createTable("t");
        harness.processElement(task("db", "t", 1), 0);
        IcebergSyncOperator dropping = operatorWithStorage("recording");
        dropping.syncTask(IcebergSyncTask.drop("db", "t"));
        dropping.close();
        assertThat(RecordingListener.events)
                .containsExactly("open", "synced db.t@1 -> db.t", "open", "dropped db.t -> db.t");
        assertThat(RecordingListener.configuration).containsEntry("job.key", "job.value");
    }

    @Test
    void testTheListenerGetsTheIcebergNamesOfTheTable() throws Exception {
        RecordingListener.events.clear();
        startHarness("table-location");
        Map<String, String> options = new HashMap<>();
        options.put(IcebergOptions.METASTORE_DATABASE.key(), "ice_db");
        options.put(IcebergOptions.METASTORE_TABLE.key(), "ice_t");
        createTable("named", options);
        harness.processElement(task("db", "named", 1), 0);
        assertThat(syncedEvents()).containsExactly("synced db.named@1 -> ice_db.ice_t");

        IcebergSyncOperator dropping = operatorWithStorage("recording");
        dropping.syncTask(IcebergSyncTask.drop("db", "named", options));
        dropping.close();
        assertThat(RecordingListener.events).contains("dropped db.named -> ice_db.ice_t");
    }

    @Test
    void testADropWithoutADropperIsNotAnnounced() throws Exception {
        RecordingListener.events.clear();
        IcebergSyncOperator dropping = operatorWithStorage("table-location");
        dropping.syncTask(IcebergSyncTask.drop("db", "t"));
        dropping.close();
        assertThat(RecordingListener.events).containsExactly("open");
    }

    @Test
    void testAListenerThatThrowsDoesNotFailTheSync() throws Exception {
        RecordingListener.events.clear();
        startHarness("table-location");
        createTable("t");
        ThrowingListener.armed = true;
        harness.processElement(task("db", "t", 1), 0);
        assertThat(IcebergSync.lastMirroredSnapshot(mirrored("t"))).isEqualTo(1L);
        assertThat(operator.failures("db.t")).isEqualTo(0);
        assertThat(listenerFailures(this)).isEqualTo(1L);
        assertThat(syncedEvents()).as("the other listener still hears it").hasSize(1);
        assertThat(TestingMetricUtils.getCounter(metrics(), "sync_failures").getCount())
                .isEqualTo(0L);
    }

    @Test
    void testAListenerThatCannotLoadAClassDoesNotFailTheDrop() throws Exception {
        RecordingListener.events.clear();
        ThrowingListener.linkageError = true;
        startHarness("recording");
        harness.processElement(IcebergSyncTask.drop("db", "gone"), 0);
        assertThat(TestingMetricUtils.getCounter(metrics(), "mirrors_dropped").getCount())
                .isEqualTo(1L);
        assertThat(listenerFailures(this)).isEqualTo(1L);
        assertThat(operator.failures("db.gone")).isEqualTo(0);
        assertThat(RecordingListener.events).contains("dropped db.gone -> db.gone");
    }

    @Test
    void testAListenerThatCannotOpenIsNotCalled() throws Exception {
        RecordingListener.events.clear();
        ThrowingListener.openFails = true;
        ThrowingListener.calls.set(0);
        startHarness("table-location");
        createTable("t");
        harness.processElement(task("db", "t", 1), 0);
        assertThat(IcebergSync.lastMirroredSnapshot(mirrored("t"))).isEqualTo(1L);
        assertThat(ThrowingListener.calls.get()).isEqualTo(0);
        assertThat(listenerFailures(this)).isEqualTo(1L);
        assertThat(syncedEvents()).hasSize(1);
    }

    @Test
    void testAProviderThatCannotBeLoadedDoesNotFailTheJob() throws Exception {
        java.nio.file.Path services = tempDir.resolve("providers/META-INF/services");
        java.nio.file.Files.createDirectories(services);
        java.nio.file.Files.write(
                services.resolve("org.apache.paimon.iceberg.IcebergSyncListener"),
                "org.apache.paimon.flink.iceberg.NoSuchListener\n"
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8));
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try (java.net.URLClassLoader loader =
                new java.net.URLClassLoader(
                        new java.net.URL[] {tempDir.resolve("providers").toUri().toURL()},
                        original)) {
            Thread.currentThread().setContextClassLoader(loader);
            startHarness("table-location");
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
        createTable("t");
        harness.processElement(task("db", "t", 1), 0);
        assertThat(IcebergSync.lastMirroredSnapshot(mirrored("t"))).isEqualTo(1L);
        assertThat(listenerFailures(this)).isEqualTo(1L);
        assertThat(syncedEvents()).containsExactly("synced db.t@1 -> db.t");
    }

    @Test
    void testARetryThatSucceedsCallsTheListenerOnce() throws Exception {
        RecordingListener.events.clear();
        startHarness("table-location");
        createTable("bad");
        harness.processElement(task("db", "bad", 1), 0);
        assertThat(syncedEvents()).isEmpty();
        failing.set(false);
        harness.setProcessingTime(30_000);
        assertThat(syncedEvents()).containsExactly("synced db.bad@1 -> db.bad");
    }

    @Test
    void testARetryWhoseHintCannotBeReadCallsNoListener() throws Exception {
        RecordingListener.events.clear();
        startHarness("table-location");
        operator.mirroredIdReader =
                table -> {
                    throw new java.io.UncheckedIOException(new java.io.IOException("read"));
                };
        createTable("bad");
        harness.processElement(task("db", "bad", 1), 0);
        failing.set(false);
        harness.setProcessingTime(30_000);
        assertThat(IcebergSync.lastMirroredSnapshot(mirrored("bad"))).isEqualTo(1L);
        assertThat(syncedEvents()).isEmpty();
    }

    @Test
    void testADropTaskDropsTheMirror() throws Exception {
        RecordingDropperFactory.drops.clear();
        IcebergSyncOperator operator = operatorWithStorage("recording");
        operator.syncTask(IcebergSyncTask.drop("db", "t"));
        operator.close();
        assertThat(RecordingDropperFactory.drops).containsExactly("db.t");
    }

    @Test
    void testAStorageWithoutADropperKeepsTheMetadata() throws Exception {
        RecordingDropperFactory.drops.clear();
        IcebergSyncOperator operator = operatorWithStorage("table-location");
        operator.syncTask(IcebergSyncTask.drop("db", "t"));
        operator.close();
        assertThat(RecordingDropperFactory.drops).isEmpty();
    }

    @Test
    void testAFailingTableDoesNotStopTheOthers() throws Exception {
        startHarness("table-location");
        createTable("bad");
        createTable("good");
        harness.processElement(task("db", "bad", 1), 0);
        harness.processElement(task("db", "good", 1), 0);
        assertThat(IcebergSync.lastMirroredSnapshot(mirrored("good"))).isEqualTo(1L);
        assertThat(IcebergSync.lastMirroredSnapshot(mirrored("bad"))).isEqualTo(-1L);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);
    }

    private MetricGroup metrics() {
        return harness.getOneInputOperator().getMetricGroup().addGroup("iceberg_metadata");
    }

    private Object tableGauge(String table, String name) {
        return TestingMetricUtils.getGauge(metrics().addGroup("table", table), name).getValue();
    }

    @Test
    void testTheMetricsFollowASyncAndAHold() throws Exception {
        startHarness("table-location");
        createTable("bad");
        createTable("good");
        harness.processElement(task("db", "good", 1), 0);
        assertThat(tableGauge("db.good", "mirrored_snapshot_id")).isEqualTo(1L);
        assertThat((Long) tableGauge("db.good", "mirrored_snapshot_timestamp_ms")).isPositive();
        assertThat(tableGauge("db.good", "on_hold")).isEqualTo(0);
        assertThat(TestingMetricUtils.getCounter(metrics(), "snapshots_synced").getCount())
                .isEqualTo(1L);

        harness.processElement(task("db", "bad", 1), 0);
        assertThat(tableGauge("db.bad", "on_hold")).isEqualTo(1);
        assertThat(tableGauge("db.bad", "mirrored_snapshot_id")).isEqualTo(-1L);
        assertThat(TestingMetricUtils.getCounter(metrics(), "sync_failures").getCount())
                .isEqualTo(1L);

        failing.set(false);
        harness.setProcessingTime(30_000);
        assertThat(tableGauge("db.bad", "on_hold")).isEqualTo(0);
        assertThat(tableGauge("db.bad", "mirrored_snapshot_id")).isEqualTo(1L);
        assertThat(TestingMetricUtils.getCounter(metrics(), "snapshots_synced").getCount())
                .isEqualTo(2L);
        assertThat(TestingMetricUtils.getCounter(metrics(), "sync_failures").getCount())
                .isEqualTo(1L);
    }

    @Test
    void testARetryWhoseMetricReadFailsIsStillARecovery() throws Exception {
        startHarness("table-location");
        operator.mirroredIdReader =
                table -> {
                    throw new java.io.UncheckedIOException(new java.io.IOException("read"));
                };
        createTable("bad");
        harness.processElement(task("db", "bad", 1), 0);
        failing.set(false);
        harness.setProcessingTime(30_000);
        assertThat(IcebergSync.lastMirroredSnapshot(mirrored("bad"))).isEqualTo(1L);
        assertThat(operator.failures("db.bad")).isEqualTo(0);
        assertThat(tableGauge("db.bad", "on_hold")).isEqualTo(0);
        assertThat(TestingMetricUtils.getCounter(metrics(), "sync_failures").getCount())
                .isEqualTo(1L);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
    }

    @Test
    void testADropIsCounted() throws Exception {
        RecordingDropperFactory.drops.clear();
        startHarness("recording");
        harness.processElement(IcebergSyncTask.drop("db", "gone"), 0);
        assertThat(TestingMetricUtils.getCounter(metrics(), "mirrors_dropped").getCount())
                .isEqualTo(1L);
    }

    @Test
    void testAFailedTableIsRetriedFromTheHint() throws Exception {
        startHarness("table-location");
        createTable("bad");
        harness.processElement(task("db", "bad", 1), 0);
        write("bad");
        harness.processElement(task("db", "bad", 2), 1000);
        assertThat(syncsBuilt.get()).as("a task on hold is not synced").isEqualTo(1);
        failing.set(false);
        harness.setProcessingTime(30_000);
        assertThat(IcebergSync.lastMirroredSnapshot(mirrored("bad")))
                .as("both snapshots, from the hint")
                .isEqualTo(2L);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
        assertThat(operator.failures("db.bad")).isEqualTo(0);
    }

    @Test
    void testTheDelayGrowsAndThenStays() throws Exception {
        startHarness("table-location");
        createTable("bad");
        harness.processElement(task("db", "bad", 1), 0);
        harness.setProcessingTime(30_000);
        harness.setProcessingTime(150_000);
        harness.setProcessingTime(750_000);
        harness.setProcessingTime(1_350_000);
        assertThat(operator.failures("db.bad")).isEqualTo(5);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);
        harness.setProcessingTime(1_949_999);
        assertThat(operator.failures("db.bad")).as("ten minutes, not less").isEqualTo(5);
    }

    @Test
    void testABatchSyncFailureFailsTheJob() throws Exception {
        startHarness("table-location", false);
        createTable("bad");
        assertThatThrownBy(() -> harness.processElement(task("db", "bad", 1), 0))
                .hasMessageContaining("The mirror cannot be written.");
        assertThat(operator.failures("db.bad")).as("nothing is held").isEqualTo(0);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
    }

    @Test
    void testABatchDropFailureFailsTheJob() throws Exception {
        RecordingDropperFactory.failing.set(true);
        startHarness("recording", false);
        assertThatThrownBy(() -> harness.processElement(IcebergSyncTask.drop("db", "gone"), 0))
                .isInstanceOf(Exception.class);
        assertThat(operator.failures("db.gone")).as("nothing is held").isEqualTo(0);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
    }

    @Test
    void testAHeldSyncOfAGoneTableIsReleased() throws Exception {
        startHarness("table-location");
        createTable("bad");
        harness.processElement(task("db", "bad", 1), 0);
        catalog.dropTable(Identifier.create("db", "bad"), false);
        harness.setProcessingTime(30_000);
        assertThat(operator.failures("db.bad")).isEqualTo(0);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
    }

    @Test
    void testAFailingDropIsRetriedAsADrop() throws Exception {
        RecordingDropperFactory.drops.clear();
        RecordingDropperFactory.failing.set(true);
        startHarness("recording");
        harness.processElement(IcebergSyncTask.drop("db", "gone"), 0);
        assertThat(RecordingDropperFactory.drops).isEmpty();
        assertThat(operator.failures("db.gone")).isEqualTo(1);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);

        harness.setProcessingTime(30_000);
        assertThat(operator.failures("db.gone")).isEqualTo(2);

        RecordingDropperFactory.failing.set(false);
        harness.setProcessingTime(150_000);
        assertThat(RecordingDropperFactory.drops).containsExactly("db.gone");
        assertThat(operator.failures("db.gone")).isEqualTo(0);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
        assertThat(syncsBuilt.get()).as("the retry of a drop is no sync").isEqualTo(0);
    }

    @Test
    void testAFailedDropOfAGoneTableIsDone() throws Exception {
        RecordingDropperFactory.drops.clear();
        RecordingDropperFactory.failing.set(true);
        RecordingDropperFactory.exists.set(false);
        startHarness("recording");
        harness.processElement(IcebergSyncTask.drop("db", "gone"), 0);
        assertThat(operator.failures("db.gone")).as("nothing is held").isEqualTo(0);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
        assertThat(TestingMetricUtils.getCounter(metrics(), "mirrors_dropped").getCount())
                .isEqualTo(1L);
    }

    @Test
    void testAFailedDropOfAPresentTableIsHeld() throws Exception {
        RecordingDropperFactory.failing.set(true);
        RecordingDropperFactory.exists.set(true);
        startHarness("recording");
        harness.processElement(IcebergSyncTask.drop("db", "gone"), 0);
        assertThat(operator.failures("db.gone")).isEqualTo(1);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);
    }

    @Test
    void testAFailedDropIsHeldWhenTheCheckFails() throws Exception {
        RecordingDropperFactory.failing.set(true);
        RecordingDropperFactory.exists.set(null);
        startHarness("recording");
        harness.processElement(IcebergSyncTask.drop("db", "gone"), 0);
        assertThat(operator.failures("db.gone")).isEqualTo(1);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);
    }

    @Test
    void testAHeldDropOfATableThatIsGoneLaterIsDone() throws Exception {
        RecordingDropperFactory.failing.set(true);
        startHarness("recording");
        harness.processElement(IcebergSyncTask.drop("db", "gone"), 0);
        assertThat(operator.failures("db.gone")).isEqualTo(1);

        RecordingDropperFactory.exists.set(false);
        harness.setProcessingTime(30_000);
        assertThat(operator.failures("db.gone")).isEqualTo(0);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
    }

    @Test
    void testASuccessfulDropDoesNotCheckTheExistence() throws Exception {
        RecordingDropperFactory.drops.clear();
        RecordingDropperFactory.exists.set(null);
        startHarness("recording");
        harness.processElement(IcebergSyncTask.drop("db", "gone"), 0);
        assertThat(RecordingDropperFactory.drops).containsExactly("db.gone");
        assertThat(operator.failures("db.gone")).isEqualTo(0);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
    }

    @Test
    void testADropReplacesAHeldSync() throws Exception {
        RecordingDropperFactory.drops.clear();
        startHarness("recording");
        createTable("bad");
        harness.processElement(task("db", "bad", 1), 0);
        assertThat(operator.failures("db.bad")).isEqualTo(1);

        catalog.dropTable(Identifier.create("db", "bad"), false);
        RecordingDropperFactory.failing.set(true);
        harness.setProcessingTime(10_000);
        harness.processElement(IcebergSyncTask.drop("db", "bad"), 10_000);
        assertThat(harness.numProcessingTimeTimers())
                .as("the timer of the held sync is replaced")
                .isEqualTo(1);

        RecordingDropperFactory.failing.set(false);
        harness.setProcessingTime(30_000);
        assertThat(RecordingDropperFactory.drops).as("the old sync timer is gone").isEmpty();
        harness.setProcessingTime(40_000);
        assertThat(RecordingDropperFactory.drops).containsExactly("db.bad");
        assertThat(operator.failures("db.bad")).isEqualTo(0);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
    }

    /**
     * An operator on the recording storage. It records the sync tasks it syncs, and their sync
     * fails while {@link #failing} is set.
     */
    private IcebergSyncOperator recordingSyncs(List<String> synced) {
        Options catalogOptions = catalogOptions();
        mirrorOptions =
                Collections.singletonMap(
                        IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "recording");
        return new IcebergSyncOperator(
                () -> CatalogFactory.createCatalog(CatalogContext.create(catalogOptions)),
                mirrorOptions,
                new HashMap<>(),
                true) {
            private static final long serialVersionUID = 1L;

            @Override
            public void syncTask(IcebergSyncTask task) throws Exception {
                if (task.isDrop()) {
                    super.syncTask(task);
                    return;
                }
                if (failing.get()) {
                    throw new IllegalStateException("The mirror cannot be written.");
                }
                synced.add(task.fullName() + "@" + task.snapshotId);
            }
        };
    }

    @Test
    void testASyncTaskReleasesAHeldDropAndIsSynced() throws Exception {
        RecordingDropperFactory.drops.clear();
        RecordingDropperFactory.failing.set(true);
        List<String> synced = new java.util.ArrayList<>();
        startHarness(recordingSyncs(synced));
        failing.set(false);
        harness.processElement(IcebergSyncTask.drop("db", "back"), 0);
        assertThat(operator.failures("db.back")).isEqualTo(1);
        createTable("back");
        harness.processElement(task("db", "back", 1), 1000);
        assertThat(synced).containsExactly("db.back@1");
        assertThat(operator.failures("db.back")).as("the drop is released").isEqualTo(0);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
        RecordingDropperFactory.failing.set(false);
        harness.setProcessingTime(30_000);
        assertThat(RecordingDropperFactory.drops).isEmpty();
    }

    @Test
    void testASyncTaskThatFailsBehindAHeldDropIsHeldAsASync() throws Exception {
        RecordingDropperFactory.drops.clear();
        RecordingDropperFactory.failing.set(true);
        List<String> synced = new java.util.ArrayList<>();
        startHarness(recordingSyncs(synced));
        harness.processElement(IcebergSyncTask.drop("db", "back"), 0);
        createTable("back");
        harness.processElement(task("db", "back", 1), 1000);
        assertThat(operator.failures("db.back")).as("a new hold, for the sync").isEqualTo(1);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);
        RecordingDropperFactory.failing.set(false);
        harness.setProcessingTime(31_000);
        assertThat(RecordingDropperFactory.drops).as("nothing is dropped").isEmpty();
        assertThat(operator.failures("db.back"))
                .as("retried as a sync, not released as a drop")
                .isEqualTo(2);
    }

    @Test
    void testAHeldDropOfARecreatedTableIsReleased() throws Exception {
        RecordingDropperFactory.drops.clear();
        RecordingDropperFactory.failing.set(true);
        startHarness("recording");
        harness.processElement(IcebergSyncTask.drop("db", "back"), 0);
        createTable("back");
        RecordingDropperFactory.failing.set(false);
        harness.setProcessingTime(30_000);
        assertThat(RecordingDropperFactory.drops).isEmpty();
        assertThat(operator.failures("db.back")).isEqualTo(0);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
    }
}
