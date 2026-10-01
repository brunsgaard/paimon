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
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

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
        Options catalogOptions = catalogOptions();
        mirrorOptions =
                Collections.singletonMap(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), storage);
        return new IcebergSyncOperator(
                () -> CatalogFactory.createCatalog(CatalogContext.create(catalogOptions)),
                mirrorOptions);
    }

    private IcebergSyncOperator operatorWithStorage(String storage) {
        IcebergSyncOperator operator = newOperator(storage);
        operator.open(null);
        return operator;
    }

    /** A harness around an operator whose sync of table {@code bad} fails while failing. */
    private void startHarness(String storage) throws Exception {
        failing.set(true);
        syncsBuilt.set(0);
        catalog = CatalogFactory.createCatalog(CatalogContext.create(catalogOptions()));
        catalog.createDatabase("db", true);
        operator = newOperator(storage);
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

    @AfterEach
    void tearDown() throws Exception {
        RecordingDropperFactory.failing.set(false);
        if (harness != null) {
            harness.close();
        }
        if (catalog != null) {
            catalog.close();
        }
    }

    private void createTable(String name) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.STRING())
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

        harness.processElement(task("db", "bad", 2), 11_000);
        assertThat(operator.failures("db.bad"))
                .as("a sync task behind a held drop is ignored")
                .isEqualTo(1);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);

        RecordingDropperFactory.failing.set(false);
        harness.setProcessingTime(30_000);
        assertThat(RecordingDropperFactory.drops).as("the old sync timer is gone").isEmpty();
        harness.setProcessingTime(40_000);
        assertThat(RecordingDropperFactory.drops).containsExactly("db.bad");
        assertThat(operator.failures("db.bad")).isEqualTo(0);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
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
