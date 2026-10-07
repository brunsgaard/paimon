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

package org.apache.paimon.flink.action;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.flink.iceberg.IcebergSyncOperator;
import org.apache.paimon.flink.iceberg.IcebergSyncTask;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.IcebergCommitCallback;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.iceberg.IcebergSync;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergSnapshot;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link IcebergSyncAction}. */
public class IcebergSyncActionITCase extends ActionITCaseBase {

    private static final RowType ROW_TYPE =
            RowType.of(new DataType[] {DataTypes.INT(), DataTypes.INT()}, new String[] {"k", "v"});

    private static final String STORAGE_CONF = "metadata.iceberg.storage=table-location";

    @Test
    public void testTheOldActionNameIsGone() {
        assertThatThrownBy(
                        () ->
                                createAction(
                                        IcebergSyncAction.class,
                                        "iceberg_sync",
                                        "--warehouse",
                                        warehouse,
                                        "--database",
                                        "default",
                                        "--table",
                                        "t",
                                        "--table_conf",
                                        STORAGE_CONF))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unknown action \"iceberg_sync\"");
    }

    @Test
    public void testAPollIntervalUnderASecondIsRefused() {
        IcebergSyncAction action =
                new IcebergSyncAction(Collections.singletonMap("warehouse", warehouse));
        assertThatThrownBy(() -> action.withPollInterval(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--poll_interval must be at least 1 s");
        assertThatThrownBy(() -> action.withPollInterval(Duration.ofMillis(999)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--poll_interval must be at least 1 s");
        action.withPollInterval(Duration.ofSeconds(1));
    }

    @Test
    public void testBatchSyncsEveryMatchingTable() throws Exception {
        FileStoreTable t1 = createTable("t1", Collections.emptyMap());
        FileStoreTable t2 = createTable("t2", Collections.emptyMap());
        FileStoreTable other = createTable("skip_me", Collections.emptyMap());
        writeOne(t1, 1, 10);
        writeOne(t1, 2, 20);
        writeOne(t2, 5, 50);
        writeOne(other, 9, 90);

        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        createAction(
                        IcebergSyncAction.class,
                        "write_iceberg_metadata",
                        "--warehouse",
                        warehouse,
                        "--including_databases",
                        database,
                        "--including_tables",
                        database + "\\.t.*",
                        "--table_conf",
                        STORAGE_CONF)
                .withStreamExecutionEnvironment(env)
                .run();

        assertThat(IcebergSync.lastMirroredSnapshot(mirror(t1))).isEqualTo(2L);
        assertThat(IcebergSync.lastMirroredSnapshot(mirror(t2))).isEqualTo(1L);
        assertThat(IcebergSync.lastMirroredSnapshot(mirror(other))).isEqualTo(-1L);
    }

    @Test
    public void testBatchSyncsOnlyTablesWithTheFilteredOption() throws Exception {
        FileStoreTable on = createTable("on", Collections.singletonMap("x.enabled", "true"));
        FileStoreTable off = createTable("off", Collections.emptyMap());
        writeOne(on, 1, 10);
        writeOne(off, 2, 20);

        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        createAction(
                        IcebergSyncAction.class,
                        "write_iceberg_metadata",
                        "--warehouse",
                        warehouse,
                        "--including_databases",
                        database,
                        "--table_option_filter",
                        "x.enabled=true",
                        "--table_conf",
                        STORAGE_CONF)
                .withStreamExecutionEnvironment(env)
                .run();

        assertThat(IcebergSync.lastMirroredSnapshot(mirror(on))).isEqualTo(1L);
        assertThat(IcebergSync.lastMirroredSnapshot(mirror(off))).isEqualTo(-1L);
    }

    @Test
    public void testSingleTableForm() throws Exception {
        FileStoreTable t1 = createTable("t1", Collections.emptyMap());
        writeOne(t1, 1, 10);

        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        createAction(
                        IcebergSyncAction.class,
                        "write_iceberg_metadata",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        "t1",
                        "--table_conf",
                        STORAGE_CONF)
                .withStreamExecutionEnvironment(env)
                .run();

        assertThat(IcebergSync.lastMirroredSnapshot(mirror(t1))).isEqualTo(1L);
    }

    @Test
    public void testRefusesWithoutStorageOption() throws Exception {
        createTable("t1", Collections.emptyMap());

        assertThatThrownBy(
                        () ->
                                createAction(
                                        IcebergSyncAction.class,
                                        "write_iceberg_metadata",
                                        "--warehouse",
                                        warehouse,
                                        "--database",
                                        database,
                                        "--table",
                                        "t1"))
                .hasMessageContaining(IcebergOptions.METADATA_ICEBERG_STORAGE.key());
    }

    @Test
    public void testRefusesATableMirroredByItsWriters() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "table-location");
        createTable("t1", options);

        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        assertThatThrownBy(
                        () ->
                                createAction(
                                                IcebergSyncAction.class,
                                                "write_iceberg_metadata",
                                                "--warehouse",
                                                warehouse,
                                                "--database",
                                                database,
                                                "--table",
                                                "t1",
                                                "--table_conf",
                                                STORAGE_CONF)
                                        .withStreamExecutionEnvironment(env)
                                        .run())
                .hasStackTraceContaining("its writers already commit Iceberg metadata");
    }

    @Test
    public void testStreamingFollowsNewSnapshots() throws Exception {
        FileStoreTable t1 = createTable("t1", Collections.emptyMap());
        writeOne(t1, 1, 10);

        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder().streamingMode().build();
        createAction(
                        IcebergSyncAction.class,
                        "write_iceberg_metadata",
                        "--warehouse",
                        warehouse,
                        "--including_databases",
                        database,
                        "--including_tables",
                        database + "\\.t1",
                        "--table_conf",
                        STORAGE_CONF,
                        "--poll_interval",
                        "1 s")
                .withStreamExecutionEnvironment(env)
                .build();
        JobClient client = env.executeAsync();
        try {
            waitUntil(() -> IcebergSync.lastMirroredSnapshot(mirror(t1)) == 1L);
            writeOne(t1, 2, 20);
            writeOne(t1, 3, 30);
            waitUntil(() -> IcebergSync.lastMirroredSnapshot(mirror(t1)) == 3L);
            IcebergMetadata metadata =
                    IcebergMetadata.fromPath(
                            t1.fileIO(),
                            new Path(
                                    IcebergCommitCallback.catalogTableMetadataPath(mirror(t1)),
                                    "v3.metadata.json"));
            assertThat(metadata.snapshots().stream().map(IcebergSnapshot::snapshotId))
                    .containsExactly(1L, 2L, 3L);
        } finally {
            client.cancel().get();
        }
    }

    @Test
    public void testStreamingReportsMetricsPerTable() throws Exception {
        FileStoreTable t1 = createTable("t1", Collections.emptyMap());
        writeOne(t1, 1, 10);
        writeOne(t1, 2, 20);

        // the shared mini cluster has no reporter, so the job runs on a mini cluster of its own
        InMemoryReporter reporter = InMemoryReporter.createWithRetainedMetrics();
        Configuration conf = reporter.addToConfiguration(new Configuration());
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
        conf.set(RestOptions.PORT, 0);
        try (MiniCluster cluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setConfiguration(conf)
                                .setNumTaskManagers(1)
                                .setNumSlotsPerTaskManager(2)
                                .build())) {
            cluster.start();
            StreamExecutionEnvironment env =
                    streamExecutionEnvironmentBuilder().streamingMode().parallelism(1).build();
            env.enableCheckpointing(500);
            createAction(
                            IcebergSyncAction.class,
                            "write_iceberg_metadata",
                            "--warehouse",
                            warehouse,
                            "--including_databases",
                            database,
                            "--including_tables",
                            database + "\\.t1",
                            "--table_conf",
                            STORAGE_CONF,
                            "--poll_interval",
                            "1 s")
                    .withStreamExecutionEnvironment(env)
                    .build();
            JobGraph jobGraph = env.getStreamGraph().getJobGraph();
            JobID jobId = jobGraph.getJobID();
            cluster.submitJob(jobGraph).get();
            try {
                String table = database + ".t1";
                waitUntil(
                        () ->
                                Long.valueOf(2L)
                                        .equals(
                                                gauge(
                                                        reporter,
                                                        jobId,
                                                        "mirrored_snapshot_id",
                                                        table)));
                assertThat(gauge(reporter, jobId, "on_hold", table)).isEqualTo(0);
                waitUntil(
                        () -> {
                            Object ts =
                                    gauge(reporter, jobId, "mirrored_snapshot_timestamp_ms", table);
                            return ts != null && (Long) ts > 0L;
                        });
                waitUntil(
                        () ->
                                Long.valueOf(2L)
                                        .equals(
                                                gauge(
                                                        reporter,
                                                        jobId,
                                                        "latest_snapshot_id",
                                                        table)));
                // the next poll sees the mirror up to date
                waitUntil(
                        () ->
                                Long.valueOf(0L)
                                        .equals(
                                                gauge(
                                                        reporter,
                                                        jobId,
                                                        "pending_snapshots",
                                                        table)));
                assertThat(metric(reporter, jobId, "polls", null)).isNotNull();
            } finally {
                cluster.cancelJob(jobId).get();
            }
        }
    }

    private static Metric metric(
            InMemoryReporter reporter, JobID jobId, String name, String table) {
        String suffix = table == null ? "iceberg_metadata." + name : table + "." + name;
        return reporter.findMetrics(jobId, "iceberg_metadata").entrySet().stream()
                .filter(e -> e.getKey().contains("iceberg_metadata"))
                .filter(e -> e.getKey().endsWith(suffix))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }

    private static Object gauge(InMemoryReporter reporter, JobID jobId, String name, String table) {
        Metric metric = metric(reporter, jobId, name, table);
        return metric == null ? null : ((Gauge<?>) metric).getValue();
    }

    @Test
    public void testStreamingPicksUpATableCreatedLater() throws Exception {
        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder().streamingMode().build();
        createAction(
                        IcebergSyncAction.class,
                        "write_iceberg_metadata",
                        "--warehouse",
                        warehouse,
                        "--including_databases",
                        database,
                        "--including_tables",
                        database + "\\.late.*",
                        "--table_conf",
                        STORAGE_CONF,
                        "--poll_interval",
                        "1 s")
                .withStreamExecutionEnvironment(env)
                .build();
        JobClient client = env.executeAsync();
        try {
            Thread.sleep(2000);
            FileStoreTable late = createTable("late_t", Collections.emptyMap());
            writeOne(late, 1, 10);
            waitUntil(() -> IcebergSync.lastMirroredSnapshot(mirror(late)) == 1L);
        } finally {
            client.cancel().get();
        }
    }

    @Test
    public void testSyncOperatorSkipsAMissingTable() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "table-location");
        IcebergSyncOperator operator =
                new IcebergSyncOperator(uncachedCatalogLoader(), options, new HashMap<>());
        operator.open((OpenContext) null);
        try {
            operator.syncTask(new IcebergSyncTask(database, "nope", 1L));
        } finally {
            operator.close();
        }
    }

    @Test
    public void testStreamingFollowsARollback() throws Exception {
        FileStoreTable t1 = createTable("t1", Collections.emptyMap());
        writeOne(t1, 1, 10);
        writeOne(t1, 2, 20);
        writeOne(t1, 3, 30);

        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder().streamingMode().build();
        createAction(
                        IcebergSyncAction.class,
                        "write_iceberg_metadata",
                        "--warehouse",
                        warehouse,
                        "--including_databases",
                        database,
                        "--including_tables",
                        database + "\\.t1",
                        "--table_conf",
                        STORAGE_CONF,
                        "--poll_interval",
                        "1 s")
                .withStreamExecutionEnvironment(env)
                .build();
        JobClient client = env.executeAsync();
        try {
            waitUntil(() -> IcebergSync.lastMirroredSnapshot(mirror(t1)) == 3L);

            // the table goes back to snapshot 1 and reuses id 2 with a different row set
            t1.rollbackTo(1);
            writeRows(t1, 2, GenericRow.of(4, 40), GenericRow.of(5, 50));
            waitUntil(() -> "3".equals(totalRecords(t1, 2)));
            assertThat(IcebergSync.lastMirroredSnapshot(mirror(t1))).isEqualTo(2L);
        } finally {
            client.cancel().get();
        }
    }

    @Test
    public void testStreamingSkipsATableWhoseMirrorOptionsAreRefused() throws Exception {
        FileStoreTable t1 = createTable("t1", Collections.emptyMap());
        Map<String, String> vectors = new HashMap<>();
        vectors.put("deletion-vectors.enabled", "true");
        vectors.put("deletion-vectors.bitmap64", "true");
        vectors.put("bucket", "1");
        createFileStoreTable(
                "t2",
                ROW_TYPE,
                Collections.emptyList(),
                Collections.singletonList("k"),
                Collections.emptyList(),
                vectors);
        writeOne(t1, 1, 10);

        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder().streamingMode().build();
        createAction(
                        IcebergSyncAction.class,
                        "write_iceberg_metadata",
                        "--warehouse",
                        warehouse,
                        "--including_databases",
                        database,
                        "--including_tables",
                        database + "\\.t.*",
                        "--table_conf",
                        STORAGE_CONF,
                        "--table_conf",
                        "metadata.iceberg.format-version=2",
                        "--poll_interval",
                        "1 s")
                .withStreamExecutionEnvironment(env)
                .build();
        JobClient client = env.executeAsync();
        try {
            // t2 is refused (format version 2 on a deletion-vector table); t1 keeps syncing
            waitUntil(() -> IcebergSync.lastMirroredSnapshot(mirror(t1)) == 1L);
            writeOne(t1, 2, 20);
            waitUntil(() -> IcebergSync.lastMirroredSnapshot(mirror(t1)) == 2L);
            Thread.sleep(3000);
            assertThat(client.getJobStatus().get()).isEqualTo(JobStatus.RUNNING);
        } finally {
            client.cancel().get();
        }
    }

    @Test
    public void testSyncOperatorFollowsOptionChanges() throws Exception {
        Map<String, String> pk = new HashMap<>();
        pk.put("bucket", "1");
        FileStoreTable table =
                createFileStoreTable(
                        "pk",
                        ROW_TYPE,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        Collections.emptyList(),
                        pk);
        writeOne(table, 1, 10);
        Map<String, String> options = new HashMap<>();
        options.put(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "table-location");
        IcebergSyncOperator operator =
                new IcebergSyncOperator(uncachedCatalogLoader(), options, new HashMap<>());
        operator.open((OpenContext) null);
        try {
            operator.syncTask(new IcebergSyncTask(database, "pk", 1L));
            assertThat(metadata(table, 1).formatVersion()).isEqualTo(2);

            // the table turns on deletion vectors; the mirror must move to format version 3
            catalog.alterTable(
                    Identifier.create(database, "pk"),
                    Collections.singletonList(
                            SchemaChange.setOption("deletion-vectors.modifiable", "true")),
                    false);
            catalog.alterTable(
                    Identifier.create(database, "pk"),
                    Arrays.asList(
                            SchemaChange.setOption("deletion-vectors.enabled", "true"),
                            SchemaChange.setOption("deletion-vectors.bitmap64", "true")),
                    false);
            FileStoreTable altered = getFileStoreTable("pk");
            String user = UUID.randomUUID().toString();
            try (TableWriteImpl<?> write =
                            altered.newWrite(user)
                                    .withIOManager(new IOManagerImpl(warehouse + "/tmp"));
                    TableCommitImpl commit = altered.newCommit(user)) {
                write.write(GenericRow.of(2, 20));
                commit.commit(2, write.prepareCommit(false, 2));
            }
            operator.syncTask(new IcebergSyncTask(database, "pk", 2L));
            assertThat(metadata(altered, 2).formatVersion()).isEqualTo(3);
        } finally {
            operator.close();
        }
    }

    /** The catalog an action gives its operators: ActionBase turns the table cache off. */
    private CatalogLoader uncachedCatalogLoader() {
        String warehouse = this.warehouse;
        return () -> {
            CatalogContext context = CatalogContext.create(new Path(warehouse));
            context.options().set(CatalogOptions.CACHE_ENABLED, false);
            return CatalogFactory.createCatalog(context);
        };
    }

    private IcebergMetadata metadata(FileStoreTable table, long version) {
        return IcebergMetadata.fromPath(
                table.fileIO(),
                new Path(
                        IcebergCommitCallback.catalogTableMetadataPath(mirror(table)),
                        "v" + version + ".metadata.json"));
    }

    private String totalRecords(FileStoreTable table, long version) {
        try {
            return metadata(table, version).currentSnapshot().summary().get("total-records");
        } catch (Exception e) {
            return null;
        }
    }

    private void writeRows(FileStoreTable table, long commitIdentifier, GenericRow... rows)
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

    private static void waitUntil(BooleanSupplier condition) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("condition not met in 60 s");
            }
            Thread.sleep(200);
        }
    }

    // ------------------------------------------------------------------------------------------

    /** An append-only table without mirror options, as a writer would create it. */
    private FileStoreTable createTable(String name, Map<String, String> options) throws Exception {
        Map<String, String> withBucket = new HashMap<>(options);
        withBucket.put("bucket", "-1");
        return createFileStoreTable(
                name,
                ROW_TYPE,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                withBucket);
    }

    private FileStoreTable mirror(FileStoreTable table) {
        Map<String, String> mirror = new HashMap<>();
        mirror.put(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), "table-location");
        return table.copy(mirror);
    }

    private void writeOne(FileStoreTable table, int k, int v) throws Exception {
        String user = UUID.randomUUID().toString();
        try (TableWriteImpl<?> write = table.newWrite(user);
                TableCommitImpl commit = table.newCommit(user)) {
            write.write(GenericRow.of(k, v));
            commit.commit(k, write.prepareCommit(false, k));
        }
    }
}
