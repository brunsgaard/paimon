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

import org.apache.paimon.flink.iceberg.IcebergSyncOperator;
import org.apache.paimon.flink.iceberg.IcebergSyncSource;
import org.apache.paimon.flink.iceberg.IcebergSyncTask;
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.iceberg.IcebergOptions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Mirrors the snapshots of the selected tables into Iceberg metadata from a separate job. Batch
 * mode syncs every matching table to its latest snapshot and exits; streaming mode keeps
 * discovering tables and polling for new snapshots.
 */
public class IcebergSyncAction extends ActionBase {

    private Pattern databasePattern = Pattern.compile(".*");
    private Pattern includingPattern = Pattern.compile(".*");
    @Nullable private Pattern excludingPattern;
    private Map<String, String> tableOptions = new HashMap<>();
    private List<String> tableOptionFilters = new ArrayList<>();
    private Duration pollInterval = Duration.ofSeconds(10);

    public IcebergSyncAction(Map<String, String> catalogConfig) {
        super(catalogConfig);
    }

    public IcebergSyncAction table(String database, String table) {
        this.databasePattern = Pattern.compile(Pattern.quote(database));
        this.includingPattern = Pattern.compile(Pattern.quote(database + "." + table));
        return this;
    }

    public IcebergSyncAction includingDatabases(@Nullable String regex) {
        if (regex != null) {
            this.databasePattern = Pattern.compile(regex);
        }
        return this;
    }

    public IcebergSyncAction includingTables(@Nullable String regex) {
        if (regex != null) {
            this.includingPattern = Pattern.compile(regex);
        }
        return this;
    }

    public IcebergSyncAction excludingTables(@Nullable String regex) {
        this.excludingPattern = regex == null ? null : Pattern.compile(regex);
        return this;
    }

    public IcebergSyncAction withTableOptions(Map<String, String> options) {
        checkArgument(
                options.containsKey(IcebergOptions.METADATA_ICEBERG_STORAGE.key()),
                "--table_conf must set %s; the tables themselves stay without mirror options.",
                IcebergOptions.METADATA_ICEBERG_STORAGE.key());
        this.tableOptions = new HashMap<>(options);
        return this;
    }

    public IcebergSyncAction withTableOptionFilters(List<String> filters) {
        for (String f : filters) {
            IcebergSyncSource.parseFilter(f);
        }
        this.tableOptionFilters = new ArrayList<>(filters);
        return this;
    }

    public IcebergSyncAction withPollInterval(Duration pollInterval) {
        checkArgument(
                pollInterval.compareTo(Duration.ofSeconds(1)) >= 0,
                "--poll_interval must be at least 1 s, got %s.",
                pollInterval);
        this.pollInterval = pollInterval;
        return this;
    }

    @Override
    public void build() {
        boolean isStreaming =
                env.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;
        HashMap<String, String> configuration = new HashMap<>();
        if (env.getConfiguration() instanceof Configuration) {
            configuration.putAll(((Configuration) env.getConfiguration()).toMap());
        }
        configuration.putAll(tableOptions);
        env.fromSource(
                        new IcebergSyncSource(
                                catalogLoader(),
                                databasePattern,
                                includingPattern,
                                excludingPattern,
                                tableOptionFilters,
                                tableOptions,
                                isStreaming,
                                pollInterval),
                        WatermarkStrategy.noWatermarks(),
                        "iceberg-sync-source",
                        new JavaTypeInfo<>(IcebergSyncTask.class))
                .forceNonParallel()
                .keyBy(IcebergSyncTask::fullName)
                .process(new IcebergSyncOperator(catalogLoader(), tableOptions, configuration))
                .name("iceberg-sync")
                .sinkTo(new DiscardingSink<>());
    }

    @Override
    public void run() throws Exception {
        build();
        execute("iceberg_sync");
    }
}
