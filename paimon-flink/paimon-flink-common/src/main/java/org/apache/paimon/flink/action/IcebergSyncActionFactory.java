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

import org.apache.paimon.utils.TimeUtils;

import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Factory to create {@link IcebergSyncAction}. */
public class IcebergSyncActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "write_iceberg_metadata";

    private static final String INCLUDING_DATABASES = "including_databases";
    private static final String INCLUDING_TABLES = "including_tables";
    private static final String EXCLUDING_TABLES = "excluding_tables";
    private static final String TABLE_OPTION_FILTER = "table_option_filter";
    private static final String POLL_INTERVAL = "poll_interval";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        IcebergSyncAction action = new IcebergSyncAction(catalogConfigMap(params));
        boolean singleTable = params.has(DATABASE) || params.has(TABLE);
        boolean patterns =
                params.has(INCLUDING_DATABASES)
                        || params.has(INCLUDING_TABLES)
                        || params.has(EXCLUDING_TABLES);
        checkArgument(
                !(singleTable && patterns),
                "Use either --%s and --%s, or the --%s/--%s/--%s patterns, not both.",
                DATABASE,
                TABLE,
                INCLUDING_DATABASES,
                INCLUDING_TABLES,
                EXCLUDING_TABLES);
        if (singleTable) {
            action.table(params.getRequired(DATABASE), params.getRequired(TABLE));
        } else {
            action.includingDatabases(params.get(INCLUDING_DATABASES))
                    .includingTables(params.get(INCLUDING_TABLES))
                    .excludingTables(params.get(EXCLUDING_TABLES));
        }
        action.withTableOptions(optionalConfigMap(params, TABLE_CONF));
        if (params.has(TABLE_OPTION_FILTER)) {
            action.withTableOptionFilters(
                    new java.util.ArrayList<>(params.getMultiParameter(TABLE_OPTION_FILTER)));
        }
        String pollInterval = params.get(POLL_INTERVAL);
        if (pollInterval != null) {
            action.withPollInterval(TimeUtils.parseDuration(pollInterval));
        }
        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"write_iceberg_metadata\" mirrors the snapshots of Paimon tables into Iceberg"
                        + " metadata from a separate job, so writers stay unaware of the mirror.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  write_iceberg_metadata --warehouse <warehouse_path> --database <database_name>"
                        + " --table <table_name> \\\n"
                        + "    --table_conf metadata.iceberg.storage=<storage>"
                        + " [--table_conf <key>=<value> ...] \\\n"
                        + "    [--poll_interval <duration>]"
                        + " [--catalog_conf <key>=<value> ...]");
        System.out.println(
                "  write_iceberg_metadata --warehouse <warehouse_path>"
                        + " --including_databases <database_name|name_regular_expr> \\\n"
                        + "    [--including_tables <paimon_table_name|name_regular_expr>] \\\n"
                        + "    [--excluding_tables <paimon_table_name|name_regular_expr>] \\\n"
                        + "    --table_conf metadata.iceberg.storage=<storage>"
                        + " [--table_conf <key>=<value> ...] \\\n"
                        + "    [--poll_interval <duration>]"
                        + " [--catalog_conf <key>=<value> ...]");
        System.out.println();

        System.out.println(
                "--table_conf carries the metadata.iceberg.* options; they apply to a table copy"
                        + " inside the job and are never set on the tables. A table that sets"
                        + " metadata.iceberg.storage itself is refused, because its writers"
                        + " already commit the mirror.");
        System.out.println(
                "--including_tables and --excluding_tables match the full name"
                        + " <database>.<table>; --excluding_tables wins over --including_tables.");
        System.out.println(
                "--table_option_filter <key>=<value>  mirror only tables whose options hold this"
                        + " pair; repeat for any-of. A table is mirrored only while its options"
                        + " match. A table that stops matching is forgotten, not dropped. A table"
                        + " that is turned off and then dropped keeps its Iceberg table; a person"
                        + " drops it.");
        System.out.println(
                "--poll_interval is the time between two polls in streaming mode, default 10 s."
                        + " It must be at least 1 s. In batch mode every matching table is synced"
                        + " to its latest snapshot and the job exits.");
        System.out.println();

        System.out.println("Drops:");
        System.out.println(
                "  The Iceberg table is dropped only when the Paimon table is missing from the"
                        + " catalog listing on two consecutive polls, and only through a REST"
                        + " catalog. Other storages log and keep the metadata.");
        System.out.println();

        System.out.println("Failures:");
        System.out.println(
                "  A table whose catalog or snapshot reads fail in the source is skipped for"
                        + " that poll and counted in table_failures; it is not dropped. A failing"
                        + " table is held and retried after 30 s, after 2 min, and then every"
                        + " 10 min. The other tables go on. Holds and pending drops are in memory"
                        + " only, so a restart loses a pending drop; a person drops that Iceberg"
                        + " table.");
        System.out.println();

        System.out.println("Metrics (group iceberg_metadata):");
        System.out.println(
                "  Per-table gauges, with the label table=\"db.t\": latest_snapshot_id and"
                        + " pending_snapshots (source); mirrored_snapshot_id,"
                        + " mirrored_snapshot_timestamp_ms and on_hold (operator)."
                        + " mirrored_snapshot_id reads -1 after a restart until the next sync.");
        System.out.println(
                "  Counters: polls, failed_polls, table_failures, snapshots_synced, sync_failures,"
                        + " mirrors_dropped, listener_failures.");
        System.out.println();

        System.out.println("Listeners:");
        System.out.println(
                "  An IcebergSyncListener is found through ServiceLoader with the user code class"
                        + " loader. A listener failure is logged and counted and never changes a"
                        + " sync. Calls run on the task thread.");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  write_iceberg_metadata --warehouse hdfs:///path/to/warehouse --database test_db"
                        + " --table test_table --table_conf metadata.iceberg.storage=rest-catalog"
                        + " --table_conf metadata.iceberg.uri=http://localhost:8181");
        System.out.println(
                "  write_iceberg_metadata --warehouse hdfs:///path/to/warehouse --including_databases"
                        + " test_db --including_tables 'test_db\\.orders_.*'"
                        + " --table_conf metadata.iceberg.storage=hadoop-catalog");
    }
}
