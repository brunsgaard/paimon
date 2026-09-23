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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.iceberg.IcebergSync;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for {@link IcebergSyncProcedure}. */
public class IcebergSyncProcedureITCase extends CatalogITCaseBase {

    private static final String CALL =
            "CALL sys.iceberg_sync(`table` => 'default.T',"
                    + " `options` => 'metadata.iceberg.storage=table-location')";

    @Test
    public void testProcedureSyncsToLatest() throws Exception {
        sql("CREATE TABLE T (k INT, v INT) WITH ('bucket' = '-1')");
        sql("INSERT INTO T VALUES (1, 10)");
        sql("INSERT INTO T VALUES (2, 20)");

        List<Row> result = sql(CALL);
        assertThat(result).containsExactly(Row.of("1"));

        sql("INSERT INTO T VALUES (3, 30)");
        result = sql(CALL);
        assertThat(result).containsExactly(Row.of("1"));

        FileStoreTable mirrored =
                paimonTable("T")
                        .copy(
                                Collections.singletonMap(
                                        "metadata.iceberg.storage", "table-location"));
        assertThat(IcebergSync.lastMirroredSnapshot(mirrored)).isEqualTo(3L);
    }

    @Test
    public void testProcedureRefusesATableMirroredByItsWriters() {
        sql(
                "CREATE TABLE T (k INT, v INT) WITH ('bucket' = '-1',"
                        + " 'metadata.iceberg.storage' = 'table-location')");
        sql("INSERT INTO T VALUES (1, 10)");

        assertThatThrownBy(() -> sql(CALL))
                .hasStackTraceContaining("its writers already commit Iceberg metadata");
    }
}
