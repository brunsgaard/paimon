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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class IcebergSyncOperatorTest {

    @TempDir java.nio.file.Path tempDir;

    private IcebergSyncOperator operatorWithStorage(String storage) {
        Options catalogOptions = new Options();
        catalogOptions.set("warehouse", new Path(tempDir.toString(), "warehouse").toString());
        Map<String, String> options =
                Collections.singletonMap(IcebergOptions.METADATA_ICEBERG_STORAGE.key(), storage);
        IcebergSyncOperator operator =
                new IcebergSyncOperator(
                        () -> CatalogFactory.createCatalog(CatalogContext.create(catalogOptions)),
                        options);
        operator.open(null);
        return operator;
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
}
