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

import org.apache.paimon.iceberg.IcebergSync;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ProcedureUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.HashMap;

/**
 * Mirrors every snapshot of one table since the last mirrored one into Iceberg metadata and returns
 * how many were mirrored. The {@code metadata.iceberg.*} options come from {@code options}; the
 * table itself stays without them.
 *
 * <pre><code>
 *  CALL sys.iceberg_sync(`table` => 'db.t', `options` => 'metadata.iceberg.storage=rest-catalog,metadata.iceberg.uri=http://...')
 * </code></pre>
 */
public class IcebergSyncProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "iceberg_sync";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING"), isOptional = false),
                @ArgumentHint(name = "options", type = @DataTypeHint("STRING"), isOptional = false)
            })
    public String[] call(ProcedureContext procedureContext, String tableId, String options)
            throws Exception {
        FileStoreTable original = (FileStoreTable) table(tableId);
        IcebergSync.checkNotMirroredByWriters(original);
        HashMap<String, String> mirrorOptions = new HashMap<>();
        ProcedureUtils.putAllOptions(mirrorOptions, options);
        try (IcebergSync sync =
                new IcebergSync(IcebergSync.withMirrorDefaults(original, mirrorOptions))) {
            return new String[] {String.valueOf(sync.syncPending())};
        }
    }
}
