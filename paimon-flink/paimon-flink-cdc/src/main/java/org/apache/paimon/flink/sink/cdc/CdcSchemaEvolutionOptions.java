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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;

import java.io.Serializable;

/**
 * Table options that let the CDC sink follow column renames and drops from the source schema.
 *
 * <p>Both are off by default. They are only safe with a source format whose record schema is the
 * complete schema derived from a descriptor, such as protobuf, because a sparse per-record schema
 * would look like every omitted column had been dropped.
 */
public class CdcSchemaEvolutionOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final ConfigOption<Boolean> RENAME_BY_COMMENT =
            ConfigOptions.key("cdc.schema-evolution.rename-by-comment")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Rename a column instead of adding a new one when a source field with "
                                    + "an unknown name carries the same identity marker in its "
                                    + "comment as an existing column that the source no longer has, "
                                    + "for example '[proto:8]'.");

    public static final ConfigOption<Boolean> DROP_MISSING_COLUMNS =
            ConfigOptions.key("cdc.schema-evolution.drop-missing-columns")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Drop a column that carries an identity marker when the source schema "
                                    + "no longer has it. Only for formats that provide the complete "
                                    + "schema with every record.");

    private final boolean renameByComment;
    private final boolean dropMissingColumns;

    public CdcSchemaEvolutionOptions(boolean renameByComment, boolean dropMissingColumns) {
        this.renameByComment = renameByComment;
        this.dropMissingColumns = dropMissingColumns;
    }

    public static CdcSchemaEvolutionOptions disabled() {
        return new CdcSchemaEvolutionOptions(false, false);
    }

    public static CdcSchemaEvolutionOptions from(Options options) {
        return new CdcSchemaEvolutionOptions(
                options.get(RENAME_BY_COMMENT), options.get(DROP_MISSING_COLUMNS));
    }

    public boolean renameByComment() {
        return renameByComment;
    }

    public boolean dropMissingColumns() {
        return dropMissingColumns;
    }

    /** Rename and drop detection both need every field of the record schema, not only changes. */
    public boolean needsCompleteSchema() {
        return renameByComment || dropMissingColumns;
    }
}
