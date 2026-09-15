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

package org.apache.paimon.flink.action.cdc.format.protobuf;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.format.AbstractRecordParser;
import org.apache.paimon.flink.sink.cdc.CdcSchema;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.BIGINT_UNSIGNED_TO_BIGINT;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_STRING;

/**
 * Parses {@link ProtobufSourceRecord}s into insert-only {@link RichCdcMultiplexRecord}s.
 *
 * <p>Protobuf messages carry no database or table name, so both are taken from the topic. For
 * {@code kafka_sync_table} the target is named by the action. For {@code kafka_sync_database} use
 * {@code --table_mapping} or the prefix and suffix options to rename.
 */
public class ProtobufRecordParser extends AbstractRecordParser {

    private ProtobufSourceRecord root;

    // records from one descriptor share one schema byte array; parse it once
    private byte[] cachedSchema;
    private List<DataField> cachedFields;

    public ProtobufRecordParser(TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        super(typeMapping, computedColumns);
    }

    @Override
    protected void setRoot(CdcSourceRecord record) {
        super.setRoot(record);
        root = (ProtobufSourceRecord) record.getValue();
    }

    @Override
    protected List<RichCdcMultiplexRecord> extractRecords() {
        CdcSchema.Builder schemaBuilder = CdcSchema.newBuilder();
        for (DataField field : fieldsOf(root)) {
            schemaBuilder.column(field.name(), applyTypeMapping(field.type()), field.description());
        }
        Map<String, String> rowData = new LinkedHashMap<>(root.values());
        evalComputedColumns(rowData, schemaBuilder);
        evalMetadataColumns(rowData, schemaBuilder);
        return Collections.singletonList(createRecord(RowKind.INSERT, rowData, schemaBuilder));
    }

    private List<DataField> fieldsOf(ProtobufSourceRecord record) {
        if (cachedSchema == null || !Arrays.equals(cachedSchema, record.schemaBytes())) {
            cachedSchema = record.schemaBytes();
            cachedFields = record.fields();
        }
        return cachedFields;
    }

    private DataType applyTypeMapping(DataType type) {
        if (typeMapping.containsMode(TO_STRING)) {
            return DataTypes.STRING();
        }
        if (typeMapping.containsMode(BIGINT_UNSIGNED_TO_BIGINT)) {
            return unsignedToBigint(type);
        }
        return type;
    }

    /** The converter maps uint64 to DECIMAL(20, 0). Protobuf has no other source of decimals. */
    private static DataType unsignedToBigint(DataType type) {
        switch (type.getTypeRoot()) {
            case DECIMAL:
                DecimalType decimal = (DecimalType) type;
                return decimal.getPrecision() == 20 && decimal.getScale() == 0
                        ? DataTypes.BIGINT().copy(type.isNullable())
                        : type;
            case ARRAY:
                return new ArrayType(
                        type.isNullable(), unsignedToBigint(((ArrayType) type).getElementType()));
            case MAP:
                MapType map = (MapType) type;
                return new MapType(
                        type.isNullable(),
                        unsignedToBigint(map.getKeyType()),
                        unsignedToBigint(map.getValueType()));
            case ROW:
                RowType row = (RowType) type;
                List<DataField> fields = new ArrayList<>();
                for (DataField field : row.getFields()) {
                    fields.add(field.newType(unsignedToBigint(field.type())));
                }
                return new RowType(type.isNullable(), fields);
            default:
                return type;
        }
    }

    @Override
    protected List<String> extractPrimaryKeys() {
        return Collections.emptyList();
    }

    @Override
    protected String getTableName() {
        return currentRecord.getTopic();
    }

    @Override
    protected String getDatabaseName() {
        return currentRecord.getTopic();
    }

    @Override
    protected String format() {
        return ProtobufDataFormatFactory.IDENTIFIER;
    }
}
