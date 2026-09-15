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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TypeUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Utils for {@link CdcRecord}. */
public class CdcRecordUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CdcRecordUtils.class);

    /**
     * Project {@code fields} to a {@link GenericRow}. The fields of row are specified by the given
     * {@code dataFields} and its {@link RowKind} will always be {@link RowKind#INSERT}.
     *
     * <p>NOTE: This method will always return a {@link GenericRow} even if some keys of {@code
     * fields} are not in {@code dataFields}. If you want to make sure all field names of {@code
     * dataFields} existed in keys of {@code fields}, use {@link CdcRecordUtils#toGenericRow}
     * instead.
     *
     * @param dataFields {@link DataField}s of the converted {@link GenericRow}.
     * @return the projected {@link GenericRow}.
     */
    public static GenericRow projectAsInsert(CdcRecord record, List<DataField> dataFields) {
        GenericRow genericRow = new GenericRow(dataFields.size());
        for (int i = 0; i < dataFields.size(); i++) {
            DataField dataField = dataFields.get(i);
            String fieldValue = record.data().get(dataField.name());
            if (fieldValue != null) {
                genericRow.setField(
                        i, TypeUtils.castFromCdcValueString(fieldValue, dataField.type()));
            }
        }
        return genericRow;
    }

    /**
     * Convert {@code fields} to a {@link GenericRow}. The fields of row are specified by the given
     * {@code dataFields} and its {@link RowKind} is determined by {@code kind} of this {@link
     * CdcRecord}.
     *
     * <p>NOTE: This method requires all field names of {@code dataFields} existed in keys of {@code
     * fields}. If you only want to convert some {@code fields}, use {@link
     * CdcRecordUtils#projectAsInsert} instead.
     *
     * @param dataFields {@link DataField}s of the converted {@link GenericRow}.
     * @param logCorruptRecord whether to log data during conversion error
     * @return if all field names of {@code dataFields} existed in keys of {@code fields} and all
     *     values of {@code fields} can be correctly converted to the specified type, an {@code
     *     Optional#of(GenericRow)} will be returned, otherwise an {@code Optional#empty()} will be
     *     returned
     */
    public static Optional<GenericRow> toGenericRow(
            CdcRecord record, List<DataField> dataFields, boolean logCorruptRecord) {
        GenericRow genericRow = new GenericRow(record.kind(), dataFields.size());
        List<String> fieldNames =
                dataFields.stream().map(DataField::name).collect(Collectors.toList());

        for (Map.Entry<String, String> field : record.data().entrySet()) {
            String key = field.getKey();
            String value = field.getValue();

            int idx = fieldNames.indexOf(key);
            if (idx < 0) {
                LOG.info("Field '{}' not found. Waiting for schema update.", key);
                return Optional.empty();
            }

            if (value == null) {
                continue;
            }

            DataType type = dataFields.get(idx).type();
            if (hasUnknownNestedKeys(value, type)) {
                // A field inside a ROW that the writer's schema does not have yet. The cast would
                // silently drop it, so wait for the schema change like a new top-level column.
                LOG.info(
                        "Field '{}' has nested keys missing from {}. Waiting for schema update.",
                        key,
                        type);
                return Optional.empty();
            }
            try {
                genericRow.setField(idx, TypeUtils.castFromCdcValueString(value, type));
            } catch (Exception e) {
                LOG.info(
                        "Failed to convert field '{}' value {} to type {}. Waiting for schema update.",
                        key,
                        logCorruptRecord ? value : "<redacted>",
                        type,
                        e);
                return Optional.empty();
            }
        }
        return Optional.of(genericRow);
    }

    public static CdcRecord fromGenericRow(GenericRow row, List<String> fieldNames) {
        Map<String, String> data = new HashMap<>();
        for (int i = 0; i < row.getFieldCount(); i++) {
            Object field = row.getField(i);
            if (field != null) {
                data.put(fieldNames.get(i), field.toString());
            }
        }

        return new CdcRecord(row.getRowKind(), data);
    }

    /**
     * True when a JSON value carries an object key that the ROW type, nested at any depth, lacks.
     */
    static boolean hasUnknownNestedKeys(String value, DataType type) {
        if (!containsRow(type)) {
            return false;
        }
        try {
            return hasUnknownKeys(TypeUtils.OBJECT_MAPPER.readTree(value), type);
        } catch (Exception e) {
            // not JSON; let the cast report the problem
            return false;
        }
    }

    private static boolean containsRow(DataType type) {
        switch (type.getTypeRoot()) {
            case ROW:
                return true;
            case ARRAY:
                return containsRow(((ArrayType) type).getElementType());
            case MULTISET:
                return containsRow(((MultisetType) type).getElementType());
            case MAP:
                return containsRow(((MapType) type).getValueType());
            default:
                return false;
        }
    }

    private static boolean hasUnknownKeys(JsonNode node, DataType type) throws IOException {
        if (node == null || node.isNull()) {
            return false;
        }
        if (node.isTextual() && containsRow(type)) {
            // some formats carry nested values as JSON text
            node = TypeUtils.OBJECT_MAPPER.readTree(node.asText());
        }
        switch (type.getTypeRoot()) {
            case ROW:
                if (!node.isObject()) {
                    return false;
                }
                RowType rowType = (RowType) type;
                Iterator<String> names = node.fieldNames();
                while (names.hasNext()) {
                    String name = names.next();
                    if (!rowType.containsField(name)) {
                        return true;
                    }
                    if (hasUnknownKeys(node.get(name), rowType.getField(name).type())) {
                        return true;
                    }
                }
                return false;
            case ARRAY:
            case MULTISET:
                if (!node.isArray()) {
                    return false;
                }
                DataType elementType =
                        type.getTypeRoot() == DataTypeRoot.ARRAY
                                ? ((ArrayType) type).getElementType()
                                : ((MultisetType) type).getElementType();
                for (JsonNode element : node) {
                    if (hasUnknownKeys(element, elementType)) {
                        return true;
                    }
                }
                return false;
            case MAP:
                if (!node.isObject()) {
                    return false;
                }
                DataType valueType = ((MapType) type).getValueType();
                Iterator<JsonNode> values = node.elements();
                while (values.hasNext()) {
                    if (hasUnknownKeys(values.next(), valueType)) {
                        return true;
                    }
                }
                return false;
            default:
                return false;
        }
    }
}
