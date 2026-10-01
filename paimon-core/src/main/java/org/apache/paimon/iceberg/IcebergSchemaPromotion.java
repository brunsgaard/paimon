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

package org.apache.paimon.iceberg;

import org.apache.paimon.iceberg.metadata.IcebergDataField;
import org.apache.paimon.iceberg.metadata.IcebergListType;
import org.apache.paimon.iceberg.metadata.IcebergMapType;
import org.apache.paimon.iceberg.metadata.IcebergSchema;
import org.apache.paimon.iceberg.metadata.IcebergStructType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The type changes under which an Iceberg reader can still read an old data file. Paimon casts old
 * files to a new column type when it reads them. Iceberg readers do not: they read each file under
 * the current type, and they accept only the promotions of the Iceberg specification.
 *
 * <p>The rule also applies inside a struct, a list and a map. Fields of a struct match by field id.
 * A struct field that only one side has is not checked.
 */
public final class IcebergSchemaPromotion {

    private static final Pattern DECIMAL = Pattern.compile("decimal\\((\\d+), ?(\\d+)\\)");

    private IcebergSchemaPromotion() {}

    /** True when a file written with {@code from} can be read as {@code to}. */
    public static boolean promotes(Object from, Object to) {
        if (from.equals(to)) {
            return true;
        }
        if (from instanceof IcebergStructType && to instanceof IcebergStructType) {
            return structPromotes((IcebergStructType) from, (IcebergStructType) to);
        }
        if (from instanceof IcebergListType && to instanceof IcebergListType) {
            IcebergListType f = (IcebergListType) from;
            IcebergListType t = (IcebergListType) to;
            return f.elementId() == t.elementId() && promotes(f.element(), t.element());
        }
        if (from instanceof IcebergMapType && to instanceof IcebergMapType) {
            IcebergMapType f = (IcebergMapType) from;
            IcebergMapType t = (IcebergMapType) to;
            return f.keyId() == t.keyId()
                    && f.valueId() == t.valueId()
                    && promotes(f.key(), t.key())
                    && promotes(f.value(), t.value());
        }
        if (!(from instanceof String) || !(to instanceof String)) {
            return false;
        }
        String f = (String) from;
        String t = (String) to;
        if ("int".equals(f) && "long".equals(t) || "float".equals(f) && "double".equals(t)) {
            return true;
        }
        Matcher a = DECIMAL.matcher(f);
        Matcher b = DECIMAL.matcher(t);
        return a.matches()
                && b.matches()
                && a.group(2).equals(b.group(2))
                && Integer.parseInt(b.group(1)) > Integer.parseInt(a.group(1));
    }

    private static boolean structPromotes(IcebergStructType from, IcebergStructType to) {
        Map<Integer, IcebergDataField> toById = byId(to.fields());
        for (IcebergDataField field : from.fields()) {
            IcebergDataField now = toById.get(field.id());
            if (now != null && !promotes(field.type(), now.type())) {
                return false;
            }
        }
        return true;
    }

    private static Map<Integer, IcebergDataField> byId(List<IcebergDataField> fields) {
        Map<Integer, IcebergDataField> result = new HashMap<>();
        for (IcebergDataField field : fields) {
            result.put(field.id(), field);
        }
        return result;
    }

    /**
     * Throws when a field id of {@code fileSchema} has a type that does not promote to the current
     * one.
     */
    public static void checkReadable(
            IcebergSchema fileSchema, IcebergSchema current, String table) {
        Map<Integer, IcebergDataField> currentById = byId(current.fields());
        for (IcebergDataField field : fileSchema.fields()) {
            IcebergDataField now = currentById.get(field.id());
            if (now == null || promotes(field.type(), now.type())) {
                continue;
            }
            throw new IcebergSchemaNotReadableException(
                    String.format(
                            "Table %s has data files of schema %d in which field %s (id %d) is %s; the current schema %d makes it %s, which Iceberg cannot read. A full compaction rewrites the files.",
                            table,
                            fileSchema.schemaId(),
                            field.name(),
                            field.id(),
                            field.type(),
                            current.schemaId(),
                            now.type()),
                    table,
                    field.name(),
                    fileSchema.schemaId());
        }
    }
}
