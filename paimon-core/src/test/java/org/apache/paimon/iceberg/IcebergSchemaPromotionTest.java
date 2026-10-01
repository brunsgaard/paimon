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

import org.apache.paimon.iceberg.metadata.IcebergSchema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link IcebergSchemaPromotion}. */
class IcebergSchemaPromotionTest {

    private static IcebergSchema schema(long id, DataField... fields) {
        return IcebergSchema.create(
                new TableSchema(
                        id,
                        Arrays.asList(fields),
                        100,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        ""));
    }

    @Test
    public void testPromotions() {
        assertThat(IcebergSchemaPromotion.promotes("int", "long")).isTrue();
        assertThat(IcebergSchemaPromotion.promotes("float", "double")).isTrue();
        assertThat(IcebergSchemaPromotion.promotes("decimal(10, 2)", "decimal(12, 2)")).isTrue();
        assertThat(IcebergSchemaPromotion.promotes("string", "string")).isTrue();
        assertThat(IcebergSchemaPromotion.promotes("long", "int")).isFalse();
        assertThat(IcebergSchemaPromotion.promotes("int", "double")).isFalse();
        assertThat(IcebergSchemaPromotion.promotes("string", "int")).isFalse();
        assertThat(IcebergSchemaPromotion.promotes("decimal(10, 2)", "decimal(12, 3)")).isFalse();
    }

    @Test
    public void testRefusedChangeNamesTheField() {
        IcebergSchema file =
                schema(
                        0,
                        new DataField(0, "k", DataTypes.INT()),
                        new DataField(1, "v", DataTypes.STRING()));
        IcebergSchema current =
                schema(
                        1,
                        new DataField(0, "k", DataTypes.INT()),
                        new DataField(1, "v", DataTypes.INT()));
        assertThatThrownBy(() -> IcebergSchemaPromotion.checkReadable(file, current, "db.t"))
                .isInstanceOf(IcebergSchemaNotReadableException.class)
                .hasMessage(
                        "Table db.t has data files of schema 0 in which field v (id 1) is string; the current schema 1 makes it int, which Iceberg cannot read. A full compaction rewrites the files.");
    }

    @Test
    public void testNewFieldIdUnderAnOldNameIsNotCompared() {
        IcebergSchema file = schema(0, new DataField(1, "v", DataTypes.STRING()));
        IcebergSchema current = schema(1, new DataField(2, "v", DataTypes.INT()));
        assertThatCode(() -> IcebergSchemaPromotion.checkReadable(file, current, "db.t"))
                .doesNotThrowAnyException();
    }

    @Test
    public void testPromotionPasses() {
        IcebergSchema file = schema(0, new DataField(1, "v", DataTypes.INT()));
        IcebergSchema current = schema(1, new DataField(1, "v", DataTypes.BIGINT()));
        assertThatCode(() -> IcebergSchemaPromotion.checkReadable(file, current, "db.t"))
                .doesNotThrowAnyException();
    }

    @Test
    public void testNestedPromotionPasses() {
        IcebergSchema file =
                schema(
                        0,
                        new DataField(
                                1, "s", DataTypes.ROW(new DataField(2, "a", DataTypes.INT()))),
                        new DataField(3, "l", DataTypes.ARRAY(DataTypes.FLOAT())),
                        new DataField(4, "m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
        IcebergSchema current =
                schema(
                        1,
                        new DataField(
                                1,
                                "s",
                                DataTypes.ROW(
                                        new DataField(2, "a", DataTypes.BIGINT()),
                                        new DataField(5, "b", DataTypes.STRING()))),
                        new DataField(3, "l", DataTypes.ARRAY(DataTypes.DOUBLE())),
                        new DataField(
                                4, "m", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())));
        assertThatCode(() -> IcebergSchemaPromotion.checkReadable(file, current, "db.t"))
                .doesNotThrowAnyException();
    }

    @Test
    public void testNestedRefusedChangeNamesTheTopLevelField() {
        IcebergSchema current =
                schema(
                        1,
                        new DataField(
                                1, "s", DataTypes.ROW(new DataField(2, "a", DataTypes.STRING()))),
                        new DataField(3, "l", DataTypes.ARRAY(DataTypes.INT())),
                        new DataField(
                                4, "m", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())));
        IcebergSchema structFile =
                schema(
                        0,
                        new DataField(
                                1, "s", DataTypes.ROW(new DataField(2, "a", DataTypes.INT()))));
        IcebergSchema listFile =
                schema(0, new DataField(3, "l", DataTypes.ARRAY(DataTypes.BIGINT())));
        IcebergSchema mapFile =
                schema(
                        0,
                        new DataField(4, "m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
        assertThatThrownBy(() -> IcebergSchemaPromotion.checkReadable(structFile, current, "db.t"))
                .isInstanceOf(IcebergSchemaNotReadableException.class)
                .extracting("field")
                .isEqualTo("s");
        assertThatThrownBy(() -> IcebergSchemaPromotion.checkReadable(listFile, current, "db.t"))
                .isInstanceOf(IcebergSchemaNotReadableException.class)
                .extracting("field")
                .isEqualTo("l");
        assertThatThrownBy(() -> IcebergSchemaPromotion.checkReadable(mapFile, current, "db.t"))
                .isInstanceOf(IcebergSchemaNotReadableException.class)
                .extracting("field")
                .isEqualTo("m");
    }

    @Test
    public void testNestedRefusalMessageRendersTypes() {
        IcebergSchema file = schema(0, new DataField(3, "l", DataTypes.ARRAY(DataTypes.STRING())));
        IcebergSchema current = schema(1, new DataField(3, "l", DataTypes.ARRAY(DataTypes.INT())));
        assertThatThrownBy(() -> IcebergSchemaPromotion.checkReadable(file, current, "db.t"))
                .isInstanceOf(IcebergSchemaNotReadableException.class)
                .hasMessage(
                        "Table db.t has data files of schema 0 in which field l (id 3) is list<string>; the current schema 1 makes it list<int>, which Iceberg cannot read. A full compaction rewrites the files.");
        IcebergSchema structFile =
                schema(
                        0,
                        new DataField(
                                4,
                                "m",
                                DataTypes.MAP(
                                        DataTypes.STRING(),
                                        DataTypes.ROW(
                                                new DataField(5, "a", DataTypes.INT()),
                                                new DataField(6, "b", DataTypes.STRING())))));
        IcebergSchema structCurrent =
                schema(
                        1,
                        new DataField(4, "m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
        assertThatThrownBy(
                        () ->
                                IcebergSchemaPromotion.checkReadable(
                                        structFile, structCurrent, "db.t"))
                .hasMessageContaining("is map<string, struct<a: int, b: string>>;")
                .hasMessageContaining("makes it map<string, int>,");
    }

    @Test
    public void testListElementChangedToStructIsRefused() {
        IcebergSchema file = schema(0, new DataField(3, "l", DataTypes.ARRAY(DataTypes.INT())));
        IcebergSchema current =
                schema(
                        1,
                        new DataField(
                                3,
                                "l",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(new DataField(7, "a", DataTypes.INT())))));
        assertThatThrownBy(() -> IcebergSchemaPromotion.checkReadable(file, current, "db.t"))
                .isInstanceOf(IcebergSchemaNotReadableException.class)
                .extracting("field")
                .isEqualTo("l");
    }

    @Test
    public void testMapWithMismatchedKeyIdIsRefused() {
        IcebergSchema file =
                schema(
                        0,
                        new DataField(4, "m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
        IcebergSchema current =
                schema(
                        1,
                        new DataField(9, "m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
        // Same top-level id is needed for the comparison, so compare the types directly.
        Object from = file.fields().get(0).type();
        Object to = current.fields().get(0).type();
        assertThat(IcebergSchemaPromotion.promotes(from, to)).isFalse();
    }
}
