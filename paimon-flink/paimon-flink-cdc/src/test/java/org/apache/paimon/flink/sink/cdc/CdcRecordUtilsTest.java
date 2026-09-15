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

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CdcRecordUtils}. */
public class CdcRecordUtilsTest {

    private static final RowType ADDRESS =
            RowType.of(
                    new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                    new String[] {"city", "zip"});

    @Test
    public void testKnownNestedKeysPass() {
        assertThat(
                        CdcRecordUtils.hasUnknownNestedKeys(
                                "{\"city\":\"Aarhus\",\"zip\":\"8000\"}", ADDRESS))
                .isFalse();
        assertThat(CdcRecordUtils.hasUnknownNestedKeys("{\"city\":\"Aarhus\"}", ADDRESS)).isFalse();
        assertThat(CdcRecordUtils.hasUnknownNestedKeys("8000", DataTypes.STRING())).isFalse();
    }

    @Test
    public void testUnknownNestedKeyDetected() {
        assertThat(
                        CdcRecordUtils.hasUnknownNestedKeys(
                                "{\"city\":\"Aarhus\",\"postal\":\"8000\"}", ADDRESS))
                .isTrue();
        assertThat(
                        CdcRecordUtils.hasUnknownNestedKeys(
                                "[{\"city\":\"Aarhus\",\"postal\":\"8000\"}]",
                                DataTypes.ARRAY(ADDRESS)))
                .isTrue();
        assertThat(
                        CdcRecordUtils.hasUnknownNestedKeys(
                                "{\"home\":{\"city\":\"Aarhus\",\"postal\":\"8000\"}}",
                                DataTypes.MAP(DataTypes.STRING(), ADDRESS)))
                .isTrue();
    }

    @Test
    public void testMalformedJsonIsLeftToTheCast() {
        assertThat(CdcRecordUtils.hasUnknownNestedKeys("not json", ADDRESS)).isFalse();
    }
}
