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

/** Iceberg metadata would name a type under which some published data files cannot be read. */
public class IcebergSchemaNotReadableException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String table;
    private final String field;
    private final int fileSchemaId;

    public IcebergSchemaNotReadableException(
            String message, String table, String field, int fileSchemaId) {
        super(message);
        this.table = table;
        this.field = field;
        this.fileSchemaId = fileSchemaId;
    }

    public String table() {
        return table;
    }

    public String field() {
        return field;
    }

    public int fileSchemaId() {
        return fileSchemaId;
    }
}
