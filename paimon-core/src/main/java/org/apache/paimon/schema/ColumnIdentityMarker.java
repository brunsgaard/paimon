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

package org.apache.paimon.schema;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A stable identity for a column, carried as a trailing {@code [scheme:value]} token in the column
 * description. Source formats whose fields have an identity that survives a rename, such as a
 * protobuf field number, append the marker so a sink can tell a rename from a drop and add.
 *
 * <p>Example description: {@code "Spend in micros. [proto:8]"}.
 */
public final class ColumnIdentityMarker {

    private static final Pattern MARKER =
            Pattern.compile("\\s*\\[([A-Za-z0-9_-]+:[^\\]\\s]+)\\]\\s*$");

    private ColumnIdentityMarker() {}

    /**
     * Returns the identity token, for example {@code proto:8}, when the description ends in one.
     */
    public static Optional<String> identityOf(@Nullable String description) {
        if (description == null) {
            return Optional.empty();
        }
        Matcher m = MARKER.matcher(description);
        return m.find() ? Optional.of(m.group(1)) : Optional.empty();
    }

    /** Returns the description without its identity marker, or null when nothing else remains. */
    @Nullable
    public static String commentOf(@Nullable String description) {
        if (description == null) {
            return null;
        }
        String comment = MARKER.matcher(description).replaceFirst("").trim();
        return comment.isEmpty() ? null : comment;
    }

    /**
     * Builds a description from a free-text comment and an identity. The comment is trimmed and its
     * whitespace collapsed so the result is byte-stable for the same input.
     */
    public static String withIdentity(@Nullable String comment, String identity) {
        String marker = "[" + identity + "]";
        if (comment == null) {
            return marker;
        }
        String normalized = comment.trim().replaceAll("\\s+", " ");
        return normalized.isEmpty() ? marker : normalized + " " + marker;
    }
}
