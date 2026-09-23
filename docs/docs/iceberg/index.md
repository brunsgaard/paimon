---
title: "Iceberg Compatibility"
sidebar_position: 98
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Iceberg Compatibility

Paimon can publish Iceberg metadata that points to its existing data files. This lets applications
query a Paimon table through an Iceberg connector while Paimon continues to manage writes,
compaction, and data retention.

![Paimon commits publish Iceberg metadata; both readers access the same data files.](/img/iceberg-publication.svg)

## Start Here

| Task | Guide |
| --- | --- |
| Read your first Paimon table through Flink or Spark's Iceberg connector | [Append tables](./append-table.mdx) |
| Read updates and deletes from a primary key table | [Primary key tables](./primary-key-table.mdx) |
| Choose Hadoop, Hive, REST, or path-based access | [Catalogs and metadata layout](./catalogs.md) |
| Query a named historical snapshot | [Tags](./iceberg-tags.md) |
| Connect Trino, Athena, or DuckDB | [Query engines](./ecosystem.mdx) |
| Check column types and format requirements | [Data types](./data-types.md) |
| Look up table options | [Configuration reference](./configurations.mdx) |

## How Publication Works

1. A writer commits a Paimon snapshot.
2. Paimon generates Iceberg manifests and snapshot metadata for the files eligible for Iceberg reads.
3. With Hive or REST storage, Paimon also publishes the metadata to the external catalog.
4. An Iceberg reader loads the published metadata and reads the referenced data files directly.

Enable publication with the Paimon table option `metadata.iceberg.storage`; its default is `disabled`.
For a first example, use `hadoop-catalog`. The Iceberg warehouse is then
`<paimon-warehouse>/iceberg`, using the default metadata layout.

```sql
'metadata.iceberg.storage' = 'hadoop-catalog'
```

Metadata publication does not copy the table's data. Iceberg readers therefore need access to both
the metadata location and the original Paimon data files, including the required filesystem
configuration and credentials.

## Mirror from a Separate Job

By default every job that commits to a table also publishes the Iceberg metadata. To keep writers
free of the mirror, leave the `metadata.iceberg.*` options off the table and run the `iceberg_sync`
action instead. It mirrors every snapshot since the last mirrored one, in order, so the Iceberg
history follows the Paimon history.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-@@VERSION@@.jar \
    iceberg_sync \
    --warehouse <warehouse-path> \
    --including_databases <database-name|name-regular-expr> \
    [--including_tables <database.table|name-regular-expr>] \
    [--excluding_tables <database.table|name-regular-expr>] \
    [--database <database-name> --table <table-name>] \
    [--poll_interval <duration>] \
    --table_conf metadata.iceberg.storage=<storage> \
    [--table_conf <key>=<value> ...] \
    [--catalog_conf <key>=<value> ...]
```

In batch mode the action syncs every matching table to its latest snapshot and exits. In streaming
mode it rediscovers the tables and polls for new snapshots every `--poll_interval`, 10 seconds by
default. The table patterns match the full `database.table` name, as they do for `compact_database`.

The mirror options come only from `--table_conf` and apply to a table copy inside the job. A table
that sets `metadata.iceberg.storage` itself is refused, because its writers already publish the
mirror. A table with `deletion-vectors.enabled` and `deletion-vectors.bitmap64` is mirrored as
Iceberg format version 3 unless `--table_conf` sets `metadata.iceberg.format-version`; an explicit
version 2 is refused, because it would drop the deletion vectors from the mirror.

The first sync of a table mirrors its latest snapshot. When snapshots expired before they were
mirrored, the action logs a warning and mirrors the latest snapshot from a scan of the table.

The same for one table from Flink SQL:

```sql
CALL sys.iceberg_sync(
    `table` => 'db.t',
    `options` => 'metadata.iceberg.storage=rest-catalog,metadata.iceberg.uri=http://localhost:8181');
```

The procedure returns the number of snapshots it mirrored. Arguments:

- `table` (required): the target table identifier.
- `options` (required): the `metadata.iceberg.*` options for the mirror, comma separated.

## What Iceberg Readers See

| Paimon table | Files eligible for incremental publication | When changes become visible |
| --- | --- | --- |
| Append table | Data files in the committed snapshot | After metadata publication and reader refresh |
| Primary key table without Iceberg deletion vectors | Files at the highest LSM level | After full compaction, metadata publication, and reader refresh |
| Primary key table with Iceberg v3 deletion vectors | Files above L0, together with deletion vectors | After changes reach those files and metadata is published and refreshed |

Initial publication and metadata rebuilds use snapshot splits that can be read directly without
Paimon's merge logic. The table above describes subsequent incremental publication. Use compaction
to establish a predictable visibility boundary for primary key tables.

The [primary key guide](./primary-key-table.mdx) explains both modes and their configuration.
Disabling an Iceberg catalog's cache can help with interactive verification, but cannot make
uncompacted or unpublished changes visible.

:::caution Manage the table through Paimon

Use the Iceberg representation for reads. Perform writes, schema changes, compaction, snapshot
expiration, and file cleanup through Paimon. Both representations refer to shared data files;
independent Iceberg mutations or cleanup can invalidate Paimon's view of the table.

:::

## Supported Types

Compatibility depends on the column types, data file format, Iceberg format version, and reader.
See [supported data types and precision limits](./data-types.md) before enabling publication on an
existing table. Primary key deletion vectors and geospatial columns require Iceberg format v3.
