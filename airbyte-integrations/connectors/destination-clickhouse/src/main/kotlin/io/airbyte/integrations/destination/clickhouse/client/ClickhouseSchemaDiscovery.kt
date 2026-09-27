/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.clickhouse.client

import com.clickhouse.client.api.Client
import com.clickhouse.client.api.metadata.TableSchema
import com.clickhouse.data.ClickHouseColumn

/**
 * Build a [TableSchema] by reading `system.columns`, instead of [Client.getTableSchema].
 *
 * `client.getTableSchema()` runs `DESCRIBE TABLE ... FORMAT TSKV` and parses the response as TSKV
 * text, but the client always sends the header `X-ClickHouse-Format: RowBinaryWithNamesAndTypes`.
 * On ClickHouse server 26.8+ that header overrides the query's `FORMAT` clause (ClickHouse PR
 * #105249), so the server returns RowBinary, the TSKV parser reads binary as text, and the column
 * name/type come back null:
 *
 *   com.clickhouse.client.api.ClientException: Failed to get table schema
 *   -> java.lang.IllegalArgumentException: Non-null columnName and columnType are required
 *
 * Reading `system.columns` over the RowBinaryWithNamesAndTypes path the client already forces sides
 * steps the whole conflict. `ClickHouseColumn.of(name, type)` still parses each column, and
 * `ORDER BY position` preserves the physical column order the RowBinary writer relies on.
 *
 * Fixed upstream in clickhouse-java #3069, but unreleased as of client-v2 0.9.4 / 0.10.0. Remove
 * this and go back to getTableSchema() once a fixed client version is in use.
 */
internal fun Client.discoverTableSchema(namespace: String, name: String): TableSchema {
    val ns = namespace.replace("'", "\\'")
    val tbl = name.replace("'", "\\'")
    val response =
        query(
                "SELECT name, type FROM system.columns " +
                    "WHERE database = '$ns' AND table = '$tbl' ORDER BY position",
            )
            .get()
    val reader = newBinaryFormatReader(response)
    val columns = mutableListOf<ClickHouseColumn>()
    while (reader.next() != null) {
        columns.add(ClickHouseColumn.of(reader.getString("name"), reader.getString("type")))
    }
    return TableSchema(name, "", namespace, columns)
}
