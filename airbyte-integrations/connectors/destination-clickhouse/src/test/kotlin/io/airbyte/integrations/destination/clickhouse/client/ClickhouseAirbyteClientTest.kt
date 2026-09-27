/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.clickhouse.client

import com.clickhouse.client.api.Client as ClickHouseClientRaw
import com.clickhouse.client.api.command.CommandResponse
import com.clickhouse.client.api.query.QueryResponse
import io.airbyte.cdk.ConfigErrorException
import io.airbyte.cdk.load.command.Append
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.component.ColumnChangeset
import io.airbyte.cdk.load.component.ColumnType
import io.airbyte.cdk.load.component.ColumnTypeChange
import io.airbyte.cdk.load.component.TableSchema
import io.airbyte.cdk.load.data.FieldType
import io.airbyte.cdk.load.data.StringType
import io.airbyte.cdk.load.message.Meta
import io.airbyte.cdk.load.schema.model.StreamTableSchema
import io.airbyte.cdk.load.schema.model.TableName
import io.airbyte.cdk.load.table.ColumnNameMapping
import io.airbyte.cdk.load.table.TempTableNameGenerator
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.coVerifyOrder
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import java.util.concurrent.CompletableFuture
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class ClickhouseAirbyteClientTest {
    // Mocks
    private val client: ClickHouseClientRaw = mockk(relaxed = true)
    private val clickhouseSqlGenerator: ClickhouseSqlGenerator = mockk(relaxed = true)
    private val tempTableNameGenerator: TempTableNameGenerator = mockk(relaxed = true)
    private val testConfig = io.airbyte.integrations.destination.clickhouse.spec.ClickhouseConfiguration(
        hostname = "localhost", port = "8123", protocol = "http", database = "default",
        username = "default", password = "", enableJson = false,
        tunnelConfig = io.airbyte.cdk.ssh.SshNoTunnelMethod, recordWindowSize = 100_000L,
        useReplicatedEngine = false, useOnCluster = false, clusterName = "",
    )

    // Client
    private val clickhouseAirbyteClient =
        spyk(
            ClickhouseAirbyteClient(
                client,
                clickhouseSqlGenerator,
                tempTableNameGenerator,
                testConfig,
            )
        )

    @Test
    fun testExecute() = runTest {
        val expectedResponse = mockk<CommandResponse>(relaxed = true)
        val completableFuture = CompletableFuture.completedFuture(expectedResponse)
        coEvery { client.execute(DUMMY_SENTENCE) } returns completableFuture

        clickhouseAirbyteClient.execute(DUMMY_SENTENCE)

        coVerify { client.execute(DUMMY_SENTENCE) }
    }

    @Test
    fun testQuery() = runTest {
        val expectedResponse = mockk<QueryResponse>(relaxed = true)
        val completableFuture = CompletableFuture.completedFuture(expectedResponse)
        coEvery { client.query(DUMMY_SENTENCE) } returns completableFuture

        clickhouseAirbyteClient.query(DUMMY_SENTENCE)

        coVerify { client.query(DUMMY_SENTENCE) }
    }

    private fun mockCHSchemaWithAirbyteColumns() {
        every { client.getTableSchema(any(), any()) } returns
            mockk {
                every { columns } returns
                    listOf(
                        mockk { every { columnName } returns Meta.COLUMN_NAME_AB_RAW_ID },
                        mockk { every { columnName } returns Meta.COLUMN_NAME_AB_EXTRACTED_AT },
                        mockk { every { columnName } returns Meta.COLUMN_NAME_AB_META },
                        mockk { every { columnName } returns Meta.COLUMN_NAME_AB_GENERATION_ID },
                    )
            }
    }

    @Test
    fun `test ensure schema matches`() = runTest {
        val columnChangeset =
            ColumnChangeset(
                columnsToAdd = mapOf("new_col" to ColumnType("String", true)),
                columnsToChange = emptyMap(),
                columnsToDrop = emptyMap(),
                columnsToRetain = emptyMap(),
            )

        val mockTableName = mockk<TableName>(relaxed = true)
        val alterTableStatement = "ALTER TABLE my_table ADD COLUMN new_col String"

        coEvery { clickhouseSqlGenerator.alterTable(columnChangeset, mockTableName) } returns
            alterTableStatement
        coEvery { clickhouseAirbyteClient.execute(alterTableStatement) } returns
            mockk(relaxed = true)

        mockCHSchemaWithAirbyteColumns()

        val columnMapping = ColumnNameMapping(mapOf())
        val stream =
            mockk<DestinationStream> {
                every { mappedDescriptor } returns
                    mockk(relaxed = true) {
                        every { name } returns "my_table"
                        every { namespace } returns "my_namespace"
                    }
                every { tableSchema } returns
                    mockk(relaxed = true) {
                        every { columnSchema } returns
                            mockk(relaxed = true) {
                                every { inputSchema } returns LinkedHashMap.newLinkedHashMap(0)
                                every { inputToFinalColumnNames } returns emptyMap()
                            }
                        every { importType } returns Append
                        every { getPrimaryKey() } returns emptyList()
                        every { getCursor() } returns emptyList()
                    }
                every { shouldBeTruncatedAtEndOfSync() } returns false
            }
        clickhouseAirbyteClient.applyChangeset(
            stream,
            columnMapping,
            mockTableName,
            mapOf("new_col" to ColumnType("String", true)),
            columnChangeset,
        )

        coVerifyOrder {
            clickhouseSqlGenerator.alterTable(columnChangeset, mockTableName)
            clickhouseAirbyteClient.execute(alterTableStatement)
        }
    }

    @Test
    fun `test ensure schema matches with dedup changes`() = runTest {
        val columnChangeset =
            ColumnChangeset(
                columnsToAdd = emptyMap(),
                // Note that we're changing the nullability of the column.
                // This will trigger the table-recreate logic.
                columnsToChange =
                    mapOf(
                        "something" to
                            ColumnTypeChange(
                                ColumnType("IrrelevantValue", false),
                                ColumnType("IrrelevantValue", true)
                            )
                    ),
                columnsToDrop = mapOf("test" to ColumnType("String", true)),
                columnsToRetain = emptyMap(),
            )

        val finalTableName = TableName("fin", "al")
        val tempTableName = TableName("temp", "orary")

        coEvery { clickhouseAirbyteClient.execute(any()) } returns mockk(relaxed = true)
        every { tempTableNameGenerator.generate(any()) } returns tempTableName

        mockCHSchemaWithAirbyteColumns()

        val columnMapping = ColumnNameMapping(mapOf())
        val tableSchema1: StreamTableSchema =
            mockk(relaxed = true) {
                every { columnSchema } returns
                    mockk(relaxed = true) {
                        every { inputSchema } returns LinkedHashMap.newLinkedHashMap(0)
                        every { inputToFinalColumnNames } returns emptyMap()
                    }
                every { importType } returns Append
                every { getPrimaryKey() } returns emptyList()
                every { getCursor() } returns emptyList()
            }
        val stream =
            mockk<DestinationStream> {
                every { mappedDescriptor } returns
                    mockk(relaxed = true) {
                        every { name } returns "my_table"
                        every { namespace } returns "my_namespace"
                    }
                every { tableSchema } returns tableSchema1
                every { shouldBeTruncatedAtEndOfSync() } returns false
            }
        clickhouseAirbyteClient.applyChangeset(
            stream,
            columnMapping,
            finalTableName,
            emptyMap(),
            columnChangeset,
        )

        coVerify(exactly = 0) { clickhouseSqlGenerator.alterTable(any(), any()) }

        coVerifyOrder {
            clickhouseSqlGenerator.createNamespace(tempTableName.namespace)
            clickhouseSqlGenerator.createTable(tempTableName, tableSchema1, true)
            clickhouseSqlGenerator.copyTable(setOf("something"), finalTableName, tempTableName)
            clickhouseSqlGenerator.exchangeTable(tempTableName, finalTableName)
            clickhouseSqlGenerator.dropTable(tempTableName)
        }
        coVerify(exactly = 5) { clickhouseAirbyteClient.execute(any()) }
    }

    @Test
    fun `test ensure schema matches fails if no airbyte columns`() = runTest {
        val finalTableName = TableName("fin", "al")

        val columnMapping = ColumnNameMapping(mapOf())
        val stream =
            mockk<DestinationStream> {
                every { mappedDescriptor } returns
                    mockk(relaxed = true) {
                        every { name } returns "my_table"
                        every { namespace } returns "my_namespace"
                    }
            }

        // discoverSchema now reads system.columns; a table with no columns (empty reader) is
        // missing the Airbyte internal columns and must raise ConfigErrorException.
        val response = mockk<QueryResponse>(relaxed = true)
        coEvery {
            client.query(match { it.startsWith("SELECT name, type FROM system.columns") })
        } returns CompletableFuture.completedFuture(response)
        every { client.newBinaryFormatReader(response) } returns
            mockk(relaxed = true) { every { next() } returns null }

        assertThrows<ConfigErrorException> {
            clickhouseAirbyteClient.ensureSchemaMatches(stream, finalTableName, columnMapping)
        }
    }

    @Test
    fun `test overwrite table`() = runTest {
        val sourceTableName = TableName("source_db", "source_table")
        val targetTableName = TableName("target_db", "target_table")
        val exchangeTableSql =
            "EXCHANGE TABLES `source_db`.`source_table` AND `target_db`.`target_table`"
        val dropTableSql = "DROP TABLE `source_db`.`source_table`"

        every { clickhouseSqlGenerator.exchangeTable(sourceTableName, targetTableName) } returns
            exchangeTableSql
        every { clickhouseSqlGenerator.dropTable(sourceTableName) } returns dropTableSql
        coEvery { clickhouseAirbyteClient.execute(exchangeTableSql) } returns mockk()
        coEvery { clickhouseAirbyteClient.execute(dropTableSql) } returns mockk()

        clickhouseAirbyteClient.overwriteTable(sourceTableName, targetTableName)

        verify { clickhouseSqlGenerator.exchangeTable(sourceTableName, targetTableName) }
        verify { clickhouseSqlGenerator.dropTable(sourceTableName) }
        coVerifyOrder {
            clickhouseAirbyteClient.execute(exchangeTableSql)
            clickhouseAirbyteClient.execute(dropTableSql)
        }
    }

    @Test
    fun `test createTable with replace drops before creating`() = runTest {
        // Replace must be DROP ... SYNC + CREATE (not CREATE OR REPLACE) to avoid Keeper orphans.
        val tableName = TableName("db", "t")
        val dropSql = "DROP TABLE IF EXISTS `db`.`t` SYNC"
        val createSql = "CREATE TABLE `db`.`t` (...)"
        val schema = mockk<StreamTableSchema>(relaxed = true)
        val stream = mockk<DestinationStream> { every { tableSchema } returns schema }
        every { clickhouseSqlGenerator.dropTable(tableName) } returns dropSql
        every { clickhouseSqlGenerator.createTable(tableName, schema, true) } returns createSql
        coEvery { clickhouseAirbyteClient.execute(dropSql) } returns mockk()
        coEvery { clickhouseAirbyteClient.execute(createSql) } returns mockk()

        clickhouseAirbyteClient.createTable(stream, tableName, ColumnNameMapping(mapOf()), true)

        coVerifyOrder {
            clickhouseAirbyteClient.execute(dropSql)
            clickhouseAirbyteClient.execute(createSql)
        }
    }

    @Test
    fun `test createTable without replace does not drop`() = runTest {
        val tableName = TableName("db", "t")
        val createSql = "CREATE TABLE IF NOT EXISTS `db`.`t` (...)"
        val schema = mockk<StreamTableSchema>(relaxed = true)
        val stream = mockk<DestinationStream> { every { tableSchema } returns schema }
        every { clickhouseSqlGenerator.createTable(tableName, schema, false) } returns createSql
        coEvery { clickhouseAirbyteClient.execute(createSql) } returns mockk()

        clickhouseAirbyteClient.createTable(stream, tableName, ColumnNameMapping(mapOf()), false)

        verify(exactly = 0) { clickhouseSqlGenerator.dropTable(any()) }
        coVerify { clickhouseAirbyteClient.execute(createSql) }
    }

    @Test
    fun `test getAirbyteSchemaWithClickhouseType with simple schema`() {
        val columns = LinkedHashMap.newLinkedHashMap<String, FieldType>(1)
        columns["field 1"] = FieldType(StringType, true)

        val stream =
            mockk<DestinationStream> {
                every { mappedDescriptor } returns
                    mockk(relaxed = true) {
                        every { name } returns "my_table"
                        every { namespace } returns "my_namespace"
                    }
                every { tableSchema } returns
                    mockk(relaxed = true) {
                        every { columnSchema } returns
                            mockk(relaxed = true) {
                                every { inputSchema } returns columns
                                every { inputToFinalColumnNames } returns
                                    mapOf("field 1" to "field_1")
                                every { finalSchema } returns
                                    mapOf("field_1" to ColumnType("String", true))
                            }
                        every { importType } returns Append
                        every { getPrimaryKey() } returns emptyList()
                        every { getCursor() } returns emptyList()
                    }
            }

        val columnMapping = ColumnNameMapping(mapOf("field 1" to "field_1"))

        val expected =
            TableSchema(
                mapOf(
                    "field_1" to ColumnType("String", true),
                ),
            )
        val actual = clickhouseAirbyteClient.computeSchema(stream, columnMapping)
        Assertions.assertEquals(expected, actual)
    }

    @Test
    fun `countTable skips the counting query when the table is absent`() = runTest {
        // Regression guard for the UNKNOWN_TABLE log storm: the status gatherer probes a temp
        // table that is absent by design on every sync. countTable must decide "missing" via
        // EXISTS TABLE, never by firing SELECT count(1) against it and swallowing the server error.
        val absentTable = TableName("db", "leftover_tmp_deadbeef")
        val existsResponse = mockk<QueryResponse>(relaxed = true)
        coEvery { client.query(match { it.startsWith("EXISTS TABLE") }) } returns
            CompletableFuture.completedFuture(existsResponse)
        every { client.newBinaryFormatReader(existsResponse) } returns
            mockk(relaxed = true) { every { getInteger("result") } returns 0 }

        val result = clickhouseAirbyteClient.countTable(absentTable)

        Assertions.assertNull(result)
        // Existence was probed with EXISTS TABLE...
        coVerify { client.query(match { it.startsWith("EXISTS TABLE") }) }
        // ...and no counting query was ever generated against the absent table.
        verify(exactly = 0) { clickhouseSqlGenerator.countTable(any(), any()) }
    }

    @Test
    fun `countTable returns the row count when the table exists`() = runTest {
        val table = TableName("db", "real_table")
        val countSql = "SELECT count(1) cnt FROM `db`.`real_table`;"
        val existsResponse = mockk<QueryResponse>(relaxed = true)
        val countResponse = mockk<QueryResponse>(relaxed = true)
        coEvery { client.query(match { it.startsWith("EXISTS TABLE") }) } returns
            CompletableFuture.completedFuture(existsResponse)
        every { client.newBinaryFormatReader(existsResponse) } returns
            mockk(relaxed = true) { every { getInteger("result") } returns 1 }
        every { clickhouseSqlGenerator.countTable(table, "cnt") } returns countSql
        coEvery { client.query(countSql) } returns CompletableFuture.completedFuture(countResponse)
        every { client.newBinaryFormatReader(countResponse) } returns
            mockk(relaxed = true) { every { getLong("cnt") } returns 42L }

        val result = clickhouseAirbyteClient.countTable(table)

        Assertions.assertEquals(42L, result)
    }

    @Test
    fun `discoverSchema reads system columns instead of the fragile DESCRIBE TSKV parser`() =
        runTest {
            // Regression guard: client.getTableSchema() parses DESCRIBE ... FORMAT TSKV through
            // java.util.Properties and throws on the Replicated/on-cluster engine. discoverSchema
            // must read system.columns via the binary reader instead.
            val table = TableName("db", "real_table")
            val response = mockk<QueryResponse>(relaxed = true)
            coEvery {
                client.query(match { it.startsWith("SELECT name, type FROM system.columns") })
            } returns CompletableFuture.completedFuture(response)
            val names =
                listOf(
                        Meta.COLUMN_NAME_AB_RAW_ID,
                        Meta.COLUMN_NAME_AB_EXTRACTED_AT,
                        Meta.COLUMN_NAME_AB_META,
                        Meta.COLUMN_NAME_AB_GENERATION_ID,
                        "id",
                    )
                    .iterator()
            val types = listOf("String", "DateTime64(3)", "JSON", "Int64", "Int64").iterator()
            val rows = ArrayDeque(List(5) { emptyMap<String, Any>() })
            every { client.newBinaryFormatReader(response) } returns
                mockk(relaxed = true) {
                    every { next() } answers { rows.removeFirstOrNull() }
                    every { getString("name") } answers { names.next() }
                    every { getString("type") } answers { types.next() }
                }

            val schema = clickhouseAirbyteClient.discoverSchema(table)

            Assertions.assertEquals(
                TableSchema(mapOf("id" to ColumnType("Int64", false))),
                schema,
            )
        }

    @Test
    fun `discoverSchema rejects a table missing Airbyte columns`() = runTest {
        val table = TableName("db", "foreign_table")
        val response = mockk<QueryResponse>(relaxed = true)
        coEvery {
            client.query(match { it.startsWith("SELECT name, type FROM system.columns") })
        } returns CompletableFuture.completedFuture(response)
        val rows = ArrayDeque(List(1) { emptyMap<String, Any>() })
        every { client.newBinaryFormatReader(response) } returns
            mockk(relaxed = true) {
                every { next() } answers { rows.removeFirstOrNull() }
                every { getString("name") } returns "id"
                every { getString("type") } returns "Int64"
            }

        assertThrows<ConfigErrorException> { clickhouseAirbyteClient.discoverSchema(table) }
    }

    companion object {
        // Constants
        private const val DUMMY_SENTENCE = "SELECT 1"
    }
}
