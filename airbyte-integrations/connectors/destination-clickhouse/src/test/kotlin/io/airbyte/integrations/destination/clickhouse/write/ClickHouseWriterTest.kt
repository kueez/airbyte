/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.clickhouse.write

import io.airbyte.cdk.load.command.DestinationCatalog
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.schema.model.TableName
import io.airbyte.cdk.load.table.DatabaseInitialStatusGatherer
import io.airbyte.cdk.load.table.directload.DirectLoadInitialStatus
import io.airbyte.cdk.load.table.directload.DirectLoadTableExecutionConfig
import io.airbyte.cdk.load.write.StreamStateStore
import io.airbyte.integrations.destination.clickhouse.client.ClickhouseAirbyteClient
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test

class ClickHouseWriterTest {
    private val clickhouseClient: ClickhouseAirbyteClient = mockk(relaxed = true)
    private val stateGatherer: DatabaseInitialStatusGatherer<DirectLoadInitialStatus> =
        mockk(relaxed = true)
    private val streamStateStore: StreamStateStore<DirectLoadTableExecutionConfig> =
        mockk(relaxed = true)

    private fun streamIn(namespace: String, name: String): DestinationStream =
        mockk(relaxed = true) {
            every { tableSchema } returns
                mockk(relaxed = true) {
                    every { tableNames } returns
                        mockk(relaxed = true) {
                            every { finalTableName } returns TableName(namespace, name)
                        }
                }
        }

    private fun writerFor(vararg streams: DestinationStream): ClickHouseWriter {
        val catalog =
            mockk<DestinationCatalog> { every { this@mockk.streams } returns streams.toList() }
        coEvery { stateGatherer.gatherInitialStatus() } returns emptyMap()
        return ClickHouseWriter(catalog, stateGatherer, streamStateStore, clickhouseClient)
    }

    @Test
    fun `setup creates each namespace once regardless of stream count`() = runTest {
        // On the Replicated database engine CREATE DATABASE is issued ON CLUSTER, so one statement
        // per stream floods /clickhouse/task_queue/ddl. Many streams, one namespace => one call.
        val writer =
            writerFor(
                streamIn("yoto_metrics", "fa_ads"),
                streamIn("yoto_metrics", "fa_adsets"),
                streamIn("yoto_metrics", "fa_campaigns"),
            )

        writer.setup()

        coVerify(exactly = 1) { clickhouseClient.createNamespace("yoto_metrics") }
    }

    @Test
    fun `setup still creates every distinct namespace`() = runTest {
        val writer =
            writerFor(
                streamIn("yoto_metrics", "fa_ads"),
                streamIn("yoto_metrics", "fa_adsets"),
                streamIn("yoto_cms", "posts"),
            )

        writer.setup()

        coVerify(exactly = 1) { clickhouseClient.createNamespace("yoto_metrics") }
        coVerify(exactly = 1) { clickhouseClient.createNamespace("yoto_cms") }
    }
}
