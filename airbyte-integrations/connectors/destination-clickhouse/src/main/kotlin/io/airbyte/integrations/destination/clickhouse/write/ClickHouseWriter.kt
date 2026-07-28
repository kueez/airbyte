/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.clickhouse.write

import io.airbyte.cdk.SystemErrorException
import io.airbyte.cdk.load.command.DestinationCatalog
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.table.ColumnNameMapping
import io.airbyte.cdk.load.table.DatabaseInitialStatusGatherer
import io.airbyte.cdk.load.table.directload.DirectLoadInitialStatus
import io.airbyte.cdk.load.table.directload.DirectLoadTableAppendStreamLoader
import io.airbyte.cdk.load.table.directload.DirectLoadTableAppendTruncateStreamLoader
import io.airbyte.cdk.load.table.directload.DirectLoadTableExecutionConfig
import io.airbyte.cdk.load.write.DestinationWriter
import io.airbyte.cdk.load.write.StreamLoader
import io.airbyte.cdk.load.write.StreamStateStore
import io.airbyte.integrations.destination.clickhouse.client.ClickhouseAirbyteClient
import jakarta.inject.Singleton

@Singleton
class ClickHouseWriter(
    private val names: DestinationCatalog,
    private val stateGatherer: DatabaseInitialStatusGatherer<DirectLoadInitialStatus>,
    private val streamStateStore: StreamStateStore<DirectLoadTableExecutionConfig>,
    private val clickhouseClient: ClickhouseAirbyteClient,
) : DestinationWriter {
    private lateinit var initialStatuses: Map<DestinationStream, DirectLoadInitialStatus>

    override suspend fun setup() {
        // Deduplicate namespaces before creating them. CREATE DATABASE IF NOT EXISTS is a cheap
        // local no-op on single-node ClickHouse, but on the Replicated database engine it is issued
        // ON CLUSTER (the database cannot self-propagate its own creation), so every call costs a
        // Keeper-coordinated entry in /clickhouse/task_queue/ddl that all replicas must process
        // with distributed_ddl.pool_size = 1. Without distinct(), a connection emits one
        // identical statement per stream on every sync -- 51 for a 51-stream connection, 50 of
        // them redundant.
        names.streams
            .map { it.tableSchema.tableNames.finalTableName!!.namespace }
            .distinct()
            .forEach { clickhouseClient.createNamespace(it) }

        initialStatuses = stateGatherer.gatherInitialStatus()
    }

    override fun createStreamLoader(stream: DestinationStream): StreamLoader {
        val initialStatus = initialStatuses[stream]!!
        val realTableName = stream.tableSchema.tableNames.finalTableName!!
        val tempTableName = stream.tableSchema.tableNames.tempTableName!!
        val columnNameMapping =
            ColumnNameMapping(stream.tableSchema.columnSchema.inputToFinalColumnNames)
        return when (stream.minimumGenerationId) {
            0L ->
                DirectLoadTableAppendStreamLoader(
                    stream,
                    initialStatus,
                    realTableName = realTableName,
                    tempTableName = tempTableName,
                    columnNameMapping,
                    clickhouseClient,
                    clickhouseClient,
                    streamStateStore,
                )
            stream.generationId ->
                DirectLoadTableAppendTruncateStreamLoader(
                    stream,
                    initialStatus,
                    realTableName = realTableName,
                    tempTableName = tempTableName,
                    columnNameMapping,
                    clickhouseClient,
                    clickhouseClient,
                    streamStateStore,
                )
            else ->
                throw SystemErrorException(
                    "Cannot execute a hybrid refresh - current generation ${stream.generationId}; minimum generation ${stream.minimumGenerationId}"
                )
        }
    }
}
