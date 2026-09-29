/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.clickhouse.config

import com.clickhouse.client.api.Client
import com.clickhouse.client.api.internal.ServerSettings
import io.airbyte.cdk.command.ConfigurationSpecificationSupplier
import io.airbyte.cdk.load.dataflow.config.model.AggregatePublishingConfig
import io.airbyte.cdk.load.table.DefaultTempTableNameGenerator
import io.airbyte.cdk.load.table.TempTableNameGenerator
import io.airbyte.cdk.ssh.startTunnelAndGetEndpoint
import io.airbyte.integrations.destination.clickhouse.spec.ClickhouseConfiguration
import io.airbyte.integrations.destination.clickhouse.spec.ClickhouseConfigurationFactory
import io.airbyte.integrations.destination.clickhouse.spec.ClickhouseSpecification
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.temporal.ChronoUnit

@Factory
class ClickhouseBeanFactory {
    private val log = KotlinLogging.logger {}

    companion object {
        private val ENV_OVERRIDES = mapOf(
            "CLICKHOUSE_HOST" to "hostname",
            "CLICKHOUSE_PORT" to "port",
            "CLICKHOUSE_USERNAME" to "username",
            "CLICKHOUSE_PASSWORD" to "password",
            "CLICKHOUSE_DATABASE" to "database",
            "CLICKHOUSE_PROTOCOL" to "protocol",
            "CLICKHOUSE_CLUSTER_NAME" to "cluster_name",
            "CLICKHOUSE_USE_REPLICATED_ENGINE" to "use_replicated_engine",
            "CLICKHOUSE_USE_ON_CLUSTER" to "use_on_cluster",
            "CLICKHOUSE_ASYNC_INSERT" to "async_insert",
        )
    }
    /**
     * The endpoint the client connects through.
     *
     * Either the raw clickhouse instance endpoint or an SSH tunnel.
     */
    @Singleton
    @Named("resolvedEndpoint")
    fun resolvedEndpoint(config: ClickhouseConfiguration): String {
        val baseEndpoint =
            startTunnelAndGetEndpoint(config.tunnelConfig, config.hostname, config.port.toInt())
        return "${config.protocol}://$baseEndpoint"
    }

    @Singleton
    fun clickhouseClient(
        config: ClickhouseConfiguration,
        @Named("resolvedEndpoint") endpoint: String,
    ): Client {
        val builder =
            Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(config.username)
                .setPassword(config.password)
                .compressClientRequest(true)
                .setClientName("airbyte-v2")
                .setConnectTimeout(5, ChronoUnit.MINUTES)
                // DDL safety on the Replicated database engine behind a non-deterministic load
                // balancer. Lift the drop-size guard so overwrite swaps can drop the old (possibly
                // large) table; make drops wait for Keeper cleanup; and wait for cluster-wide DDL so
                // a follow-up statement landing on another replica sees the prior DDL.
                .serverSetting("max_table_size_to_drop", "0")
                .serverSetting("max_partition_size_to_drop", "0")
                .serverSetting("database_atomic_wait_for_drop_and_detach_synchronously", "1")
                .serverSetting("distributed_ddl_task_timeout", "300")
                .serverSetting("distributed_ddl_output_mode", "null_status_on_timeout_only_active")

        // Set async_insert explicitly in BOTH states, not just when enabled: some ClickHouse
        // client-v2 builds hardcode async_insert, so turning the option off must send
        // async_insert = 0 to actually get synchronous inserts back.
        builder.serverSetting("async_insert", if (config.asyncInsert) "1" else "0")
        if (config.asyncInsert) {
            // Coalesce our many small inserts (median ~410 rows, p10 = 3) into fewer parts
            // instead of one part per insert. wait_for_async_insert = 1 keeps each insert
            // synchronous from our side, so a batch is durably flushed before the CDK advances
            // sync state.
            builder.serverSetting("wait_for_async_insert", "1")
        }

        if (config.enableJson) {
            builder
                // allow experimental JSON type
                .serverSetting("allow_experimental_json_type", "1")
                // allow JSON transcoding as a string. We need this to be able to provide a string
                // as a JSON input.
                .serverSetting(ServerSettings.INPUT_FORMAT_BINARY_READ_JSON_AS_STRING, "1")
                .serverSetting(ServerSettings.OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING, "1")
        }

        return builder.build()
    }

    @Singleton
    fun clickhouseConfiguration(
        configFactory: ClickhouseConfigurationFactory,
        specFactory: ConfigurationSpecificationSupplier<ClickhouseSpecification>,
    ): ClickhouseConfiguration {
        val spec = specFactory.get()
        val envOverrides = buildMap {
            ENV_OVERRIDES.forEach { (envKey, configKey) ->
                System.getenv(envKey)?.let {
                    put(configKey, it)
                    log.info { "Config override from env: $envKey -> $configKey" }
                }
            }
        }
        return if (envOverrides.isEmpty()) {
            configFactory.makeWithoutExceptionHandling(spec)
        } else {
            configFactory.makeWithOverrides(spec, envOverrides)
        }
    }

    @Singleton
    fun tempTableNameGenerator(): TempTableNameGenerator = DefaultTempTableNameGenerator()

    @Singleton
    fun aggregatePublishingConfig(clickhouseConfiguration: ClickhouseConfiguration) =
        AggregatePublishingConfig(
            maxRecordsPerAgg = clickhouseConfiguration.resolvedRecordWindowSize,
        )
}
