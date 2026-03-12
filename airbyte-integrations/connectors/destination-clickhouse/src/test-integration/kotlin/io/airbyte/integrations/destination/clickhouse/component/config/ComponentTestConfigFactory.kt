/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.clickhouse.component.config

import io.airbyte.cdk.load.util.Jsons
import io.airbyte.integrations.destination.clickhouse.spec.ClickhouseConfiguration
import io.airbyte.integrations.destination.clickhouse.spec.ClickhouseConfigurationFactory
import io.airbyte.integrations.destination.clickhouse.spec.ClickhouseSpecificationOss
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.nio.file.Files
import java.nio.file.Path

@Requires(env = ["component"])
@Factory
class ComponentTestConfigFactory {
    companion object {
        private val ENV_MAP = mapOf(
            "CLICKHOUSE_HOST" to "hostname",
            "CLICKHOUSE_PORT" to "port",
            "CLICKHOUSE_USERNAME" to "username",
            "CLICKHOUSE_PASSWORD" to "password",
            "CLICKHOUSE_DATABASE" to "database",
            "CLICKHOUSE_PROTOCOL" to "protocol",
            "CLICKHOUSE_CLUSTER_NAME" to "cluster_name",
            "CLICKHOUSE_USE_REPLICATED_ENGINE" to "use_replicated_engine",
            "CLICKHOUSE_USE_ON_CLUSTER" to "use_on_cluster",
        )
    }

    @Singleton
    @Primary
    fun config(): ClickhouseConfiguration {
        val factory = ClickhouseConfigurationFactory()
        val spec = Jsons.readValue(
            Files.readString(Path.of("secrets/test-instance.json")),
            ClickhouseSpecificationOss::class.java,
        )
        val envOverrides = buildMap {
            ENV_MAP.forEach { (envKey, configKey) ->
                System.getenv(envKey)?.let { put(configKey, it) }
            }
        }
        return if (envOverrides.isEmpty()) {
            factory.makeWithoutExceptionHandling(spec)
        } else {
            factory.makeWithOverrides(spec, envOverrides)
        }
    }
}
