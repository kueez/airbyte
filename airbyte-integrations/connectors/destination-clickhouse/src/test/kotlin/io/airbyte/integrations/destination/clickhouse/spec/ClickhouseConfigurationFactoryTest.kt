/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.clickhouse.spec

import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class ClickhouseConfigurationFactoryTest {
    private val factory = ClickhouseConfigurationFactory()

    @Test
    fun `asyncInsert defaults on`() {
        val config = factory.makeWithoutExceptionHandling(ClickhouseSpecificationOss())

        assertTrue(config.asyncInsert)
    }

    @Test
    fun `asyncInsert can be disabled via env override`() {
        val config =
            factory.makeWithOverrides(
                ClickhouseSpecificationOss(),
                mapOf("async_insert" to "false"),
            )

        assertFalse(config.asyncInsert)
    }
}
