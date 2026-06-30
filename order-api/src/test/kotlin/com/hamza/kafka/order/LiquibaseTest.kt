package com.hamza.kafka.order

import org.junit.jupiter.api.Test
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest
import org.springframework.context.annotation.Import

// for GraalVM tracing-agent to intercept pre/liquibase
@DataJpaTest(
    properties = [
        "preliquibase.enabled=true",
        $$"spring.datasource.hikari.connection-init-sql=set search_path to \"${spring.application.name}\"",
        "spring.jpa.hibernate.ddl-auto=validate",
        $$"spring.jpa.properties.hibernate.default_schema=${spring.application.name}",
        $$"spring.liquibase.default-schema=${spring.application.name}",
        "spring.liquibase.enabled=true",
    ],
)
@Import(PgTestContainer::class)
class LiquibaseTest {
    @Test
    fun `liquibase migration is valid`() {
    }
}
