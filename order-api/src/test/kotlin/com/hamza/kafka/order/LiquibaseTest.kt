package com.hamza.kafka.order

import org.junit.jupiter.api.Test
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest
import org.springframework.context.annotation.Import

// for GraalVM tracing-agent to intercept liquibase
@DataJpaTest(properties = ["spring.jpa.hibernate.ddl-auto=validate", "spring.liquibase.enabled=true"])
@Import(PgTestContainer::class)
class LiquibaseTest {
    @Test
    fun `liquibase migration is valid`() {
    }
}
