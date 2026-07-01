package com.hamza.kafka.order

import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean
import org.testcontainers.DockerClientFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import java.time.Duration

@Disabled // FIXME: pausing is wrong, need to kill and create a new one for ex branch to exec
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = [
        "custom.batch_size=0",
        "spring.jpa.hibernate.ddl-auto=validate",
        "spring.liquibase.enabled=true",
    ],
)
@Import(PausablePgTestContainer::class)
class OutboxInsertionListenerTest {
    @Autowired
    private lateinit var orderRepo: OrderRepository

    @Autowired
    private lateinit var outboxRepo: OutboxRepository

    @Autowired
    private lateinit var persistenceService: IPersistenceService

    @MockitoSpyBean
    private lateinit var trigger: IDrainTrigger

    @Autowired
    private lateinit var pPgContainer: PostgreSQLContainer

    private val dockerClient = DockerClientFactory.instance().client()

    private val request =
        OrderPostDto(
            customerId = "user-2203",
            items = listOf(ItemDto(sku = "sku-01", quantity = 10, unitPriceCents = 199)),
        )

    @BeforeEach
    fun beforeEach() {
        verify(trigger, never()).trigger()
    }

    @AfterEach
    fun afterEach() {
        orderRepo.deleteAll()
        outboxRepo.deleteAll()
    }

    @Test
    fun `trigger fires again after PG pause and resume`() {
        persistenceService.save(request)
        await().atMost(Duration.ofSeconds(5)).untilAsserted {
            verify(trigger, times(1)).trigger()
        }

        dockerClient.pauseContainerCmd(pPgContainer.containerId).exec()
        Thread.sleep(Duration.ofSeconds(5).toMillis())
        dockerClient.unpauseContainerCmd(pPgContainer.containerId).exec()

        persistenceService.save(request)
        await().atMost(Duration.ofSeconds(15)).untilAsserted {
            verify(trigger, times(2)).trigger()
        }
    }
}
