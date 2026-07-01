package com.hamza.kafka.order

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.testcontainers.DockerClientFactory
import org.testcontainers.kafka.KafkaContainer
import tools.jackson.databind.ObjectMapper
import java.time.Duration
import java.util.UUID
import kotlin.collections.set

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = ["custom.publish_timeout=PT2S"], // tighten timeout
)
@Import(PgTestContainer::class, PausableKafkaTestContainer::class)
class PublishServiceTimeoutTest {
    @Autowired
    private lateinit var service: IPublishService

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @Autowired
    private lateinit var pKafkaContainer: KafkaContainer

    @Value($$"${custom.topic_name}")
    private lateinit var topicName: String

    private val consumer by lazy {
        val props = KafkaTestUtils.consumerProps(pKafkaContainer.bootstrapServers, "${UUID.randomUUID()}", true)
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
        props[ConsumerConfig.METADATA_MAX_AGE_CONFIG] = "1000"
        props[ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG] = "2000"
        DefaultKafkaConsumerFactory<String, String>(
            props,
            StringDeserializer(),
            StringDeserializer(),
        ).createConsumer()
    }

    private val dockerClient = DockerClientFactory.instance().client()

    @AfterEach
    fun afterEach() {
        consumer.close()
        runCatching { dockerClient.unpauseContainerCmd(pKafkaContainer.containerId).exec() }
    }

    @Test
    fun `publish should not inc attempts when kafka does not ack in time (transient failure)`() {
        // gen event + outbox
        val event =
            Event(
                orderId = "order_2203",
                customerId = "user_2203",
                items = listOf(Item(sku = "sku-01", quantity = 10, unitPriceCents = 199)),
                totalAmountCents = 1990,
            )
        val outbox =
            event.toOutbox(objectMapper, topicName).also {
                assertThat(it.attempts).isZero
                assertThat(it.publishedAt).isNull()
                assertThat(it.lastError).isNull()
            }

        // subscribe to kafka
        consumer.subscribe(listOf(topicName))
        await().atMost(Duration.ofSeconds(10)).until {
            consumer.poll(Duration.ofMillis(500))
            consumer.assignment().isNotEmpty()
        }

        // pause kafka
        dockerClient.pauseContainerCmd(pKafkaContainer.containerId).exec()

        // publish event to kafka
        service.publish(listOf(outbox)).also {
            assertThat(it).isEmpty() // kafka is paused, nothing succeeds
        }
        // timeout is a transient infrastructure failure, attempts should not be inc
        assertThat(outbox.attempts).isZero
        assertThat(outbox.publishedAt).isNull() // check still not published
        assertThat(outbox.lastError).isNull()

        // resume kafka
        dockerClient.unpauseContainerCmd(pKafkaContainer.containerId).exec()

        // check nothing was sent to kafka
        KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(30)).also {
            assertThat(it).isEmpty()
        }
    }
}
