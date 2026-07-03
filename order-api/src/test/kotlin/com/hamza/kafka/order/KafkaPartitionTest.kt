package com.hamza.kafka.order

import com.hamza.kafka.avro.OrderPlacedEvent
import com.hamza.kafka.commons.createEventItem
import com.hamza.kafka.commons.createOrderPlacedEvent
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
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
import org.springframework.test.context.ActiveProfiles
import org.testcontainers.kafka.KafkaContainer
import java.time.Duration
import java.util.UUID

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = ["custom.partitions=3"],
)
@ActiveProfiles("default", "h2")
@Import(KafkaTestContainer::class)
class KafkaPartitionTest {
    @Autowired
    private lateinit var service: IPublishService

    @Autowired
    private lateinit var kafkaContainer: KafkaContainer

    @Value($$"${custom.topic_name}")
    private lateinit var topicName: String

    @Value($$"${spring.kafka.producer.properties.schema.registry.url}")
    private lateinit var schemaRegistryUrl: String

    private val consumer by lazy {
        KafkaTestUtils
            .consumerProps(kafkaContainer.bootstrapServers, UUID.randomUUID().toString(), true)
            .apply {
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
                put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000")
                put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "2000")
                put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
                put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer::class.java)
                put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
                put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true)
            }.let {
                DefaultKafkaConsumerFactory<String, OrderPlacedEvent>(it).createConsumer()
            }
    }

    @AfterEach
    fun afterEach() {
        consumer.close()
    }

    @Test
    fun `same orderId = same partition`() {
        // gen many events with same orderId
        val orderId = "order_2203"
        val outboxes =
            (1..15).map {
                createOrderPlacedEvent(
                    orderId = orderId,
                    customerId = "user_220$it",
                    items = listOf(createEventItem(sku = "sku-0$it", quantity = 1 * it, unitPriceCents = 10 * it)),
                ).toOutbox(topicName)
            }

        // subscribe to kafka
        consumer.subscribe(listOf(topicName))
        await().atMost(Duration.ofSeconds(10)).until {
            consumer.poll(Duration.ofMillis(500))
            consumer.assignment().isNotEmpty()
        }

        // publish events to kafka
        service.publish(outboxes)

        // check events landed in same topic
        val records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(30)).records(topicName)
        val partitionsHit = records.map { it.partition() }.toSet()
        assertThat(partitionsHit).hasSize(1)
    }

    @Test
    fun `different orderId = spread across more than 1 partition`() {
        // gen many events with different orderId
        val outboxes =
            (1..15).map {
                createOrderPlacedEvent(
                    orderId = "order_20$it",
                    customerId = "user_220$it",
                    items = listOf(createEventItem(sku = "sku-0$it", quantity = 1 * it, unitPriceCents = 10 * it)),
                ).toOutbox(topicName)
            }

        // subscribe to kafka
        consumer.subscribe(listOf(topicName))
        await().atMost(Duration.ofSeconds(10)).until {
            consumer.poll(Duration.ofMillis(500))
            consumer.assignment().isNotEmpty()
        }

        // publish events to kafka
        service.publish(outboxes)

        // check events spread across more than 1 partition
        val records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(30)).records(topicName)
        val partitionsHit = records.map { it.partition() }.toSet()
        assertThat(partitionsHit.size).isGreaterThan(1)
    }
}
