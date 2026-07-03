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

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("default", "h2")
@Import(KafkaTestContainer::class)
class PublishServiceTest {
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
    fun `publish should send order to kafka and mark related outbox as published`() {
        // gen event + outbox
        val event =
            createOrderPlacedEvent(
                orderId = "order_2203",
                customerId = "user_2203",
                items = listOf(createEventItem(sku = "sku-01", quantity = 10, unitPriceCents = 199)),
            )
        val outbox = event.toOutbox(topicName)

        // subscribe to kafka
        consumer.subscribe(listOf(topicName))
        await().atMost(Duration.ofSeconds(10)).until {
            consumer.poll(Duration.ofMillis(500))
            consumer.assignment().isNotEmpty()
        }

        // publish event to kafka
        service.publish(listOf(outbox)).also {
            assertThat(it.publishedCount).isOne
        }

        // check event was sent to kafka
        KafkaTestUtils.getSingleRecord(consumer, topicName, Duration.ofSeconds(30)).also {
            assertThat(it.key()).isEqualTo(outbox.orderId)
            assertThat(it.value()).isEqualTo(event)
        }
    }
}
