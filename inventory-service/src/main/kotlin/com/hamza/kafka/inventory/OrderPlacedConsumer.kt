package com.hamza.kafka.inventory

import com.hamza.commons.OrderPlacedEvent
import com.hamza.kafka.commons.KafkaConsumer
import com.hamza.kafka.commons.toJson
import jakarta.transaction.Transactional
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

@Component
class OrderPlacedConsumer(
    private val service: IPersistenceService<OrderPlacedEvent>,
) : KafkaConsumer<OrderPlacedEvent> {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    @KafkaListener(topics = [$$"${custom.topics.placed}"])
    override fun onMessage(
        record: ConsumerRecord<String, OrderPlacedEvent>,
        ack: Acknowledgment,
    ) {
        val event = record.value()
        logger.info(
            "received new event '{}' from '{}-{}@{}'",
            event.toJson(),
            record.topic(),
            record.partition(),
            record.offset(),
        )
        service.consume(event)
        ack.acknowledge()
    }
}
