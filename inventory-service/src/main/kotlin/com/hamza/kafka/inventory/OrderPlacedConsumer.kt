package com.hamza.kafka.inventory

import com.hamza.commons.OrderPlacedEvent
import com.hamza.kafka.commons.BaseKafkaConsumer
import com.hamza.kafka.commons.IConsumerService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

@Component
class OrderPlacedConsumer(
    service: IConsumerService<OrderPlacedEvent, Inbox>,
) : BaseKafkaConsumer<OrderPlacedEvent, Inbox>(service) {
    @KafkaListener(topics = [$$"${custom.topics.placed}"])
    override fun onMessage(
        record: ConsumerRecord<String, OrderPlacedEvent>,
        ack: Acknowledgment,
    ) {
        super.onMessage(record, ack)
    }
}
