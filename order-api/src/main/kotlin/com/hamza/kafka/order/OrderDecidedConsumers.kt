package com.hamza.kafka.order

import com.hamza.commons.OrderDecidedEvent
import com.hamza.kafka.commons.BaseKafkaConsumer
import com.hamza.kafka.commons.IConsumerService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

@Component
class OrderAcceptedConsumer(
    service: IConsumerService<OrderDecidedEvent, Inbox>,
) : BaseKafkaConsumer<OrderDecidedEvent, Inbox>(service) {
    @KafkaListener(topics = [$$"${custom.topics.accepted}"])
    override fun onMessage(
        record: ConsumerRecord<String, OrderDecidedEvent>,
        ack: Acknowledgment,
    ) {
        super.onMessage(record, ack)
    }
}

@Component
class OrderRejectedConsumer(
    service: IConsumerService<OrderDecidedEvent, Inbox>,
) : BaseKafkaConsumer<OrderDecidedEvent, Inbox>(service) {
    @KafkaListener(topics = [$$"${custom.topics.rejected}"])
    override fun onMessage(
        record: ConsumerRecord<String, OrderDecidedEvent>,
        ack: Acknowledgment,
    ) {
        super.onMessage(record, ack)
    }
}
