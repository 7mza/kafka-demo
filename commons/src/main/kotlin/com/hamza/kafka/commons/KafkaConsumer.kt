package com.hamza.kafka.commons

import org.apache.avro.specific.SpecificRecordBase
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.support.Acknowledgment

interface KafkaConsumer<T : SpecificRecordBase> {
    fun onMessage(
        record: ConsumerRecord<String, T>,
        ack: Acknowledgment,
    )
}
