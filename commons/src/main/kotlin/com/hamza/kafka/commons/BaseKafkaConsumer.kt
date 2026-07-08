package com.hamza.kafka.commons

import jakarta.transaction.Transactional
import org.apache.avro.specific.SpecificRecordBase
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.support.Acknowledgment

interface IKafkaConsumer<T : SpecificRecordBase> {
    fun onMessage(
        record: ConsumerRecord<String, T>,
        ack: Acknowledgment,
    )
}

abstract class BaseKafkaConsumer<T : SpecificRecordBase, U : BaseInbox>(
    private val appName: String,
    private val service: IConsumerService<T, U>,
) : IKafkaConsumer<T> {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    override fun onMessage(
        record: ConsumerRecord<String, T>,
        ack: Acknowledgment,
    ) {
        val event = record.value()
        logger.info(
            "{}: received new event '{}' from '{}-{}@{}'",
            appName,
            event.toJson(),
            record.topic(),
            record.partition(),
            record.offset(),
        )
        service.consume(event)
        ack.acknowledge()
    }
}
