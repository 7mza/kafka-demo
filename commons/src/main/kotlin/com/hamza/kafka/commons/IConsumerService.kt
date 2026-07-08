package com.hamza.kafka.commons

import org.apache.avro.specific.SpecificRecordBase

interface IConsumerService<T : SpecificRecordBase, U : BaseInbox> {
    fun consume(event: T): U
}
