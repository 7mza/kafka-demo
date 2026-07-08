package com.hamza.kafka.inventory

import com.hamza.commons.OrderDecidedEvent
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.specific.SpecificRecordBase
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

/* FIXME:
 * for Debezium to produce avro schema output and not it's own JSON schema, find a better way */
interface IAvroSerializer<T : SpecificRecordBase> {
    fun toAvroBytes(
        topic: String,
        event: T,
    ): ByteArray
}

@Component
class AvroSerializer(
    @Value($$"${spring.kafka.consumer.properties.schema.registry.url}") registryUrl: String,
) : IAvroSerializer<OrderDecidedEvent> {
    private val delegate =
        KafkaAvroSerializer().apply {
            configure(
                mapOf(
                    AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to registryUrl,
                    AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS to true,
                ),
                false,
            )
        }

    override fun toAvroBytes(
        topic: String,
        event: OrderDecidedEvent,
    ): ByteArray = delegate.serialize(topic, event)
}
