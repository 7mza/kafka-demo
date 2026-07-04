package com.hamza.kafka.commons

import com.hamza.kafka.avro.Item
import com.hamza.kafka.avro.OrderPlacedEvent
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.specific.SpecificRecordBase
import tools.jackson.databind.ObjectMapper
import java.io.ByteArrayOutputStream
import java.time.Instant
import java.time.temporal.ChronoUnit

inline fun <reified T> parseJson(
    json: String,
    objectMapper: ObjectMapper,
): T = objectMapper.readValue(json.trimIndent(), T::class.java)

inline fun <reified T> writeJson(
    t: T,
    objectMapper: ObjectMapper,
): String = objectMapper.writeValueAsString(t)

fun SpecificRecordBase.toJson(): String {
    val writer = SpecificDatumWriter<SpecificRecordBase>(this.schema, this.specificData)
    val out = ByteArrayOutputStream()
    val encoder = EncoderFactory.get().jsonEncoder(this.schema, out, false)
    writer.write(this, encoder)
    encoder.flush()
    return out.toString(Charsets.UTF_8)
}

inline fun <reified T : SpecificRecordBase> fromJson(json: String): T {
    val template = T::class.java.getDeclaredConstructor().newInstance()
    val reader = SpecificDatumReader<T>(template.schema, template.schema, template.specificData)
    val decoder = DecoderFactory.get().jsonDecoder(template.schema, json)
    return reader.read(null, decoder)
}

fun createEventItem(
    sku: String,
    quantity: Int,
    unitPriceCents: Int,
): Item =
    Item
        .newBuilder()
        .setSku(sku)
        .setQuantity(quantity)
        .setUnitPriceCents(unitPriceCents)
        .build()

fun createOrderPlacedEvent(
    evenId: String? = null,
    occurredAt: Instant? = null,
    orderId: String,
    customerId: String,
    items: List<Item>,
): OrderPlacedEvent =
    OrderPlacedEvent
        .newBuilder()
        .setEventId(evenId ?: TSIDGenerator.next())
        .setOccurredAt(occurredAt ?: Instant.now().truncatedTo(ChronoUnit.MILLIS))
        .setOrderId(orderId)
        .setCustomerId(customerId)
        .setItems(items)
        .setTotalAmountCents(items.sumOf { it.unitPriceCents * it.quantity })
        .build()
