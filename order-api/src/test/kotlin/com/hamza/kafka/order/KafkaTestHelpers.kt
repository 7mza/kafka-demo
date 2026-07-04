package com.hamza.kafka.order

import com.hamza.kafka.commons.createEventItem
import com.hamza.kafka.commons.createOrderPlacedEvent
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartitionInfo
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import java.time.Duration
import java.util.concurrent.TimeUnit

internal val warmupEvent =
    createOrderPlacedEvent(
        orderId = "warmup",
        customerId = "warmup",
        items = listOf(createEventItem(sku = "warmup", quantity = 1, unitPriceCents = 1)),
    )

internal fun AdminClient.describeTopicPartitions(topicName: String): List<TopicPartitionInfo> =
    this
        .describeTopics(listOf(topicName))
        .topicNameValues()[topicName]!!
        .get(10, TimeUnit.SECONDS)
        .partitions()

internal fun AdminClient.getPartitionLeader(
    topicName: String,
    partition: Int,
): Node? = this.describeTopicPartitions(topicName)[partition].leader()

// subscribe while cluster is healthy so consumer is aware of everything
internal fun Consumer<String, *>.assertSubscription(topicName: String) {
    this.subscribe(listOf(topicName))
    await().atMost(Duration.ofSeconds(10)).until {
        this.poll(Duration.ofMillis(500))
        this.assignment().isNotEmpty()
    }
}

// wait for all partitions to be replicated
// ignoreExceptions: AdminClient calls can throw if a broker is down/recovering (routed to paused node)
internal fun AdminClient.assertNodes(
    topicName: String,
    partitions: Int,
    replicas: Int,
    isr: Int,
) {
    await().ignoreExceptions().atMost(Duration.ofSeconds(30)).untilAsserted {
        this
            .describeTopicPartitions(topicName)
            .also { assertThat(it).hasSize(partitions) }
            .forEach {
                assertThat(it.replicas()).hasSize(replicas)
                assertThat(it.isr()).hasSize(isr)
            }
    }
}

// warm up avro schema registration while cluster is healthy
internal fun IPublishService.warmupSchemaRegistry(topicName: String) {
    await().atMost(Duration.ofSeconds(30)).untilAsserted {
        this.publish(listOf(warmupEvent.toOutbox(topicName))).also { assertThat(it.publishedCount).isOne }
    }
}
