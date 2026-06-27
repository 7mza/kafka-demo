package com.hamza.kafka.order

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Instant

class PollingServiceTest {
    private val repository = mock<IOrderOutboxRepository>()
    private val publishService = mock<IPublishService>()
    private val service = PollingService(repository, publishService, batchSize = 10, maxAttempts = 10)

    @Test
    fun `poll retrieves unpublished batch and forwards it to publish service then return its result`() {
        val unpublished =
            listOf(OrderOutbox(id = "1", orderId = "a1", eventType = "event1", topic = "topic1", payload = "{}"))
        val published = unpublished.map { it.apply { publishedAt = Instant.now() } }

        whenever(repository.retrieveUnpublished(10, 10)).thenReturn(unpublished)
        whenever(publishService.publish(unpublished)).thenReturn(published)

        val response = service.poll()

        verify(repository).retrieveUnpublished(10, 10)
        verify(publishService).publish(unpublished)
        assertThat(response).isEqualTo(published)
    }
}
