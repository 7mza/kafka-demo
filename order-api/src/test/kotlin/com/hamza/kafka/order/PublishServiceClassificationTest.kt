package com.hamza.kafka.order

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.kafka.common.errors.SerializationException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.io.IOException
import java.net.SocketTimeoutException
import java.util.concurrent.TimeoutException
import org.apache.kafka.common.errors.TimeoutException as KafkaTimeoutException

class PublishServiceClassificationTest {
    companion object {
        @JvmStatic
        fun cases() =
            listOf(
                // transient
                Arguments.of("kafka retriable", KafkaTimeoutException(""), true),
                Arguments.of("service ack backstop", TimeoutException(""), true),
                Arguments.of("registry unreachable", SerializationException("x", SocketTimeoutException("")), true),
                Arguments.of("registry unreachable (io)", SerializationException("x", IOException("")), true),
                Arguments.of(
                    "registry unhealthy 503",
                    SerializationException("x", RestClientException("", 503, 50301)),
                    true,
                ),
                // permanent
                Arguments.of(
                    "incompatible schema 409",
                    SerializationException("x", RestClientException("", 409, 409)),
                    false,
                ),
                Arguments.of("bad request 400", SerializationException("x", RestClientException("", 400, 400)), false),
                Arguments.of("bad payload", SerializationException(""), false),
                Arguments.of("unexpected", IllegalStateException("forced failure for demo"), false),
                Arguments.of("unhandled", Exception(""), false),
            )
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    fun `classifies publish failures`(
        name: String,
        cause: Throwable,
        expected: Boolean,
    ) {
        assertThat(cause.isTransientFailure()).isEqualTo(expected)
    }
}
