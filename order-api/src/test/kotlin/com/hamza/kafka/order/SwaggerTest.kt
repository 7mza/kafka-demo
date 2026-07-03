package com.hamza.kafka.order

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.MediaType
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.servlet.client.RestTestClient

// for GraalVM tracing-agent to intercept swagger
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("default", "h2")
@AutoConfigureRestTestClient
class SwaggerTest {
    @Autowired
    private lateinit var client: RestTestClient

    @Test
    fun swaggerUIRedirect() {
        client
            .get()
            .uri("swagger-ui")
            .accept(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus()
            .value { assertThat(it).isIn(200, 302) }
    }

    @Test
    fun swaggerUI() {
        client
            .get()
            .uri("swagger-ui/index.html")
            .accept(MediaType.TEXT_HTML)
            .exchange()
            .expectStatus()
            .isOk
    }

    @Test
    fun apiDocsJson() {
        client
            .get()
            .uri("api-docs")
            .accept(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus()
            .isOk
    }

    @Test
    fun apiDocsJYaml() {
        client
            .get()
            .uri("api-docs.yaml")
            .exchange()
            .expectStatus()
            .isOk
    }
}
