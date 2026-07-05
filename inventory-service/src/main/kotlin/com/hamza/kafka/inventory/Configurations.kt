package com.hamza.kafka.inventory

import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.info.Info
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.ansi.AnsiColor
import org.springframework.boot.ansi.AnsiOutput
import org.springframework.boot.ansi.AnsiStyle
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.boot.info.BuildProperties
import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer
import org.springframework.boot.restclient.RestClientCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.event.EventListener
import org.zalando.logbook.spring.LogbookClientHttpRequestInterceptor
import java.net.InetAddress

@Configuration
class Configurations(
    @Value($$"${server.port}") private val port: Int,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Bean
    fun jacksonCustomizer() = JsonMapperBuilderCustomizer { it.findAndAddModules() }

    @EventListener(ApplicationReadyEvent::class)
    fun readyListener() {
        val address = "http://${InetAddress.getLocalHost().hostAddress}:$port"
        val message = "inventory-service running at $address/swagger-ui"
        logger.info(AnsiOutput.toString(AnsiColor.BRIGHT_GREEN, AnsiStyle.BOLD, message))
    }

    @Bean
    fun openAPI(buildProperties: BuildProperties): OpenAPI =
        OpenAPI().info(
            Info()
                .title("${buildProperties.name} API")
                .version(buildProperties.version)
                .description("TODO"),
        )

    @Bean
    fun logbookCustomizer(interceptor: LogbookClientHttpRequestInterceptor) =
        RestClientCustomizer { it.requestInterceptor(interceptor) }
}
