package com.hamza.kafka.commons

import io.micrometer.observation.ObservationPredicate
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.http.server.observation.ServerRequestObservationContext

@AutoConfiguration
@EnableConfigurationProperties(TracingProperties::class)
class DefaultConfiguration {
    @Bean
    fun jacksonCustomizer() = JsonMapperBuilderCustomizer { it.findAndAddModules() }

    @Bean // don't pollute tracer
    fun observationPredicate(tracingProperties: TracingProperties): ObservationPredicate =
        ObservationPredicate { _, context ->
            // if (name.startsWith("task")) return@ObservationPredicate false // polling
            (context as? ServerRequestObservationContext)
                ?.carrier
                ?.requestURI
                ?.let { uri -> tracingProperties.excludeUris?.none { uri.startsWith(it.trim()) } }
                ?: true
        }
}

@ConfigurationProperties(prefix = "tracing")
class TracingProperties {
    var excludeUris: List<String>? = null
}
