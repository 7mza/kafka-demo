package com.hamza.kafka.commons

import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer
import org.springframework.context.annotation.Bean

@AutoConfiguration
class DefaultConfiguration {
    @Bean
    fun jacksonCustomizer() = JsonMapperBuilderCustomizer { it.findAndAddModules() }
}
