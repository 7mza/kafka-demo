package com.hamza.kafka.commons

import tools.jackson.databind.ObjectMapper

inline fun <reified T> parseJson(
    json: String,
    objectMapper: ObjectMapper,
): T = objectMapper.readValue(json.trimIndent(), T::class.java)

inline fun <reified T> writeJson(
    t: T,
    objectMapper: ObjectMapper,
): String = objectMapper.writeValueAsString(t)
