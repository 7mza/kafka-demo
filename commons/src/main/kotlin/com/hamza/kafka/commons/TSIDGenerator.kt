package com.hamza.kafka.commons

import io.hypersistence.tsid.TSID

object TSIDGenerator {
    fun next(): String = TSID.Factory.getTsid().toLowerCase()
}
