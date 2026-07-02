package com.hamza.kafka.commons

data class KafkaPublishResult(
    var publishedCount: Int = 0,
    var recoverableErrorsCount: Int = 0,
    var deadLettersCount: Int = 0,
) {
    // progress = getting outboxes off the backlog (whether sent to kafka or flagged as dead letters)
    fun isProgressing() = this.publishedCount + this.deadLettersCount > 0

    // used for simple linear time back off based on transient errors (if something is wrong with kafka, give it time to heal)
    fun hasRecoverable() = this.recoverableErrorsCount > 0
}
