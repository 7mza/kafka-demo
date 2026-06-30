package com.hamza.kafka.order

import com.hamza.kafka.commons.BaseDeadLetterRepository

interface DeadLetterRepository : BaseDeadLetterRepository<Outbox>
