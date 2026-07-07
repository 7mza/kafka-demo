package com.hamza.kafka.inventory

import com.hamza.commons.OrderStatus
import com.hamza.kafka.commons.ICDCListener
import jakarta.persistence.EntityManagerFactory
import org.assertj.core.api.Assertions.assertThat
import org.hibernate.SessionFactory
import org.hibernate.stat.Statistics
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.bean.override.mockito.MockitoBean

// for GraalVM tracing-agent to intercept jcache caching + invalidation
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(PgTestContainer::class)
class JCacheTest {
    @Autowired
    private lateinit var repo: InboxRepository

    @Autowired
    private lateinit var entityManagerFactory: EntityManagerFactory

    @MockitoBean
    private lateinit var listener: ICDCListener

    private lateinit var statistics: Statistics

    private val inbox =
        Inbox(
            orderId = "0qsbs74grkjq3",
            eventType = "eventType",
            payload = "{}".trimIndent(),
            status = OrderStatus.entries.random(),
        )

    @BeforeEach
    fun beforeEach() {
        val sessionFactory = entityManagerFactory.unwrap(SessionFactory::class.java)
        sessionFactory.cache.evictAllRegions()
        statistics = sessionFactory.statistics
        statistics.clear()

        statistics.also {
            assertThat(it.prepareStatementCount).isZero
            assertThat(it.secondLevelCachePutCount).isZero
            assertThat(it.secondLevelCacheHitCount).isZero
        }
    }

    @AfterEach
    fun afterEach() {
        repo.deleteAll()
    }

    @Test
    fun `L2 cache should be set immediately on write and don't read from db after`() {
        // write + read
        val id = repo.saveAndFlush(inbox).id

        statistics.also {
            // 2 calls to db
            assertThat(it.prepareStatementCount).isEqualTo(2)
            // cache set
            assertThat(it.secondLevelCachePutCount).isOne
            // no cache read
            assertThat(it.secondLevelCacheHitCount).isZero
        }

        // 2nd read
        repo.findById(id)

        statistics.also {
            // no new call to db (+ 2 previous)
            assertThat(it.prepareStatementCount).isEqualTo(2)
            // no new cache set
            assertThat(it.secondLevelCachePutCount).isOne
            // cache read
            assertThat(it.secondLevelCacheHitCount).isOne
        }
    }

    @Test
    fun `L2 cache invalidation`() {
        // write + read
        val id = repo.saveAndFlush(inbox).id

        statistics.also {
            // 2 calls to db
            assertThat(it.prepareStatementCount).isEqualTo(2)
            // cache set
            assertThat(it.secondLevelCachePutCount).isOne
            // no cache read
            assertThat(it.secondLevelCacheHitCount).isZero
        }

        // cache invalidation
        entityManagerFactory.unwrap(SessionFactory::class.java).cache.evict(Inbox::class.java, id)

        // 2nd read
        repo.findById(id)

        statistics.also {
            // 1 new call to db (+ 2 previous)
            assertThat(it.prepareStatementCount).isEqualTo(3)
            // 1 new cache set (+1 previous)
            assertThat(it.secondLevelCachePutCount).isEqualTo(2)
            // no cache read
            assertThat(it.secondLevelCacheHitCount).isZero
        }
    }

    @Test
    fun `deleting a cached entity invalidates the L2 cache entry`() {
        // write + read
        val id = repo.saveAndFlush(inbox).id

        statistics.also {
            // 2 calls to db
            assertThat(it.prepareStatementCount).isEqualTo(2)
            // cache set
            assertThat(it.secondLevelCachePutCount).isOne
            // no cache read
            assertThat(it.secondLevelCacheHitCount).isZero
        }

        // deleteById loads the entity first = cache hit
        repo.deleteById(id)

        // how many cache miss before find
        val missesBeforeRead = statistics.secondLevelCacheMissCount

        // 2nd read + entry is gone from L2
        repo.findById(id)

        // +1 cache miss
        assertThat(statistics.secondLevelCacheMissCount).isEqualTo(missesBeforeRead + 1)
    }

    @Test
    fun `updating a cached entity replaces it in place, it does not evict it`() {
        // write + read
        val id = repo.saveAndFlush(inbox).id

        statistics.also {
            // 2 calls to db
            assertThat(it.prepareStatementCount).isEqualTo(2)
            // cache set
            assertThat(it.secondLevelCachePutCount).isOne
            // no cache read
            assertThat(it.secondLevelCacheHitCount).isZero
        }

        // 2nd read
        repo.findById(id)

        // how many cache miss before update
        val missesBeforeUpdate = statistics.secondLevelCacheMissCount

        inbox.orderId = "0qsbs74grkjq4"
        repo.saveAndFlush(inbox)

        // still served from L2 / replaced in place
        repo.findById(id).also {
            assertThat(it).isPresent
            assertThat(it.get().orderId).isEqualTo("0qsbs74grkjq4")
        }

        // no new cache miss
        assertThat(statistics.secondLevelCacheMissCount).isEqualTo(missesBeforeUpdate)
    }
}
