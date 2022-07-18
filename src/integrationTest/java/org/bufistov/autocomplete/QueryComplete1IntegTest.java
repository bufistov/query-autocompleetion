package org.bufistov.autocomplete;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryLogger;
import lombok.extern.log4j.Log4j2;
import org.bufistov.SpringConfiguration;
import org.bufistov.handler.QueryComplete;
import org.bufistov.model.SuffixCount;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * It seems there is no an easy way to inject random exposed port into spring configuration.
 * So, to test cassandra integration we build spring web handler manually.
 */
@Log4j2
@Testcontainers
public class QueryComplete1IntegTest {

    final static Long TOPK = 10L;
    final static Long MAX_RETRIES_TO_UPDATE_TOPK = 10L;
    final static Integer MAX_SLEEP_DELAY_MILLIS = 3000;

    final static int MAX_THREAD_POOL_SIZE = 1000;

    static final int NUM_QUERIES = 100;

    SpringConfiguration springConfiguration = new SpringConfiguration();
    QueryComplete queryComplete;

    String queryPrefix;

    Random random = new Random();

    private static final CassandraContainer cassandra = new CassandraContainer("cassandra:latest")
            .withInitScript("create_tables.cqlsh")
            .withJmxReporting(false);

    QueryComplete provideQueryComplete() {
        log.info("Cassandra port: {}", cassandra.getFirstMappedPort());
        var storage = springConfiguration.provideStorage(provideCluster());
        var queryHandler = new QueryHandlerImpl1(storage, TOPK, MAX_RETRIES_TO_UPDATE_TOPK,
                100,
                provideRandomInterval(),
                suffixUpdateExecutorService(), null);
        return new QueryComplete(queryHandler);
    }

    Cluster provideCluster() {
        QueryLogger queryLogger = QueryLogger.builder()
                .withMaxQueryStringLength(1000)
                .withMaxParameterValueLength(1000)
                .build();
        Cluster cluster = cassandra.getCluster();
        cluster.register(queryLogger);
        return cluster;
    }

    RandomInterval provideRandomInterval() {
        return new UniformRandomInterval(new Random(0), MAX_SLEEP_DELAY_MILLIS);
    }

    public ExecutorService suffixUpdateExecutorService() {
        int cpuNum = Runtime.getRuntime().availableProcessors();
        return new ThreadPoolExecutor(cpuNum, MAX_THREAD_POOL_SIZE, 10, TimeUnit.SECONDS,
                new SynchronousQueue<>(), new ThreadPoolExecutor.AbortPolicy());
    }

    @BeforeAll
    static void beforeAll() {
        cassandra.start();
    }

    @AfterAll
    static void afterAll() {
        cassandra.stop();
    }

    @BeforeEach
    void setUp() {
        queryComplete = provideQueryComplete();
        queryPrefix = Integer.toHexString(random.nextInt());
        if (queryPrefix.length() > 4) {
            queryPrefix = queryPrefix.substring(0, 4);
        }
        queryPrefix = queryPrefix + "_";
    }

    @Test
    void test_100QueriesAdded_suffixCountsAreUpdated() {
        log.info("Query prefix: {}", queryPrefix);
        for (int q = 1; q <= NUM_QUERIES; ++q) {
            String query = queryPrefix + q;
            for (int i = 0; i < q; ++i) {
                queryComplete.addQuery(query);
            }
        }

        HashMap<String, Long> expectedMap = new HashMap<>();
        for (long q = NUM_QUERIES; q > Math.max(NUM_QUERIES - TOPK, 0); --q) {
            expectedMap.put(queryPrefix + q, q);
        }
        await().atMost(1, TimeUnit.MINUTES)
                .pollInterval(5, TimeUnit.SECONDS)
                .until(() -> getCurrentTopK(queryPrefix.substring(0, 1)), is(expectedMap));

        for (int prefixLength = 2; prefixLength <= queryPrefix.length(); ++prefixLength) {
            assertThat(getCurrentTopK(queryPrefix.substring(0, prefixLength)), is(expectedMap));
        }

        var with1 = queryPrefix + "1";
        expectedMap.clear();
        for (long i = 11; i < 20; ++i) {
            expectedMap.put(queryPrefix + i, i);
        }
        expectedMap.put(queryPrefix + "100", 100L);
        assertThat(getCurrentTopK(with1), is(expectedMap));

    }

    Map<String, Long> getCurrentTopK(String prefix) {
        var res = queryComplete.queries(prefix);
        log.info(res.toString());
        return res.getQueries1();
    }
}
