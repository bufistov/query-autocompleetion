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

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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
public class QueryCompleteMapIntegTest {

    final static Long TOPK = 10L;
    final static Long MAX_RETRIES_TO_UPDATE_TOPK = 10L;
    final static Integer MAX_SLEEP_DELAY_MILLIS = 3000;

    final static long QUERY_UPDATE_MILLIS = 100;


    final static QueryHandlerConfig CONFIG = QueryHandlerConfig.builder()
            .topK(TOPK)
            .maxQuerySize(100)
            .maxRetriesToUpdateTopK(MAX_RETRIES_TO_UPDATE_TOPK)
            .queryUpdateMillis(QUERY_UPDATE_MILLIS)
            .queryUpdateCount(1L)
            .firstQueryUpdateCount(1L)
            .build();

    static final int NUM_QUERIES = 100;

    SpringConfiguration springConfiguration = new SpringConfiguration();
    QueryComplete queryComplete;

    String queryPrefix;

    Random random = new Random();

    private static final CassandraContainer cassandra = new CassandraContainer("cassandra:latest")
            .withInitScript("create_tables.cqlsh")
            .withJmxReporting(false);

    QueryComplete provideQueryComplete(QueryHandlerConfig config) {
        log.info("Cassandra port: {}", cassandra.getFirstMappedPort());
        var storage = springConfiguration.provideStorage(provideCluster(), 3600);
        var updateSuffixes = new UpdateSuffixesMap(storage);
        var queryHandler = new QueryHandlerImpl(storage, config,
                updateSuffixes,
                provideRandomInterval(),
                Clock.systemUTC());
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
        queryComplete = provideQueryComplete(CONFIG);
        queryPrefix = Integer.toHexString(random.nextInt());
        if (queryPrefix.length() > 4) {
            queryPrefix = queryPrefix.substring(0, 4);
        }
        queryPrefix = queryPrefix + "_";
    }

    @Test
    void test_100QueriesAdded_suffixCountsAreUpdated() {
        log.info("Query prefix: {}", queryPrefix);
        submitQueries(NUM_QUERIES);

        List<SuffixCount> expectedMap = new ArrayList<>();
        for (long q = Math.max(NUM_QUERIES - TOPK, 0) + 1; q <= NUM_QUERIES; ++q) {
            expectedMap.add(getQuery(Long.toString(q), q));
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
            expectedMap.add(getQuery(Long.toString(i), i));
        }
        expectedMap.add(getQuery(Integer.toString(NUM_QUERIES), NUM_QUERIES));
        assertThat(getCurrentTopK(with1), is(expectedMap));
    }

    @Test
    void test_configChanged_extraSuffixesPruned() {
        log.info("Query prefix: {}", queryPrefix);
        int NUM_QUERIES = 10;
        submitQueries(NUM_QUERIES);
        List<SuffixCount> expectedMap = new ArrayList<>();
        for (long q = 1; q <= NUM_QUERIES; ++q) {
            expectedMap.add(getQuery(Long.toString(q), q));
        }
        await().atMost(1, TimeUnit.MINUTES)
                .pollInterval(5, TimeUnit.SECONDS)
                .until(() -> getCurrentTopK(queryPrefix.substring(0, 1)), is(expectedMap));

        queryComplete = provideQueryComplete(QueryHandlerConfig.builder()
                .topK(3L)
                .maxQuerySize(100)
                .maxRetriesToUpdateTopK(MAX_RETRIES_TO_UPDATE_TOPK)
                .queryUpdateMillis(QUERY_UPDATE_MILLIS)
                .queryUpdateCount(1L)
                .build());

        queryComplete.addQuery(queryPrefix + "10");

        expectedMap.clear();
        expectedMap.add(getQuery("8", 8));
        expectedMap.add(getQuery("9", 9));
        expectedMap.add(getQuery("10", 11));
        await().atMost(1, TimeUnit.MINUTES)
                .pollInterval(5, TimeUnit.SECONDS)
                .until(() -> getCurrentTopK(queryPrefix.substring(0, 1)), is(expectedMap));

        for (int prefixLength = 2; prefixLength <= queryPrefix.length(); ++prefixLength) {
            assertThat(getCurrentTopK(queryPrefix.substring(0, prefixLength)), is(expectedMap));
        }
    }

    List<SuffixCount> getCurrentTopK(String prefix) {
        var res = queryComplete.queries(prefix);
        log.info(res.toString());
        return res.getQueries();
    }

    SuffixCount getQuery(String suffix, long count) {
        return SuffixCount.builder()
                .suffix(queryPrefix + suffix)
                .count(count)
                .build();
    }

    void submitQueries(int numQueries) {
        for (int q = 1; q <= numQueries; ++q) {
            String query = queryPrefix + q;
            for (int i = 0; i < q; ++i) {
                queryComplete.addQuery(query);
            }
        }
    }
}
