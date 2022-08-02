package org.bufistov.storage;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryLogger;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import lombok.extern.log4j.Log4j2;
import org.bufistov.model.PrefixTopK;
import org.bufistov.model.QueryCount;
import org.bufistov.model.QueryCountCassandra;
import org.bufistov.model.QueryUpdateCassandra;
import org.bufistov.model.SuffixCount;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@Log4j2
@Testcontainers
public class CassandraQueriesTest {

    static final String TEST_QUERY = "query";
    static final String TEST_KEY = "1";
    static final String TEST_KEY2 = "key2";
    static final Long TEST_VALUE = 2L;
    static final Long TEST_VALUE2 = 3L;
    static final Long TEST_NEW_VERSION = 1L;

    static final Integer TTL_SECONDS = 3600;

    static final Map<String, Long> TEST_SUFFIXES = Map.of(TEST_KEY, TEST_VALUE, TEST_KEY2, TEST_VALUE2,
            "key3", 4L);
    static final TupleType TUPLE_TYPE = TupleType.of(ProtocolVersion.V4, CodecRegistry.DEFAULT_INSTANCE,
            DataType.bigint(), DataType.text());
    static final Set<TupleValue> TEST_SUFFIXES2 = Set.of(TUPLE_TYPE.newValue(TEST_VALUE, TEST_KEY),
            TUPLE_TYPE.newValue(TEST_VALUE2, TEST_KEY2));

    static Random random = new Random();

    static Mapper<QueryCountCassandra> queryCountMapper;
    static Mapper<QueryUpdateCassandra> queryUpdateMapper;

    String prefix;

    private static final CassandraContainer cassandra = new CassandraContainer("cassandra:latest")
                .withInitScript("create_tables.cqlsh")
                .withJmxReporting(false);

    private static CassandraStorage cassandraStorage;

    private static MappingManager manager;

    @BeforeAll
    static void beforeAll() {
        cassandra.start();
        Cluster cluster = cassandra.getCluster();
        QueryLogger queryLogger = QueryLogger.builder()
                .withMaxQueryStringLength(1000)
                .withMaxParameterValueLength(1000)
                .build();
        cluster.register(queryLogger);
        Session session = cluster.connect();
        manager = new MappingManager(session);
        queryCountMapper = manager.mapper(QueryCountCassandra .class);
        queryUpdateMapper = manager.mapper(QueryUpdateCassandra .class);
        cassandraStorage = new CassandraStorage(manager, TTL_SECONDS);
    }

    @AfterAll
    static void afterAll() {
        cassandra.stop();
    }

    @BeforeEach
    void setUp() {
        prefix = getRandomPrefix();
    }

    @Test
    void test_updateQueryCounter_success() {
        assertThat(cassandraStorage.addQuery(prefix), is(QueryCount.builder()
                .query(prefix)
                .count(1L)
                .sinceLastUpdate(1L)
                .build()));
    }

    @Test
    void test_updateQueryCounterUpdateTime_success() {
        Date current = Date.from(Instant.now());
        queryUpdateMapper.save(QueryUpdateCassandra.builder()
                        .query(prefix)
                        .topkUpdate(current)
                .build());
        assertThat(cassandraStorage.addQuery(prefix), is(QueryCount.builder()
                .query(prefix)
                .count(1L)
                .sinceLastUpdate(1L)
                .lastUpdateTime(current)
                .build()));
    }

    @Test
    void test_addNewEntryQuery_success() {
        assertThat(cassandraStorage.addSuffixes(prefix, Map.of(TEST_KEY, TEST_VALUE), null), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(Map.of(TEST_KEY, TEST_VALUE))
                .topK2(List.of())
                .version(TEST_NEW_VERSION)
                .build()));

        Long newValue = 23L;
        assertThat(cassandraStorage.addSuffixes(prefix, Map.of(TEST_KEY, newValue), TEST_NEW_VERSION), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(Map.of(TEST_KEY, newValue))
                .topK2(List.of())
                .version(TEST_NEW_VERSION + 1)
                .build()));
    }

    @Test
    void test_addTwoItems_success() {
        String key2 = "key2";
        var newValues = Map.of(TEST_KEY, TEST_VALUE, key2, TEST_VALUE);
        assertThat(cassandraStorage.addSuffixes(prefix, newValues, null), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(newValues)
                .topK2(List.of())
                .version(TEST_NEW_VERSION)
                .build()));
    }

    @Test
    void test_removeExistingItem_success() {
        String key2 = "key2";
        var newValues = Map.of(TEST_KEY, TEST_VALUE, key2, TEST_VALUE);
        assertThat(cassandraStorage.addSuffixes(prefix, newValues, null), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(newValues)
                .topK2(List.of())
                .version(TEST_NEW_VERSION)
                .build()));

        assertThat(cassandraStorage.removeSuffixes(prefix, Set.of(key2), TEST_NEW_VERSION), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(Map.of(TEST_KEY, TEST_VALUE))
                .topK2(List.of())
                .version(TEST_NEW_VERSION + 1)
                .build()));
    }

    @Test
    void test_removeNonExistingItem_success() {
        String key2 = "key2";
        var newValues = Map.of(TEST_KEY, TEST_VALUE, key2, TEST_VALUE);
        assertThat(cassandraStorage.addSuffixes(prefix, newValues, null), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(newValues)
                .topK2(List.of())
                .version(TEST_NEW_VERSION)
                .build()));

        assertThat(cassandraStorage.removeSuffixes(prefix, Set.of("nonExistingKey"), TEST_NEW_VERSION), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(newValues)
                .topK2(List.of())
                .version(TEST_NEW_VERSION + 1)
                .build()));
    }

    @Test
    void test_removeEmptySet_success() {
        assertThat(cassandraStorage.removeSuffixes("prefix", Set.of(), null), is(true));
        assertThat(cassandraStorage.getTopKQueries("prefix"), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(Map.of())
                .topK2(List.of())
                .version(TEST_NEW_VERSION)
                .build()));
    }

    @Test
    void test_updateTopK1_success() {
        assertThat(cassandraStorage.addSuffixes(prefix, TEST_SUFFIXES, null), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(TEST_SUFFIXES)
                .topK2(List.of())
                .version(TEST_NEW_VERSION)
                .build()));

        String newSuffix = getRandomPrefix();
        Long newValue = 123L;
        assertThat(cassandraStorage.updateTopK1Queries(prefix, Set.of(),
                Map.of(newSuffix, newValue), TEST_NEW_VERSION), is(true));
        HashMap<String, Long> expectedMap = new HashMap<>(TEST_SUFFIXES);
        expectedMap.put(newSuffix, newValue);
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(expectedMap)
                .topK2(List.of())
                .version(TEST_NEW_VERSION + 1)
                .build()));

        assertThat(cassandraStorage.updateTopK1Queries(prefix, Set.of(TEST_KEY),
                Map.of(), TEST_NEW_VERSION + 1), is(true));
        expectedMap.remove(TEST_KEY);
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(expectedMap)
                .topK2(List.of())
                .version(TEST_NEW_VERSION + 2)
                .build()));

        assertThat(cassandraStorage.updateTopK1Queries(prefix, Set.of(TEST_KEY, "key2"),
                Map.of("key5", 5L), TEST_NEW_VERSION + 2), is(true));
        expectedMap.remove("key2");
        expectedMap.put("key5", 5L);
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(expectedMap)
                .topK2(List.of())
                .version(TEST_NEW_VERSION + 3)
                .build()));
    }

    @Test
    void test_updateExisting_success() {
        assertThat(cassandraStorage.addSuffixes(prefix, TEST_SUFFIXES, null), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(TEST_SUFFIXES)
                .topK2(List.of())
                .version(TEST_NEW_VERSION)
                .build()));

        Long newValue = 124L;
        assertThat(cassandraStorage.replaceSuffixCounter(prefix, TEST_KEY, newValue, TEST_NEW_VERSION), is(true));
        HashMap<String, Long> expectedMap = new HashMap<>(TEST_SUFFIXES);
        expectedMap.put(TEST_KEY, newValue);
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(expectedMap)
                .topK2(List.of())
                .version(TEST_NEW_VERSION + 1)
                .build()));
    }

    @Test
    void test_mapTtl_success() {
        var ttlSeconds = 2;
        var fastStorage = new CassandraStorage(manager, ttlSeconds);
        final String key3 = "key3";
        var testSuffixes = Map.of(TEST_KEY, TEST_VALUE, TEST_KEY2, TEST_VALUE2, key3, 4L);
        assertThat(cassandraStorage.addSuffixes(prefix, testSuffixes, null), is(true));
        assertThat(fastStorage.replaceSuffixCounter(prefix, key3, 0L, 1L), is(true));
        // key3 is evicted
        await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(5, TimeUnit.SECONDS)
                .until(() -> fastStorage.getTopKQueries(prefix), is(
                        PrefixTopK.builder()
                                .topK(Set.of())
                                .topK1(Map.of(TEST_KEY, TEST_VALUE, TEST_KEY2, TEST_VALUE2))
                                .topK2(List.of())
                                .version(null) // version is also evicted, as it is always updated
                                .build())
                );
    }

    @Test
    void test_updateNonExisting_entryCreated() {
        assertThat(cassandraStorage.addSuffixes(prefix, TEST_SUFFIXES, null), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(TEST_SUFFIXES)
                .topK2(List.of())
                .version(TEST_NEW_VERSION)
                .build()));

        Long newValue = 124L;
        String newKey = "newKey";
        assertThat(cassandraStorage.replaceSuffixCounter(prefix, newKey, newValue, TEST_NEW_VERSION), is(true));
        HashMap<String, Long> expectedMap = new HashMap<>(TEST_SUFFIXES);
        expectedMap.put(newKey, newValue);
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(expectedMap)
                .topK2(List.of())
                .version(TEST_NEW_VERSION + 1)
                .build()));
    }

    @Test
    void test_updateTopK2_success() {
        assertThat(cassandraStorage.updateTopK2Queries(prefix, Set.of(), TEST_SUFFIXES2, null), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(Map.of())
                .topK2(List.of(getSuffix(TEST_VALUE, TEST_KEY), getSuffix(TEST_VALUE2, TEST_KEY2)))
                .version(TEST_NEW_VERSION)
                .build()));

        String newSuffix = getRandomPrefix();
        Long newValue = 123L;
        assertThat(cassandraStorage.updateTopK2Queries(prefix, Set.of(),
                Set.of(TUPLE_TYPE.newValue(newValue, newSuffix)), TEST_NEW_VERSION), is(true));

        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(Map.of())
                .topK2(List.of(getSuffix(TEST_VALUE, TEST_KEY),
                        getSuffix(TEST_VALUE2, TEST_KEY2),
                        getSuffix(newValue, newSuffix)
                        ))
                .version(TEST_NEW_VERSION + 1)
                .build()));

        assertThat(cassandraStorage.updateTopK2Queries(prefix, Set.of(TUPLE_TYPE.newValue(TEST_VALUE, TEST_KEY)),
                Set.of(), TEST_NEW_VERSION + 1), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(Map.of())
                .topK2(List.of(getSuffix(TEST_VALUE2, TEST_KEY2),
                        getSuffix(newValue, newSuffix)
                ))
                .version(TEST_NEW_VERSION + 2)
                .build()));

        assertThat(cassandraStorage.updateTopK2Queries(prefix, Set.of(
                TUPLE_TYPE.newValue(TEST_VALUE,TEST_KEY),
                        TUPLE_TYPE.newValue(TEST_VALUE2, TEST_KEY2)),
                Set.of(TUPLE_TYPE.newValue(5L, "key5")),
                TEST_NEW_VERSION + 2), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(Map.of())
                .topK2(List.of(getSuffix(5L, "key5"),
                        getSuffix(newValue, newSuffix)))
                .version(TEST_NEW_VERSION + 3)
                .build()));
    }

    @Test
    void test_resetTemporalCounter_success() {
        cassandraStorage.addQuery(TEST_QUERY);
        cassandraStorage.addQuery(TEST_QUERY);
        assertThat(queryCountMapper.get(TEST_QUERY), is(QueryCountCassandra.builder()
                .count(2L)
                .query(TEST_QUERY)
                .sinceLastUpdate(2L)
                .build()));
        cassandraStorage.updateTemporalCounter(TEST_QUERY, -2L);
        assertThat(queryCountMapper.get(TEST_QUERY), is(QueryCountCassandra.builder()
                .count(2L)
                .query(TEST_QUERY)
                .sinceLastUpdate(0L)
                .build()));
    }

    @Test
    void test_setQueryUpdateTime_success() {
        Instant currentInstant = Instant.now();
        Date lastUpdateDate = Date.from(currentInstant.minus(1, ChronoUnit.MINUTES));
        queryUpdateMapper.save(QueryUpdateCassandra.builder()
                        .query(TEST_QUERY)
                        .topkUpdate(lastUpdateDate)
                .build());
        Date currentDate = Date.from(currentInstant);
        assertThat(cassandraStorage.lockQueryForTopKUpdate(TEST_QUERY, lastUpdateDate, currentDate), is(true));
        assertThat(queryUpdateMapper.get(TEST_QUERY), is(QueryUpdateCassandra.builder()
                .query(TEST_QUERY)
                .topkUpdate(currentDate)
                .build()));

        assertThat(cassandraStorage.lockQueryForTopKUpdate(TEST_QUERY, lastUpdateDate, currentDate), is(false));
        assertThat(queryUpdateMapper.get(TEST_QUERY), is(QueryUpdateCassandra.builder()
                .query(TEST_QUERY)
                .topkUpdate(currentDate)
                .build()));
    }

    private String getRandomPrefix() {
        String prefix = Integer.toHexString(random.nextInt());
        if (prefix.length() > 4) {
            prefix = prefix.substring(0, 4);
        }
        return prefix + "_";
    }

    private static SuffixCount getSuffix(Long count, String suffix) {
        return SuffixCount.builder()
                .count(count)
                .suffix(suffix)
                .build();
    }
}
