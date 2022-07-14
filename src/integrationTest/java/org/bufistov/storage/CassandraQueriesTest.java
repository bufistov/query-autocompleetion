package org.bufistov.storage;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryLogger;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import lombok.extern.log4j.Log4j2;
import org.bufistov.model.PrefixTopK;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@Log4j2
@Testcontainers
public class CassandraQueriesTest {

    static final String TEST_KEY = "1";
    static final Long TEST_VALUE = 2L;
    static final Long TEST_NEW_VERSION = 1L;

    static final Map<String, Long> TEST_SUFFIXES = Map.of(TEST_KEY, TEST_VALUE, "key2", 3L, "key3", 4L);

    static Random random = new Random();

    String prefix;

    private static final CassandraContainer cassandra = new CassandraContainer("cassandra:latest")
                .withInitScript("create_tables.cqlsh")
                .withJmxReporting(false);

    private static CassandraStorage cassandraStorage;

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
        var manager = new MappingManager(session);
        cassandraStorage = new CassandraStorage(manager);
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
    void test_addNewEntryQuery_success() {
        assertThat(cassandraStorage.addSuffixes(prefix, Map.of(TEST_KEY, TEST_VALUE), null), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK1(Map.of(TEST_KEY, TEST_VALUE))
                .version(TEST_NEW_VERSION)
                .build()));

        Long newValue = 23L;
        assertThat(cassandraStorage.addSuffixes(prefix, Map.of(TEST_KEY, newValue), TEST_NEW_VERSION), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK1(Map.of(TEST_KEY, newValue))
                .version(TEST_NEW_VERSION + 1)
                .build()));
    }

    @Test
    void test_addTwoItems_success() {
        String key2 = "key2";
        var newValues = Map.of(TEST_KEY, TEST_VALUE, key2, TEST_VALUE);
        assertThat(cassandraStorage.addSuffixes(prefix, newValues, null), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK1(newValues)
                .version(TEST_NEW_VERSION)
                .build()));
    }

    @Test
    void test_removeExistingItem_success() {
        String key2 = "key2";
        var newValues = Map.of(TEST_KEY, TEST_VALUE, key2, TEST_VALUE);
        assertThat(cassandraStorage.addSuffixes(prefix, newValues, null), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK1(newValues)
                .version(TEST_NEW_VERSION)
                .build()));

        assertThat(cassandraStorage.removeSuffixes(prefix, Set.of(key2), TEST_NEW_VERSION), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK1(Map.of(TEST_KEY, TEST_VALUE))
                .version(TEST_NEW_VERSION + 1)
                .build()));
    }

    @Test
    void test_removeNonExistingItem_success() {
        String key2 = "key2";
        var newValues = Map.of(TEST_KEY, TEST_VALUE, key2, TEST_VALUE);
        assertThat(cassandraStorage.addSuffixes(prefix, newValues, null), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK1(newValues)
                .version(TEST_NEW_VERSION)
                .build()));

        assertThat(cassandraStorage.removeSuffixes(prefix, Set.of("nonExistingKey"), TEST_NEW_VERSION), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK1(newValues)
                .version(TEST_NEW_VERSION + 1)
                .build()));
    }

    @Test
    void test_removeEmptySet_success() {
        assertThat(cassandraStorage.removeSuffixes("prefix", Set.of(), null), is(true));
        assertThat(cassandraStorage.getTopKQueries("prefix"), is(PrefixTopK.builder()
                .version(TEST_NEW_VERSION)
                .build()));
    }

    @Test
    void test_updateTopK1_success() {
        assertThat(cassandraStorage.addSuffixes(prefix, TEST_SUFFIXES, null), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK1(TEST_SUFFIXES)
                .version(TEST_NEW_VERSION)
                .build()));

        String newSuffix = getRandomPrefix();
        Long newValue = 123L;
        assertThat(cassandraStorage.updateTopK1Queries(prefix, Set.of(),
                Map.of(newSuffix, newValue), TEST_NEW_VERSION), is(true));
        HashMap<String, Long> expectedMap = new HashMap<>(TEST_SUFFIXES);
        expectedMap.put(newSuffix, newValue);
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK1(expectedMap)
                .version(TEST_NEW_VERSION + 1)
                .build()));

        assertThat(cassandraStorage.updateTopK1Queries(prefix, Set.of(TEST_KEY),
                Map.of(), TEST_NEW_VERSION + 1), is(true));
        expectedMap.remove(TEST_KEY);
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK1(expectedMap)
                .version(TEST_NEW_VERSION + 2)
                .build()));

        assertThat(cassandraStorage.updateTopK1Queries(prefix, Set.of(TEST_KEY, "key2"),
                Map.of("key5", 5L), TEST_NEW_VERSION + 2), is(true));
        expectedMap.remove("key2");
        expectedMap.put("key5", 5L);
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK1(expectedMap)
                .version(TEST_NEW_VERSION + 3)
                .build()));
    }

    @Test
    void test_updateExisting_success() {
        assertThat(cassandraStorage.addSuffixes(prefix, TEST_SUFFIXES, null), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK1(TEST_SUFFIXES)
                .version(TEST_NEW_VERSION)
                .build()));

        Long newValue = 124L;
        assertThat(cassandraStorage.replaceSuffixCounter(prefix, TEST_KEY, newValue, TEST_NEW_VERSION), is(true));
        HashMap<String, Long> expectedMap = new HashMap<>(TEST_SUFFIXES);
        expectedMap.put(TEST_KEY, newValue);
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK1(expectedMap)
                .version(TEST_NEW_VERSION + 1)
                .build()));
    }

    @Test
    void test_updateNonExisting_entryCreated() {
        assertThat(cassandraStorage.addSuffixes(prefix, TEST_SUFFIXES, null), is(true));
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK1(TEST_SUFFIXES)
                .version(TEST_NEW_VERSION)
                .build()));

        Long newValue = 124L;
        String newKey = "newKey";
        assertThat(cassandraStorage.replaceSuffixCounter(prefix, newKey, newValue, TEST_NEW_VERSION), is(true));
        HashMap<String, Long> expectedMap = new HashMap<>(TEST_SUFFIXES);
        expectedMap.put(newKey, newValue);
        assertThat(cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                .topK1(expectedMap)
                .version(TEST_NEW_VERSION + 1)
                .build()));
    }

    private String getRandomPrefix() {
        String prefix = Integer.toHexString(random.nextInt());
        if (prefix.length() > 4) {
            prefix = prefix.substring(0, 4);
        }
        return prefix + "_";
    }
}
