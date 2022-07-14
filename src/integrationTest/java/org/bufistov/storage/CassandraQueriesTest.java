package org.bufistov.storage;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import lombok.extern.log4j.Log4j2;
import org.bufistov.model.PrefixTopK;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

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

    static final String TEST_KEY = "1";
    static final Long TEST_VALUE = 2L;
    static final Long TEST_NEW_VERSION = 1L;

    static Random random = new Random();

    private static final CassandraContainer cassandra = new CassandraContainer("cassandra:latest")
                .withInitScript("create_tables.cqlsh")
                .withJmxReporting(false);

    private static CassandraStorage cassandraStorage;

    @BeforeAll
    static void beforeAll() {
        cassandra.start();
        Cluster cluster = cassandra.getCluster();
        Session session = cluster.connect();
        var manager = new MappingManager(session);
        cassandraStorage = new CassandraStorage(manager);
    }

    @AfterAll
    static void afterAll() {
        cassandra.stop();
    }

    @Test
    void test_addNewEntryQuery_success() {
        String prefix = getRandomPrefix();
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
        String prefix = getRandomPrefix();
        String key2 = "key2";
        var newValues = Map.of(TEST_KEY, TEST_VALUE, key2, TEST_VALUE);
        assertThat(cassandraStorage.addSuffixes(prefix, newValues, null), is(true));
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                        .topK1(newValues)
                        .version(TEST_NEW_VERSION)
                        .build()));
    }

    @Test
    void test_removeExistingItem_success() {
        String prefix = getRandomPrefix();
        String key2 = "key2";
        var newValues = Map.of(TEST_KEY, TEST_VALUE, key2, TEST_VALUE);
        assertThat(cassandraStorage.addSuffixes(prefix, newValues, null), is(true));
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                        .topK1(newValues)
                        .version(TEST_NEW_VERSION)
                        .build()));

        assertThat(cassandraStorage.removeSuffixes(prefix, Set.of(key2), TEST_NEW_VERSION), is(true));
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> cassandraStorage.getTopKQueries(prefix), is(PrefixTopK.builder()
                        .topK1(Map.of(TEST_KEY, TEST_VALUE))
                        .version(TEST_NEW_VERSION + 1)
                        .build()));
    }

    @Test
    void test_removeNonExistingItem_success() {
        String prefix = getRandomPrefix();
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
        assertThat(cassandraStorage.removeSuffixes("prefix", Set.of(), TEST_NEW_VERSION), is(false));
        assertThat(cassandraStorage.getTopKQueries("prefix"), is(PrefixTopK.builder()
                .topK(Set.of())
                .topK1(Map.of())
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
