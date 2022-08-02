package org.bufistov;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryLogger;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import lombok.extern.log4j.Log4j2;
import org.bufistov.autocomplete.QueryHandler;
import org.bufistov.autocomplete.QueryHandlerConfig;
import org.bufistov.autocomplete.QueryHandlerImpl;
import org.bufistov.autocomplete.RandomInterval;
import org.bufistov.autocomplete.UniformRandomInterval;
import org.bufistov.autocomplete.UpdateSuffixesMap;
import org.bufistov.autocomplete.UpdateSuffixes;
import org.bufistov.storage.CassandraStorage;
import org.bufistov.storage.Storage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.Random;

@Configuration
@Log4j2
public class SpringConfiguration {

    @Value("${org.bufistov.autocomplete.max_thread_pool_size}")
    private int maxThreadPoolSize;

    @Value("${org.bufistov.autocomplete.max_retry_delay_millis}")
    private int maxRetryDelayMillis;

    @Value("${org.bufistov.autocomplete.K}")
    private Long topK;

    @Value("${org.bufistov.autocomplete.max_retries_to_update_topk}")
    private Long maxRetriesToUpdateTopK;

    @Value("${org.bufistov.autocomplete.max_query_size}")
    private Integer maxQuerySize;

    @Value("${org.bufistov.autocomplete.query_update_millis}")
    private Long queryUpdateMillis;

    @Value("${org.bufistov.autocomplete.query_update_count}")
    private Long queryUpdateCount;

    @Value("${org.bufistov.autocomplete.first_query_update_count}")
    private Long firstQueryUpdateCount;

    @Value("${org.bufistov.storage.cassandra_host}")
    private String cassandraHost;

    @Bean
    public QueryHandler queryHandler() {
        return new QueryHandlerImpl();
    }

    @Bean
    public UpdateSuffixes updateSuffix() {
        return new UpdateSuffixesMap();
    }

    @Bean
    public Storage provideStorage(Cluster cluster) {
        Session session = cluster.connect();
        var manager = new MappingManager(session);
        return new CassandraStorage(manager);
    }

    @Bean
    Cluster provideCluster() {
        Cluster cluster = Cluster.builder()
                .addContactPoint(cassandraHost)
                .withoutJMXReporting()
                .build();
        QueryLogger queryLogger = QueryLogger.builder()
                .withMaxQueryStringLength(1000)
                .withMaxParameterValueLength(1000)
                .build();
        cluster.register(queryLogger);
        return cluster;
    }

    @Bean
    public RandomInterval provideRandomInterval() {
        return new UniformRandomInterval(new Random(0), maxRetryDelayMillis);
    }

    @Bean
    public QueryHandlerConfig provideQueryHandlerConfig() {
        var result = QueryHandlerConfig.builder()
                .maxQuerySize(maxQuerySize)
                .topK(topK)
                .maxRetriesToUpdateTopK(maxRetriesToUpdateTopK)
                .queryUpdateMillis(queryUpdateMillis)
                .queryUpdateCount(queryUpdateCount)
                .firstQueryUpdateCount(firstQueryUpdateCount)
                .build();
        log.info(result);
        return result;
    }

    @Bean
    public Clock provideClock() {
        return Clock.systemUTC();
    }
}
