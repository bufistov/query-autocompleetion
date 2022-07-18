package org.bufistov;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryLogger;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.MoreExecutors;
import org.bufistov.autocomplete.QueryHandler;
import org.bufistov.autocomplete.QueryHandlerImpl1;
import org.bufistov.autocomplete.RandomInterval;
import org.bufistov.autocomplete.UniformRandomInterval;
import org.bufistov.storage.CassandraStorage;
import org.bufistov.storage.Storage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Random;
import java.util.concurrent.ExecutorService;

@Configuration
public class SpringConfiguration {

    @Value("${org.bufistov.autocomplete.max_thread_pool_size}")
    private int maxThreadPoolSize;

    @Value("${org.bufistov.autocomplete.max_retry_delay_millis}")
    private int maxRetryDelayMillis;

    @Bean
    public QueryHandler queryHandler() {
        return new QueryHandlerImpl1();
    }

    @Bean
    public Storage provideStorage(Cluster cluster) {
        Session session = cluster.connect();
        var manager = new MappingManager(session);
        return new CassandraStorage(manager);
    }

    @Bean
    Cluster provideCluster() {
        Cluster cluster = Cluster.builder().addContactPoint("localhost")
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
    public ExecutorService suffixUpdateExecutorService() {
        return MoreExecutors.newDirectExecutorService();
    }
}
