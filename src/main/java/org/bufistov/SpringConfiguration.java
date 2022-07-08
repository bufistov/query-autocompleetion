package org.bufistov;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import org.bufistov.autocomplete.QueryHandler;
import org.bufistov.autocomplete.QueryHandlerImpl;
import org.bufistov.autocomplete.RandomInterval;
import org.bufistov.autocomplete.UniformRandomInterval;
import org.bufistov.storage.CassandraStorage;
import org.bufistov.storage.Storage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
public class SpringConfiguration {

    @Value("${org.bufistov.autocomplete.max_thread_pool_size}")
    private int maxThreadPoolSize;

    @Bean
    public QueryHandler queryHandler() {
        return new QueryHandlerImpl();
    }

    @Bean
    public Storage provideStorage(Cluster cluster) {
        Session session = cluster.connect();
        var manager = new MappingManager(session);
        return new CassandraStorage(manager);
    }

    @Bean
    Cluster provideCluster() {
        return Cluster.builder().addContactPoint("localhost")
                .withoutJMXReporting()
                .build();
    }

    @Bean
    RandomInterval provideRandomInterval() {
        return new UniformRandomInterval(new Random(0));
    }

    @Bean("new1_Thread")
    public ExecutorService suffixUpdateExecutorService() {
        int cpuNum = Runtime.getRuntime().availableProcessors();
        return new ThreadPoolExecutor(cpuNum, maxThreadPoolSize, 10, TimeUnit.SECONDS,
                new SynchronousQueue<>(), new ThreadPoolExecutor.AbortPolicy());
    }
}
