package org.bufistov;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import org.bufistov.autocomplete.QueryHandler;
import org.bufistov.autocomplete.QueryHandlerImpl;
import org.bufistov.storage.CassandraStorage;
import org.bufistov.storage.Storage;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SpringConfiguration {
    @Bean
    public QueryHandler queryHandler() {
        return new QueryHandlerImpl();
    }

    @Bean
    public Storage provideStorage(Cluster cluster) {
        Session session = cluster.connect();
        var manager = new MappingManager(session);
        return new CassandraStorage(session, manager);
    }

    @Bean
    Cluster provideCluster() {
        return Cluster.builder().addContactPoint("localhost")
                .withoutJMXReporting()
                .build();
    }
}
