package org.bufistov;

import org.bufistov.autocomplete.QueryHandler;
import org.bufistov.autocomplete.QueryHandlerImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SpringConfiguration {
    @Bean
    public QueryHandler queryHandler() {
        return new QueryHandlerImpl();
    }
}
