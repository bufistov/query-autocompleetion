package org.bufistov.autocomplete;

import lombok.extern.log4j.Log4j2;
import org.bufistov.model.TopKQueries;
import org.bufistov.storage.Storage;
import org.springframework.beans.factory.annotation.Autowired;

@Log4j2
public class QueryHandlerImpl implements QueryHandler {

    @Autowired
    private Storage storage;

    @Override
    public void addQuery(String query) {
        log.debug("Adding query: {}", query);
        long newValue = storage.addQuery(query);
        log.debug("New value: {}", newValue);
    }

    @Override
    public TopKQueries getQueries(String prefix) {
        log.debug("Getting queries for prefix: {}", prefix);
        return TopKQueries.builder()
                .queries(storage.getTopKQueries(prefix))
                .build();
    }
}
