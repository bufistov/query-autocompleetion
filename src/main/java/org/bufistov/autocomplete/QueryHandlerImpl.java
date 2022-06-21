package org.bufistov.autocomplete;

import java.util.List;

public class QueryHandlerImpl implements QueryHandler {
    @Override
    public void addQuery(String query) {
        System.out.println("Adding query: " + query);
    }

    @Override
    public List<String> getQueries(String prefix) {
        System.out.println("Getting queries for prefix: " + prefix);
        return null;
    }
}
