package org.bufistov.autocomplete;

import org.bufistov.model.TopKQueries;

public interface QueryHandler {

    void addQuery(String query);

    TopKQueries getQueries(String prefix);
}
