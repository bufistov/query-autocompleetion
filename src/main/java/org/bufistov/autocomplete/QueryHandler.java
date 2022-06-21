package org.bufistov.autocomplete;

import org.springframework.context.annotation.Bean;

import java.util.List;
public interface QueryHandler {

    void addQuery(String query);

    List<String> getQueries(String prefix);
}
