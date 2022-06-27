package org.bufistov.storage;

import org.bufistov.model.CompletionCount;
import org.bufistov.model.TopKQueries;

import java.util.List;
import java.util.Set;

public interface Storage {
    Long addQuery(String query);

    Set<CompletionCount> getTopKQueries(String prefix);

    void updateTopKQueries(String prefix, Set<CompletionCount> newTopK);
}
