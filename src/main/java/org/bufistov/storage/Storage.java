package org.bufistov.storage;

import org.bufistov.model.SuffixCount;
import org.bufistov.model.PrefixTopK;

import java.util.Set;

public interface Storage {
    Long addQuery(String query);

    PrefixTopK getTopKQueries(String prefix);

    /**
     * Updates topK suffixes for given prefix only if version matches the provided one.
     * @param prefix Prefix to update.
     * @param newTopK New topK suffixes.
     * @param version Version that ensures atomic read/modify/write operation
     * @return true if update was successfull, false if condition
     */
    boolean updateTopKQueries(String prefix, Set<SuffixCount> newTopK, Long version);
}
