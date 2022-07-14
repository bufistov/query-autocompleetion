package org.bufistov.storage;

import org.bufistov.model.PrefixTopK;
import org.bufistov.model.SuffixCount;

import java.util.List;
import java.util.Map;
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

    /**
     * Add new entries to the suffix map.
     * @param prefix Query prefix to update.
     * @param suffixes New suffixes to add, if already exists will be replaced.
     * @param version Current version of the entry.
     * @return True if conditional update succeeded false otherwise.
     */
    boolean addSuffixes(String prefix, Map<String, Long> suffixes, Long version);

    /**
     * Removes given suffixes for given prefix
     * @param prefix Query prefix
     * @param suffixes Query suffixes
     * @param version Current version of the item
     * @return Return true if conditional update succeeded, false otherwise
     */
    boolean removeSuffixes(String prefix, Set<String> suffixes, Long version);
}
