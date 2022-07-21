package org.bufistov.autocomplete;

import org.bufistov.model.PrefixTopK;
import org.bufistov.model.SuffixCount;

import java.util.List;

public interface UpdateSuffixes {

    /**
     *  Update topK suffixes for given query prefix.
     * @param query The query.
     * @param count New query count.
     * @param prefix Query prefix.
     * @param topK topK value.
     * @return Result of the update which might be: Success, No update required or condition failed.
     */
    TopKUpdateStatus updateTopKSuffixes(String query, Long count, String prefix, Long topK);

    /**
     *  Tnransforms internal cassandra representation of topK suffixes to general representation.
     * @param suffixCount topK suffixes from cassandra table.
     * @return List of Suffix counts.
     */
    List<SuffixCount> toSortedList(PrefixTopK suffixCount);
}
