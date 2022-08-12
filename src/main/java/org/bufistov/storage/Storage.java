package org.bufistov.storage;

import com.datastax.driver.core.TupleValue;
import org.bufistov.model.PrefixTopK;
import org.bufistov.model.QueryCount;
import org.bufistov.model.SuffixCount;

import java.util.Date;
import java.util.Map;
import java.util.Set;

public interface Storage {
    QueryCount addQuery(String query);

    PrefixTopK getTopKQueries(String prefix);

    /**
     * Updates topK suffixes for given prefix only if version matches the provided one.
     * @param prefix Prefix to update.
     * @param newTopK New topK suffixes.
     * @param version Version that ensures atomic read/modify/write operation
     * @return true if update was successfull, false if condition
     */
    boolean updateTopK1Queries(String prefix, Set<SuffixCount> newTopK, Long version);

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

    /**
     * Update topK suffixes for given prefix.
     * @param prefix Prefix to update
     * @param toRemove entries to remove from topK
     * @param toAdd entries to add into topK
     * @param version version for atomic update
     * @return true if update was applied false otherwise
     */
    boolean updateTopKQueries(String prefix, Set<String> toRemove, Map<String, Long> toAdd, Long version);

    /**
     * Replace value for one suffix in topK map.
     * @param prefix Query prefix to update
     * @param suffix Query suffix to update
     * @param newValue New value of counter
     * @param version version for atomic read-modify-write
     * @return true if update was applied false otherwise. False means that item was modified in the middle of update.
     */
    boolean replaceSuffixCounter(String prefix, String suffix, Long newValue, Long version);

    /**
     * Update topK suffixes for given prefix.
     * @param prefix Prefix to update
     * @param toRemove entries to remove from topK2 set
     * @param toAdd entries to add into topK2 set
     * @param version version for atomic update
     * @return true if update was applied false otherwise
     */
    boolean updateTopK2Queries(String prefix, Set<TupleValue> toRemove, Set<TupleValue> toAdd, Long version);

    /**
     * Get last topK update from the query udpate table
     * @param query The query.
     * @param lastUpdateTime Time of the last  update.
     * @param currentTime Current time.
     * @return Query update time or null if query was never updated.
     */
    boolean lockQueryForTopKUpdate(String query, Date lastUpdateTime, Date currentTime);

    /**
     * Increement temporal counter. Used mainly to reset it to 0.
     * @param query The query.
     * @param increment Value of the increment.
     */
    void updateTemporalCounter(String query, Long increment);
}
