package org.bufistov.autocomplete;

import com.datastax.driver.core.TupleValue;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.bufistov.model.PrefixTopK;
import org.bufistov.model.SuffixCount;
import org.bufistov.storage.Storage;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.bufistov.autocomplete.TopKUpdateStatus.CONDITION_FAILED;
import static org.bufistov.autocomplete.TopKUpdateStatus.NO_UPDATE_REQUIRED;
import static org.bufistov.autocomplete.TopKUpdateStatus.SUCCESS;
import static org.bufistov.storage.CassandraStorage.toTuple;
import static org.bufistov.storage.CassandraStorage.toTuple1;

@NoArgsConstructor
@AllArgsConstructor
public class UpdateSuffixTupleSet implements UpdateSuffixes {

    @Autowired
    protected Storage storage;

    @Override
    public TopKUpdateStatus updateTopKSuffixes(String query, Long count, String prefix, Long topK) {
        var result = storage.getTopKQueries(prefix);
        var topKSuffixes = result.getTopK2();
        Long currentVersion = result.getVersion();
        if (topKSuffixes.size() > topK) {
            // Special case when topK parameter was decreased. We need to prune extra suffixes from the table.
            topKSuffixes = pruneExtraSuffixes(prefix, topKSuffixes, topK, currentVersion);
            if (topKSuffixes.isEmpty()) {
                return CONDITION_FAILED;
            }
            currentVersion = currentVersion == null ? 1 : currentVersion + 1;

        }
        String suffix = query.substring(prefix.length());
        var currentSuffixCount = getCurrentSuffixCount(suffix, topKSuffixes);
        if (topKSuffixes.size() < topK) {
            if (currentSuffixCount == null || currentSuffixCount < count) {
                return getStatus(storage.updateTopK2Queries(prefix,
                        Set.of(toTuple1(currentSuffixCount, suffix)),
                        Set.of(toTuple1(count, suffix)),
                        currentVersion));
            }
        } else {
            if (currentSuffixCount != null) {
                if (currentSuffixCount < count) {
                    return getStatus(storage.updateTopK2Queries(prefix, Set.of(toTuple1(currentSuffixCount, suffix)),
                            Set.of(toTuple1(count, suffix)), currentVersion));
                }
            } else {
                var currentMin = topKSuffixes.iterator().next();
                if (count > currentMin.getCount()) {
                    return getStatus(storage.updateTopK2Queries(prefix, Set.of(toTuple(currentMin)),
                            Set.of(toTuple1(count, suffix)), currentVersion));
                }
            }
        }
        return NO_UPDATE_REQUIRED;
    }

    @Override
    public List<SuffixCount> toSortedList(PrefixTopK suffixCount) {
        if (suffixCount == null) {
            return List.of();
        }
        return suffixCount.getTopK2()
                .stream()
                .sorted()
                .map(sc -> SuffixCount.builder()
                        .count(sc.getCount())
                        .suffix(sc.getSuffix())
                        .build()
                )
                .collect(Collectors.toList());
    }

    private List<SuffixCount> pruneExtraSuffixes(String prefix, List<SuffixCount> topKSuffixes,
                                                 Long topK, Long currentVersion) {
        final var toRemove = new HashSet<TupleValue>();
        final List<SuffixCount> finalSet = new ArrayList<>(topKSuffixes.size());
        Iterator<SuffixCount> iterator = topKSuffixes.iterator();
        for (int i = 0; i < topKSuffixes.size() - topK; ++i) {
            toRemove.add(toTuple(iterator.next()));
        }
        while (iterator.hasNext()) {
            finalSet.add(iterator.next());
        }
        if (storage.updateTopK2Queries(prefix, toRemove, Set.of(), currentVersion)) {
            return finalSet;
        } else {
            return List.of();
        }
    }

    private TopKUpdateStatus getStatus(boolean applied) {
        return applied ? SUCCESS : CONDITION_FAILED;
    }

    private Long getCurrentSuffixCount(String suffix, List<SuffixCount> topKSuffixes) {
        return topKSuffixes.stream()
                .filter(x -> x.getSuffix().equals(suffix))
                .map(SuffixCount::getCount)
                .findFirst()
                .orElse(null);
    }
}
