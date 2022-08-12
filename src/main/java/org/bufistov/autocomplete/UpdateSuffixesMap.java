package org.bufistov.autocomplete;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.bufistov.model.PrefixTopK;
import org.bufistov.model.SuffixCount;
import org.bufistov.storage.Storage;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.bufistov.autocomplete.TopKUpdateStatus.CONDITION_FAILED;
import static org.bufistov.autocomplete.TopKUpdateStatus.NO_UPDATE_REQUIRED;
import static org.bufistov.autocomplete.TopKUpdateStatus.SUCCESS;

@NoArgsConstructor
@AllArgsConstructor
public class UpdateSuffixesMap implements UpdateSuffixes {

    @Autowired
    protected Storage storage;

    @Override
    public TopKUpdateStatus updateTopKSuffixes(String query, Long count, String prefix, Long topK) {
        var result = storage.getTopKQueries(prefix);
        var topKSuffixes = result.getTopK();
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
        var currentSuffixCount = topKSuffixes.get(suffix);
        if (topKSuffixes.size() < topK) {
            if (currentSuffixCount == null || currentSuffixCount < count) {
                return getStatus(storage.addSuffixes(prefix, Map.of(suffix, count), currentVersion));
            }
        } else {
            var currentMin = Collections.min(topKSuffixes.entrySet(), suffixComparator());
            if (currentSuffixCount != null) {
                if (currentSuffixCount < count) {
                    return getStatus(storage.replaceSuffixCounter(prefix, suffix, count, currentVersion));
                }
            } else if (count > currentMin.getValue()) {
                return getStatus(storage.updateTopKQueries(prefix, Set.of(currentMin.getKey()),
                        Map.of(suffix, count), currentVersion));
            }
        }
        return NO_UPDATE_REQUIRED;
    }

    @Override
    public List<SuffixCount> toSortedList(PrefixTopK suffixCount) {
        if (suffixCount == null) {
            return List.of();
        }
        return suffixCount.getTopK().entrySet().stream()
                .map(x -> SuffixCount.builder()
                        .count(x.getValue())
                        .suffix(x.getKey())
                        .build())
                .sorted()
                .collect(Collectors.toList());
    }

    private static Comparator<Map.Entry<String, Long>> suffixComparator() {
        return (o1, o2) -> {
            if (!Objects.equals(o1.getValue(), o2.getValue())) {
                return Long.compare(o1.getValue(), o2.getValue());
            }
            return o1.getKey().compareTo(o2.getKey());
        };
    }

    private Map<String, Long> pruneExtraSuffixes(String prefix, Map<String, Long> topKSuffixes,
                                                 Long topK, Long currentVersion) {
        var toRemove = new HashSet<String>();
        var finalSet = new TreeSet<>(suffixComparator());
        finalSet.addAll(topKSuffixes.entrySet());
        while (finalSet.size() > topK) {
            var first = finalSet.first();
            toRemove.add(first.getKey());
            finalSet.remove(first);
        }
        if (storage.removeSuffixes(prefix, toRemove, currentVersion)) {
            return finalSet.stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        } else {
            return Map.of();
        }
    }

    private TopKUpdateStatus getStatus(boolean applied) {
        return applied ? SUCCESS : CONDITION_FAILED;
    }
}
