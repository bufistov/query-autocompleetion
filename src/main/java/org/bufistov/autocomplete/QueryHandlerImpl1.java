package org.bufistov.autocomplete;

import com.google.common.util.concurrent.ListeningExecutorService;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.bufistov.model.PrefixTopK;
import org.bufistov.model.SuffixCount;
import org.bufistov.storage.Storage;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

@Log4j2
@NoArgsConstructor
public class QueryHandlerImpl1 extends QueryHandlerImpl {

    QueryHandlerImpl1(Storage storage, Long topK, Long maxRetriesToUpdateTopK,
                      Integer maxQuerySize,
                      RandomInterval randomInterval,
                      ExecutorService executorService,
                      ListeningExecutorService listeningExecutorService) {
        super(storage, topK, maxRetriesToUpdateTopK, maxQuerySize, randomInterval, executorService, listeningExecutorService);
    }

    @Override
    protected UpdateStatus tryUpdateTopKSuffixes(String query, Long count, String prefix, Long topK) {
        var result = storage.getTopKQueries(prefix);
        var topKSuffixes = result.getTopK1();
        Long currentVersion = result.getVersion();
        if (topKSuffixes.size() > topK) {
            // Special case when topK parameter was decreased. We need to prune extra suffixes from the table.
            topKSuffixes = pruneExtraSuffixes(prefix, topKSuffixes, topK, currentVersion);
            if (topKSuffixes.isEmpty()) {
                return UpdateStatus.CONDITION_FAILED;
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
                return getStatus(storage.updateTopK1Queries(prefix, Set.of(currentMin.getKey()),
                        Map.of(suffix, count), currentVersion));
            }
        }
        return UpdateStatus.NO_UPDATE_REQUIRED;
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

    private UpdateStatus getStatus(boolean applied) {
        return applied ? UpdateStatus.SUCCESS : UpdateStatus.CONDITION_FAILED;
    }

    @Override
    protected List<SuffixCount> addPrefix(PrefixTopK suffixCount, String prefix) {
        if (suffixCount == null) {
            return List.of();
        }
        return suffixCount.getTopK1().entrySet().stream()
                .map(x -> SuffixCount.builder()
                        .count(x.getValue())
                        .suffix(prefix + x.getKey())
                        .build())
                .sorted()
                .collect(Collectors.toList());
    }
}
