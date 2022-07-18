package org.bufistov.autocomplete;

import com.datastax.driver.core.TupleValue;
import com.google.common.util.concurrent.ListeningExecutorService;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.bufistov.model.SuffixCount;
import org.bufistov.model.TopKQueries;
import org.bufistov.storage.Storage;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.bufistov.storage.CassandraStorage.toTuple;
import static org.bufistov.storage.CassandraStorage.toTuple1;

@Log4j2
@NoArgsConstructor
public class QueryHandlerImpl2 extends QueryHandlerImpl {

    QueryHandlerImpl2(Storage storage, Long topK, Long maxRetriesToUpdateTopK,
                      Integer maxQuerySize,
                      RandomInterval randomInterval,
                      ExecutorService executorService,
                      ListeningExecutorService listeningExecutorService) {
        super(storage, topK, maxRetriesToUpdateTopK, maxQuerySize, randomInterval, executorService, listeningExecutorService);
    }

    @Override
    public TopKQueries getQueries(String prefix) {
        log.debug("Getting queries for prefix: {}", prefix);
        return TopKQueries.builder()
                .queries2(addPrefix(storage.getTopKQueries(prefix).getTopK2(), prefix))
                .build();
    }

    @Override
    protected UpdateStatus tryUpdateTopKSuffixes(String query, Long count, String prefix, Long topK) {
        var result = storage.getTopKQueries(prefix);
        var topKSuffixes = result.getTopK2();
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
        return UpdateStatus.NO_UPDATE_REQUIRED;
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

    private UpdateStatus getStatus(boolean applied) {
        return applied ? UpdateStatus.SUCCESS : UpdateStatus.CONDITION_FAILED;
    }

    private List<SuffixCount> addPrefix(List<SuffixCount> suffixCount, String prefix) {
        return suffixCount.stream().map(sc -> SuffixCount.builder()
                .count(sc.getCount())
                .suffix(prefix + sc.getSuffix())
                .build()
        ).collect(Collectors.toList());
    }

    private Long getCurrentSuffixCount(String suffix, List<SuffixCount> topKSuffixes) {
        return topKSuffixes.stream()
                .filter(x -> x.getSuffix().equals(suffix))
                .map(SuffixCount::getCount)
                .findFirst()
                .orElse(null);
    }
}
