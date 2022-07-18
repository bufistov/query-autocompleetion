package org.bufistov.autocomplete;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.bufistov.model.SuffixCount;
import org.bufistov.model.TopKQueries;
import org.bufistov.storage.Storage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

@Log4j2
@AllArgsConstructor
@NoArgsConstructor
public class QueryHandlerImpl implements QueryHandler {

    @Autowired
    protected Storage storage;

    @Value("${org.bufistov.autocomplete.K}")
    private Long topK;

    @Value("${org.bufistov.autocomplete.max_retries_to_update_topk}")
    private Long maxRetriesToUpdateTopK;

    @Value("${org.bufistov.autocomplete.max_query_size}")
    private Integer maxQuerySize;

    @Autowired
    private RandomInterval randomInterval;

    @Autowired
    private ExecutorService executorService;

    private ListeningExecutorService listeningExecutorService;

    protected enum UpdateStatus {
        NO_UPDATE_REQUIRED,
        CONDITION_FAILED,
        SUCCESS
    };

    @Override
    public void addQuery(String query) {
        String truncatedQuery = query.length() > maxQuerySize ? query.substring(0, maxQuerySize) : query;
        log.debug("Adding query: {}", query);
        long newValue = storage.addQuery(truncatedQuery);
        log.debug("New value: {}", newValue);

        getListeningExecutorService().submit(() -> updateTopKSuffixesAndLogErrors(truncatedQuery, newValue))
                .addListener(() -> log.debug("Execution finished for query '{}'", truncatedQuery),
                        MoreExecutors.directExecutor());
    }

    @Override
    public TopKQueries getQueries(String prefix) {
        log.debug("Getting queries for prefix: {}", prefix);
        return TopKQueries.builder()
                .queries(addPrefix(storage.getTopKQueries(prefix).getTopK(), prefix))
                .build();
    }

    private void updateTopKSuffixesAndLogErrors(String query, Long count) {
        try {
            updateTopKSuffixes(query, count);
        } catch (Throwable throwable) {
            log.error("Error: ", throwable);
        }
    }
    private void updateTopKSuffixes(String query, Long count) {
        for (int prefixLength = query.length(); prefixLength > 0; --prefixLength) {
            String prefix = query.substring(0, prefixLength);
            int retry = 0;
            for (; retry < maxRetriesToUpdateTopK; ++retry) {
                UpdateStatus status = tryUpdateTopKSuffixes(query, count, prefix, topK);
                if (status == UpdateStatus.CONDITION_FAILED) {
                    long sleepInterval = randomInterval.getMillis();
                    log.debug("Race updating prefix {} retry number {} after {} millis", prefix, retry + 1, sleepInterval);
                    try {
                        Thread.sleep(sleepInterval);
                    } catch (InterruptedException exception) {
                        log.error("Interrupted while sleeping");
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(exception);
                    }
                } else if (status == UpdateStatus.NO_UPDATE_REQUIRED) {
                    log.debug("{} updates finished for query '{}'", query.length() - prefixLength, query);
                    return;
                } else {
                    break;
                }
            }
            if (retry == maxRetriesToUpdateTopK) {
                log.warn("Update topk suffixes for prefix {} failed {} times, give up", prefix, retry);
            }
        }
        log.debug("{} updates finished for query '{}' ALL", query.length(), query);
    }

    protected UpdateStatus tryUpdateTopKSuffixes(String query, Long count, String prefix, Long topK) {
        String suffix = query.substring(prefix.length());
        var topKSuffixes = storage.getTopKQueries(prefix);
        var finalSet = new TreeSet<>(topKSuffixes.getTopK());
        while (finalSet.size() > topK) {
            finalSet.remove(finalSet.first());
        }
        var currentMin = Optional.ofNullable(finalSet.isEmpty() ? null : finalSet.first());
        var currentSuffix = finalSet.stream()
                .filter(x -> x.getSuffix().equals(suffix)).findFirst();
        boolean currentSuffixUpdated = currentSuffix.isPresent() && currentSuffix.get().getCount() < count;
        boolean addCurrentSuffix = currentSuffixUpdated
                || (currentSuffix.isEmpty() && finalSet.size() < topK)
                || (currentSuffix.isEmpty() && currentMin.get().getCount() < count);
        boolean updated = addCurrentSuffix || topKSuffixes.getTopK().size() > topK;

        if (currentSuffixUpdated) {
            finalSet.remove(currentSuffix.get());
        } else if (finalSet.size() == topK
                && currentSuffix.isEmpty()
                && currentMin.get().getCount() < count) {
            finalSet.remove(currentMin.get());
        }
        if (updated) {
            if (addCurrentSuffix) {
                finalSet.add(SuffixCount.builder()
                        .suffix(suffix)
                        .count(count)
                        .build());
            }
            boolean applied = storage.updateTopKQueries(prefix, finalSet, topKSuffixes.getVersion());
            if (!applied) {
                return UpdateStatus.CONDITION_FAILED;
            }
            return UpdateStatus.SUCCESS;
        }
        return UpdateStatus.NO_UPDATE_REQUIRED;
    }

    private synchronized ListeningExecutorService getListeningExecutorService() {
        if (listeningExecutorService == null) {
            listeningExecutorService = MoreExecutors.listeningDecorator(executorService);
        }
        return listeningExecutorService;
    }

    private Set<SuffixCount> addPrefix(Set<SuffixCount> suffixCount, String prefix) {
        return suffixCount.stream().map(sc -> SuffixCount.builder()
                        .count(sc.getCount())
                        .suffix(prefix + sc.getSuffix())
                        .build()
                ).collect(Collectors.toSet());
    }
}
