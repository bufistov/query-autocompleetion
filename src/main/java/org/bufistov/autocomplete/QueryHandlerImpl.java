package org.bufistov.autocomplete;

import lombok.extern.log4j.Log4j2;
import org.bufistov.model.CompletionCount;
import org.bufistov.model.TopKQueries;
import org.bufistov.storage.Storage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.time.Clock;
import java.util.*;

@Log4j2
public class QueryHandlerImpl implements QueryHandler {

    @Autowired
    private Storage storage;

    @Value("${org.bufistov.autocomplete.K}")
    private Long topK;

    @Value("${org.bufistov.autocomplete.max_retries_to_update_topk}")
    private Long maxRetriesToUpdateTopK;

    @Autowired
    private RandomInterval randomInterval;

    private enum UpdateStatus {
        NO_UPDATE_REQUIRED,
        CONDITION_FAILED,
        SUCCESS
    };

    @Override
    public void addQuery(String query) {
        log.debug("Adding query: {}", query);
        long newValue = storage.addQuery(query);
        log.debug("New value: {}", newValue);
        updateTopKSuffixes(query, newValue);
    }

    @Override
    public TopKQueries getQueries(String prefix) {
        log.debug("Getting queries for prefix: {}", prefix);
        return TopKQueries.builder()
                .queries(storage.getTopKQueries(prefix).getTopK())
                .build();
    }
    private void updateTopKSuffixes(String query, Long count) {
        for (int prefixLength = query.length(); prefixLength > 0; --prefixLength) {
            String prefix = query.substring(0, prefixLength);
            int retry = 0;
            for (; retry < maxRetriesToUpdateTopK; ++retry) {
                var status = tryUpdateTopKSuffixes(query, count, prefix);
                if (status == UpdateStatus.CONDITION_FAILED) {
                    long sleepInterval = randomInterval.getMillis();
                    log.info("Race updating prefix {} retry number {} after {} millis", prefix, retry + 1, sleepInterval);
                    try {
                        Thread.sleep(sleepInterval);
                    } catch (InterruptedException exception) {
                        log.error("Interrupted while sleeping");
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(exception);
                    }
                } else if (status == UpdateStatus.NO_UPDATE_REQUIRED) {
                    return;
                } else {
                    break;
                }
            }
            if (retry == maxRetriesToUpdateTopK) {
                log.warn("Update topk suffixes for prefix {} failed {} times, give up", prefix, retry);
            }
        }
    }

    private UpdateStatus tryUpdateTopKSuffixes(String query, Long count, String prefix) {
        String suffix = query.substring(prefix.length());
        var topKSuffixes = storage.getTopKQueries(prefix);
        var finalSet = new TreeSet<>(topKSuffixes.getTopK());
        while (finalSet.size() > topK) {
            finalSet.remove(finalSet.first());
        }
        var currentMin = Optional.ofNullable(finalSet.first());
        var currentSuffix = finalSet.stream()
                .filter(x -> x.getSuffix().equals(suffix)).findFirst();
        boolean currentSuffixUpdated = currentSuffix.isPresent() && currentSuffix.get().getCount() < count;
        boolean updated = currentSuffixUpdated
                || finalSet.size() < topK
                || currentMin.get().getCount() < count;

        if (currentSuffixUpdated) {
            finalSet.remove(currentSuffix.get());
        } else if (finalSet.size() == topK
                && currentSuffix.isEmpty()
                && currentMin.get().getCount() < count) {
            finalSet.remove(currentMin.get());
        }
        if (updated) {
            finalSet.add(CompletionCount.builder()
                    .suffix(suffix)
                    .count(count)
                    .build());
            boolean applied = storage.updateTopKQueries(prefix, finalSet, topKSuffixes.getVersion());
            if (!applied) {
                return UpdateStatus.CONDITION_FAILED;
            }
            return UpdateStatus.SUCCESS;
        }
        return UpdateStatus.NO_UPDATE_REQUIRED;
    }
}
