package org.bufistov.autocomplete;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.bufistov.model.QueryCount;
import org.bufistov.model.SuffixCount;
import org.bufistov.model.TopKQueries;
import org.bufistov.storage.Storage;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static org.bufistov.autocomplete.TopKUpdateStatus.CONDITION_FAILED;
import static org.bufistov.autocomplete.TopKUpdateStatus.NO_UPDATE_REQUIRED;

@Log4j2
@AllArgsConstructor
@NoArgsConstructor
public class QueryHandlerImpl implements QueryHandler {

    @Autowired
    private Storage storage;

    @Autowired
    private QueryHandlerConfig config;

    @Autowired
    private UpdateSuffixes updateSuffixes;

    @Autowired
    private RandomInterval randomInterval;

    @Autowired
    private Clock clock;

    @Override
    public void addQuery(String query) {
        String truncatedQuery = query.length() > config.getMaxQuerySize() ?
                query.substring(0, config.getMaxQuerySize()) : query;
        log.debug("Adding query: {}", query);
        var result = storage.addQuery(truncatedQuery);
        log.debug("New count value: {}", result);
        if (topKUpdateRequired(result)) {
            log.debug("Update is required, {}", result);
            if (storage.lockQueryForTopKUpdate(query, result.getLastUpdateTime(), Date.from(clock.instant()))) {
                storage.updateTemporalCounter(query, -result.getSinceLastUpdate());
                updateTopKSuffixesAndLogErrors(truncatedQuery, result.getCount());
            }
        } else {
            log.debug("Skip topk update, {}", result);
        }
    }

    @Override
    public TopKQueries getQueries(String prefix) {
        log.debug("Getting queries for prefix: {}", prefix);
        return TopKQueries.builder()
                .queries(addPrefix(updateSuffixes.toSortedList(storage.getTopKQueries(prefix)), prefix))
                .build();
    }

    private boolean topKUpdateRequired(QueryCount currentCount) {
        return  currentCount.getSinceLastUpdate() >= config.getQueryUpdateCount()
             || (currentCount.getLastUpdateTime() != null && currentCount.getLastUpdateTime().before(Date.from(
                        clock.instant().minus(config.getQueryUpdateMillis(), ChronoUnit.MILLIS))))
             || (currentCount.getLastUpdateTime() == null
                && currentCount.getCount() >= config.getFirstQueryUpdateCount());
    }

    private List<SuffixCount> addPrefix(List<SuffixCount> suffixCount, String prefix) {
        return suffixCount.stream()
                .map(sc -> SuffixCount.builder()
                        .count(sc.getCount())
                        .suffix(prefix + sc.getSuffix())
                        .build()
                )
                .collect(Collectors.toList());
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
            for (; retry < config.getMaxRetriesToUpdateTopK(); ++retry) {
                TopKUpdateStatus status = updateSuffixes.updateTopKSuffixes(query, count, prefix, config.getTopK());
                if (status == CONDITION_FAILED) {
                    long sleepInterval = randomInterval.getMillis();
                    log.debug("Race updating prefix {} retry number {} after {} millis", prefix, retry + 1, sleepInterval);
                    try {
                        Thread.sleep(sleepInterval);
                    } catch (InterruptedException exception) {
                        log.error("Interrupted while sleeping");
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(exception);
                    }
                } else if (status == NO_UPDATE_REQUIRED) {
                    log.debug("{} updates finished for query '{}'", query.length() - prefixLength, query);
                    return;
                } else {
                    break;
                }
            }
            if (retry == config.getMaxRetriesToUpdateTopK()) {
                log.warn("Update topk suffixes for prefix {} failed {} times, give up", prefix, retry);
            }
        }
        log.debug("{} updates finished for query '{}' ALL", query.length(), query);
    }
}
