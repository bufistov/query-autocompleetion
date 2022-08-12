package org.bufistov.autocomplete;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.bufistov.model.PrefixTopK;
import org.bufistov.model.SuffixCount;
import org.bufistov.storage.Storage;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.bufistov.autocomplete.TopKUpdateStatus.CONDITION_FAILED;
import static org.bufistov.autocomplete.TopKUpdateStatus.NO_UPDATE_REQUIRED;
import static org.bufistov.autocomplete.TopKUpdateStatus.SUCCESS;

@NoArgsConstructor
@AllArgsConstructor
public class UpdateSuffixesUdtSet implements UpdateSuffixes {

    @Autowired
    protected Storage storage;

    @Override
    public TopKUpdateStatus updateTopKSuffixes(String query, Long count, String prefix, Long topK) {
        String suffix = query.substring(prefix.length());
        var topKSuffixes = storage.getTopKQueries(prefix);
        var finalSet = new TreeSet<>(topKSuffixes.getTopK1());
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
        boolean updated = addCurrentSuffix || topKSuffixes.getTopK1().size() > topK;

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
            boolean applied = storage.updateTopK1Queries(prefix, finalSet, topKSuffixes.getVersion());
            if (!applied) {
                return CONDITION_FAILED;
            }
            return SUCCESS;
        }
        return NO_UPDATE_REQUIRED;
    }

    @Override
    public List<SuffixCount> toSortedList(PrefixTopK suffixCount) {
        if (suffixCount == null) {
            return List.of();
        }
        return suffixCount.getTopK1().stream().sorted()
                .map(sc -> SuffixCount.builder()
                        .count(sc.getCount())
                        .suffix(sc.getSuffix())
                        .build()
                )
                .collect(Collectors.toList());
    }
}
