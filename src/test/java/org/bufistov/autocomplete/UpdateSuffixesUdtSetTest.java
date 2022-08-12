package org.bufistov.autocomplete;

import org.bufistov.model.PrefixTopK;
import org.bufistov.model.SuffixCount;
import org.bufistov.storage.Storage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.List;
import java.util.Set;

import static org.bufistov.autocomplete.TopKUpdateStatus.CONDITION_FAILED;
import static org.bufistov.autocomplete.TopKUpdateStatus.NO_UPDATE_REQUIRED;
import static org.bufistov.autocomplete.TopKUpdateStatus.SUCCESS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class UpdateSuffixesUdtSetTest {

    private final static String QUERY = "queso";

    private final static long TOPK = 3;

    private final static long NEW_COUNTER_VALUE = 2;

    private final static String PREFIX = "que";

    private final static String SUFFIX1 = "1";

    private final static String SUFFIX2 = "2";

    private final static Long SUFFIX1_COUNT = 1L;

    private final static Long SUFFIX2_COUNT = 2L;

    private final static long VERSION = 1;

    private final static PrefixTopK TOPK_SUFFIXES = PrefixTopK.builder()
            .topK1(Set.of(toSuffixCount(SUFFIX1, SUFFIX1_COUNT)))
            .version(VERSION)
            .build();

    @Captor
    ArgumentCaptor<String> getPrefixCaptor;

    @Captor
    ArgumentCaptor<String> updatePrefixCaptor;

    @Captor
    ArgumentCaptor<Set<SuffixCount>> newSuffixesCaptor;

    @Captor
    ArgumentCaptor<Long> versionCaptor;

    @Mock
    Storage storage;

    @InjectMocks
    UpdateSuffixesUdtSet updateSuffixesUdtSet;

    @BeforeEach
    void beforeAll() {
        openMocks(this);
        when(storage.getTopKQueries(getPrefixCaptor.capture())).thenReturn(TOPK_SUFFIXES);
        when(storage.updateTopK1Queries(updatePrefixCaptor.capture(), newSuffixesCaptor.capture(),
                versionCaptor.capture())).thenReturn(true);
    }

    @Test
    void test_addNewSuffix_success() {
        var result = updateSuffixesUdtSet.updateTopKSuffixes(QUERY, NEW_COUNTER_VALUE, PREFIX, TOPK);
        assertThat(result, is(SUCCESS));
        verify(storage, times(1)).updateTopK1Queries(any(), any(), any());
        assertThat(getPrefixCaptor.getValue(), is(PREFIX));
        assertThat(updatePrefixCaptor.getValue(), is(PREFIX));
        assertThat(newSuffixesCaptor.getValue(), is(Set.of(
                toSuffixCount(SUFFIX1, SUFFIX1_COUNT),
                toSuffixCount("so", NEW_COUNTER_VALUE)
        )));
        assertThat(versionCaptor.getValue(), is(VERSION));
    }

    @Test
    void test_addNewSuffixOverride_success() {
        String query = "query1";
        String prefix = "query";
        String suffix = "1";
        var result = updateSuffixesUdtSet.updateTopKSuffixes(query, SUFFIX1_COUNT + 1, prefix, TOPK);
        assertThat(result, is(SUCCESS));
        verify(storage, times(1)).updateTopK1Queries(any(), any(), any());
        assertThat(getPrefixCaptor.getValue(), is(prefix));
        assertThat(updatePrefixCaptor.getValue(), is(prefix));
        assertThat(newSuffixesCaptor.getValue(), is(Set.of(
                toSuffixCount(suffix, SUFFIX1_COUNT + 1))));
        assertThat(versionCaptor.getValue(), is(VERSION));
    }

    @Test
    void test_addNewSuffixOverrideSmallValue_noUpdate() {
        when(storage.getTopKQueries(getPrefixCaptor.capture())).thenReturn(PrefixTopK.builder()
                .topK1(Set.of(toSuffixCount("so", NEW_COUNTER_VALUE + 1)))
                .version(VERSION)
                .build());
        var result = updateSuffixesUdtSet.updateTopKSuffixes(QUERY, NEW_COUNTER_VALUE, PREFIX, TOPK);
        assertThat(result, is(NO_UPDATE_REQUIRED));
        verify(storage, never()).updateTopK1Queries(any(), any(), any());
        verify(storage, never()).addSuffixes(any(), any(), any());
    }

    @Test
    void test_replaceCurrentSuffix_success() {
        when(storage.getTopKQueries(getPrefixCaptor.capture())).thenReturn(PrefixTopK.builder()
                .topK1(Set.of(toSuffixCount(SUFFIX1, SUFFIX1_COUNT),
                        toSuffixCount(SUFFIX2, SUFFIX2_COUNT),
                        toSuffixCount("so", NEW_COUNTER_VALUE - 1))
                )
                .version(VERSION)
                .build());

        var result = updateSuffixesUdtSet.updateTopKSuffixes(QUERY, NEW_COUNTER_VALUE, PREFIX, TOPK);
        assertThat(result, is(SUCCESS));
        verify(storage, times(1)).updateTopK1Queries(any(), any(), any());
        assertThat(updatePrefixCaptor.getValue(), is(PREFIX));
        assertThat(newSuffixesCaptor.getValue(), is(Set.of(
                toSuffixCount(SUFFIX1, SUFFIX1_COUNT),
                toSuffixCount(SUFFIX2, SUFFIX2_COUNT),
                toSuffixCount("so", NEW_COUNTER_VALUE)
        )));
        assertThat(versionCaptor.getValue(), is(VERSION));
    }

    @Test
    void test_replaceCurrentMin_success() {
        when(storage.getTopKQueries(getPrefixCaptor.capture())).thenReturn(PrefixTopK.builder()
                .topK1(Set.of(toSuffixCount(SUFFIX1, SUFFIX1_COUNT),
                        toSuffixCount(SUFFIX2, SUFFIX2_COUNT),
                        toSuffixCount("so", NEW_COUNTER_VALUE + 3))
                )
                .version(VERSION)
                .build());
        String query = "quell";
        Long newValue = NEW_COUNTER_VALUE + 2;
        var result = updateSuffixesUdtSet.updateTopKSuffixes(query, newValue, PREFIX, TOPK);
        assertThat(result, is(SUCCESS));
        assertThat(updatePrefixCaptor.getValue(), is(PREFIX));
        assertThat(newSuffixesCaptor.getValue(), is(Set.of(
                toSuffixCount("ll", newValue),
                toSuffixCount(SUFFIX2, SUFFIX2_COUNT),
                toSuffixCount("so", NEW_COUNTER_VALUE + 3)
        )));
        assertThat(versionCaptor.getValue(), is(VERSION));
    }

    @Test
    void test_smallValue_noUpdate() {
        when(storage.getTopKQueries(getPrefixCaptor.capture())).thenReturn(PrefixTopK.builder()
                .topK1(Set.of(toSuffixCount(SUFFIX1, SUFFIX1_COUNT),
                        toSuffixCount(SUFFIX2, SUFFIX2_COUNT),
                        toSuffixCount("so", NEW_COUNTER_VALUE + 3))
                )
                .version(VERSION)
                .build());
        String query = "quell";
        Long newValue = 0L;
        var result = updateSuffixesUdtSet.updateTopKSuffixes(query, newValue, PREFIX, TOPK);
        assertThat(result, is(NO_UPDATE_REQUIRED));
        verify(storage, never()).updateTopK1Queries(any(), any(), any());
    }

    @Test
    void test_replaceCurrentMin_conditionFailed() {
        when(storage.getTopKQueries(getPrefixCaptor.capture())).thenReturn(PrefixTopK.builder()
                .topK1(Set.of(toSuffixCount(SUFFIX1, SUFFIX1_COUNT),
                        toSuffixCount(SUFFIX2, SUFFIX2_COUNT),
                        toSuffixCount("so", NEW_COUNTER_VALUE))
                )
                .version(VERSION)
                .build());
        when(storage.updateTopK1Queries(updatePrefixCaptor.capture(),
                newSuffixesCaptor.capture(), versionCaptor.capture()))
                .thenReturn(false);
        String query = "quell";
        Long newValue = NEW_COUNTER_VALUE + 2;
        var result = updateSuffixesUdtSet.updateTopKSuffixes(query, newValue, PREFIX, TOPK);
        assertThat(result, is(CONDITION_FAILED));
        assertThat(updatePrefixCaptor.getValue(), is(PREFIX));
        assertThat(newSuffixesCaptor.getValue(), is(Set.of(
                toSuffixCount("ll", newValue),
                toSuffixCount(SUFFIX2, SUFFIX2_COUNT),
                toSuffixCount("so", NEW_COUNTER_VALUE))));
        assertThat(versionCaptor.getValue(), is(VERSION));
    }
    @Test
    void test_configChange_success() {
        when(storage.getTopKQueries(getPrefixCaptor.capture())).thenReturn(PrefixTopK.builder()
                .topK1(Set.of(toSuffixCount(SUFFIX1, SUFFIX1_COUNT),
                        toSuffixCount(SUFFIX2, SUFFIX2_COUNT),
                        toSuffixCount("so", NEW_COUNTER_VALUE + 1))
                )
                .version(VERSION)
                .build());

        String query = "quell";
        Long newValue = 3L;
        var result = updateSuffixesUdtSet.updateTopKSuffixes(query, newValue, PREFIX, TOPK - 1);
        assertThat(result, is(SUCCESS));
        assertThat(updatePrefixCaptor.getValue(), is(PREFIX));
        assertThat(newSuffixesCaptor.getValue(), is(Set.of(
                toSuffixCount("so", NEW_COUNTER_VALUE + 1),
                toSuffixCount("ll", newValue))));
        assertThat(versionCaptor.getValue(), is(VERSION));
    }

    @Test
    void test_configChange_updateFailed() {
        when(storage.getTopKQueries(getPrefixCaptor.capture())).thenReturn(PrefixTopK.builder()
                .topK1(Set.of(toSuffixCount(SUFFIX1, SUFFIX1_COUNT),
                        toSuffixCount(SUFFIX2, SUFFIX2_COUNT),
                        toSuffixCount("so", NEW_COUNTER_VALUE + 1))
                )
                .version(VERSION)
                .build());

        ArgumentCaptor<Set<SuffixCount>> newSuffixesCaptor = ArgumentCaptor.forClass(Set.class);
        when(storage.updateTopK1Queries(any(), newSuffixesCaptor.capture(), any())).thenReturn(false);
        String query = "quell";
        Long newValue = 3L;
        var result = updateSuffixesUdtSet.updateTopKSuffixes(query, newValue, PREFIX, TOPK - 1);
        assertThat(result, is(CONDITION_FAILED));
        verify(storage, times(1)).updateTopK1Queries(any(), any(), any());
        assertThat(newSuffixesCaptor.getValue(), is(Set.of(
                toSuffixCount("so", NEW_COUNTER_VALUE + 1),
                toSuffixCount("ll", newValue))));
    }

    @Test
    void test_toSortedListNonNull_success() {
        assertThat(updateSuffixesUdtSet.toSortedList(PrefixTopK.builder()
                .topK1(Set.of(toSuffixCount(SUFFIX2, SUFFIX2_COUNT),
                        toSuffixCount(SUFFIX1, SUFFIX1_COUNT),
                        toSuffixCount("3", 3L)))
                .build()), is(List.of(toSuffixCount(SUFFIX1, SUFFIX1_COUNT),
                toSuffixCount(SUFFIX2, SUFFIX2_COUNT),
                toSuffixCount("3", 3L)
                )));
    }

    @Test
    void test_toSortedListNull_emptyList() {
        assertThat(new UpdateSuffixesUdtSet().toSortedList(null), is(List.of()));
    }

    static SuffixCount toSuffixCount(String suffix, Long count) {
        return SuffixCount.builder()
                .suffix(suffix)
                .count(count)
                .build();
    }
}
