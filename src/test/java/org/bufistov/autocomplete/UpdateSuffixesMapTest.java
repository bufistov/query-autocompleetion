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
import java.util.Map;
import java.util.Set;

import static org.bufistov.autocomplete.TopKUpdateStatus.CONDITION_FAILED;
import static org.bufistov.autocomplete.TopKUpdateStatus.NO_UPDATE_REQUIRED;
import static org.bufistov.autocomplete.TopKUpdateStatus.SUCCESS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class UpdateSuffixesMapTest {

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
            .topK(Map.of(SUFFIX1, SUFFIX1_COUNT))
            .version(VERSION)
            .build();

    @Captor
    ArgumentCaptor<String> getPrefixCaptor;

    @Captor
    ArgumentCaptor<String> updatePrefixCaptor;

    @Captor
    ArgumentCaptor<Map<String, Long>> newSuffixesCaptor;

    @Captor
    ArgumentCaptor<Set<String>> toRemoveCaptor;

    @Captor
    ArgumentCaptor<String> updateSuffixCaptor;

    @Captor
    ArgumentCaptor<Long> versionCaptor;

    @Mock
    Storage storage;

    @InjectMocks
    UpdateSuffixesMap updateSuffixesMap;

    @BeforeEach
    void beforeAll() {
        openMocks(this);
        when(storage.getTopKQueries(getPrefixCaptor.capture())).thenReturn(TOPK_SUFFIXES);
        when(storage.addSuffixes(updatePrefixCaptor.capture(), newSuffixesCaptor.capture(), versionCaptor.capture()))
                .thenReturn(true);
        when(storage.updateTopKQueries(updatePrefixCaptor.capture(), toRemoveCaptor.capture(),
                newSuffixesCaptor.capture(), versionCaptor.capture()))
                .thenReturn(true);
    }

    @Test
    void test_addNewSuffix_success() {
        var result = updateSuffixesMap.updateTopKSuffixes(QUERY, NEW_COUNTER_VALUE, PREFIX, TOPK);
        assertThat(result, is(SUCCESS));
        verify(storage, never()).updateTopKQueries(any(), any(), any(), any());
        assertThat(getPrefixCaptor.getValue(), is(PREFIX));
        assertThat(updatePrefixCaptor.getValue(), is(PREFIX));
        assertThat(newSuffixesCaptor.getValue(), is(Map.of("so", NEW_COUNTER_VALUE)));
        assertThat(versionCaptor.getValue(), is(VERSION));
    }

    @Test
    void test_addNewSuffixOverride_success() {

        when(storage.getTopKQueries(getPrefixCaptor.capture())).thenReturn(PrefixTopK.builder()
                .topK(Map.of("so", NEW_COUNTER_VALUE - 1))
                .version(VERSION)
                .build());

        var result = updateSuffixesMap.updateTopKSuffixes(QUERY, NEW_COUNTER_VALUE, PREFIX, TOPK);
        assertThat(result, is(SUCCESS));
        verify(storage, never()).updateTopKQueries(any(), any(), any(), any());
        assertThat(getPrefixCaptor.getValue(), is(PREFIX));
        assertThat(updatePrefixCaptor.getValue(), is(PREFIX));
        assertThat(newSuffixesCaptor.getValue(), is(Map.of("so", NEW_COUNTER_VALUE)));
        assertThat(versionCaptor.getValue(), is(VERSION));
    }

    @Test
    void test_addNewSuffixOverrideSmallValue_noUpdate() {

        when(storage.getTopKQueries(getPrefixCaptor.capture())).thenReturn(PrefixTopK.builder()
                .topK(Map.of("so", NEW_COUNTER_VALUE + 1))
                .version(VERSION)
                .build());
        var result = updateSuffixesMap.updateTopKSuffixes(QUERY, NEW_COUNTER_VALUE, PREFIX, TOPK);
        assertThat(result, is(NO_UPDATE_REQUIRED));
        verify(storage, never()).updateTopKQueries(any(), any(), any(), any());
        verify(storage, never()).addSuffixes(any(), any(), any());
    }

    @Test
    void test_replaceCurrentSuffix_success() {
        when(storage.getTopKQueries(getPrefixCaptor.capture())).thenReturn(PrefixTopK.builder()
                .topK(Map.of(SUFFIX1, SUFFIX1_COUNT, SUFFIX2, SUFFIX2_COUNT, "so", NEW_COUNTER_VALUE - 1))
                .version(VERSION)
                .build());

         ArgumentCaptor<Long> newValueCaptor = ArgumentCaptor.forClass(Long.class);
        when(storage.replaceSuffixCounter(updatePrefixCaptor.capture(), updateSuffixCaptor.capture(),
                newValueCaptor.capture(), versionCaptor.capture()))
                 .thenReturn(true);
        var result = updateSuffixesMap.updateTopKSuffixes(QUERY, NEW_COUNTER_VALUE, PREFIX, TOPK);
        assertThat(result, is(SUCCESS));
        verify(storage, never()).updateTopKQueries(any(), any(), any(), any());
        assertThat(updatePrefixCaptor.getValue(), is(PREFIX));
        assertThat(updateSuffixCaptor.getValue(), is("so"));
        assertThat(newValueCaptor.getValue(), is(NEW_COUNTER_VALUE));
        assertThat(versionCaptor.getValue(), is(VERSION));
    }

    @Test
    void test_replaceCurrentMin_success() {
        when(storage.getTopKQueries(getPrefixCaptor.capture())).thenReturn(PrefixTopK.builder()
                .topK(Map.of(SUFFIX1, SUFFIX1_COUNT, SUFFIX2, SUFFIX2_COUNT, "so", NEW_COUNTER_VALUE))
                .version(VERSION)
                .build());

        when(storage.updateTopKQueries(updatePrefixCaptor.capture(), toRemoveCaptor.capture(),
                newSuffixesCaptor.capture(), versionCaptor.capture()))
                .thenReturn(true);
        String query = "quell";
        Long newValue = NEW_COUNTER_VALUE + 2;
        var result = updateSuffixesMap.updateTopKSuffixes(query, newValue, PREFIX, TOPK);
        assertThat(result, is(SUCCESS));
        assertThat(updatePrefixCaptor.getValue(), is(PREFIX));
        assertThat(newSuffixesCaptor.getValue(), is(Map.of("ll", newValue)));
        assertThat(toRemoveCaptor.getValue(), is(Set.of(SUFFIX1)));
        assertThat(versionCaptor.getValue(), is(VERSION));
    }

    @Test
    void test_smallValue_noUpdate() {
        when(storage.getTopKQueries(getPrefixCaptor.capture())).thenReturn(PrefixTopK.builder()
                .topK(Map.of(SUFFIX1, SUFFIX1_COUNT, SUFFIX2, SUFFIX2_COUNT, "so", NEW_COUNTER_VALUE))
                .version(VERSION)
                .build());

        when(storage.updateTopKQueries(updatePrefixCaptor.capture(), toRemoveCaptor.capture(),
                newSuffixesCaptor.capture(), versionCaptor.capture()))
                .thenReturn(true);
        String query = "quell";
        Long newValue = 0L;
        var result = updateSuffixesMap.updateTopKSuffixes(query, newValue, PREFIX, TOPK);
        assertThat(result, is(NO_UPDATE_REQUIRED));
        verify(storage, never()).updateTopKQueries(any(), any(), any(), any());
        verify(storage, never()).addSuffixes(any(), any(), any());
    }

    @Test
    void test_staleValue_noUpdate() {
        when(storage.getTopKQueries(getPrefixCaptor.capture())).thenReturn(PrefixTopK.builder()
                .topK(Map.of(SUFFIX1, SUFFIX1_COUNT, SUFFIX2, SUFFIX2_COUNT, "so", NEW_COUNTER_VALUE))
                .version(VERSION)
                .build());

        when(storage.updateTopKQueries(updatePrefixCaptor.capture(), toRemoveCaptor.capture(),
                newSuffixesCaptor.capture(), versionCaptor.capture()))
                .thenReturn(true);
        Long newValue = 0L;
        var result = updateSuffixesMap.updateTopKSuffixes(QUERY, newValue, PREFIX, TOPK);
        assertThat(result, is(NO_UPDATE_REQUIRED));
        verify(storage, never()).updateTopKQueries(any(), any(), any(), any());
        verify(storage, never()).addSuffixes(any(), any(), any());
    }

    @Test
    void test_replaceCurrentMin_conditionFailed() {
        when(storage.getTopKQueries(getPrefixCaptor.capture())).thenReturn(PrefixTopK.builder()
                .topK(Map.of(SUFFIX1, SUFFIX1_COUNT, SUFFIX2, SUFFIX2_COUNT, "so", NEW_COUNTER_VALUE))
                .version(VERSION)
                .build());

        when(storage.updateTopKQueries(updatePrefixCaptor.capture(), toRemoveCaptor.capture(),
                newSuffixesCaptor.capture(), versionCaptor.capture()))
                .thenReturn(false);
        String query = "quell";
        Long newValue = NEW_COUNTER_VALUE + 2;
        var result = updateSuffixesMap.updateTopKSuffixes(query, newValue, PREFIX, TOPK);
        assertThat(result, is(CONDITION_FAILED));
        assertThat(updatePrefixCaptor.getValue(), is(PREFIX));
        assertThat(newSuffixesCaptor.getValue(), is(Map.of("ll", newValue)));
        assertThat(toRemoveCaptor.getValue(), is(Set.of(SUFFIX1)));
        assertThat(versionCaptor.getValue(), is(VERSION));
    }
    @Test
    void test_configChange_success() {
        when(storage.getTopKQueries(getPrefixCaptor.capture())).thenReturn(PrefixTopK.builder()
                .topK(Map.of(SUFFIX1, SUFFIX1_COUNT, SUFFIX2, SUFFIX2_COUNT, "so", NEW_COUNTER_VALUE))
                .version(VERSION)
                .build());

        ArgumentCaptor<String> removeSuffixPrefix = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Long> removeSuffixVersion = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Set<String>> setArgumentCaptor = ArgumentCaptor.forClass(Set.class);
        when(storage.removeSuffixes(removeSuffixPrefix.capture(), setArgumentCaptor.capture(),
                        removeSuffixVersion.capture()))
                .thenReturn(true);

        when(storage.updateTopKQueries(updatePrefixCaptor.capture(), toRemoveCaptor.capture(),
                newSuffixesCaptor.capture(), versionCaptor.capture()))
                .thenReturn(true);
        String query = "quell";
        Long newValue = 3L;
        var result = updateSuffixesMap.updateTopKSuffixes(query, newValue, PREFIX, TOPK - 1);
        assertThat(result, is(SUCCESS));
        assertThat(removeSuffixPrefix.getValue(), is(PREFIX));
        assertThat(removeSuffixVersion.getValue(), is(VERSION));
        assertThat(setArgumentCaptor.getValue(), is(Set.of(SUFFIX1)));
        assertThat(updatePrefixCaptor.getValue(), is(PREFIX));
        assertThat(newSuffixesCaptor.getValue(), is(Map.of("ll", newValue)));
        assertThat(toRemoveCaptor.getValue(), is(Set.of(SUFFIX2)));
        assertThat(versionCaptor.getValue(), is(VERSION + 1));
    }

    @Test
    void test_configChange_updateFailed() {
        when(storage.getTopKQueries(getPrefixCaptor.capture())).thenReturn(PrefixTopK.builder()
                .topK(Map.of(SUFFIX1, SUFFIX1_COUNT, SUFFIX2, SUFFIX2_COUNT, "so", NEW_COUNTER_VALUE))
                .version(VERSION)
                .build());

        ArgumentCaptor<String> removeSuffixPrefix = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Long> removeSuffixVersion = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Set<String>> setArgumentCaptor = ArgumentCaptor.forClass(Set.class);
        when(storage.removeSuffixes(removeSuffixPrefix.capture(), setArgumentCaptor.capture(),
                removeSuffixVersion.capture()))
                .thenReturn(false);
        String query = "quell";
        Long newValue = 3L;
        var result = updateSuffixesMap.updateTopKSuffixes(query, newValue, PREFIX, TOPK - 1);
        assertThat(result, is(CONDITION_FAILED));
        assertThat(removeSuffixPrefix.getValue(), is(PREFIX));
        assertThat(removeSuffixVersion.getValue(), is(VERSION));
        assertThat(setArgumentCaptor.getValue(), is(Set.of(SUFFIX1)));
        verify(storage, never()).updateTopKQueries(any(), any(), any(), any());
    }

    @Test
    void test_configChangeNullVersion_success() {
        when(storage.getTopKQueries(getPrefixCaptor.capture())).thenReturn(PrefixTopK.builder()
                .topK(Map.of(SUFFIX1, SUFFIX1_COUNT, SUFFIX2, SUFFIX2_COUNT, "so", NEW_COUNTER_VALUE))
                .version(null)
                .build());

        ArgumentCaptor<String> removeSuffixPrefix = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Long> removeSuffixVersion = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Set<String>> setArgumentCaptor = ArgumentCaptor.forClass(Set.class);
        when(storage.removeSuffixes(removeSuffixPrefix.capture(), setArgumentCaptor.capture(),
                removeSuffixVersion.capture()))
                .thenReturn(true);

        when(storage.updateTopKQueries(updatePrefixCaptor.capture(), toRemoveCaptor.capture(),
                newSuffixesCaptor.capture(), versionCaptor.capture()))
                .thenReturn(true);
        String query = "quell";
        Long newValue = 3L;
        var result = updateSuffixesMap.updateTopKSuffixes(query, newValue, PREFIX, TOPK - 1);
        assertThat(result, is(SUCCESS));
        assertThat(removeSuffixPrefix.getValue(), is(PREFIX));
        assertThat(removeSuffixVersion.getValue(), is(nullValue()));
        assertThat(setArgumentCaptor.getValue(), is(Set.of(SUFFIX1)));
        assertThat(updatePrefixCaptor.getValue(), is(PREFIX));
        assertThat(newSuffixesCaptor.getValue(), is(Map.of("ll", newValue)));
        assertThat(toRemoveCaptor.getValue(), is(Set.of(SUFFIX2)));
        assertThat(versionCaptor.getValue(), is(1L));
    }

    @Test
    void test_toSortedListNonNull_success() {
        assertThat(updateSuffixesMap.toSortedList(PrefixTopK.builder()
                .topK(Map.of(SUFFIX2, SUFFIX2_COUNT, SUFFIX1, SUFFIX1_COUNT, "3", 3L))
                .build()), is(List.of(toSuffixCount(SUFFIX1, SUFFIX1_COUNT),
                toSuffixCount(SUFFIX2, SUFFIX2_COUNT),
                toSuffixCount("3", 3L)
                )));
    }

    @Test
    void test_toSortedListNull_emptyList() {
        assertThat(new UpdateSuffixesMap().toSortedList(null), is(List.of()));
    }

    SuffixCount toSuffixCount(String suffix, Long count) {
        return SuffixCount.builder()
                .suffix(suffix)
                .count(count)
                .build();
    }
}
