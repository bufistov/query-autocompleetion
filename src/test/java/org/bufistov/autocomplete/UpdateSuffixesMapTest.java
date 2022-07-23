package org.bufistov.autocomplete;

import org.bufistov.model.PrefixTopK;
import org.bufistov.storage.Storage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.Map;

import static org.bufistov.autocomplete.TopKUpdateStatus.SUCCESS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
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
            .topK1(Map.of(SUFFIX1, SUFFIX1_COUNT))
            .version(VERSION)
            .build();

    @Captor
    ArgumentCaptor<String> getPrefixCaptor;

    @Captor
    ArgumentCaptor<String> prefixAddSuffixesCaptor;

    @Captor
    ArgumentCaptor<Map<String, Long>> newSuffixesCaptor;

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
        when(storage.addSuffixes(prefixAddSuffixesCaptor.capture(), newSuffixesCaptor.capture(), versionCaptor.capture()))
                .thenReturn(true);
    }

    @Test
    void test_addNewSuffix_success() {
        var result = updateSuffixesMap.updateTopKSuffixes(QUERY, 2L, PREFIX, TOPK);
        assertThat(result, is(SUCCESS));
        verify(storage, never()).updateTopK1Queries(any(), any(), any(), any());
        assertThat(getPrefixCaptor.getValue(), is(PREFIX));
        assertThat(prefixAddSuffixesCaptor.getValue(), is(PREFIX));
        assertThat(newSuffixesCaptor.getValue(), is(Map.of("so", 2L)));
        assertThat(versionCaptor.getValue(), is(VERSION));
    }
}
