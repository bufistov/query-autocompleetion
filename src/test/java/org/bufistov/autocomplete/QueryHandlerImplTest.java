package org.bufistov.autocomplete;

import com.google.common.util.concurrent.MoreExecutors;
import org.bufistov.model.SuffixCount;
import org.bufistov.model.PrefixTopK;
import org.bufistov.storage.Storage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.openMocks;

public class QueryHandlerImplTest {

    @Mock
    Storage storage;

    private final static long TOPK = 3;
    private final static long MAX_RETRIES_TO_UPDATE_TOPK = 10;

    private final static long NEW_COUNTER_VALUE = 2;

    private final static String QUERY = "que";

    private final static long VERSION = 1;

    @Mock
    RandomInterval randomInterval;
    ExecutorService executorService = MoreExecutors.newDirectExecutorService();

    QueryHandlerImpl queryHandler;

    @Captor
    ArgumentCaptor<String> queryCaptor;

    @Captor
    ArgumentCaptor<String> updatePrefixCaptor;

    @Captor
    ArgumentCaptor<Set<SuffixCount>> topKCaptor;

    @Captor
    ArgumentCaptor<Long> versionCaptor;

    @Captor
    ArgumentCaptor<String> prefixCaptor;

    @BeforeEach
    public void setUp() {
        openMocks(this);
        when(storage.addQuery(queryCaptor.capture())).thenReturn(NEW_COUNTER_VALUE);
        when(storage.getTopKQueries(anyString())).thenReturn(PrefixTopK.builder()
                    .topK(Set.of())
                    .version(VERSION)
                    .build());
        when(storage.updateTopKQueries(updatePrefixCaptor.capture(), topKCaptor.capture(), versionCaptor.capture()))
                .thenReturn(true);
        queryHandler = new QueryHandlerImpl(storage, TOPK, MAX_RETRIES_TO_UPDATE_TOPK, randomInterval,
                executorService, null);
    }

    @Test
    public void addQuery_oneQuery_updateAll() {
        queryHandler.addQuery(QUERY);
        verify(storage, times(1)).addQuery(anyString());
        assertEquals(QUERY, queryCaptor.getValue());
        verify(storage, times(QUERY.length())).getTopKQueries(anyString());
        assertThat(versionCaptor.getAllValues(), is(List.of(VERSION, VERSION, VERSION)));
        assertThat(topKCaptor.getAllValues(), is(List.of(
                Set.of(getSuffixCount("", NEW_COUNTER_VALUE)),
                Set.of(getSuffixCount("e", NEW_COUNTER_VALUE)),
                Set.of(getSuffixCount("ue", NEW_COUNTER_VALUE))
        )));
        assertThat(updatePrefixCaptor.getAllValues(), is(List.of(QUERY, "qu", "q")));
    }

    @Test
    public void addQuery_secondQuery_noUpdate() {
        when(storage.getTopKQueries(eq(QUERY))).thenAnswer((args) -> PrefixTopK.builder()
                .topK(getBigTopK())
                .version(VERSION)
                .build());
        queryHandler.addQuery(QUERY);
        verify(storage, times(1)).addQuery(anyString());
        assertEquals(QUERY, queryCaptor.getValue());
        verify(storage, times(1)).getTopKQueries(anyString());
        verify(storage, never()).updateTopKQueries(anyString(), any(), anyLong());
    }

    @Test
    public void addQuery_updateThisQuery() {
        when(storage.getTopKQueries(prefixCaptor.capture())).thenAnswer(getAnswerForTheQuery(QUERY,
                Set.of(getSuffixCount("1", 10),
                        getSuffixCount("2", 11),
                        getSuffixCount("", 0))));
        queryHandler.addQuery(QUERY);
        verify(storage, times(1)).addQuery(anyString());
        verify(storage, times(2)).getTopKQueries(anyString());
        assertThat(prefixCaptor.getAllValues(), is(List.of(QUERY, "qu")));
        verify(storage, times(1)).updateTopKQueries(anyString(), any(), anyLong());

        assertThat(topKCaptor.getAllValues(), is(List.of(
                Set.of(getSuffixCount("1", 10),
                        getSuffixCount("2", 11),
                        getSuffixCount("", NEW_COUNTER_VALUE)))
        ));
        assertThat(updatePrefixCaptor.getAllValues(), is(List.of(QUERY)));
    }

    private SuffixCount getSuffixCount(String suffix, long count) {
        return SuffixCount.builder()
                .count(count)
                .suffix(suffix)
                .build();
    }

    private Set<SuffixCount> getBigTopK() {
        return Set.of(getSuffixCount("1", 10),
                getSuffixCount("2", 11),
                getSuffixCount("3", 12));
    }

    private Answer<PrefixTopK> getAnswerForTheQuery(String query, Set<SuffixCount> answer) {
        return (args) -> {
            String prefix = args.getArgument(0, String.class);
            String suffix = query.substring(prefix.length());
            if (suffix.length() == 0) {
                return PrefixTopK.builder()
                        .topK(answer)
                        .version(VERSION)
                        .build();
            } else {
                return PrefixTopK.builder()
                        .topK(getBigTopK())
                        .version(VERSION)
                        .build();
            }
        };
    }
}
