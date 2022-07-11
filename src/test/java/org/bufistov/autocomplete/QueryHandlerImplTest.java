package org.bufistov.autocomplete;

import com.google.common.util.concurrent.MoreExecutors;
import org.bufistov.model.PrefixTopK;
import org.bufistov.model.SuffixCount;
import org.bufistov.model.TopKQueries;
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
    private final static long MAX_RETRIES_TO_UPDATE_TOPK = 4;

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
        when(randomInterval.getMillis()).thenReturn(1L);
        queryHandler = new QueryHandlerImpl(storage, TOPK, MAX_RETRIES_TO_UPDATE_TOPK,
                randomInterval,
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

    @Test
    public void addQuery_updateThisQuery_newTopK() {
        when(storage.getTopKQueries(prefixCaptor.capture())).thenAnswer(getAnswerForTheQuery(QUERY,
                Set.of(getSuffixCount("1", 10),
                        getSuffixCount("2", 11),
                        getSuffixCount("3", 0))));
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

    @Test
    public void addQuery_updateThisQuery_addNewTopK() {
        when(storage.getTopKQueries(prefixCaptor.capture())).thenAnswer(getAnswerForTheQuery(QUERY,
                Set.of(getSuffixCount("1", 10),
                       getSuffixCount("2", 11))));
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

    @Test
    public void addQuery_updateThisQuery_removeTopK() {
        when(storage.getTopKQueries(prefixCaptor.capture())).thenAnswer(getAnswerForTheQuery(QUERY,
                Set.of(getSuffixCount("1", 10),
                        getSuffixCount("2", 11),
                        getSuffixCount("3", 12),
                        getSuffixCount("4", 13)
                        )));
        queryHandler.addQuery(QUERY);
        verify(storage, times(1)).addQuery(anyString());
        verify(storage, times(2)).getTopKQueries(anyString());
        assertThat(prefixCaptor.getAllValues(), is(List.of(QUERY, "qu")));
        verify(storage, times(1)).updateTopKQueries(anyString(), any(), anyLong());

        assertThat(topKCaptor.getAllValues(), is(List.of(
                Set.of(getSuffixCount("4", 13),
                        getSuffixCount("2", 11),
                        getSuffixCount("3", 12)))
        ));
        assertThat(updatePrefixCaptor.getAllValues(), is(List.of(QUERY)));
    }

    @Test
    public void addQuery_presentButNotUpdated() {
        when(storage.getTopKQueries(prefixCaptor.capture())).thenAnswer(getAnswerForTheQuery(QUERY,
                Set.of(getSuffixCount("1", 10),
                        getSuffixCount("2", 11),
                        getSuffixCount("", NEW_COUNTER_VALUE + 2)
                )));
        queryHandler.addQuery(QUERY);
        verify(storage, times(1)).addQuery(anyString());
        verify(storage, times(1)).getTopKQueries(anyString());
        assertThat(prefixCaptor.getAllValues(), is(List.of(QUERY)));
        verify(storage, never()).updateTopKQueries(anyString(), any(), anyLong());
    }

    @Test
    public void addQuery_conditionFailedOnce_success() {
        ArgumentCaptor<String> updatePrefixCaptor = ArgumentCaptor.forClass(String.class);
        when(storage.updateTopKQueries(updatePrefixCaptor.capture(), topKCaptor.capture(), versionCaptor.capture()))
                .thenReturn(false)
                .thenReturn(true)
                .thenReturn(true);
        when(storage.getTopKQueries(prefixCaptor.capture())).thenAnswer(getAnswerForTheQuery(QUERY,
                Set.of(getSuffixCount("1", 10),
                        getSuffixCount("2", 11),
                        getSuffixCount("", NEW_COUNTER_VALUE - 1)
                )));
        queryHandler.addQuery(QUERY);
        verify(storage, times(3)).getTopKQueries(anyString());
        assertThat(prefixCaptor.getAllValues(), is(List.of(QUERY, QUERY, "qu")));
        verify(storage, times(2)).updateTopKQueries(anyString(), any(), anyLong());
        assertThat(updatePrefixCaptor.getAllValues(), is(List.of(QUERY, QUERY)));
    }

    @Test
    public void addQuery_conditionFailedAlways_success() {
        ArgumentCaptor<String> updatePrefixCaptor = ArgumentCaptor.forClass(String.class);
        when(storage.updateTopKQueries(updatePrefixCaptor.capture(), topKCaptor.capture(), versionCaptor.capture()))
                .thenReturn(false);
        when(storage.getTopKQueries(prefixCaptor.capture())).thenAnswer(getAnswerForTheQuery(QUERY,
                Set.of(getSuffixCount("1", 10),
                        getSuffixCount("2", 11),
                        getSuffixCount("", NEW_COUNTER_VALUE - 1)
                )));
        queryHandler.addQuery(QUERY);
        verify(storage, times((int)MAX_RETRIES_TO_UPDATE_TOPK + 1)).getTopKQueries(anyString());
        assertThat(prefixCaptor.getAllValues(), is(List.of(QUERY, QUERY, QUERY, QUERY, "qu")));
        verify(storage, times((int)MAX_RETRIES_TO_UPDATE_TOPK))
                .updateTopKQueries(anyString(), any(), anyLong());
        assertThat(updatePrefixCaptor.getAllValues(), is(List.of(QUERY, QUERY, QUERY, QUERY)));
    }

    @Test
    public void zeroAgrugmentContructed_success() {
        new QueryHandlerImpl();
    }

    @Test
    public void addQuery_repeatedQuery_updateAll() {
        queryHandler.addQuery(QUERY);
        queryHandler.addQuery(QUERY);
        verify(storage, times(2)).addQuery(anyString());
        assertEquals(QUERY, queryCaptor.getValue());
        verify(storage, times(QUERY.length() * 2)).getTopKQueries(anyString());
    }

    @Test
    public void getQueries_success() {
        when(storage.getTopKQueries(prefixCaptor.capture()))
                .thenReturn(PrefixTopK.builder()
                        .version(VERSION)
                        .topK(getBigTopK())
                        .build());
        var result = queryHandler.getQueries(QUERY);
        assertThat(result, is(TopKQueries.builder()
                .queries(Set.of(getSuffixCount("que1", 10),
                        getSuffixCount("que2", 11),
                        getSuffixCount("que3", 12)
                        ))
                .build()));
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
