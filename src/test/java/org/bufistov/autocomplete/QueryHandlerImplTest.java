package org.bufistov.autocomplete;

import com.google.common.util.concurrent.MoreExecutors;
import org.bufistov.model.CompletionCount;
import org.bufistov.model.PrefixTopK;
import org.bufistov.storage.Storage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

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

    private final static long OLD_COUNTER_VALUE = 1;
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
    ArgumentCaptor<Set<CompletionCount>> topKCaptor;

    @Captor
    ArgumentCaptor<Long> versionCaptor;

    @Captor
    ArgumentCaptor<String> prefixCaptor;

    @BeforeEach
    public void setUp() {
        openMocks(this);
        when(storage.addQuery(queryCaptor.capture())).thenReturn(NEW_COUNTER_VALUE);
        when(storage.getTopKQueries(anyString())).thenAnswer((args) -> {
            String prefix = args.getArgument(0, String.class);
            String suffix = QUERY.substring(prefix.length());
            return PrefixTopK.builder()
                    .topK(Set.of(CompletionCount.builder()
                            .count(OLD_COUNTER_VALUE)
                            .suffix(suffix)
                    .build()))
                    .version(VERSION)
                    .build();
        });
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
        when(storage.getTopKQueries(eq(QUERY))).thenAnswer((args) -> {
            return PrefixTopK.builder()
                    .topK(getBigTopK())
                    .version(VERSION)
                    .build();
        });
        queryHandler.addQuery(QUERY);
        verify(storage, times(1)).addQuery(anyString());
        assertEquals(QUERY, queryCaptor.getValue());
        verify(storage, times(1)).getTopKQueries(anyString());
        verify(storage, never()).updateTopKQueries(anyString(), any(), anyLong());
    }

    @Test
    public void addQuery_updateThisQuery() {
        when(storage.getTopKQueries(prefixCaptor.capture())).thenAnswer((args) -> {
            String prefix = args.getArgument(0, String.class);
            String suffix = QUERY.substring(prefix.length());
            if (suffix.length() == 0) {
                return PrefixTopK.builder()
                        .topK(Set.of(getSuffixCount("1", 10),
                                getSuffixCount("2", 11),
                                getSuffixCount(suffix, 0)))
                        .version(VERSION)
                        .build();
            } else {
                return PrefixTopK.builder()
                        .topK(getBigTopK())
                        .version(VERSION)
                        .build();
            }
        });
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

    private CompletionCount getSuffixCount(String suffix, long count) {
        return CompletionCount.builder()
                .count(count)
                .suffix(suffix)
                .build();
    }

    private Set<CompletionCount> getBigTopK() {
        return Set.of(getSuffixCount("1", 10),
                getSuffixCount("2", 11),
                getSuffixCount("3", 12));
    }
}
