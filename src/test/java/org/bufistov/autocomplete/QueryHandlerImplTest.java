package org.bufistov.autocomplete;

import com.google.common.util.concurrent.MoreExecutors;
import org.bufistov.model.CompletionCount;
import org.bufistov.model.PrefixTopK;
import org.bufistov.storage.Storage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import java.util.Set;
import java.util.concurrent.ExecutorService;

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

    private final static String QUERY = "query";

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

    @Before
    public void setUp() {
        openMocks(this);
        when(storage.addQuery(queryCaptor.capture())).thenReturn(NEW_COUNTER_VALUE);
        when(storage.getTopKQueries(anyString())).thenAnswer((args) -> {
            String prefix = args.getArgument(0, String.class);
            String suffix = QUERY.substring(prefix.length());
            return PrefixTopK.builder().topK(Set.of(CompletionCount.builder()
                            .count(1L)
                            .suffix(suffix)
                    .build())).build();
        });
        when(storage.updateTopKQueries(updatePrefixCaptor.capture(), topKCaptor.capture(), versionCaptor.capture()))
                .thenReturn(true);
        queryHandler = new QueryHandlerImpl(storage, TOPK, MAX_RETRIES_TO_UPDATE_TOPK, randomInterval,
                executorService, null);
    }

    @Test
    public void addQuery_oneQuery_success() {
        queryHandler.addQuery(QUERY);
        verify(storage, times(1)).addQuery(anyString());
        assertEquals(QUERY, queryCaptor.getValue());
        verify(storage, times(QUERY.length())).getTopKQueries(anyString());
    }
}
