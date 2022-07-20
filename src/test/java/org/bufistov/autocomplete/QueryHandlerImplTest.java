package org.bufistov.autocomplete;

import com.google.common.util.concurrent.MoreExecutors;
import org.bufistov.model.PrefixTopK;
import org.bufistov.model.QueryCount;
import org.bufistov.model.SuffixCount;
import org.bufistov.model.TopKQueries;
import org.bufistov.storage.Storage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class QueryHandlerImplTest {

    @Mock
    Storage storage;

    private final static long TOPK = 3;
    private final static long MAX_RETRIES_TO_UPDATE_TOPK = 4;

    private final static long NEW_COUNTER_VALUE = 2;

    private final static String QUERY = "que";

    private final static long VERSION = 1;

    private final static int MAX_QUERY_SIZE = 100;

    private final static QueryHandlerConfig TEST_CONFIG = QueryHandlerConfig.builder()
            .maxQuerySize(MAX_QUERY_SIZE)
            .maxRetriesToUpdateTopK(MAX_RETRIES_TO_UPDATE_TOPK)
            .topK(TOPK)
            .queryUpdateCount(2L)
            .queryUpdateMillis(1000L)
            .build();

    @Mock
    RandomInterval randomInterval;

    Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
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

    @Captor
    ArgumentCaptor<Long> temporalIncrement;

    @BeforeEach
    public void setUp() {
        openMocks(this);
        when(storage.addQuery(queryCaptor.capture())).thenReturn(
                QueryCount.builder()
                        .query(QUERY)
                        .count(NEW_COUNTER_VALUE)
                        .sinceLastUpdate(NEW_COUNTER_VALUE)
                        .lastUpdateTime(Date.from(clock.instant()))
                        .build()
        );
        when(storage.lockQueryForTopKUpdate(anyString(), any(Date.class), any(Date.class)))
                .thenReturn(true);
        doNothing().when(storage).updateTemporalCounter(anyString(), temporalIncrement.capture());

        when(storage.getTopKQueries(anyString())).thenReturn(PrefixTopK.builder()
                    .topK(Set.of())
                    .version(VERSION)
                    .build());
        when(storage.updateTopKQueries(updatePrefixCaptor.capture(), topKCaptor.capture(), versionCaptor.capture()))
                .thenReturn(true);
        when(randomInterval.getMillis()).thenReturn(1L);
        queryHandler = new QueryHandlerImpl(storage, TEST_CONFIG,
                randomInterval,
                executorService,
                clock,
                null);
    }

    @Test
    public void addQuery_oneQuery_updateAll() {
        queryHandler.addQuery(QUERY);
        verify(storage, times(1)).addQuery(anyString());
        assertEquals(QUERY, queryCaptor.getValue());
        verify(storage, times(QUERY.length())).getTopKQueries(anyString());
        ArgumentCaptor<Date> lastDateCaptor = ArgumentCaptor.forClass(Date.class);
        ArgumentCaptor<Date> currentDateCaptor = ArgumentCaptor.forClass(Date.class);
        verify(storage, times(1)).lockQueryForTopKUpdate(anyString(),
                lastDateCaptor.capture(), currentDateCaptor.capture());
        assertThat(lastDateCaptor.getValue(), is(Date.from(clock.instant())));
        assertThat(currentDateCaptor.getValue(), is(Date.from(clock.instant())));

        verify(storage, times(1)).updateTemporalCounter(anyString(), temporalIncrement.capture());
        assertThat(temporalIncrement.getValue(), is(-NEW_COUNTER_VALUE));
        assertThat(versionCaptor.getAllValues(), is(List.of(VERSION, VERSION, VERSION)));
        assertThat(topKCaptor.getAllValues(), is(List.of(
                Set.of(getSuffixCount("", NEW_COUNTER_VALUE)),
                Set.of(getSuffixCount("e", NEW_COUNTER_VALUE)),
                Set.of(getSuffixCount("ue", NEW_COUNTER_VALUE))
        )));
        assertThat(updatePrefixCaptor.getAllValues(), is(List.of(QUERY, "qu", "q")));
    }

    @Test
    public void addQuery_oneQueryNoTopKUpdate_updateAll() {
        when(storage.addQuery(queryCaptor.capture())).thenReturn(
                QueryCount.builder()
                        .query(QUERY)
                        .count(NEW_COUNTER_VALUE)
                        .sinceLastUpdate(1L)
                        .lastUpdateTime(Date.from(clock.instant()))
                        .build()
        );
        queryHandler.addQuery(QUERY);
        verify(storage, times(1)).addQuery(anyString());
        assertEquals(QUERY, queryCaptor.getValue());
        verify(storage, never()).getTopKQueries(anyString());
        verify(storage, never()).lockQueryForTopKUpdate(anyString(), any(Date.class), any(Date.class));

        verify(storage, never()).updateTemporalCounter(anyString(), anyLong());
    }

    @Test
    public void addQuery_oneQueryTimeFlush_updateAll() {
        Instant lastUpdateTime = clock.instant().minus(2, ChronoUnit.SECONDS);
        Long temporalCounter = 1L;
        when(storage.addQuery(queryCaptor.capture())).thenReturn(
                QueryCount.builder()
                        .query(QUERY)
                        .count(NEW_COUNTER_VALUE)
                        .sinceLastUpdate(temporalCounter)
                        .lastUpdateTime(Date.from(lastUpdateTime))
                        .build()
        );
        queryHandler.addQuery(QUERY);
        verify(storage, times(1)).addQuery(anyString());
        assertEquals(QUERY, queryCaptor.getValue());
        verify(storage, times(QUERY.length())).getTopKQueries(anyString());
        ArgumentCaptor<Date> lastDateCaptor = ArgumentCaptor.forClass(Date.class);
        ArgumentCaptor<Date> currentDateCaptor = ArgumentCaptor.forClass(Date.class);
        verify(storage, times(1)).lockQueryForTopKUpdate(anyString(),
                lastDateCaptor.capture(), currentDateCaptor.capture());
        assertThat(lastDateCaptor.getValue(), is(Date.from(lastUpdateTime)));
        assertThat(currentDateCaptor.getValue(), is(Date.from(clock.instant())));

        verify(storage, times(1)).updateTemporalCounter(anyString(), temporalIncrement.capture());
        assertThat(temporalIncrement.getValue(), is(-temporalCounter));
        assertThat(versionCaptor.getAllValues(), is(List.of(VERSION, VERSION, VERSION)));
        assertThat(topKCaptor.getAllValues(), is(List.of(
                Set.of(getSuffixCount("", NEW_COUNTER_VALUE)),
                Set.of(getSuffixCount("e", NEW_COUNTER_VALUE)),
                Set.of(getSuffixCount("ue", NEW_COUNTER_VALUE))
        )));
        assertThat(updatePrefixCaptor.getAllValues(), is(List.of(QUERY, "qu", "q")));
    }

    @Test
    public void addQuery_oneQueryFirstUpdate_updateAll() {
        Long counter = 1L;
        when(storage.addQuery(queryCaptor.capture())).thenReturn(
                QueryCount.builder()
                        .query(QUERY)
                        .count(counter)
                        .sinceLastUpdate(counter)
                        .lastUpdateTime(Date.from(clock.instant()))
                        .build()
        );
        queryHandler.addQuery(QUERY);
        verify(storage, times(1)).addQuery(anyString());
        assertEquals(QUERY, queryCaptor.getValue());
        verify(storage, times(QUERY.length())).getTopKQueries(anyString());
        ArgumentCaptor<Date> lastDateCaptor = ArgumentCaptor.forClass(Date.class);
        ArgumentCaptor<Date> currentDateCaptor = ArgumentCaptor.forClass(Date.class);
        verify(storage, times(1)).lockQueryForTopKUpdate(anyString(),
                lastDateCaptor.capture(), currentDateCaptor.capture());
        assertThat(lastDateCaptor.getValue(), is(Date.from(clock.instant())));
        assertThat(currentDateCaptor.getValue(), is(Date.from(clock.instant())));

        verify(storage, times(1)).updateTemporalCounter(anyString(), temporalIncrement.capture());
        assertThat(temporalIncrement.getValue(), is(-counter));
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
    public void addQuery_presentButNotUpdated_lessThanK_noUpdate() {
        when(storage.getTopKQueries(prefixCaptor.capture())).thenAnswer(getAnswerForTheQuery(QUERY,
                Set.of(getSuffixCount("1", 10),
                        getSuffixCount("", NEW_COUNTER_VALUE + 2)
                )));
        queryHandler.addQuery(QUERY);
        verify(storage, never()).updateTopKQueries(anyString(), any(), anyLong());
        verify(storage, times(1)).addQuery(anyString());
        verify(storage, times(1)).getTopKQueries(anyString());
        assertThat(prefixCaptor.getAllValues(), is(List.of(QUERY)));
    }

    @Test
    public void addQuery_presentButNotUpdated_isCurrentMin_noUpdate() {
        when(storage.getTopKQueries(prefixCaptor.capture())).thenAnswer(getAnswerForTheQuery(QUERY,
                Set.of(getSuffixCount("1", NEW_COUNTER_VALUE - 1),
                        getSuffixCount("", NEW_COUNTER_VALUE + 1)
                )));
        queryHandler.addQuery(QUERY);
        verify(storage, never()).updateTopKQueries(anyString(), any(), anyLong());
        verify(storage, times(1)).addQuery(anyString());
        verify(storage, times(1)).getTopKQueries(anyString());
        assertThat(prefixCaptor.getAllValues(), is(List.of(QUERY)));
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
                .queries(List.of(getSuffixCount("que1", 10),
                        getSuffixCount("que2", 11),
                        getSuffixCount("que3", 12)
                        ))
                .build()));
    }

    @Test
    public void addQuery_interrupted_success() {
        ExecutorService executorService = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS,
                new SynchronousQueue<>(), new ThreadPoolExecutor.AbortPolicy());
        when(randomInterval.getMillis()).thenReturn(100000L);
        when(storage.updateTopKQueries(updatePrefixCaptor.capture(), topKCaptor.capture(), versionCaptor.capture()))
                .thenReturn(false);
        var queryHandler = new QueryHandlerImpl(storage, TEST_CONFIG,
                randomInterval,
                executorService, clock,
                null);
        queryHandler.addQuery(QUERY);
        executorService.shutdownNow();
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
