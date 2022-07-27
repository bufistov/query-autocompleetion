package org.bufistov.autocomplete;

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

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

public class QueryHandlerImplTest {

    private final static long TOPK = 3;
    private final static long MAX_RETRIES_TO_UPDATE_TOPK = 4;

    private final static long NEW_COUNTER_VALUE = 2;

    private final static String QUERY = "que";

    private final static int MAX_QUERY_SIZE = 10;

    private final static QueryHandlerConfig TEST_CONFIG = QueryHandlerConfig.builder()
            .maxQuerySize(MAX_QUERY_SIZE)
            .maxRetriesToUpdateTopK(MAX_RETRIES_TO_UPDATE_TOPK)
            .topK(TOPK)
            .queryUpdateCount(2L)
            .queryUpdateMillis(1000L)
            .build();

    @Mock
    Storage storage;

    @Mock
    RandomInterval randomInterval;

    Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

    UpdateSuffixes updateSuffixes;

    QueryHandlerImpl queryHandler;

    @Captor
    ArgumentCaptor<String> queryCaptor;

    @Captor
    ArgumentCaptor<Long> topKCaptor;

    @Captor
    ArgumentCaptor<Long> newCounterCaptor;

    @Captor
    ArgumentCaptor<String> prefixCaptor;

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

        when(randomInterval.getMillis()).thenReturn(1L);
        when(storage.lockQueryForTopKUpdate(anyString(), any(Date.class), any(Date.class))).thenReturn(true);
        doNothing().when(storage).updateTemporalCounter(anyString(), anyLong());
        updateSuffixes = spy(new UpdateSuffixesMap(storage));
        doReturn(TopKUpdateStatus.SUCCESS).when(updateSuffixes)
                .updateTopKSuffixes(queryCaptor.capture(), newCounterCaptor.capture(),
                        prefixCaptor.capture(), topKCaptor.capture());
        doCallRealMethod().when(updateSuffixes).toSortedList(any());
        queryHandler = new QueryHandlerImpl(storage, TEST_CONFIG,
                updateSuffixes,
                randomInterval,
                clock);
    }

    @Test
    public void addQuery_oneQuery_success() {
        queryHandler.addQuery(QUERY);
        verify(storage, times(1)).addQuery(anyString());
        assertEquals(QUERY, queryCaptor.getValue());
        verify(updateSuffixes, times(QUERY.length())).updateTopKSuffixes(anyString(), anyLong(), anyString(), anyLong());
        ArgumentCaptor<Date> lastDateCaptor = ArgumentCaptor.forClass(Date.class);
        ArgumentCaptor<Date> currentDateCaptor = ArgumentCaptor.forClass(Date.class);
        verify(storage, times(1)).lockQueryForTopKUpdate(anyString(),
                lastDateCaptor.capture(), currentDateCaptor.capture());
        assertThat(lastDateCaptor.getValue(), is(Date.from(clock.instant())));
        assertThat(currentDateCaptor.getValue(), is(Date.from(clock.instant())));

        assertThat(topKCaptor.getValue(), is(TOPK));
        assertThat(prefixCaptor.getAllValues(), is(List.of(QUERY, "qu", "q")));
    }

    @Test
    public void addQuery_oneQueryNoTopKUpdate_success() {
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
        ArgumentCaptor<Long> temporalIncrement = ArgumentCaptor.forClass(Long.class);
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
        verify(updateSuffixes, times(QUERY.length())).updateTopKSuffixes(anyString(), anyLong(), anyString(), anyLong());
        ArgumentCaptor<Date> lastDateCaptor = ArgumentCaptor.forClass(Date.class);
        ArgumentCaptor<Date> currentDateCaptor = ArgumentCaptor.forClass(Date.class);
        verify(storage, times(1)).lockQueryForTopKUpdate(anyString(),
                lastDateCaptor.capture(), currentDateCaptor.capture());
        assertThat(lastDateCaptor.getValue(), is(Date.from(lastUpdateTime)));
        assertThat(currentDateCaptor.getValue(), is(Date.from(clock.instant())));

        verify(storage, times(1)).updateTemporalCounter(anyString(), temporalIncrement.capture());
        assertThat(temporalIncrement.getValue(), is(-temporalCounter));
    }

    @Test
    public void addQuery_oneQueryFirstUpdate_noUpdate() {
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
        verify(storage, never()).getTopKQueries(anyString());
        verify(storage, never()).lockQueryForTopKUpdate(anyString(), any(Date.class), any(Date.class));
    }

    @Test
    public void addQuery_conditionFailedOnce_success() {
        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> prefixCaptor = ArgumentCaptor.forClass(String.class);
        when(updateSuffixes.updateTopKSuffixes(queryCaptor.capture(), newCounterCaptor.capture(),
                prefixCaptor.capture(), topKCaptor.capture()))
                .thenReturn(TopKUpdateStatus.CONDITION_FAILED)
                .thenReturn(TopKUpdateStatus.SUCCESS);
        queryHandler.addQuery(QUERY);
        verify(updateSuffixes, times(QUERY.length() + 1))
                .updateTopKSuffixes(anyString(), anyLong(), anyString(), anyLong());
        assertThat(prefixCaptor.getAllValues(), is(List.of(QUERY, QUERY, "qu", "q")));
        assertThat(queryCaptor.getAllValues(), is(List.of(QUERY, QUERY, QUERY, QUERY)));
    }

    @Test
    public void addQuery_conditionFailedAlways_success() {
        when(updateSuffixes.updateTopKSuffixes(anyString(), anyLong(), anyString(), anyLong()))
                .thenReturn(TopKUpdateStatus.CONDITION_FAILED);
        queryHandler.addQuery(QUERY);
        verify(updateSuffixes, times((int)MAX_RETRIES_TO_UPDATE_TOPK * QUERY.length()))
                .updateTopKSuffixes(anyString(), anyLong(), anyString(), anyLong());
    }

    @Test
    public void addQuery_noUpdateRequired_success() {
        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> prefixCaptor = ArgumentCaptor.forClass(String.class);
        when(updateSuffixes.updateTopKSuffixes(queryCaptor.capture(), newCounterCaptor.capture(),
                prefixCaptor.capture(), topKCaptor.capture()))
                .thenReturn(TopKUpdateStatus.NO_UPDATE_REQUIRED);
        queryHandler.addQuery(QUERY);
        verify(updateSuffixes, times(1))
                .updateTopKSuffixes(anyString(), anyLong(), anyString(), anyLong());
    }

    @Test
    public void addQuery_updateSuffixesThrows_success() {
        when(updateSuffixes.updateTopKSuffixes(queryCaptor.capture(), newCounterCaptor.capture(),
                prefixCaptor.capture(), topKCaptor.capture()))
                .thenThrow(RuntimeException.class);
        queryHandler.addQuery(QUERY);
        verify(updateSuffixes, times(1))
                .updateTopKSuffixes(anyString(), anyLong(), anyString(), anyLong());
    }

    @Test
    public void addQuery_longQueryTruncated_success() {
        String longQuery = "long query query";
        assertThat(longQuery.length(), greaterThan(MAX_QUERY_SIZE));
        queryHandler.addQuery(longQuery);
        assertThat(queryCaptor.getValue(), is(longQuery.substring(0, MAX_QUERY_SIZE)));
    }

    @Test
    public void addQuery_interruptedException_success() throws InterruptedException {
        when(updateSuffixes.updateTopKSuffixes(queryCaptor.capture(), newCounterCaptor.capture(),
                prefixCaptor.capture(), topKCaptor.capture()))
                .thenReturn(TopKUpdateStatus.CONDITION_FAILED);
        when(randomInterval.getMillis()).thenReturn(1000_000L);
        var threadPool = Executors.newSingleThreadExecutor();
        threadPool.submit(() -> queryHandler.addQuery(QUERY));
        Thread.yield();
        threadPool.shutdownNow();
        assertThat(threadPool.isShutdown(), is(true));
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
        verify(updateSuffixes, times(QUERY.length() * 2))
                .updateTopKSuffixes(anyString(), anyLong(), anyString(), anyLong());
    }

    @Test
    public void getQueries_success() {
        String suffix1 = "ry1";
        String suffix2 = "ry2";
        long count1 = 2;
        long count2 = 1;
        when(storage.getTopKQueries(prefixCaptor.capture()))
                .thenReturn(PrefixTopK.builder()
                        .topK1(Map.of(suffix1, count1, suffix2, count2))
                        .build());
        assertThat(queryHandler.getQueries("que"), is(TopKQueries.builder()
                .queries(List.of(getSuffixCount("query2", count2),
                        getSuffixCount("query1", count1)
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
}
