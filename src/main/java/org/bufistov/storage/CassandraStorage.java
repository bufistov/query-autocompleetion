package org.bufistov.storage;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import org.bufistov.exception.DependencyException;
import org.bufistov.model.*;

import java.util.*;
import java.util.stream.Collectors;

public class CassandraStorage implements Storage {

    static final TupleType SUFFIX_TUPLE_TYPE = TupleType.of(ProtocolVersion.V4, CodecRegistry.DEFAULT_INSTANCE,
            DataType.bigint(), DataType.text());
    private final Mapper<QueryCountCassandra> queryCounterMapper;

    private final Mapper<PrefixTopKCassandra> topKMapper;

    private final Mapper<QueryUpdateCassandra> queryUpdateMapper;

    private final CassandraQueries cassandraQueries;

    public CassandraStorage(MappingManager manager) {
        this.queryCounterMapper = manager.mapper(QueryCountCassandra.class);
        this.topKMapper = manager.mapper(PrefixTopKCassandra.class);
        this.queryUpdateMapper = manager.mapper(QueryUpdateCassandra.class);
        this.cassandraQueries = manager.createAccessor(CassandraQueries.class);
    }

    @Override
    public QueryCount addQuery(String query) {
        cassandraQueries.incrementCounter(1, query);
        QueryCountCassandra result = queryCounterMapper.get(query);
        if (result == null) {
            throw new DependencyException("Cannot find query " + query, null);
        }
        var lastUpdate = queryUpdateMapper.get(query);
        return QueryCount.builder()
                .query(query)
                .count(result.getCount())
                .sinceLastUpdate(result.getSinceLastUpdate())
                .lastUpdateTime(Optional.ofNullable(lastUpdate)
                        .map(QueryUpdateCassandra::getTopkUpdate)
                        .orElse(null))
                .build();
    }

    @Override
    public PrefixTopK getTopKQueries(String prefix) {
        return Optional.ofNullable(topKMapper.get(prefix))
                .map(item -> PrefixTopK.builder()
                        .topK(Optional.ofNullable(item.getTopK()).orElse(Set.of()))
                        .topK1(Optional.ofNullable(item.getTopK1()).orElse(Map.of()))
                        .topK2(toSuffixCount(item.getTopK2()))
                        .version(item.getVersion()).build())
                .orElse(PrefixTopK.builder()
                        .topK(Set.of())
                        .topK1(Map.of())
                        .topK2(List.of())
                        .build());
    }

    @Override
    public boolean updateTopKQueries(String prefix, Set<SuffixCount> newTopK, Long version) {
        return cassandraQueries.updateTopK(prefix, newTopK, version, getNewVersion(version))
                .wasApplied();
    }

    @Override
    public boolean addSuffixes(String prefix, Map<String, Long> suffixes, Long version) {
        return cassandraQueries.updateTopK1(prefix, Set.of(), suffixes, version, getNewVersion(version)).wasApplied();
    }

    @Override
    public boolean removeSuffixes(String prefix, Set<String> suffixes, Long version) {
        return cassandraQueries.updateTopK1(prefix, suffixes, Map.of(), version, getNewVersion(version))
                .wasApplied();
    }

    @Override
    public boolean updateTopK1Queries(String prefix, Set<String> toRemove, Map<String, Long> toAdd, Long version) {
         return cassandraQueries.updateTopK1(prefix, toRemove, toAdd, version, getNewVersion(version)).wasApplied();
    }

    @Override
    public boolean replaceSuffixCounter(String prefix, String suffix, Long newValue, Long version) {
        return cassandraQueries.updateTopK1(prefix, Set.of(), Map.of(suffix, newValue), version, getNewVersion(version))
                .wasApplied();
    }

    @Override
    public boolean updateTopK2Queries(String prefix, Set<TupleValue> toRemove, Set<TupleValue> toAdd, Long version) {
        return cassandraQueries.updateTopK2(prefix, toRemove, toAdd, version, getNewVersion(version)).wasApplied();
    }

    @Override
    public void updateTemporalCounter(String query, Long increment) {
        cassandraQueries.updateTemporalCounter(increment, query);
    }

    @Override
    public boolean lockQueryForTopKUpdate(String query, Date lastUpdateTime, Date currentTime) {
        return cassandraQueries.lockForTopKUpdate(query, lastUpdateTime, currentTime).wasApplied();
    }

    private Long getNewVersion(Long version) {
        return version == null ? 1 : version + 1;
    }

    SuffixCount fromTuple(TupleValue tuple) {
        return SuffixCount.builder().count(tuple.getLong(0))
                .suffix(tuple.getString(1))
                .build();
    }

    List<SuffixCount> toSuffixCount(Set<TupleValue> tuples) {
        if (tuples == null) {
            return List.of();
        }
        return tuples.stream().map(this::fromTuple).collect(Collectors.toList());
    }

    public static TupleValue toTuple(SuffixCount suffixCount) {
        return SUFFIX_TUPLE_TYPE.newValue(suffixCount.getCount(), suffixCount.getSuffix());
    }

    public static TupleValue toTuple1(Long count, String suffix) {
        return SUFFIX_TUPLE_TYPE.newValue(count, suffix);
    }
}
