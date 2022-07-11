package org.bufistov.storage;

import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import org.bufistov.exception.DependencyException;
import org.bufistov.model.*;

import java.util.Optional;
import java.util.Set;

public class CassandraStorage implements Storage {
   private final Mapper<QueryCounter> queryCounterMapper;

    private final Mapper<PrefixTopKCassandra> topKMapper;

    private final UpdateCounter updateCounter;
    private final UpdateTopK updateTopK;

    public CassandraStorage(MappingManager manager) {
        this.queryCounterMapper = manager.mapper(QueryCounter.class);
        this.topKMapper = manager.mapper(PrefixTopKCassandra.class);
        this.updateCounter = manager.createAccessor(UpdateCounter.class);
        this.updateTopK = manager.createAccessor(UpdateTopK.class);
    }

    @Override
    public Long addQuery(String query) {
        updateCounter.incrementCounter(1, query);
        QueryCounter result = queryCounterMapper.get(query);
        if (result == null) {
            throw new DependencyException("Cannot find query " + query.toString(), null);
        }
        return result.getCount();
    }

    @Override
    public PrefixTopK getTopKQueries(String prefix) {
        return Optional.ofNullable(topKMapper.get(prefix))
                .map(item -> PrefixTopK.builder().topK(item.getTopK())
                        .version(item.getVersion()).build())
                .orElse(PrefixTopK.builder().topK(Set.of()).build());
    }

    @Override
    public boolean updateTopKQueries(String prefix, Set<SuffixCount> newTopK, Long version) {
        if (version == null) {
            return updateTopK.updateTopKIfVersionIsNull(prefix, newTopK).wasApplied();
        }
        return updateTopK.updateTopK(prefix, newTopK, version, version + 1).wasApplied();
    }
}
