package org.bufistov.storage;

import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import org.bufistov.exception.DependencyException;
import org.bufistov.model.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class CassandraStorage implements Storage {
   private final Mapper<QueryCounter> queryCounterMapper;

    private final Mapper<PrefixTopKCassandra> topKMapper;

    private final CassandraQueries cassandraQueries;

    public CassandraStorage(MappingManager manager) {
        this.queryCounterMapper = manager.mapper(QueryCounter.class);
        this.topKMapper = manager.mapper(PrefixTopKCassandra.class);
        this.cassandraQueries = manager.createAccessor(CassandraQueries.class);
    }

    @Override
    public Long addQuery(String query) {
        cassandraQueries.incrementCounter(1, query);
        QueryCounter result = queryCounterMapper.get(query);
        if (result == null) {
            throw new DependencyException("Cannot find query " + query, null);
        }
        return result.getCount();
    }

    @Override
    public PrefixTopK getTopKQueries(String prefix) {
        return Optional.ofNullable(topKMapper.get(prefix))
                .map(item -> PrefixTopK.builder().topK(item.getTopK())
                        .topK1(item.getTopK1())
                        .version(item.getVersion()).build())
                .orElse(PrefixTopK.builder()
                        .topK(Set.of())
                        .topK1(Map.of()).build());
    }

    @Override
    public boolean updateTopKQueries(String prefix, Set<SuffixCount> newTopK, Long version) {
        return cassandraQueries.updateTopK(prefix, newTopK, version, getNewVersion(version))
                .wasApplied();
    }

    @Override
    public boolean addSuffixes(String prefix, Map<String, Long> suffixes, Long version) {
        return cassandraQueries.addNewSuffix(prefix, suffixes, version, getNewVersion(version)).wasApplied();
    }

    @Override
    public boolean removeSuffixes(String prefix, Set<String> suffixes, Long version) {
        return cassandraQueries.removeSuffixes(prefix, suffixes, version, getNewVersion(version))
                .wasApplied();
    }

    private Long getNewVersion(Long version) {
        return version == null ? 1 : version + 1;
    }
}
