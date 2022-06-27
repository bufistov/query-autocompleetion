package org.bufistov.storage;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import org.bufistov.exception.DependencyException;
import org.bufistov.model.CompletionCount;
import org.bufistov.model.PrefixTopK;
import org.bufistov.model.QueryCounter;
import org.bufistov.model.UpdateCounter;

import java.util.Set;

public class CassandraStorage implements Storage {

   private final Session session;
   private final Mapper<QueryCounter> queryCounterMapper;

    private final Mapper<PrefixTopK> topKMapper;

    private final UpdateCounter updateCounter;

    public CassandraStorage(Session session, MappingManager manager) {
        this.session = session;
        this.queryCounterMapper = manager.mapper(QueryCounter.class);
        this.topKMapper = manager.mapper(PrefixTopK.class);
        this.updateCounter = manager.createAccessor(UpdateCounter.class);
    }

    public Session getSession() {
        return this.session;
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
    public Set<CompletionCount> getTopKQueries(String prefix) {
        var result = topKMapper.get(PrefixTopK.builder().prefix(prefix).build());
        if (result == null) {
            return Set.of();
        }
        return result.getTopK();
    }

    @Override
    public void updateTopKQueries(String prefix, Set<CompletionCount> newTopK) {
        topKMapper.save(PrefixTopK.builder()
                        .prefix(prefix)
                        .topK(newTopK)
                .build());
    }
}
