package org.bufistov.model;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;

import java.util.Set;

import static org.bufistov.Constants.*;

@Accessor
public interface CassandraQueries {

    @Query("UPDATE " + CASSANDRA_KEYSPACE + "." + QUERY_COUNT + " SET count = count + :d WHERE query=:q")
    void incrementCounter(@Param("d") long increment, @Param("q") String query);

    @Query("UPDATE " + CASSANDRA_KEYSPACE + "." + PREFIX_TOPK + " SET topK=:t,version=:nv WHERE prefix=:p IF version=:v")
    ResultSet updateTopK(@Param("p") String prefix, @Param("t") Set<SuffixCount> suffixCounts,
                         @Param("v") Long version, @Param("nv") Long newVersion);
}
