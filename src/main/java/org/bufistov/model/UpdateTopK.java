package org.bufistov.model;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;

import java.util.Set;

import static org.bufistov.Constants.*;

@Accessor
public interface UpdateTopK {
    @Query("UPDATE " + CASSANDRA_KEYSPACE + "." + PREFIX_TOPK + " SET topK=:t,version=:nv WHERE prefix=:p IF version=:v")
    ResultSet updateTopK(@Param("p") String prefix, @Param("t") Set<SuffixCount> suffixCounts,
                         @Param("v") Long version, @Param("nv") Long newVersion);

    @Query("UPDATE " + CASSANDRA_KEYSPACE + "." + PREFIX_TOPK + " SET topK=:t,version=1 WHERE prefix=:p IF version=NULL")
    ResultSet updateTopKIfVersionIsNull(@Param("p") String prefix, @Param("t") Set<SuffixCount> suffixCounts);
}
