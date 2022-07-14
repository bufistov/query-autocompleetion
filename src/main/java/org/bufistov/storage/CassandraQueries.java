package org.bufistov.storage;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;
import org.bufistov.model.SuffixCount;

import java.util.Map;
import java.util.Set;

import static org.bufistov.Constants.*;

@Accessor
public interface CassandraQueries {

    @Query("UPDATE " + CASSANDRA_KEYSPACE + "." + QUERY_COUNT + " SET count = count + :d WHERE query=:q")
    void incrementCounter(@Param("d") long increment, @Param("q") String query);

    @Query("UPDATE " + CASSANDRA_KEYSPACE + "." + PREFIX_TOPK + " SET topK=:t,version=:nv WHERE prefix=:p IF version=:v")
    ResultSet updateTopK(@Param("p") String prefix, @Param("t") Set<SuffixCount> suffixCounts,
                         @Param("v") Long version, @Param("nv") Long newVersion);

    @Query("UPDATE " + CASSANDRA_KEYSPACE + "." + PREFIX_TOPK + " SET topk1[:k]=:vl,version=:nv WHERE prefix=:p IF version=:v")
    ResultSet replaceSuffixCounter(@Param("p") String prefix,
                                   @Param("k") String suffix, @Param("vl") Long value,
                                   @Param("v") Long version, @Param("nv") Long newVersion);

    @Query("UPDATE " + CASSANDRA_KEYSPACE + "." + PREFIX_TOPK + " SET topk1 = topk1 - :kr, topk1=topk1 +:ns, version=:nv WHERE prefix=:p IF version=:v")
    ResultSet updateTopK1(@Param("p") String prefix,
                          @Param("kr")Set<String> suffixesToRemove,
                          @Param("ns")Map<String, Long> suffixesToAdd,
                          @Param("v") Long version, @Param("nv") Long newVersion);


}
