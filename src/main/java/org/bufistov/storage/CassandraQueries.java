package org.bufistov.storage;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;
import org.bufistov.model.SuffixCount;

import java.util.List;
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

    /*@Query("UPDATE " + CASSANDRA_KEYSPACE + "." + PREFIX_TOPK + " SET topk1=topk1 - :tr,version=:nv WHERE prefix=:p IF version=:v")
    ResultSet removeSuffixes(@Param("p") String prefix, @Param("tr") Set<String> suffixes,
                             @Param("v") Long version, @Param("nv") Long newVersion);*/

    @Query("UPDATE " + CASSANDRA_KEYSPACE + "." + PREFIX_TOPK + " SET topk1=topk1 + :nel,version=:nv WHERE prefix=:p IF version=:v")
    ResultSet addNewSuffix(@Param("p") String prefix, @Param("nel") Map<String, Long> newValue,
                           @Param("v") Long version, @Param("nv") Long newVersion);

    /*@Query("UPDATE " + CASSANDRA_KEYSPACE + "." + PREFIX_TOPK + " SET topk1[:k]=:v,version=:nv WHERE prefix=:p IF version=:v")
    ResultSet replaceSuffixCounter(@Param("p") String prefix,
                                   @Param("k")String suffix, @Param("v") Long value,
                                   @Param("v") Long version, @Param("nv") Long newVersion);

    @Query("UPDATE " + CASSANDRA_KEYSPACE + "." + PREFIX_TOPK + " SET topk1 = topk1 - {:kr}, topk1=topk1 +:ns, version=:nv WHERE prefix=:p IF version=:v")
    ResultSet replaceSuffix(@Param("p") String prefix,
                            @Param("kr")String suffixToRemove,
                            @Param("ns")Map.Entry<String, Long> suffixToAdd,
                            @Param("v") Long version, @Param("nv") Long newVersion);*/


}
