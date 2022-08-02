package org.bufistov.storage;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;
import org.bufistov.model.SuffixCount;

import java.util.Date;
import java.util.Map;
import java.util.Set;

import static org.bufistov.Constants.CASSANDRA_KEYSPACE;
import static org.bufistov.Constants.PREFIX_TOPK;
import static org.bufistov.Constants.QUERY_COUNT;
import static org.bufistov.Constants.QUERY_UPDATE;

@Accessor
public interface CassandraQueries {

    @Query("UPDATE " + CASSANDRA_KEYSPACE + "." + QUERY_COUNT + " SET count = count + :d, sinceLastUpdate = sinceLastUpdate+:d WHERE query=:q")
    void incrementCounter(@Param("d") long increment, @Param("q") String query);

    @Query("UPDATE " + CASSANDRA_KEYSPACE + "." + QUERY_COUNT + " SET sinceLastUpdate=sinceLastUpdate+:d WHERE query=:q")
    void updateTemporalCounter(@Param("d") long increment, @Param("q") String query);

    @Query("UPDATE " + CASSANDRA_KEYSPACE + "." + QUERY_UPDATE + " SET topkUpdate=:ct WHERE query=:q IF topkUpdate=:lut")
    ResultSet lockForTopKUpdate(@Param("q") String query, @Param("lut") Date lastUpdate, @Param("ct") Date currentTime);

    @Query("UPDATE " + CASSANDRA_KEYSPACE + "." + PREFIX_TOPK + " SET topK=:t,version=:nv WHERE prefix=:p IF version=:v")
    ResultSet updateTopK(@Param("p") String prefix, @Param("t") Set<SuffixCount> suffixCounts,
                         @Param("v") Long version, @Param("nv") Long newVersion);

    @Query("UPDATE " + CASSANDRA_KEYSPACE + "." + PREFIX_TOPK + " USING TTL :ttl SET topk1 = topk1 - :kr, topk1=topk1 +:ns, version=:nv WHERE prefix=:p IF version=:v")
    ResultSet updateTopK1(@Param("p") String prefix,
                          @Param("kr")Set<String> suffixesToRemove,
                          @Param("ns")Map<String, Long> suffixesToAdd,
                          @Param("v") Long version, @Param("nv") Long newVersion,
                          @Param("ttl") Integer ttlSeconds);

    @Query("UPDATE " + CASSANDRA_KEYSPACE + "." + PREFIX_TOPK + " SET topk2 = topk2 - :kr, topk2=topk2 +:ns, version=:nv WHERE prefix=:p IF version=:v")
    ResultSet updateTopK2(@Param("p") String prefix,
                          @Param("kr")Set<TupleValue> suffixesToRemove,
                          @Param("ns")Set<TupleValue> suffixesToAdd,
                          @Param("v") Long version, @Param("nv") Long newVersion);


}
