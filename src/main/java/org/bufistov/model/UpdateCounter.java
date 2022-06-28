package org.bufistov.model;

import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;

import static org.bufistov.Constants.CASSANDRA_KEYSPACE;
import static org.bufistov.Constants.QUERY_COUNT;

@Accessor
public interface UpdateCounter {

    @Query("UPDATE " + CASSANDRA_KEYSPACE + "." + QUERY_COUNT + " SET count = count + :d WHERE query=:q")
    void incrementCounter(@Param("d") long increment, @Param("q") String query);
}
