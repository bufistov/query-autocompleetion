package org.bufistov.model;

import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import static org.bufistov.Constants.CASSANDRA_KEYSPACE;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(keyspace = CASSANDRA_KEYSPACE, name = "query_count",
       readConsistency = "ONE",
       writeConsistency = "ANY")
public class QueryCountCassandra {
    @PartitionKey
    private String query;
    // Actual type is counter
    private Long count;
}
