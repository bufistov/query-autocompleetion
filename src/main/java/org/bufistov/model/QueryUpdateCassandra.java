package org.bufistov.model;

import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Date;

import static org.bufistov.Constants.CASSANDRA_KEYSPACE;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(keyspace = CASSANDRA_KEYSPACE, name = "query_update",
       readConsistency = "ONE",
       writeConsistency = "ANY")
public class QueryUpdateCassandra {
    @PartitionKey
    private String query;
    private Date topkUpdate;
}
