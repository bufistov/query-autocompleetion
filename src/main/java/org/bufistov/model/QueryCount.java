package org.bufistov.model;

import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

import static org.bufistov.Constants.CASSANDRA_KEYSPACE;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryCount {
    private String query;
    private Long count;
    private Long sinceLastUpdate;
    private Instant lastUpdateTime;
}
