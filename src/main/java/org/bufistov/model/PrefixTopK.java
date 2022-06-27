package org.bufistov.model;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import lombok.Builder;
import lombok.Data;
import org.bufistov.Constants;

import java.util.Set;

import static org.bufistov.Constants.PREFIX_TOPK;

@Data
@Builder
@Table(keyspace = Constants.CASSANDRA_KEYSPACE, name = PREFIX_TOPK,
        readConsistency = "ANY",
        writeConsistency = "ANY")
public class PrefixTopK {
    @PartitionKey
    private String prefix;
    private Set<CompletionCount> topK; // top K suffixes for given prefix
}
