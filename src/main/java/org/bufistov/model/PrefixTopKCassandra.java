package org.bufistov.model;

import com.datastax.driver.core.TupleValue;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bufistov.Constants;

import java.util.Map;
import java.util.Set;

import static org.bufistov.Constants.PREFIX_TOPK;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(keyspace = Constants.CASSANDRA_KEYSPACE, name = PREFIX_TOPK,
        readConsistency = "ONE",
        writeConsistency = "ANY")
public class PrefixTopKCassandra {
    @PartitionKey
    private String prefix;
    private Set<SuffixCount> topK; // top K suffixes for given prefix
    private Map<String, Long> topK1; // top K suffixes for given prefix
    private Set<TupleValue> topK2;//
    private Long version;
}
