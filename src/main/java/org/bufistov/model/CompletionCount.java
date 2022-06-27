package org.bufistov.model;

import com.datastax.driver.mapping.annotations.UDT;
import lombok.Data;
import org.bufistov.Constants;

@Data
@UDT(keyspace = Constants.CASSANDRA_KEYSPACE, name = "PrefixCount")
public class CompletionCount {
    private String suffix;
    private Long count;
}
