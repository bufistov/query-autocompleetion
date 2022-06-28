package org.bufistov.model;

import com.datastax.driver.mapping.annotations.UDT;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bufistov.Constants;

import java.util.Comparator;
import java.util.Objects;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@UDT(keyspace = Constants.CASSANDRA_KEYSPACE, name = "PrefixCount")
public class CompletionCount implements Comparable<CompletionCount> {
    private String suffix;
    private Long count;

    @Override
    public int compareTo(CompletionCount completionCount) {
        if (!Objects.equals(count, completionCount.getCount())) {
            return Long.compare(count, completionCount.getCount());
        }
        return suffix.compareTo(completionCount.getSuffix());
    }
}
