package org.bufistov.model;

import com.datastax.driver.mapping.annotations.UDT;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bufistov.Constants;

import java.util.Objects;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@UDT(keyspace = Constants.CASSANDRA_KEYSPACE, name = Constants.SUFFIX_COUNT_TYPE)
public class SuffixCount implements Comparable<SuffixCount> {
    private String suffix;
    private Long count;

    @Override
    public int compareTo(SuffixCount suffixCount) {
        if (!Objects.equals(count, suffixCount.getCount())) {
            return Long.compare(count, suffixCount.getCount());
        }
        return suffix.compareTo(suffixCount.getSuffix());
    }
}
