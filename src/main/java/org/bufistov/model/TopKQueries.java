package org.bufistov.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopKQueries {
    private Set<SuffixCount> queries;
    private Map<String, Long> queries1;
    private List<SuffixCount> queries2;
}
