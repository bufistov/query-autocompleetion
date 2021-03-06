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
public class PrefixTopK {
    private Set<SuffixCount> topK; // top K suffixes for given prefix
    private Map<String, Long> topK1;
    private List<SuffixCount> topK2;
    private Long version;
}
