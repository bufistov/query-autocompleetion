package org.bufistov.autocomplete;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Value;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryHandlerConfig {
    @Value("${org.bufistov.autocomplete.K}")
    private Long topK;

    @Value("${org.bufistov.autocomplete.max_retries_to_update_topk}")
    private Long maxRetriesToUpdateTopK;

    @Value("${org.bufistov.autocomplete.max_query_size}")
    private Integer maxQuerySize;

    @Value("${org.bufistov.autocomplete.query_update_millis}")
    private Long queryUpdateMillis;

    @Value("${org.bufistov.autocomplete.query_update_count}")
    private Long queryUpdateCount;
}
