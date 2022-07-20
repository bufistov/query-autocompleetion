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
    private Long topK;

    private Long maxRetriesToUpdateTopK;

    private Integer maxQuerySize;

    private Long queryUpdateMillis;

    private Long queryUpdateCount;
}
