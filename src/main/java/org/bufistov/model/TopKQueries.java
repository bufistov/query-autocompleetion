package org.bufistov.model;

import lombok.*;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopKQueries {
    private List<String> queries;
}
