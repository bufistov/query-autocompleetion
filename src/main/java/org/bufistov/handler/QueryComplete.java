package org.bufistov.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.bufistov.autocomplete.QueryHandler;
import org.bufistov.model.TopKQueries;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class QueryComplete {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private QueryHandler queryHandler;

    @GetMapping(value = "/queries", produces = {MediaType.APPLICATION_JSON_VALUE})
    public TopKQueries queries(@RequestParam("prefix") String prefix) {
        return TopKQueries.builder().queries(queryHandler.getQueries(prefix)).build();
    }

    @PostMapping(value = "/add_query", consumes = {MediaType.TEXT_PLAIN_VALUE})
    public void addQuery(@RequestBody String query) {
        queryHandler.addQuery(query);
    }
}
