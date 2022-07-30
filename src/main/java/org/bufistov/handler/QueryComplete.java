package org.bufistov.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import org.bufistov.autocomplete.QueryHandler;
import org.bufistov.model.TopKQueries;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@AllArgsConstructor
@RestController
public class QueryComplete {

    @Autowired
    private QueryHandler queryHandler;

    @GetMapping(value = "/queries", produces = {MediaType.APPLICATION_JSON_VALUE})
    @CrossOrigin(originPatterns = {"*"})
    public TopKQueries queries(@RequestParam("prefix") String prefix) {
        return queryHandler.getQueries(prefix);
    }

    @PostMapping(value = "/add_query", consumes = {MediaType.TEXT_PLAIN_VALUE})
    @CrossOrigin(originPatterns = {"*"})
    public void addQuery(@RequestBody String query) {
        queryHandler.addQuery(query);
    }
}
