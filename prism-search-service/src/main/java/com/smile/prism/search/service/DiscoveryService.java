package com.smile.prism.search.service;

import com.smile.prism.search.api.DiscoveryRequest;
import com.smile.prism.search.api.SearchResponse;
import com.smile.prism.search.model.EventDocument;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.HighlightQuery;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.data.elasticsearch.core.query.highlight.Highlight;
import org.springframework.data.elasticsearch.core.query.highlight.HighlightField;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class DiscoveryService {

    private final ElasticsearchOperations elasticsearchOperations;

    public SearchResponse discover(DiscoveryRequest request) {
        log.info("Executing Highlighting Discovery for: {}", request.query());

        // Configure highlighting for the 'title' field
        HighlightQuery highlightQuery = new HighlightQuery(
                new Highlight(List.of(new HighlightField("title"))),
                EventDocument.class
        );

        Query query = NativeQuery.builder()
                .withQuery(q -> q
                        .bool(b -> {
                            b.must(m -> m.multiMatch(mm -> mm
                                    .fields("title")
                                    .query(request.query())
                                    .fuzziness("AUTO")));

                            if (request.category() != null && !request.category().isBlank()) {
                                b.filter(f -> f.term(t -> t.field("category").value(request.category())));
                            }
                            return b;
                        })
                )
                .withHighlightQuery(highlightQuery)
                .withPageable(PageRequest.of(request.page(), request.size()))
                .build();

        SearchHits<EventDocument> searchHits = elasticsearchOperations.search(query, EventDocument.class);

        List<SearchResponse.SearchResult> results = searchHits.getSearchHits().stream()
                .map(hit -> new SearchResponse.SearchResult(
                        hit.getContent(),
                        hit.getHighlightFields()
                ))
                .toList();

        return new SearchResponse(searchHits.getTotalHits(), results, "SUCCESS");
    }
}