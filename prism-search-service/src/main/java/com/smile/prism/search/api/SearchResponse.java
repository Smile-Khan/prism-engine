package com.smile.prism.search.api;

import com.smile.prism.search.model.EventDocument;
import java.util.List;
import java.util.Map;

/**
 * Standardized Search Response for the Prism Discovery Engine.
 *
 * @param totalHits The total number of documents matching the query in Elasticsearch.
 * @param results The specific page of results.
 * @param status Status of the search operation (e.g., SUCCESS, PARTIAL_RESULTS).
 */
public record SearchResponse(
        long totalHits,
        List<SearchResult> results,
        String status
) {
    public record SearchResult(
            EventDocument document,
            Map<String, List<String>> highlights
    ) {}
}