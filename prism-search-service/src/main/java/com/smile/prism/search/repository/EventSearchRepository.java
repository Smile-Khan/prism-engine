package com.smile.prism.search.repository;

import com.smile.prism.search.model.EventDocument;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

/**
 * Data Access Object for Elasticsearch.
 * Extends the Spring Data Elasticsearch Repository for high-level Query DSL.
 */
@Repository
public interface EventSearchRepository extends ElasticsearchRepository<EventDocument, UUID> {

    /**
     * Performs a full-text search across the title field.
     * Spring Data Elasticsearch will translate this into a "match" or "contains" query.
     */
    List<EventDocument> findByTitleContainingIgnoreCase(String title);

    /**
     * Filters discovery results by category.
     */
    List<EventDocument> findByCategory(String category);
}