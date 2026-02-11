package com.smile.prism.search.repository;

import com.smile.prism.search.model.EventDocument;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface EventSearchRepository extends ElasticsearchRepository<EventDocument, UUID> {
}