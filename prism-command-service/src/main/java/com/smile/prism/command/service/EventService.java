package com.smile.prism.command.service;

import com.smile.prism.command.api.EventRequest;
import com.smile.prism.command.domain.Event;
import com.smile.prism.command.repository.EventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Service layer for managing Prism events.
 * Logic is optimized for execution within Virtual Threads.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class EventService {

    private final EventRepository eventRepository;

    @Transactional
    public Event createEvent(EventRequest request) {
        log.info("Creating new system event: {}", request.title());

        Event event = Event.builder()
                .title(request.title())
                .category(request.category())
                .metadata(request.metadata())
                .status(Event.EventStatus.RECEIVED)
                .build();

        return eventRepository.save(event);
    }
}