package com.smile.prism.command.api;

import com.smile.prism.command.domain.Event;
import com.smile.prism.command.service.EventService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

/**
 * Entry point for the Command side of the Prism engine.
 */
@RestController
@RequestMapping("/api/v1/events")
@RequiredArgsConstructor
public class EventController {

    private final EventService eventService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Event create(@RequestBody EventRequest request) {
        return eventService.createEvent(request);
    }
}