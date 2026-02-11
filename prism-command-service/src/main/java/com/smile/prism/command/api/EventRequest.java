package com.smile.prism.command.api;

import java.util.Map;

/**
 * Immutable Data Transfer Object (DTO) for incoming Event commands.
 * Using Java 21 Record for boilerplate-free data handling.
 */
public record EventRequest(
        String title,
        String category,
        Map<String, Object> metadata
) {}