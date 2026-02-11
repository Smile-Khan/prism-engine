package com.smile.prism.command.domain;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a command event in the system.
 * Metadata is stored as JSONB for flexible discovery.
 */
@Entity
@Table(name = "events", indexes = {
        @Index(name = "idx_event_category", columnList = "category")
})
@Getter @Setter @Builder
@NoArgsConstructor @AllArgsConstructor
public class Event extends BaseEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false)
    private String title;

    @Column(nullable = false)
    private String category;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private Map<String, Object> metadata;

    @Enumerated(EnumType.STRING)
    private EventStatus status;

    public enum EventStatus { RECEIVED, PROCESSED, FAILED }
}