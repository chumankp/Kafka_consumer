package com.chuman.kafka.Entity;

import lombok.*;

import javax.persistence.*;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
public class LibraryEvent {
    @Id
    @GeneratedValue
    private Integer libraryEventId;
    @Enumerated(EnumType.STRING)
    private LibraryEventType libraryEventType;
    @OneToOne(mappedBy = "libraryEvent", cascade = {CascadeType.ALL})
    @ToString.Exclude
    private Book book;
}
