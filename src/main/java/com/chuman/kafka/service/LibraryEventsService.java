package com.chuman.kafka.service;

import com.chuman.kafka.Entity.LibraryEvent;
import com.chuman.kafka.repository.LibraryEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventsService {

    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    private LibraryEventRepository libraryEventRepository;

    public void processLibraryEvent(ConsumerRecord<Integer,String> consumerRecord) throws JsonProcessingException {
       LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("libraryEvent : {} ",libraryEvent);

        switch (libraryEvent.getLibraryEventType()){
            case NEW:
                //Save operation
                save(libraryEvent);
                break;
            case UPDATE:
                //Update Operation
                validate(libraryEvent);
                save(libraryEvent);
                break;
            default:
                log.info("Invalid Library Event Type");
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if(libraryEvent.getLibraryEventId() == null){
            throw new IllegalArgumentException("library Event Id missing");
        }
       Optional<LibraryEvent> optionalLibraryEvent = libraryEventRepository.
               findById(libraryEvent.getLibraryEventId());
        if (optionalLibraryEvent.isEmpty()){
            throw  new IllegalArgumentException("Not a valid library Event");
        }
        log.info("Validation is Successful for the library Event : {}", optionalLibraryEvent.get());
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventRepository.save(libraryEvent);
        log.info("Successfully Persisted the library Event {}", libraryEvent);
    }
}
