package com.kafkaconsumer.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.retry.RetryException;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaconsumer.entity.LibraryEvent;
import com.kafkaconsumer.jpa.LibraryEventsRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LibraryEventsService {
	
	@Autowired
	ObjectMapper objectMapper;
	
	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;
	
	@Autowired
	private LibraryEventsRepository repository;
	
	public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonMappingException, JsonProcessingException {
		LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
		log.info("LibraryEvent is {}", libraryEvent);
		
		//For generating Specific Exception retry policy
		if(libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 000) {
			throw new RecoverableDataAccessException("Temporary Network Issue");
		}
		
		switch (libraryEvent.getLibraryEventType()) {
		case NEW:
			saveLibraryEvent(libraryEvent);
			break;
		case UPDATE:
			updateLibraryEvent(libraryEvent);
			break;
		default:
			log.info("Invalid Library Event Type");
		}
	}

	private void saveLibraryEvent(LibraryEvent libraryEvent) {
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		repository.save(libraryEvent);
		log.info("Successfully Persisted the library event : {}",libraryEvent);
	}

	
	private void updateLibraryEvent(LibraryEvent libraryEvent) {
		if(libraryEvent.getLibraryEventId() == null )
			throw new IllegalArgumentException("No Valid Library Event Id");
		if(repository.existsById(libraryEvent.getLibraryEventId()))
		{
			libraryEvent.getBook().setLibraryEvent(libraryEvent);
			repository.save(libraryEvent);
			log.info("Successfully Updated the library event : {}",libraryEvent);
			return;
		}
		log.info("Record not found");
		throw new IllegalArgumentException("No Valid Library Event from data source");
	}
	
	public void handleRecovery(ConsumerRecord<Integer, String> consumerRecord) {
		Integer key = consumerRecord.key();
		String data = consumerRecord.value();
				
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, data);
		listenableFutureCallback(key, data, listenableFuture);
		
	}
	
	private void listenableFutureCallback(Integer key, String value, ListenableFuture<SendResult<Integer, String>> listenableFuture) {
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				// TODO Auto-generated method stub
				log.info("Message Sent SuccessFully for the key : {} and the value is {} , partition is {}", key, value, result.getRecordMetadata().partition());
			}

			@Override
			public void onFailure(Throwable ex) {
				log.error("Error while sending the message & exception is {}", ex.getMessage());	
			}
		
		});
	}
}
