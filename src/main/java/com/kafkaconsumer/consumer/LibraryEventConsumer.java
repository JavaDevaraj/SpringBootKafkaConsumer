package com.kafkaconsumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkaconsumer.service.LibraryEventsService;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventConsumer {
	
	@Autowired
	LibraryEventsService libraryEventsService;

	/* Automatic aknowledging message to broker.*/
	@KafkaListener(topics= {"library-events"})
	public void onMessage(ConsumerRecord<Integer, String> consumerRecord) {
		log.info("Consume Message : {}", consumerRecord);
		try {
			libraryEventsService.processLibraryEvent(consumerRecord);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
