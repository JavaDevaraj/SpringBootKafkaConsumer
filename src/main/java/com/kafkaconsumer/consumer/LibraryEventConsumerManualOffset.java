package com.kafkaconsumer.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkaconsumer.service.LibraryEventsService;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class LibraryEventConsumerManualOffset implements AcknowledgingConsumerAwareMessageListener<Integer, String>{

	@Autowired
	LibraryEventsService libraryEventsService;
	/* Manaully aknowledging message to broker. */
	
	@Override
	@KafkaListener(topics= {"library-events"})
	public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment,
			Consumer<?, ?> consumer) {
		log.info(" Manaully aknowledging Consume Message : {}", consumerRecord);
		try {
			libraryEventsService.processLibraryEvent(consumerRecord);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		acknowledgment.acknowledge();
	}
}
