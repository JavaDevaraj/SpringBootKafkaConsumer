package com.kafkaconsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaconsumer.consumer.LibraryEventConsumer;
import com.kafkaconsumer.entity.Book;
import com.kafkaconsumer.entity.LibraryEvent;
import com.kafkaconsumer.entity.LibraryEventType;
import com.kafkaconsumer.jpa.LibraryEventsRepository;
import com.kafkaconsumer.service.LibraryEventsService;

import lombok.extern.slf4j.Slf4j;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics= {"library-events"}, partitions=3)
@TestPropertySource(properties= {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
		"spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
class LibraryEventConsumerApplicationTests {

	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;
	
	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;
	
	@Autowired
	KafkaListenerEndpointRegistry endpointRegistry;
	
	@Autowired
	LibraryEventsRepository repository;
	
	@SpyBean
	LibraryEventConsumer libraryEventConsumer;
	
	@SpyBean
	LibraryEventsService libraryEventsService;
	
	@Autowired
	ObjectMapper objectMapper;
	
	@BeforeEach
	void setUp() {
		for(MessageListenerContainer messageContainer : endpointRegistry.getListenerContainers()) {
			ContainerTestUtils.waitForAssignment(messageContainer, embeddedKafkaBroker.getPartitionsPerTopic());
		}
	}
	
	@AfterEach
	void tearDown() {
		repository.deleteAll();
	}
	
	@Test
	void publishNewLibraryEvent() throws InterruptedException, ExecutionException, JsonMappingException, JsonProcessingException {
		String libraryEventStr = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":505,\"bookName\":\"Spring Kafka\",\"bookAuthor\":\"Kumar Nagaraju\"}}";
		kafkaTemplate.sendDefault(libraryEventStr).get();
		
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);
		
		Mockito.verify(libraryEventConsumer, Mockito.times(1)).onMessage(Mockito.any(ConsumerRecord.class));
		Mockito.verify(libraryEventsService, Mockito.times(1)).processLibraryEvent(Mockito.any(ConsumerRecord.class));
		
		List<LibraryEvent> libraryEventList = (List<LibraryEvent>) repository.findAll();
		Optional<LibraryEvent> libraryEvent = libraryEventList.stream().filter(event -> event.getBook().getBookId().equals(505)).findAny();
		assertTrue(libraryEvent.isPresent());
	}
	
	@Test
	void publishModifyLibraryEvent() throws InterruptedException, ExecutionException, JsonMappingException, JsonProcessingException {
		String libraryEventStr = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":507,\"bookName\":\"Spring Kafka\",\"bookAuthor\":\"Kumar Nagaraju\"}}";
		LibraryEvent libraryEvent = objectMapper.readValue(libraryEventStr, LibraryEvent.class);
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEvent = repository.save(libraryEvent);
		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		Book book = new Book();
		book.setBookId(507);
		book.setBookName("Spring Kafka version 2.0");
		book.setBookAuthor("Kumar Nagaraju");
		libraryEvent.setBook(book);;
		String updateJson = objectMapper.writeValueAsString(libraryEvent);
		
		kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(),updateJson).get();
		
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);
		
		Mockito.verify(libraryEventConsumer, Mockito.times(1)).onMessage(Mockito.any(ConsumerRecord.class));
		Mockito.verify(libraryEventsService, Mockito.times(1)).processLibraryEvent(Mockito.any(ConsumerRecord.class));
		LibraryEvent libraryEventUpdate = repository.findById(libraryEvent.getLibraryEventId()).get();
		assertEquals(libraryEventUpdate.getBook().getBookName(), "Spring Kafka version 2.0");
	}
	
	@Test
	void publishModifyLibraryEvent_Not_A_Valid_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
	    //given
	    Integer libraryEventId = 123;
	    String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
	    System.out.println(json);
	    kafkaTemplate.sendDefault(libraryEventId, json).get();
	    //when
	    CountDownLatch latch = new CountDownLatch(1);
	    latch.await(3, TimeUnit.SECONDS);


	    Mockito.verify(libraryEventConsumer, Mockito.times(1)).onMessage(Mockito.any(ConsumerRecord.class));
	    Mockito.verify(libraryEventsService, Mockito.times(1)).processLibraryEvent(Mockito.any(ConsumerRecord.class));

	    Optional<LibraryEvent> libraryEventOptional = repository.findById(libraryEventId);
	    assertFalse(libraryEventOptional.isPresent());
	}
	
	@Test
	void publishModifyLibraryEvent_Null_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
	    //given
	    Integer libraryEventId = null;
	    String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
	    kafkaTemplate.sendDefault(libraryEventId, json).get();
	    //when
	    CountDownLatch latch = new CountDownLatch(1);
	    latch.await(3, TimeUnit.SECONDS);


	    Mockito.verify(libraryEventConsumer, Mockito.times(1)).onMessage(Mockito.any(ConsumerRecord.class));
	    Mockito.verify(libraryEventsService, Mockito.times(1)).processLibraryEvent(Mockito.any(ConsumerRecord.class));
	}
	
	@Test
	void publishModifyLibraryEvent_Z000_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
	    //given
	    Integer libraryEventId = 000;
	    String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
	    kafkaTemplate.sendDefault(libraryEventId, json).get();
	    //when
	    CountDownLatch latch = new CountDownLatch(1);
	    latch.await(3, TimeUnit.SECONDS);


	    Mockito.verify(libraryEventConsumer, Mockito.times(4)).onMessage(Mockito.any(ConsumerRecord.class));
	    Mockito.verify(libraryEventsService, Mockito.times(4)).processLibraryEvent(Mockito.any(ConsumerRecord.class));
	    Mockito.verify(libraryEventsService, Mockito.times(1)).handleRecovery(Mockito.any(ConsumerRecord.class));
	}

}
