package com.kafkaconsumer.config;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import com.kafkaconsumer.service.LibraryEventsService;

import ch.qos.logback.core.Context;
import javassist.expr.Instanceof;
import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {
	
	@Autowired
	LibraryEventsService libraryEventsService;

	@Bean
	@ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
	ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
			ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
			ConsumerFactory<Object, Object> kafkaConsumerFactory) {
		ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		configurer.configure(factory, kafkaConsumerFactory);
		 //Number of Thread = Number of Partition always
		factory.setConcurrency(3);
		//Handle custom exception.
		factory.setErrorHandler((thrownException, data)-> {
			log.info("Exception display from config :{} , record : {}", thrownException.getMessage(), data.value());
		});
		//Handle Manually exception.
		factory.getContainerProperties().setAckMode(AckMode.MANUAL);
		
		//Handle no. of times Retry operation
		factory.setRetryTemplate(retryTemplate());
		
		//Handle Recovery 
		factory.setRecoveryCallback((context ->{
			if(context.getLastThrowable().getCause() instanceof RecoverableDataAccessException) {
				log.info("Inside recoverable exception block");
				Arrays.asList(context.attributeNames()).
				forEach(attributeName ->{
					log.info(" Attribute Name is : {}, Value is : {}",attributeName, context.getAttribute(attributeName));
				});
				ConsumerRecord<Integer, String> consumerRecord = (ConsumerRecord<Integer, String>) context.getAttribute("record");
				libraryEventsService.handleRecovery(consumerRecord);
				
			} else {
				log.info("Inside non recoverable exception block");
				throw new RuntimeException(context.getLastThrowable().getMessage());
			}
			
			return null;
		}));
		return factory;
	}



	private RetryTemplate retryTemplate() {
		
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);//It will be in millisecond
		RetryTemplate retryTemplate = new RetryTemplate();
		retryTemplate.setRetryPolicy(retryPolicy());
		retryTemplate.setBackOffPolicy(backOffPolicy);
		return retryTemplate;
	}

	private RetryPolicy retryPolicy() {
		/*SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
		retryPolicy.setMaxAttempts(3);*/
		
		//For generating Specific Exception retry policy
		HashMap<Class<? extends Throwable>, Boolean> exectionMap = new HashMap<>();
		exectionMap.put(RecoverableDataAccessException.class, true);
		exectionMap.put(IllegalArgumentException.class, false);
		SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(3,exectionMap, true );
		return retryPolicy;
	}
}
