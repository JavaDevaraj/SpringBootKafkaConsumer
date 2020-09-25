package com.kafkaconsumer.jpa;

import org.springframework.data.repository.CrudRepository;

import com.kafkaconsumer.entity.LibraryEvent;

public interface LibraryEventsRepository extends CrudRepository<LibraryEvent, Integer>{
	default <S extends LibraryEvent> S update(S entity) {
		return entity;
	}

}
