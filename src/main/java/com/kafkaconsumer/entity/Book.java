package com.kafkaconsumer.entity;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
public class Book {

	@Id
	private Integer bookId;
	private String bookName;
	private String bookAuthor;
	@OneToOne
	@JoinColumn(name="libraryEventId")
	private LibraryEvent libraryEvent;
}
