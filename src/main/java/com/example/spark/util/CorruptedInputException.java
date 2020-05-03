package com.example.spark.util;

public class CorruptedInputException extends RuntimeException{

	private static final long serialVersionUID = -8035137423882794910L;
	
	final String input;
	public CorruptedInputException(String input) {
		this.input = input;
	}
	
	@Override
	public String getMessage() {
		return String.format("Invalid Input::{}", input);
	}

}
