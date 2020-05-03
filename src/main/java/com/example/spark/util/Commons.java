package com.example.spark.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.spark.model.Record;

public class Commons {
	
	private static final String HEADER = "TRANSACTION_ID,CUSTOMER_ID,TIME,PRODUCT_ID,COST";

	private static final int TRANSACTION_ID_INDEX = 0;
	private static final int CUSTOMER_ID_INDEX = 1;
	private static final int TIME_INDEX = 2;
	private static final int PRODUCT_ID_INDEX = 3;
	private static final int COST_INDEX = 4;
	private static final String DELIMETER=",";

	private static final Logger log = LoggerFactory.getLogger(Commons.class);

	public static Record parseLine(String line) throws CorruptedInputException {
		log.debug("Parsing input::{}", line);
		if (isHeader(line)) {
			return null;
		}
		String[] fields = line.split(DELIMETER);
		if (fields.length != 5) {
			log.warn("Invaalid input::{}", line);
			throw new CorruptedInputException(line);
		}
		return new Record(fields[TRANSACTION_ID_INDEX], fields[CUSTOMER_ID_INDEX], fields[TIME_INDEX],
				fields[PRODUCT_ID_INDEX], Double.parseDouble(fields[COST_INDEX]));
	}

	public static boolean isHeader(String line) {
		return line.equals(HEADER);
	}
}
