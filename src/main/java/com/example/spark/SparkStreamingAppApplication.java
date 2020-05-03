package com.example.spark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class SparkStreamingAppApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(SparkStreamingAppApplication.class, args);
		SparkConsumer sparkConsumer = context.getBean(SparkConsumer.class);
		try {
			sparkConsumer.startSparkStreaming();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
