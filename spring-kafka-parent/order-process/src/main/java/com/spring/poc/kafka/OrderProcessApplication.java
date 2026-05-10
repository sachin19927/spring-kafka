package com.spring.poc.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class OrderProcessApplication {

	public static void main(String[] args) {
		SpringApplication.run(OrderProcessApplication.class, args);
	}

}
