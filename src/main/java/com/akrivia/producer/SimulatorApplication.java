package com.akrivia.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

@SpringBootApplication
public class SimulatorApplication {
    
    public static void main(String[] args) {
        SpringApplication.run(SimulatorApplication.class, args);
    }
    
    @Bean
    CommandLineRunner loadData(KafkaTemplate<String, String> kafkaTemplate) {
        return args -> {
            ObjectMapper objectMapper = new ObjectMapper();
            
            InputStream inputStream =
                getClass().getClassLoader().getResourceAsStream("patient_data.json");
            
            if (inputStream == null) {
                System.out.println("File not found!");
                return;
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            
            while ((line = reader.readLine()) != null) {
                kafkaTemplate.send("patients-topic", line);
            }
            
            System.out.println("Data successfully loaded into Kafka topic!");
        };
    }
}
