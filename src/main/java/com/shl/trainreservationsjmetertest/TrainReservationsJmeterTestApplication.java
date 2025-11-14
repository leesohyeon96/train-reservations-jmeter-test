package com.shl.trainreservationsjmetertest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

//@ComponentScan(basePackages = "com.shl.trainreservationsjmetertest.step1")
@ComponentScan(basePackages = "com.shl.trainreservationsjmetertest.step2")
//@ComponentScan(basePackages = "com.shl.trainreservationsjmetertest.step3")
@SpringBootApplication
public class TrainReservationsJmeterTestApplication {

    public static void main(String[] args) {
        SpringApplication.run(TrainReservationsJmeterTestApplication.class, args);
    }

}
