package com.shl.trainreservationsjmetertest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Step별 실행 방법:
 * 
 * Step1: 
 *   - scanBasePackages = "com.shl.trainreservationsjmetertest.step1"
 *   - entityScan = "com.shl.trainreservationsjmetertest.step1.domain"
 *   - basePackages = "com.shl.trainreservationsjmetertest.step1.repository"
 * 
 * Step2:
 *   - scanBasePackages = "com.shl.trainreservationsjmetertest.step2"
 *   - entityScan = "com.shl.trainreservationsjmetertest.step2.entity"
 *   - basePackages = "com.shl.trainreservationsjmetertest.step2.repository"
 * 
 * Step3:
 *   - scanBasePackages = "com.shl.trainreservationsjmetertest.step3"
 *   - entityScan = "com.shl.trainreservationsjmetertest.step3.entity"
 *   - basePackages = "com.shl.trainreservationsjmetertest.step3.repository"
 */
@SpringBootApplication(scanBasePackages = "com.shl.trainreservationsjmetertest.step3")
@EntityScan(basePackages = "com.shl.trainreservationsjmetertest.step3.entity")
@EnableJpaRepositories(basePackages = "com.shl.trainreservationsjmetertest.step3.repository")
public class TrainReservationsJmeterTestApplication {

    public static void main(String[] args) {
        SpringApplication.run(TrainReservationsJmeterTestApplication.class, args);
    }

}
