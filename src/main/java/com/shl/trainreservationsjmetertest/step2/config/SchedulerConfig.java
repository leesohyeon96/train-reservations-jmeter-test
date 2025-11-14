package com.shl.trainreservationsjmetertest.step2.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
@EnableScheduling
public class SchedulerConfig {
    // @Scheduler 쓸때 Spring 이 내부적으로 이 쓰레드 풀 사용함
    @Bean
    public ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(4); // 동시에 4개 스케줄 작업 가능
        scheduler.setThreadNamePrefix("reservation-worker-");
        scheduler.initialize();
        return scheduler;
    }
}
