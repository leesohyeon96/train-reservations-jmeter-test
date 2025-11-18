package com.shl.trainreservationsjmetertest.step3.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * 스케줄러 설정 (Step3: 성능 최적화)
 * 
 * [개선 사항]
 * - Step2: 4개 스레드 → Step3: 8개 스레드로 증가
 * - 더 많은 병렬 처리로 큐 처리 속도 향상
 */
@Configuration
@EnableScheduling
public class SchedulerConfig {
    @Bean
    public ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(8); // Step2: 4 → Step3: 8로 증가 (병렬 처리 강화)
        scheduler.setThreadNamePrefix("reservation-worker-step3-");
        scheduler.setWaitForTasksToCompleteOnShutdown(true); // 종료 시 대기
        scheduler.setAwaitTerminationSeconds(60); // 최대 60초 대기
        scheduler.initialize();
        return scheduler;
    }
}


