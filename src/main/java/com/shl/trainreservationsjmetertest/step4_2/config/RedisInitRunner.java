package com.shl.trainreservationsjmetertest.step4_2.config;

import com.shl.trainreservationsjmetertest.step4_2.service.RedisService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Configuration;

/**
 * Redis 초기화 (Step4-2)
 * 
 * [변경사항]
 * - Step3와 동일: Redis는 재고 관리용으로만 사용
 * - Queue는 Kafka 사용
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class RedisInitRunner implements CommandLineRunner {
    private final RedisService redisService;

    @Override
    public void run(String... args) {
        long startTime = System.currentTimeMillis();
        redisService.initializedStockBatch(1, 1000, 1);
        long elapsedTime = System.currentTimeMillis() - startTime;
        log.info("✅ Redis 좌석 재고 초기화 완료 (seat 1~1000, stock=1) - 소요 시간: {}ms", elapsedTime);
    }
}




