package com.shl.trainreservationsjmetertest.step3.config;

import com.shl.trainreservationsjmetertest.step3.service.RedisService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class RedisInitRunner implements CommandLineRunner {
    private final RedisService redisService;

    @Override
    public void run(String... args) {
        // 1 ~ 1000까지 좌석 1개씩 초기화
        long startTime = System.currentTimeMillis();
        redisService.initializedStockBatch(1, 1000, 1);
        long elapsedTime = System.currentTimeMillis() - startTime;
        log.info("✅ Redis 좌석 재고 초기화 완료 (seat 1~1000, stock=1) - 소요 시간: {}ms", elapsedTime);
    }
}


