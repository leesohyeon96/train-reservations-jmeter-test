package com.shl.trainreservationsjmetertest.step2.config;

import com.shl.trainreservationsjmetertest.step2.service.RedisService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Configuration;
@Configuration
@RequiredArgsConstructor
public class RedisInitRunner implements CommandLineRunner {
    private final RedisService redisService;

    // Spring Boot 애플리케이션이 모든 Bean을 다 로드하고
    // 서버가 완전히 뜬 직후 실행되는 코드를 작성할 수 있게 해주는 interface
    // [요약] 서버 켜지자마자 딱 1번 실행되는 초기화 위치
    @Override
    public void run(String... args) {
        // 1 ~ 1000까지 좌석 1개씩 초기화
        redisService.initializedStockBatch(1, 1000, 1);
        System.out.println("✅ Redis 좌석 재고 초기화 완료 (seat 1~100, stock=1)");
    }
}
