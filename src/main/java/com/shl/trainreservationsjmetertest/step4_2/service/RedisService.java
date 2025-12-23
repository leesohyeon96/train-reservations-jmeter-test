package com.shl.trainreservationsjmetertest.step4_2.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.Arrays;

/**
 * Redis 서비스 (Step4-2)
 * 
 * [변경사항]
 * - Queue 관련 메서드 제거 (Kafka 사용)
 * - 재고 관리만 담당 (Lua 스크립트)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RedisService {
    private final RedisTemplate<String, String> redisTemplate;
    private DefaultRedisScript<Long> reservationScript;

    @PostConstruct
    public void init() {
        // Lua 스크립트로 원자적 연산 보장
        // 재고 확인/감소만 수행 (Queue 추가는 Kafka에서 처리)
        String luaScript = 
            "local stock = tonumber(redis.call('get', KEYS[1])) or 0 " +
            "if stock > 0 then " +
            "  redis.call('decr', KEYS[1]) " +
            "  return 1 " +
            "else " +
            "  return 0 " +
            "end";

        reservationScript = new DefaultRedisScript<>();
        reservationScript.setScriptText(luaScript);
        reservationScript.setResultType(Long.class);
        log.info("✅ Redis Lua 스크립트 초기화 완료 (Step4-2: Kafka 사용)");
    }

    // 좌석 재고 초기화
    public void initializedStockBatch(long start, long end, int stock) {
        long startTime = System.currentTimeMillis();
        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (long i = start; i <= end; i++) {
                byte[] key = redisTemplate.getStringSerializer().serialize("seat:" + i + ":stock");
                byte[] val = redisTemplate.getStringSerializer().serialize(String.valueOf(stock));
                connection.stringCommands().set(key, val);
            }
            return null;
        });
        long elapsedTime = System.currentTimeMillis() - startTime;
        log.debug("재고 초기화 완료: {} ~ {} ({}건) - 소요 시간: {}ms", start, end, (end - start + 1), elapsedTime);
    }

    // 예약 요청 (재고 확인 및 감소만 수행)
    public boolean tryReservationSeat(Long seatId) {
        String stockKey = "seat:" + seatId + ":stock";
        
        long startTime = System.currentTimeMillis();
        Long result = redisTemplate.execute(
            reservationScript,
            Arrays.asList(stockKey),
            ""
        );
        long elapsedTime = System.currentTimeMillis() - startTime;
        
        boolean success = result != null && result == 1;
        if (elapsedTime > 10) {
            log.warn("예약 처리 지연: seatId={}, 성공={}, 소요 시간={}ms", seatId, success, elapsedTime);
        }
        
        return success;
    }

    // 좌석 재고 확인 (캐싱 대상)
    public Long getSeatStock(Long seatId) {
        String stockStr = redisTemplate.opsForValue().get("seat:" + seatId + ":stock");
        return stockStr != null ? Long.parseLong(stockStr) : null;
    }
}




