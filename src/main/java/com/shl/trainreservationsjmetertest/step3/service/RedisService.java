package com.shl.trainreservationsjmetertest.step3.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.Arrays;

/**
 * Redis 서비스 (Step3: 성능 최적화)
 * 
 * [개선 사항]
 * - Connection Pool 최적화 (RedisClusterConfig에서 설정)
 * - Lua 스크립트 최적화 (동일하게 유지 - 이미 최적화됨)
 * - 모니터링 강화 (로깅 추가)
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
        String luaScript = 
            "local stock = tonumber(redis.call('get', KEYS[1])) or 0 " +
            "if stock > 0 then " +
            "  redis.call('decr', KEYS[1]) " +
            "  redis.call('rpush', KEYS[2], ARGV[1]) " +
            "  return 1 " +
            "else " +
            "  return 0 " +
            "end";

        reservationScript = new DefaultRedisScript<>();
        reservationScript.setScriptText(luaScript);
        reservationScript.setResultType(Long.class);
        log.info("✅ Redis Lua 스크립트 초기화 완료");
    }

    // 좌석 재고 초기화 (Pipeline 사용으로 성능 최적화)
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

    // 예약 요청 (Lua 스크립트로 원자적 연산)
    public boolean tryReservationSeat(Long seatId) {
        String stockKey = "seat:" + seatId + ":stock";
        String queueKey = "reservation_queue";
        
        long startTime = System.currentTimeMillis();
        Long result = redisTemplate.execute(
            reservationScript,
            Arrays.asList(stockKey, queueKey),
            seatId.toString()
        );
        long elapsedTime = System.currentTimeMillis() - startTime;
        
        boolean success = result != null && result == 1;
        if (elapsedTime > 10) { // 10ms 이상 걸리면 경고 로그
            log.warn("예약 처리 지연: seatId={}, 성공={}, 소요 시간={}ms", seatId, success, elapsedTime);
        }
        
        return success;
    }

    // 큐에서 꺼내기
    public String popQueue() {
        return redisTemplate.opsForList().leftPop("reservation_queue");
    }

    // 큐에 다시 넣기 (예외 발생 시 재시도용)
    public void pushQueue(String seatId) {
        redisTemplate.opsForList().rightPush("reservation_queue", seatId);
    }

    // 큐 길이 확인 (모니터링용)
    public Long getQueueLength() {
        return redisTemplate.opsForList().size("reservation_queue");
    }

    // 좌석 재고 확인 (캐싱 대상)
    public Long getSeatStock(Long seatId) {
        String stockStr = redisTemplate.opsForValue().get("seat:" + seatId + ":stock");
        return stockStr != null ? Long.parseLong(stockStr) : null;
    }
}


