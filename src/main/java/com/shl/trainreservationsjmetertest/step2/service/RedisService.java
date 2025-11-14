package com.shl.trainreservationsjmetertest.step2.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.Arrays;

@Service
@RequiredArgsConstructor
public class RedisService {
    private final RedisTemplate<String, String> redisTemplate;
    private DefaultRedisScript<Long> reservationScript;

    @PostConstruct
    public void init() {
        // Lua 스크립트로 원자적 연산 보장 (재고확인/감소를 '원자적'으로 처리하도록 변경)
        // 재고가 0보다 크면 decrement(-1)하고 1 반환, 아니면 0 반환
        String luaScript = 
            "local stock = tonumber(redis.call('get', KEYS[1])) or 0 " + // keys[1] : 재고 키 ex) "seat:stock"
            "if stock > 0 then " +
            "  redis.call('decr', KEYS[1]) " +
            "  redis.call('rpush', KEYS[2], ARGV[1]) " + // Keys[2] : 큐 키 ex) reservation_queue, Argv[1] : 이번 요청에서 큐에 넣을 값 ex) seatId.toString()
            "  return 1 " +
            "else " +
            "  return 0 " +
            "end";

        // 네트워크 왕복 1번 + 서버 내부 원자 실행 -> 동시성/안전성 우수함
        reservationScript = new DefaultRedisScript<>();
        reservationScript.setScriptText(luaScript);
        reservationScript.setResultType(Long.class);
    }

    // 좌석 재고 초기화
    public void initializedStockBatch(long start, long end, int stock) {
        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (long i = start; i <= end; i++) {
                byte[] key = redisTemplate.getStringSerializer().serialize("seat:" + i + ":stock");
                byte[] val = redisTemplate.getStringSerializer().serialize(String.valueOf(stock));
                connection.stringCommands().set(key, val);
            }
            return null;
        });
    }

    // 예약 요청
    // Lua 스크립트를 사용하여 재고 확인과 감소를 원자적으로 처리
    // 동시에 여러 요청이 와도 정확히 1개만 성공하도록 보장
    public boolean tryReservationSeat(Long seatId) {
        String stockKey = "seat:" + seatId + ":stock";
        String queueKey = "reservation_queue";
        
        Long result = redisTemplate.execute(
            reservationScript,
            Arrays.asList(stockKey, queueKey),
            seatId.toString()
        );
        
        return result != null && result == 1;
    }

    // 큐에서 꺼내기
    // - 예약 요청이 Queue에 쌓이면 백그라운드 스레드가 1개씩 꺼내서 DB에 저장함!
    // - 꺼내는 타이밍은 2가지
    // 1) 즉시 처리 - 요청이 들어오면 worker가 바로 pop해서 DB저장 > 단점은 요청 몰리면 DB 부하
    // 2) 배치 처리 - 일정 개수(ex. 10개) or 일정 시간(ex. 1초)마다 pop해서 DB저장! > 단점은 요청 처리 지연 (1~2초 정도)
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
}
