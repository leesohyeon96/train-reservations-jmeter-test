package com.shl.trainreservationsjmetertest.step4_1.service;

import com.shl.trainreservationsjmetertest.step4_1.config.RabbitMQConfig;
import com.shl.trainreservationsjmetertest.step4_1.entity.Reservation;
import com.shl.trainreservationsjmetertest.step4_1.repository.ReservationRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * 예약 서비스 (Step4-1: RabbitMQ 사용)
 * 
 * [변경사항]
 * - Redis Queue → RabbitMQ Queue로 교체
 * - @RabbitListener로 메시지 수신
 * - 배치 처리: 여러 메시지를 모아서 한 번에 저장
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ReservationService {
    private final RedisService redisService;
    private final RabbitMQService rabbitMQService;
    private final ReservationRepository reservationRepository;

    private static final int BATCH_SIZE = 500;
    private final List<String> messageBuffer = new ArrayList<>();
    private long lastBatchTime = System.currentTimeMillis();
    private static final long BATCH_TIMEOUT_MS = 200; // 200ms마다 배치 처리

    /**
     * 예약 요청
     * 
     * [흐름]
     * 1. Redis에서 재고 확인 및 감소 (Lua 스크립트)
     * 2. 성공 시 RabbitMQ에 메시지 발행
     * 3. Consumer가 메시지를 받아서 DB 저장
     */
    public String reserve(Long seatId) {
        boolean success = redisService.tryReservationSeat(seatId);
        if (success) {
            // RabbitMQ에 메시지 발행
            rabbitMQService.publishReservation(seatId);
            evictReservationCache(seatId);
        }
        return success ? "예약성공!" : "예약 실패ㅜ 좌석 없엉";
    }

    // 예약 내역 조회 (캐싱 적용)
    @Cacheable(value = "reservationHistory", key = "#seatId")
    public List<Reservation> getReservationHistory(Long seatId) {
        log.debug("DB에서 예약 내역 조회: seatId={}", seatId);
        return reservationRepository.findBySeatId(seatId);
    }

    // 좌석 재고 조회 (캐싱 적용)
    @Cacheable(value = "seatInfo", key = "#seatId")
    public Long getSeatStock(Long seatId) {
        log.debug("Redis에서 좌석 재고 조회: seatId={}", seatId);
        return redisService.getSeatStock(seatId);
    }

    // 캐시 무효화
    @CacheEvict(value = {"reservationHistory", "seatInfo"}, key = "#seatId")
    public void evictReservationCache(Long seatId) {
        log.debug("캐시 무효화: seatId={}", seatId);
    }

    /**
     * RabbitMQ 메시지 수신 및 배치 처리
     * 
     * [동작 방식]
     * - @RabbitListener로 메시지 수신
     * - 메시지를 버퍼에 모음
     * - 일정 개수 또는 시간이 지나면 배치로 DB 저장
     */
    @RabbitListener(queues = RabbitMQConfig.QUEUE_NAME)
    @Transactional
    public void receiveReservationMessage(String seatIdStr) {
        synchronized (messageBuffer) {
            messageBuffer.add(seatIdStr);
            
            long currentTime = System.currentTimeMillis();
            boolean shouldProcess = messageBuffer.size() >= BATCH_SIZE || 
                                   (currentTime - lastBatchTime) >= BATCH_TIMEOUT_MS;
            
            if (shouldProcess) {
                processBatch();
            }
        }
    }

    /**
     * 배치 처리
     */
    private void processBatch() {
        if (messageBuffer.isEmpty()) {
            return;
        }

        long startTime = System.currentTimeMillis();
        List<Reservation> batchList = new ArrayList<>();
        List<String> failedSeatIds = new ArrayList<>();

        try {
            // 버퍼에서 메시지 꺼내기
            List<String> messagesToProcess = new ArrayList<>(messageBuffer);
            messageBuffer.clear();
            lastBatchTime = System.currentTimeMillis();

            for (String seatIdStr : messagesToProcess) {
                try {
                    Long seatId = Long.parseLong(seatIdStr);
                    Reservation r = new Reservation();
                    r.setSeatId(seatId);
                    r.setUserId(0L);
                    batchList.add(r);
                } catch (NumberFormatException e) {
                    log.error("잘못된 데이터 형식: {}", seatIdStr, e);
                    failedSeatIds.add(seatIdStr);
                }
            }

            if (!batchList.isEmpty()) {
                reservationRepository.saveAll(batchList);
                long processingTime = System.currentTimeMillis() - startTime;
                double throughput = (double) batchList.size() / (processingTime / 1000.0);
                
                log.info("DB 배치 저장 완료 (RabbitMQ): {}건, 처리 시간: {}ms, 처리량: {:.2f}건/초", 
                    batchList.size(), processingTime, throughput);
                
                if (processingTime > 1000) {
                    log.warn("배치 처리 지연 경고: {}ms 소요", processingTime);
                }
            }
        } catch (Exception e) {
            log.error("배치 처리 중 예외 발생. 실패한 {}건을 DLQ로 보냅니다.", batchList.size(), e);
            
            // 실패한 메시지들은 자동으로 DLQ로 이동 (RabbitMQ 설정에 의해)
            // 여기서는 로그만 남김
            throw new RuntimeException("배치 처리 실패", e);
        }
    }
}




