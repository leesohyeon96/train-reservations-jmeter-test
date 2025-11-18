package com.shl.trainreservationsjmetertest.step3.service;

import com.shl.trainreservationsjmetertest.step3.entity.Reservation;
import com.shl.trainreservationsjmetertest.step3.repository.ReservationRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * 예약 서비스 (Step3: 성능 최적화)
 * 
 * [개선 사항]
 * - 배치 크기: 100 → 500으로 증가
 * - 처리 주기: 500ms → 200ms로 단축
 * - 캐싱 추가: 좌석 정보, 예약 조회 결과
 * - 모니터링 강화: 상세한 성능 메트릭 수집
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ReservationService {
    private final RedisService redisService;
    private final ReservationRepository reservationRepository;

    private static final int BATCH_SIZE = 500; // Step2: 100 → Step3: 500으로 증가
    private static final long PROCESSING_INTERVAL_MS = 200; // Step2: 500ms → Step3: 200ms로 단축

    public String reserve(Long seatId) {
        boolean success = redisService.tryReservationSeat(seatId);
        if (success) {
            // 예약 성공 시 캐시 무효화 (최신 정보 반영)
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
    // CacheEvict : Spring 에서 제공하는 특정 캐시 삭제시 사용하는 어노테이션
    @CacheEvict(value = {"reservationHistory", "seatInfo"}, key = "#seatId")
    // reservationHistory, seatInfo 2개의 캐시 무효화 // seatId 에 해당하는 항목이 두 개의 캐시에서 삭제됨
    public void evictReservationCache(Long seatId) {
        log.debug("캐시 무효화: seatId={}", seatId);
    }

    /**
     * 큐 처리 (배치 처리 강화)
     * 
     * [Step3 개선 사항]
     * - 배치 크기: 100 → 500
     * - 처리 주기: 500ms → 200ms
     * - 더 많은 병렬 처리 (SchedulerConfig에서 스레드 수 증가)
     */
    @Scheduled(fixedDelay = PROCESSING_INTERVAL_MS)
    @Transactional
    public void processQueue() {
        long startTime = System.currentTimeMillis();
        Long queueLengthBefore = redisService.getQueueLength();
        
        List<Reservation> batchList = new ArrayList<>();
        List<String> failedSeatIds = new ArrayList<>();

        try {
            // Step3: 배치 크기 증가 (100 → 500)
            for (int i = 0; i < BATCH_SIZE; i++) {
                String seatIdStr = redisService.popQueue();
                if (seatIdStr == null) break;

                try {
                    Long seatId = Long.parseLong(seatIdStr);
                    Reservation r = new Reservation();
                    r.setSeatId(seatId);
                    r.setUserId(0L);
                    batchList.add(r);
                } catch (NumberFormatException e) {
                    log.error("큐에서 잘못된 데이터 형식 발견: {}", seatIdStr, e);
                    failedSeatIds.add(seatIdStr);
                }
            }

            if (!batchList.isEmpty()) {
                reservationRepository.saveAll(batchList);
                long processingTime = System.currentTimeMillis() - startTime;
                Long queueLengthAfter = redisService.getQueueLength();
                
                // 성능 메트릭 로깅
                double throughput = (double) batchList.size() / (processingTime / 1000.0); // 초당 처리량
                log.info("DB 배치 저장 완료: {}건, 처리 시간: {}ms, 처리량: {:.2f}건/초, 큐 길이: {} -> {}", 
                    batchList.size(), processingTime, throughput, queueLengthBefore, queueLengthAfter);
                
                // 성능 경고 (처리 시간이 너무 오래 걸리면)
                if (processingTime > 1000) {
                    log.warn("배치 처리 지연 경고: {}ms 소요 (목표: <1000ms)", processingTime);
                }
            }
        } catch (Exception e) {
            log.error("큐 처리 중 예외 발생. 실패한 {}건을 다시 큐에 넣습니다.", batchList.size(), e);
            
            // 롤백: 실패한 항목들을 다시 큐에 넣기
            for (Reservation reservation : batchList) {
                redisService.pushQueue(reservation.getSeatId().toString());
            }
            
            for (String failedSeatId : failedSeatIds) {
                redisService.pushQueue(failedSeatId);
            }
            
            throw new RuntimeException("큐 처리 실패", e);
        }
    }
}


