package com.shl.trainreservationsjmetertest.step3.controller;

import com.shl.trainreservationsjmetertest.step3.entity.Reservation;
import com.shl.trainreservationsjmetertest.step3.service.ReservationService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 예약 컨트롤러 (Step3: 성능 최적화)
 * 
 * [Step3 개선 사항]
 * - 예약 조회 API 추가 (캐싱 적용)
 * - 좌석 재고 조회 API 추가 (캐싱 적용)
 * - 상세한 실행 순서 주석
 */
@RestController
@RequestMapping("/api3/reservation")
@RequiredArgsConstructor
public class ReservationController {
    private final ReservationService reservationService;

    /**
     * 예약 요청 API
     * 
     * [실행 순서]
     * 1. ReservationController.reserve() 호출
     *    ↓
     * 2. ReservationService.reserve(seatId) 호출
     *    ↓
     * 3. RedisService.tryReservationSeat(seatId) 호출
     *    ↓
     * 4. Redis Lua 스크립트 실행 (원자적 연산)
     *    - 재고 확인: seat:{seatId}:stock 값 확인
     *    - 재고 > 0 이면:
     *      * 재고 감소: seat:{seatId}:stock -= 1
     *      * 큐에 추가: reservation_queue에 seatId 추가 (rightPush)
     *      * return 1 (성공)
     *    - 재고 <= 0 이면:
     *      * return 0 (실패)
     *    ↓
     * 5. 예약 성공 시 캐시 무효화 (최신 정보 반영)
     *    ↓
     * 6. ReservationController가 응답 반환
     * 
     * [비동기 처리]
     * - 큐에 추가된 예약 요청은 @Scheduled로 200ms마다 실행되는
     *   ReservationService.processQueue() 메서드가 배치로 처리함
     *   - Redis 큐에서 leftPop으로 순서대로 꺼냄
     *   - 최대 500개씩 모아서 (Step2: 100 → Step3: 500)
     *   - ReservationRepository.saveAll()로 DB에 배치 저장
     * 
     * [Step3 성능 최적화]
     * - Redis Connection Pool 최적화
     * - 배치 크기 증가 (100 → 500)
     * - 처리 주기 단축 (500ms → 200ms)
     * - 병렬 처리 강화 (스레드 4개 → 8개)
     * - 캐싱으로 읽기 성능 향상
     */
    @PostMapping("/{seatId}")
    public String reserve(@PathVariable Long seatId) {
        return reservationService.reserve(seatId);
    }

    /**
     * 예약 내역 조회 API (캐싱 적용)
     * 
     * [실행 순서]
     * 1. ReservationController.getReservationHistory() 호출
     *    ↓
     * 2. 캐시 확인 (reservationHistory:{seatId})
     *    - 캐시 히트: 캐시에서 반환 (DB 조회 없음)
     *    - 캐시 미스: DB 조회 → 캐시에 저장 → 반환
     *    ↓
     * 3. 응답 반환
     * 
     * [캐시 설정]
     * - TTL: 5분
     * - 예약 성공 시 자동 무효화
     */
    @GetMapping("/{seatId}/history")
    public List<Reservation> getReservationHistory(@PathVariable Long seatId) {
        return reservationService.getReservationHistory(seatId);
    }

    /**
     * 좌석 재고 조회 API (캐싱 적용)
     * 
     * [실행 순서]
     * 1. ReservationController.getSeatStock() 호출
     *    ↓
     * 2. 캐시 확인 (seatInfo:{seatId})
     *    - 캐시 히트: 캐시에서 반환
     *    - 캐시 미스: Redis에서 조회 → 캐시에 저장 → 반환
     *    ↓
     * 3. 응답 반환
     * 
     * [캐시 설정]
     * - TTL: 30분
     * - 예약 성공 시 자동 무효화
     */
    @GetMapping("/{seatId}/stock")
    public Long getSeatStock(@PathVariable Long seatId) {
        return reservationService.getSeatStock(seatId);
    }
}


