package com.shl.trainreservationsjmetertest.step2.controller;

import com.shl.trainreservationsjmetertest.step2.service.ReservationService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api2/reservation")
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
     * 5. ReservationService.reserve() 결과 반환
     *    - 성공: "예약성공!"
     *    - 실패: "예약 실패ㅜ 좌석 없엉"
     *    ↓
     * 6. ReservationController가 응답 반환
     * 
     * [비동기 처리]
     * - 큐에 추가된 예약 요청은 @Scheduled로 500ms마다 실행되는
     *   ReservationService.processQueue() 메서드가 배치로 처리함
     *   - Redis 큐에서 leftPop으로 순서대로 꺼냄
     *   - 최대 100개씩 모아서
     *   - ReservationRepository.saveAll()로 DB에 배치 저장
     * 
     * [동시성 제어]
     * - Lua 스크립트로 재고 확인/감소를 원자적으로 처리하여
     *   동시에 여러 요청이 와도 정확히 1개만 성공하도록 보장
     * 
     * 예시: 1번 좌석에 5명이 동시에 예약 요청
     * - 재고가 1일 때: 1명만 성공, 4명은 실패
     */
    @PostMapping("/{seatId}")
    public String reserve(@PathVariable Long seatId) {
        return reservationService.reserve(seatId);
    }
}
