package com.shl.trainreservationsjmetertest.step4_1.controller;

import com.shl.trainreservationsjmetertest.step4_1.entity.Reservation;
import com.shl.trainreservationsjmetertest.step4_1.service.ReservationService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 예약 컨트롤러 (Step4-1: RabbitMQ 사용)
 * 
 * [변경사항]
 * - Step3와 동일한 API 제공
 * - 내부적으로 RabbitMQ 사용
 */
@RestController
@RequestMapping("/api4-1/reservation")
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
     *    - Redis Lua 스크립트로 재고 확인/감소 (원자적 연산)
     *    ↓
     * 4. 예약 성공 시 RabbitMQService.publishReservation() 호출
     *    - RabbitMQ Exchange에 메시지 발행
     *    ↓
     * 5. RabbitMQ가 메시지를 Queue로 전달
     *    ↓
     * 6. @RabbitListener가 메시지 수신
     *    - 메시지를 버퍼에 모음
     *    - 배치 크기 또는 시간 도달 시 DB 저장
     *    ↓
     * 7. 응답 반환
     * 
     * [RabbitMQ 특징]
     * - 메시지 지속성 보장 (Durable Queue)
     * - Dead Letter Queue로 실패 메시지 처리
     * - 여러 Consumer로 확장 가능
     */
    @PostMapping("/{seatId}")
    public String reserve(@PathVariable Long seatId) {
        return reservationService.reserve(seatId);
    }

    /**
     * 예약 내역 조회 API (캐싱 적용)
     */
    @GetMapping("/{seatId}/history")
    public List<Reservation> getReservationHistory(@PathVariable Long seatId) {
        return reservationService.getReservationHistory(seatId);
    }

    /**
     * 좌석 재고 조회 API (캐싱 적용)
     */
    @GetMapping("/{seatId}/stock")
    public Long getSeatStock(@PathVariable Long seatId) {
        return reservationService.getSeatStock(seatId);
    }
}




