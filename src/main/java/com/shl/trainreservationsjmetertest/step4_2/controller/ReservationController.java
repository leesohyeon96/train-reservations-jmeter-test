package com.shl.trainreservationsjmetertest.step4_2.controller;

import com.shl.trainreservationsjmetertest.step4_2.entity.Reservation;
import com.shl.trainreservationsjmetertest.step4_2.service.ReservationService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 예약 컨트롤러 (Step4-2: Kafka 사용)
 * 
 * [변경사항]
 * - Step3와 동일한 API 제공
 * - 내부적으로 Kafka 사용
 */
@RestController
@RequestMapping("/api4-2/reservation")
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
     * 4. 예약 성공 시 KafkaService.publishReservation() 호출
     *    - Kafka Topic에 메시지 발행
     *    - Key를 seatId로 설정하여 같은 파티션으로 전송 (순서 보장)
     *    ↓
     * 5. Kafka Consumer가 메시지 수신
     *    - 배치 모드로 여러 메시지를 한 번에 받음
     *    - 파티션별 병렬 처리
     *    ↓
     * 6. 배치로 DB 저장 후 오프셋 커밋
     *    ↓
     * 7. 응답 반환
     * 
     * [Kafka 특징]
     * - 높은 처리량 (파티션 병렬 처리)
     * - 메시지 순서 보장 (파티션 내)
     * - 오프셋 관리로 재처리 가능
     * - 확장성 우수 (파티션 수만큼 Consumer 확장 가능)
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




