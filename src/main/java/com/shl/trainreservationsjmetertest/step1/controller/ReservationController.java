package com.shl.trainreservationsjmetertest.step1.controller;

import com.shl.trainreservationsjmetertest.step1.service.ReservationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequestMapping("/api/reservation")
@RestController
@RequiredArgsConstructor
public class ReservationController {
    private final ReservationService reservationService;

    @PostMapping("/{seatId}")
    public ResponseEntity<String> reserve(@PathVariable Long seatId) {
        try {
             reservationService.reserveSeat(seatId);
             return ResponseEntity.ok("예약 성공!");
        } catch (Exception e) {
            // 이미 예약된 좌석이라는 메시지 반환
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }
}
