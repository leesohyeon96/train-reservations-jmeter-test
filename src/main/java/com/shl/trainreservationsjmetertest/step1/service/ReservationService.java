package com.shl.trainreservationsjmetertest.step1.service;

import com.shl.trainreservationsjmetertest.step1.domain.Seat;
import com.shl.trainreservationsjmetertest.step1.repository.SeatRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ReservationService {
    private final SeatRepository seatRepository;

    @Transactional
    public void reserveSeat(Long seatId) {
        Seat seat = seatRepository.findByIdForUpdate(seatId);
        if (seat.isReserved()) {
            throw new RuntimeException("이미 예약된 좌석입니다");
        }
        seat.setReserved(true);
        seatRepository.save(seat);
    }
}
