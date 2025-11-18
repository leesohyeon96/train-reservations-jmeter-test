package com.shl.trainreservationsjmetertest.step3.repository;

import com.shl.trainreservationsjmetertest.step3.entity.Reservation;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface ReservationRepository extends JpaRepository<Reservation, Long> {
    // 특정 좌석의 예약 조회 (캐싱 대상)
    @Query("SELECT r FROM Reservation r WHERE r.seatId = :seatId ORDER BY r.createdDt DESC")
    List<Reservation> findBySeatId(@Param("seatId") Long seatId);
}


