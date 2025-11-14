package com.shl.trainreservationsjmetertest.step2.repository;

import com.shl.trainreservationsjmetertest.step2.entity.Reservation;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ReservationRepository extends JpaRepository<Reservation, Long> {
}
