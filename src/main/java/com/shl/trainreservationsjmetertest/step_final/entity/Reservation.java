package com.shl.trainreservationsjmetertest.step_final.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Entity
@Setter
@Getter
public class Reservation {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String userId;
    private String trainId;
    private String seatNo;
    private String date;
    private String status; // HOLD, RESERVED, CANCELLED
    private LocalDateTime createdAt;
    private LocalDateTime expiresAt; // 선점 만료 시간
}
