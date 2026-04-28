package com.shl.trainreservationsjmetertest.step_final.entity;

import lombok.*;

import java.util.Map;

@Data
public class SeatAvailability {
    private String trainId;
    private String date;
    private Map<String, String> seats; // seatNo -> status (AVAILABLE, HOLD, SOLD)
}
