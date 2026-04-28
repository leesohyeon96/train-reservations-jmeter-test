package com.shl.trainreservationsjmetertest.step_final.dto;

import lombok.Data;

@Data
public class TokenResponse {
    private String accessToken;
    private Integer expiresIn; // 초 단위 (300초 = 5분)
    private String message;
}
