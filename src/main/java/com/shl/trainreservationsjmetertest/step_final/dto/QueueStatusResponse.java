package com.shl.trainreservationsjmetertest.step_final.dto;

import lombok.Data;

@Data
public class QueueStatusResponse {
    private Long position; // 대기 순번 (1부터 시작)
    private Long totalWaiting; // 전체 대기 인원
    private String status; // WAITING, ADMITTED
}
