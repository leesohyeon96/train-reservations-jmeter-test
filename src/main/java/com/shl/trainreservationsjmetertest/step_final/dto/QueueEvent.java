package com.shl.trainreservationsjmetertest.step_final.dto;

import lombok.Data;

@Data
public class QueueEvent {
    private String eventType; // USER_ENTER_QUEUE, TOKEN_ISSUED, USER_ADMITTED, SEAT_HOLD, RESERVATION_COMPLETED
    private String userId;
    private Long timestamp;
    private Object data; // 이벤트별 추가 데이터
}
