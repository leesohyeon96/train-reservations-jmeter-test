package com.shl.trainreservationsjmetertest.step4_1.service;

import com.shl.trainreservationsjmetertest.step4_1.config.RabbitMQConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

/**
 * RabbitMQ 서비스 (Step4-1)
 * 
 * [기능]
 * - 메시지 발행 (예약 요청을 큐에 추가)
 * - 큐 길이 확인 (모니터링용)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RabbitMQService {
    private final RabbitTemplate rabbitTemplate;

    /**
     * 예약 메시지 발행
     * 
     * @param seatId 좌석 ID
     */
    public void publishReservation(Long seatId) {
        try {
            rabbitTemplate.convertAndSend(
                RabbitMQConfig.EXCHANGE_NAME,
                RabbitMQConfig.ROUTING_KEY,
                seatId.toString()
            );
            log.debug("예약 메시지 발행: seatId={}", seatId);
        } catch (Exception e) {
            log.error("메시지 발행 실패: seatId={}", seatId, e);
            throw new RuntimeException("메시지 발행 실패", e);
        }
    }

    /**
     * 큐 길이 확인 (모니터링용)
     * 
     * 주의: RabbitMQ는 직접 큐 길이를 조회하기 어려움
     * Management API를 사용하거나 메트릭을 통해 확인해야 함
     */
    public Long getQueueLength() {
        // RabbitMQ Management API를 사용하거나
        // 메트릭을 통해 확인해야 함
        // 여기서는 간단히 -1 반환 (구현 필요)
        return -1L;
    }
}




