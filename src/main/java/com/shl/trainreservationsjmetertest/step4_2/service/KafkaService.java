package com.shl.trainreservationsjmetertest.step4_2.service;

import com.shl.trainreservationsjmetertest.step4_2.config.KafkaConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Kafka 서비스 (Step4-2)
 * 
 * [기능]
 * - 메시지 발행 (예약 요청을 Topic에 추가)
 * - 비동기 처리 결과 확인
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaService {
    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 예약 메시지 발행
     * 
     * @param seatId 좌석 ID
     */
    public void publishReservation(Long seatId) {
        try {
            // Key를 seatId로 설정하여 같은 좌석의 메시지는 같은 파티션으로 전송 (순서 보장)
            String key = seatId.toString();
            String value = seatId.toString(); // value 에는 실제 필요한 모든 정보를 json 으로 담아야함 (실무에서)

            // CompletableFuture<SenddResult<K, V>> : 메시지 전송 성공/실패 여부 관계 없이 나중에 확인
            //                                      : non-blocking 방식으로 전송 결과 처리
            //                                      : 전송 성공 시 meta data(파티션, 오프셋 등) 확인
            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(
                KafkaConfig.TOPIC_NAME,
                key,
                value
            );
            
            // 비동기 결과 처리
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.debug("메시지 발행 성공: seatId={}, partition={}, offset={}", 
                        seatId, 
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
                } else {
                    log.error("메시지 발행 실패: seatId={}", seatId, ex);
                }
            });
            
        } catch (Exception e) {
            log.error("메시지 발행 중 예외 발생: seatId={}", seatId, e);
            throw new RuntimeException("메시지 발행 실패", e);
        }
    }
}

