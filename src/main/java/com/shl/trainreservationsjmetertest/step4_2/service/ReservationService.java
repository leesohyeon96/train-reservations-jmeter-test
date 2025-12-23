package com.shl.trainreservationsjmetertest.step4_2.service;

import com.shl.trainreservationsjmetertest.step4_2.config.KafkaConfig;
import com.shl.trainreservationsjmetertest.step4_2.entity.Reservation;
import com.shl.trainreservationsjmetertest.step4_2.repository.ReservationRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * 예약 서비스 (Step4-2: Kafka 사용)
 * 
 * [변경사항]
 * - Redis Queue → Kafka Topic으로 교체
 * - @KafkaListener로 메시지 수신 (배치 모드)
 * - 파티션별 병렬 처리
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ReservationService {
    private final RedisService redisService;
    private final KafkaService kafkaService;
    private final ReservationRepository reservationRepository;

    /**
     * 예약 요청
     * 
     * [흐름]
     * 1. Redis에서 재고 확인 및 감소 (Lua 스크립트)
     * 2. 성공 시 Kafka Topic에 메시지 발행
     * 3. Consumer가 메시지를 받아서 DB 저장
     */
    public String reserve(Long seatId) {
        boolean success = redisService.tryReservationSeat(seatId);
        if (success) {
            // Kafka에 메시지 발행
            kafkaService.publishReservation(seatId);
            evictReservationCache(seatId);
        }
        return success ? "예약성공!" : "예약 실패ㅜ 좌석 없엉";
    }

    // 예약 내역 조회 (캐싱 적용)
    @Cacheable(value = "reservationHistory", key = "#seatId")
    public List<Reservation> getReservationHistory(Long seatId) {
        log.debug("DB에서 예약 내역 조회: seatId={}", seatId);
        return reservationRepository.findBySeatId(seatId);
    }

    // 좌석 재고 조회 (캐싱 적용)
    @Cacheable(value = "seatInfo", key = "#seatId")
    public Long getSeatStock(Long seatId) {
        log.debug("Redis에서 좌석 재고 조회: seatId={}", seatId);
        return redisService.getSeatStock(seatId);
    }

    // 캐시 무효화
    @CacheEvict(value = {"reservationHistory", "seatInfo"}, key = "#seatId")
    public void evictReservationCache(Long seatId) {
        log.debug("캐시 무효화: seatId={}", seatId);
    }


    // 여기서 말하는 메시지 = 좌석 예약! 그래서 -> 메시지 수신해서 한꺼번에 DB저장하는거!
    // 얘는 배치모드!
    /**
     * Kafka 메시지 수신 및 배치 처리
     * 
     * [동작 방식]
     * - @KafkaListener의 batch 모드로 여러 메시지를 한 번에 받음
     * - 배치로 DB 저장 후 수동 커밋
     * 
     * [파티션 처리]
     * - 각 파티션별로 병렬 처리
     * - 같은 좌석의 메시지는 같은 파티션으로 전송되어 순서 보장
     */
    @KafkaListener(
        topics = KafkaConfig.TOPIC_NAME,
        groupId = KafkaConfig.CONSUMER_GROUP_ID,
//      요런식으로 파티션 명시적으로 지정도 가능하긴 함
//        topicPartitions = @TopicPartition(
//                topic = KafkaConfig.TOPIC_NAME,
//                partitions = {"0", "1"}
//        )
        containerFactory = "kafkaListenerContainerFactory" // kafkaListenerContainerFactory 기본값이나, kafkaConfig에 커스텀한 설정 적용하겠다
            // 요거 단일/배치용으로 따로 @Bean 만들어서 그 메소드명을 넣어도됨 ㅇㅇ 그럼 해당 팩토리설정따라감
    )
    @Transactional
    public void receiveReservationMessages(
            @Payload List<String> messages,
            @Header(KafkaHeaders.RECEIVED_PARTITION) List<Integer> partitions,
            @Header(KafkaHeaders.OFFSET) List<Long> offsets, // ex) 101, 102, 103 (서버가 끊겨도 어느 offset 까지 진행됬는지 알기위함!!)
            Acknowledgment acknowledgment) {
        
        if (messages.isEmpty()) {
            return;
        }

        long startTime = System.currentTimeMillis();
        List<Reservation> batchList = new ArrayList<>();

        try {
            // 메시지 파싱 및 Reservation 객체 생성
            for (String seatIdStr : messages) {
                try {
                    Long seatId = Long.parseLong(seatIdStr);
                    Reservation r = new Reservation();
                    r.setSeatId(seatId);
                    r.setUserId(0L);
                    batchList.add(r);
                } catch (NumberFormatException e) {
                    log.error("잘못된 데이터 형식: {}", seatIdStr, e);
                }
            }

            if (!batchList.isEmpty()) {
                reservationRepository.saveAll(batchList);
                long processingTime = System.currentTimeMillis() - startTime;
                double throughput = (double) batchList.size() / (processingTime / 1000.0);
                
                log.info("DB 배치 저장 완료 (Kafka): {}건, 처리 시간: {}ms, 처리량: {:.2f}건/초, 파티션: {}", 
                    batchList.size(), processingTime, throughput, partitions.get(0));
                
                if (processingTime > 1000) {
                    log.warn("배치 처리 지연 경고: {}ms 소요", processingTime);
                }
            }
            
            // 수동 커밋 (배치 처리 성공 후)
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("배치 처리 중 예외 발생: {}건 처리 실패", batchList.size(), e);
            // 커밋하지 않으면 메시지가 다시 처리됨 (재시도)
            throw new RuntimeException("배치 처리 실패", e);
        }
    }
}

