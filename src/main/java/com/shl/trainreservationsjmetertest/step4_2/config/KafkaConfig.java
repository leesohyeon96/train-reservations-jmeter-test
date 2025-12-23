package com.shl.trainreservationsjmetertest.step4_2.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 설정 (Step4-2)
 * 
 * [Kafka 구조]
 * Topic: reservation-topic
 * Partition: 3개 (병렬 처리)
 *  -> 순서가 중요한 경우 주의 필요
 *  1) 파티션 1개 (파티션끼리만 순서 보장해서)
 *  2) seatId 처럼 동일한 key를 가지면 같은 파티션으로 보냄
 * Consumer Group: reservation-consumer-group
 * 
 * [메시지 흐름]
 * 1. 예약 성공 → Redis 재고 감소 → Kafka Topic에 메시지 발행
 * 2. Consumer가 Topic에서 메시지 수신
 * 3. 배치 처리 (여러 메시지를 모아서 한 번에 처리)
 * 4. DB 저장
 * 
 * [Kafka 특징]
 * - 높은 처리량 (파티션 병렬 처리)
 * - 메시지 순서 보장 (파티션 내)
 * - 오프셋 관리로 재처리 가능
 * - 확장성 우수
 */
@Configuration
@EnableKafka
public class KafkaConfig {

    // Topic 이름
    public static final String TOPIC_NAME = "reservation-topic";
    
    // Consumer Group 이름
    public static final String CONSUMER_GROUP_ID = "reservation-consumer-group";

    /**
     * Kafka Producer 설정
     * - Producer : 메시지를 보내는 프로그램!
     *   서버가 > Kafka 로 보내는 역할이 Producer
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // StringSerializer : java의 String > kafka로 보내기 위해 byte[]로 직렬화
        // kafka 는 내부적으로 메시지를 byte[] 배열로만 다룸~!
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        
        // 메시지 지속성 보장 설정
        configProps.put(ProducerConfig.ACKS_CONFIG, "all"); // 모든 replica 확인 -1, 0, 1 있음
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3); // 재시도 횟수
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); // 순서 보장
        // -> kafka producer 가 같은 브로커 연결에서 동시에 전송할 수 있는 요청 수를 뜻함 -> 기본값은 5, 여기서 1로 해서 순서 보장
        // 1 개의 Producer가 1 연결로 동시에 여러 메시지를 서버로 보낼 수 있는 최대 개수
        // 이때, 병렬작업 안하기 때문에 의미 없음 > 순서가 중요할때 사용 <-> 처리량 & 순서 중요하지 않을때 1 초과하여 작성!
        
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * KafkaTemplate (메시지 발행용)
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * Kafka Consumer 설정
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        // byte[] > java String 으로 변환 (kafka > 서버로 올때)
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        
        // 오프셋 관리
        // 저장된 오프셋이 없을때 어디서부터 읽는지? earliest : 가장 오래된 메시지 <-> latest : 가장 최신 메시지부터
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 가장 오래된 메시지부터
        // 오프셋을 kafka가 자동으로 커밋하지 않음 -> 메시지 처리중 서버 죽을때, DB 저장 실패할때, 비즈니스 로직 실패할때 좋음 ㅇㅇ
        // 보통, listener 에서 처리 끝난 뒤 commit
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // 수동 커밋 (배치 처리 후)
        
        // 배치 처리 설정
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500); // 한 번에 가져올 최대 메시지 수
        configProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024); // 최소 가져올 바이트 수 (1kb = 1024byte)
        configProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 200); // 최대 대기 시간 (ms) -> 0.2초 (1초가 = 1000ms)
        
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    /**
     * Kafka Listener Container Factory
     * - 배치 모드로 여러 메시지를 한 번에 받음
     * -> @KafkaListener 가 사용할 Listener 컨테이너 생성기
     * -> 내부적으로 Kafka Consumer thread 생성 (Consumer 1개는 = 파티션 1개만 소비 가능!)
     * -> poll / commit / rebalance 관리
     */
    @Bean // kafkaListenerContainerFactory 기본값인데 커스텀함
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        
        // 배치 모드 활성화
        factory.setBatchListener(true); // -> 1건이 아닌 List 형태로 받게됨
        
        // 동시 Consumer 수 (파티션 수와 동일하게 설정 권장)
        factory.setConcurrency(3); // 파티션 3개에 맞춤
        
        // 수동 커밋 모드
        // acknowledge() 호출 시 즉시 offset 커밋, 다음 poll 전에 반영함
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        return factory;
    }
}




