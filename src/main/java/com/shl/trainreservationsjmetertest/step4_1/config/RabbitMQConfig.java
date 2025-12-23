package com.shl.trainreservationsjmetertest.step4_1.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ 설정 (Step4-1)
 * 
 * [RabbitMQ 구조]
 * Exchange: reservation-exchange (Direct Exchange)
 * Queue: reservation-queue (메인 큐)
 * DLQ: reservation-dlq (Dead Letter Queue - 실패한 메시지)
 * 
 * [메시지 흐름]
 * 1. 예약 성공 → Exchange → reservation-queue
 * 2. 처리 실패 → reservation-dlq로 이동
 * 3. Consumer가 reservation-queue에서 메시지 수신 → DB 저장
 */
@Configuration
public class RabbitMQConfig {

    // Exchange 이름
    public static final String EXCHANGE_NAME = "reservation-exchange";
    
    // Queue 이름
    public static final String QUEUE_NAME = "reservation-queue";
    public static final String DLQ_NAME = "reservation-dlq";
    
    // Routing Key
    public static final String ROUTING_KEY = "reservation";

    /**
     * Direct Exchange 생성
     * - Routing Key가 정확히 일치하는 Queue로 메시지 전달
     * - durable : 서버 재시작 후에도 유지
     * - auto-delete / false : 사용하지 않아도 삭제하지 않음
     */
    @Bean
    public DirectExchange reservationExchange() {
        return new DirectExchange(EXCHANGE_NAME, true, false); // durable, auto-delete
    }

    /**
     * Dead Letter Queue 생성
     * - 처리 실패한 메시지가 저장되는 큐
     * - durable : 서버 재시작 후에도 유지
     */
    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(DLQ_NAME).build();
    }

    /**
     * 메인 예약 큐 생성
     * - Dead Letter Exchange 설정: 실패 시 DLQ로 이동
     * - 실패시 사용할 Exchange -> 빈 문자열 = 기본 Exchange
     * - 실패시 보낼 Queue 이름 (DLQ)
     */
    @Bean
    public Queue reservationQueue() {
        return QueueBuilder.durable(QUEUE_NAME)
                .withArgument("x-dead-letter-exchange", "") // Default Exchange 사용
                .withArgument("x-dead-letter-routing-key", DLQ_NAME) // DLQ로 라우팅
                .build();
    }

    /**
     * Queue와 Exchange 바인딩
     * - 배달 경로 설정
     */
    @Bean
    public Binding reservationBinding() {
        return BindingBuilder
                .bind(reservationQueue()) // 우편함
                .to(reservationExchange()) // 우체국
                .with(ROUTING_KEY); // 주소 라벨
    }

    /**
     * JSON 메시지 컨버터
     * - 객체를 JSON으로 직렬화/역직렬화
     */
    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    /**
     * RabbitTemplate 설정
     * - 메시지 발행용
     * - 편지를 Json 형식으로 읽고 쓰는 변환기
     */
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jsonMessageConverter()); // rabbitMQ는 메시지를 바이트(byte[])로 주고받음 > 따라서 설정 필요
        // byte[] 쓰는 이유 : 원시적인 데이터 형태 & 네트워크 전송 빠름 & 모든 언어에 독립적이여야함 (ex. java/python 등 접근가능해야함) & 바이트 배열이면 문자열/JSON/이미지/바이너리데이터/파일 등 다 담을수있음
        return template;
    }

    /**
     * Listener Container Factory 설정
     * - 메시지 수신용
     */
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(jsonMessageConverter());
        factory.setConcurrentConsumers(8); // 동시에 8개 메시지 처리
        factory.setMaxConcurrentConsumers(16); // 최대 16개까지 확장
        factory.setPrefetchCount(100); // 한 번에 가져올 메시지 수
        return factory;
    }
}




