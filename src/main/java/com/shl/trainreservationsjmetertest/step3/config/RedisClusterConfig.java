package com.shl.trainreservationsjmetertest.step3.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Redis 설정 (Step3: 성능 최적화)
 * 
 * [설정 방식]
 * - Spring Boot 자동 설정 활용 (application.yaml에서 설정)
 * - Connection Pool은 application.yaml의 lettuce.pool 설정으로 관리
 * 
 * [Redis Cluster 지원]
 * - application.yaml에서 cluster.nodes 설정 시 자동으로 Cluster 모드로 동작
 * - 단일 Redis: host/port 설정
 * - Cluster: cluster.nodes 설정
 * 
 * [성능 최적화]
 * - Connection Pool: max-active=20, max-idle=10, min-idle=5
 * - Timeout: 2000ms
 */
@Configuration
public class RedisClusterConfig {

    @Bean
    public RedisTemplate<String, String> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new StringRedisSerializer());
        template.afterPropertiesSet();
        return template;
    }
}

