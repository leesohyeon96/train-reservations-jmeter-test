package com.shl.trainreservationsjmetertest.step3.config;

import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Duration;

/**
 * 캐싱 설정
 * 
 * [캐시 전략]
 * - Cache-Aside: 애플리케이션에서 직접 캐시를 관리
 * - 읽기: 캐시 확인 → 없으면 DB 조회 → 캐시에 저장
 * - 쓰기: DB 저장 → 캐시 무효화
 * 
 * [캐시 대상]
 * - 좌석 정보 (자주 조회, 변경 빈도 낮음)
 * - 예약 조회 결과 (최근 예약 내역)
 *
 * -> 예약 성공 시 캐시 무효화 해서 최신 정보 반영하도록 하는것!
 *    예약 실패시 캐시 유지하되 TTL 30분까지 ㅇㅇ!
 */
@Configuration
@EnableCaching
public class CacheConfig {

    @Bean
    public CacheManager cacheManager(RedisConnectionFactory connectionFactory) {
        // 기본 캐시 설정
        RedisCacheConfiguration defaultConfig = RedisCacheConfiguration.defaultCacheConfig()
                .entryTtl(Duration.ofMinutes(10)) // TTL: 10분
                .serializeKeysWith(RedisSerializationContext.SerializationPair.fromSerializer(new StringRedisSerializer())) // 키는 String 으로 직렬화
                .serializeValuesWith(RedisSerializationContext.SerializationPair.fromSerializer(new GenericJackson2JsonRedisSerializer())) // 값은 json 으로 직렬화
                .disableCachingNullValues(); // null 값은 캐싱하지 않음

        // 캐시별 설정
        return RedisCacheManager.builder(connectionFactory)
                .cacheDefaults(defaultConfig)
                .withCacheConfiguration("seatInfo", defaultConfig.entryTtl(Duration.ofMinutes(30))) // 좌석 정보: 30분
                .withCacheConfiguration("reservationHistory", defaultConfig.entryTtl(Duration.ofMinutes(5))) // 예약 내역: 5분
                .build();
    }
}


