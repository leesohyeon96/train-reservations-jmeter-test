# Step1
- JPA + 비관적 락 + Jmeter 사용해서 기차 예약 시스템 테스트!


# Step2
- JPA + Queue(Redis & Lua 스크립트) + Jmeter 사용해서 동시성 충돌 문제 해결 & 예약 순서 보장

# Step3
- Step2로 동시성 문제는 해결 but 트래픽 폭주시 성능 유지용
- JPA + Redis Cluster(분산서버로 확장) + 비동기/배치 처리 강화 + 캐싱/읽기 최적화

## Step3 주요 개선 사항

### 1. Redis Connection Pool 최적화
- 최대 연결 수: 20개
- 최대 유휴 연결: 10개
- 최소 유휴 연결: 5개
- Redis Cluster 지원 (application.yaml에서 설정)

### 2. 배치 처리 강화
- 배치 크기: 100 → **500**으로 증가
- 처리 주기: 500ms → **200ms**로 단축
- Worker 스레드: 4개 → **8개**로 증가

### 3. 캐싱 추가
- 좌석 정보 캐싱 (TTL: 30분)
- 예약 내역 캐싱 (TTL: 5분)
- Cache-Aside 패턴 적용

### 4. 모니터링 강화
- 처리 시간 측정 및 로깅
- 처리량 메트릭 (건/초)
- 큐 길이 모니터링
- 성능 경고 로그

## API 엔드포인트

- `POST /api3/reservation/{seatId}` - 예약 요청
- `GET /api3/reservation/{seatId}/history` - 예약 내역 조회 (캐싱)
- `GET /api3/reservation/{seatId}/stock` - 좌석 재고 조회 (캐싱)

## 실행 방법

1. Docker Compose로 Redis 실행:
```bash
docker-compose up -d redis
```

2. 애플리케이션 실행

3. JMeter 테스트 실행 (Step2와 동일한 방식)