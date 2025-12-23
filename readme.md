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

# Step4-1
- Redis Queue → **RabbitMQ**로 교체
- JPA + Redis (재고 관리) + RabbitMQ (메시지 큐) + JMeter
- 메시지 큐의 고급 기능 활용 (지속성, Dead Letter Queue, 라우팅)

## Step4-1 주요 특징

### 1. RabbitMQ 구조
- **Exchange**: reservation-exchange (Direct Exchange)
- **Queue**: reservation-queue (메인 큐, Durable)
- **DLQ**: reservation-dlq (Dead Letter Queue)
- **Routing Key**: reservation

### 2. 메시지 흐름
1. 예약 성공 → Redis 재고 감소 → RabbitMQ에 메시지 발행
2. RabbitMQ Exchange → Queue로 메시지 전달
3. @RabbitListener가 메시지 수신
4. 배치 처리 (500개 또는 200ms마다)
5. DB 저장

### 3. RabbitMQ 장점
- 메시지 지속성 보장 (Durable Queue)
- Dead Letter Queue로 실패 메시지 자동 처리
- 여러 Consumer로 확장 가능
- Management UI 제공 (http://localhost:15672)

## API 엔드포인트

- `POST /api4-1/reservation/{seatId}` - 예약 요청
- `GET /api4-1/reservation/{seatId}/history` - 예약 내역 조회
- `GET /api4-1/reservation/{seatId}/stock` - 좌석 재고 조회

## 실행 방법

1. Docker Compose로 RabbitMQ 실행:
```bash
docker-compose up -d rabbitmq
```

2. RabbitMQ Management UI 접속:
- URL: http://localhost:15672
- Username: admin
- Password: admin123

3. 애플리케이션 실행 (step4_1 활성화)

4. JMeter 테스트 실행

# Step4-2
- Redis Queue → **Kafka**로 교체
- JPA + Redis (재고 관리) + Kafka (메시지 큐) + JMeter
- Kafka의 고급 기능 활용 (파티션, Consumer Group, 오프셋 관리)

## Step4-2 주요 특징

### 1. Kafka 구조
- **Topic**: reservation-topic
- **Partition**: 3개 (병렬 처리)
- **Consumer Group**: reservation-consumer-group
- **Key**: seatId (같은 좌석의 메시지는 같은 파티션으로 전송되어 순서 보장)

### 2. 메시지 흐름
1. 예약 성공 → Redis 재고 감소 → Kafka Topic에 메시지 발행
2. Kafka Consumer가 배치 모드로 메시지 수신
3. 파티션별 병렬 처리
4. 배치로 DB 저장 후 오프셋 커밋

### 3. Kafka 장점
- 높은 처리량 (파티션 병렬 처리)
- 메시지 순서 보장 (파티션 내)
- 오프셋 관리로 재처리 가능
- 확장성 우수 (파티션 수만큼 Consumer 확장)

## API 엔드포인트

- `POST /api4-2/reservation/{seatId}` - 예약 요청
- `GET /api4-2/reservation/{seatId}/history` - 예약 내역 조회
- `GET /api4-2/reservation/{seatId}/stock` - 좌석 재고 조회

## 실행 방법

1. Docker Compose로 Kafka 실행:
```bash
docker-compose up -d zookeeper kafka
```

2. 애플리케이션 실행 (step4_2 활성화)

3. JMeter 테스트 실행

## Step4-1 vs Step4-2 비교

| 항목 | Step4-1 (RabbitMQ) | Step4-2 (Kafka) |
|------|-------------------|-----------------|
| 메시지 큐 | RabbitMQ | Kafka |
| 구조 | Exchange → Queue | Topic → Partition |
| 순서 보장 | Queue 내 순서 보장 | 파티션 내 순서 보장 |
| 처리량 | 중간 | 매우 높음 |
| 확장성 | Consumer 수 증가 | 파티션 수 증가 |
| 특징 | Dead Letter Queue | 오프셋 관리 |
| 관리 UI | Management UI (15672) | Kafka UI 도구 필요 |