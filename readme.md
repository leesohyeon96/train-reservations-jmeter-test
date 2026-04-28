# 기차 좌석 예약 시스템 — 동시성 제어 아키텍처 진화

> JMeter 부하 테스트 기반으로 고동시성 예약 시스템의 아키텍처를 단계별로 개선한 포트폴리오/학습 프로젝트

## 목차

- [프로젝트 개요](#프로젝트-개요)
- [기술 스택](#기술-스택)
- [아키텍처 진화 과정](#아키텍처-진화-과정)
- [Step_Final (구현 중)](#step_final--구현-중)
- [로컬 실행 방법](#로컬-실행-방법)
- [JMeter 테스트](#jmeter-테스트)
- [환경 설정](#환경-설정)
- [프로젝트 구조](#프로젝트-구조)

---

## 프로젝트 개요

1,000개 좌석에 대해 동시 예약 요청이 폭주할 때 발생하는 **데이터 정합성 문제**와 **성능 저하**를 해결하기 위해 아키텍처를 단계적으로 개선한 프로젝트입니다.

- **문제**: 다수의 사용자가 동시에 같은 좌석을 예약 시도 → 중복 예약, 재고 불일치
- **접근**: DB 락 → Redis Queue → 분산 메시지 큐 순으로 점진적 개선
- **검증**: 각 단계마다 JMeter로 부하 테스트를 수행해 성능 개선 효과 측정

```
Step1          Step2               Step3                  Step4-1 / 4-2
JPA           Redis Queue      Redis Cluster            Message Queue
비관적 락  →  + Lua 원자 연산  →  + 배치/캐싱 강화  →   RabbitMQ / Kafka
              + 비동기 배치        + 수평 확장
```

---

## 기술 스택

| 분류 | 기술 |
|------|------|
| Language / Framework | Java 21, Spring Boot 3.5 |
| Build | Gradle |
| Database | PostgreSQL 15 |
| Cache / Queue | Redis 7 |
| Message Queue | RabbitMQ 3, Apache Kafka |
| ORM | Spring Data JPA |
| Load Testing | Apache JMeter 5.6 |
| Infra | Docker, Docker Compose |

---

## 아키텍처 진화 과정

### Step1 — JPA 비관적 락 (Pessimistic Lock)

DB 레벨 행 잠금(`SELECT ... FOR UPDATE`)으로 동시 접근을 제어하는 기본 구현입니다.

```
Client → Spring Boot → PostgreSQL (SELECT FOR UPDATE)
```

- `findByIdForUpdate(seatId)` 로 트랜잭션 내 DB 락 획득
- 락 보유 스레드만 예약 처리, 나머지는 해제 시까지 대기
- **한계**: 동시 요청이 많아질수록 DB 커넥션 고갈 및 처리 지연

**API**: `POST /api/reservation/{seatId}`

---

### Step2 — Redis Queue + Lua 스크립트

DB 직접 잠금 대신 Redis 인메모리 큐로 예약 순서를 보장하고, DB 쓰기를 비동기 배치로 분리합니다.

```
Client → Spring Boot → Redis Queue (Lua 원자 연산)
                              ↓ 비동기 배치 처리 (4 threads, 100건 / 500ms)
                         PostgreSQL
```

- Lua 스크립트로 재고 확인 + 감소를 **원자적**으로 처리 (Redis 싱글 스레드 모델 활용)
- HTTP 응답은 큐 진입 즉시 반환, 실제 DB 저장은 백그라운드 배치로 처리
- **개선 효과**: DB 락 제거, 빠른 응답, 예약 순서 보장

**API**:
- `POST /api2/reservation/{seatId}`
- `GET /api2/reservation/{seatId}/history`

---

### Step3 — Redis Cluster + 배치/캐싱 강화

트래픽 폭주 시에도 성능을 유지하기 위한 수평 확장과 읽기 최적화를 적용합니다.

```
Client → Spring Boot → Redis (Cache-Aside 패턴)
                              ↓
                       Redis Queue (Cluster)
                              ↓ 비동기 배치 처리 (8 threads, 500건 / 200ms)
                         PostgreSQL
```

| 항목 | Step2 | Step3 |
|------|-------|-------|
| 배치 크기 | 100건 | **500건** |
| 처리 주기 | 500ms | **200ms** |
| Worker 스레드 | 4개 | **8개** |
| Redis Connection Pool | 기본값 | max=20, idle=5~10 |
| 캐싱 | ❌ | ✅ 좌석 30분 / 예약내역 5분 TTL |

**API**:
- `POST /api3/reservation/{seatId}`
- `GET /api3/reservation/{seatId}/history`
- `GET /api3/reservation/{seatId}/stock`

---

### Step4-1 — RabbitMQ 메시지 큐

Redis Custom Queue를 검증된 메시지 브로커로 교체해 메시지 영속성과 실패 처리를 강화합니다.

```
Client → Spring Boot → Redis (재고 관리)
                              ↓ 메시지 발행
                    reservation-exchange (Direct)
                              ↓
              reservation-queue (Durable)    reservation-dlq (DLQ)
                              ↓ 8~16 concurrent consumers
                         PostgreSQL (Batch 500)
```

- **Exchange**: `reservation-exchange` (Direct Exchange)
- **Queue**: `reservation-queue` (Durable — 브로커 재시작에도 메시지 유지)
- **DLQ**: `reservation-dlq` — 처리 실패 메시지 자동 격리 후 재처리
- **Management UI**: `http://localhost:15672`

**API**:
- `POST /api4-1/reservation/{seatId}`
- `GET /api4-1/reservation/{seatId}/history`
- `GET /api4-1/reservation/{seatId}/stock`

---

### Step4-2 — Kafka 메시지 스트리밍

초고처리량 요구 환경을 위해 Kafka 파티션 병렬 처리를 활용합니다.

```
Client → Spring Boot → Redis (재고 관리)
                              ↓ 메시지 발행 (Key: seatId)
               reservation-topic
              Partition 0 | Partition 1 | Partition 2
                              ↓ 3 concurrent consumers (Manual Commit)
                         PostgreSQL (Batch 500)
```

- **Topic**: `reservation-topic`, 파티션 3개
- **메시지 키**: `seatId` — 동일 좌석 요청이 항상 같은 파티션으로 라우팅되어 순서 보장
- **Offset 관리**: Manual IMMEDIATE commit — DB 저장 성공 후에만 커밋
- **Consumer Group**: `reservation-consumer-group`

**API**:
- `POST /api4-2/reservation/{seatId}`
- `GET /api4-2/reservation/{seatId}/history`
- `GET /api4-2/reservation/{seatId}/stock`

---

### Step4-1 vs Step4-2 비교

| 항목 | RabbitMQ (Step4-1) | Kafka (Step4-2) |
|------|-------------------|-----------------|
| 메시지 모델 | Exchange → Queue | Topic → Partition |
| 순서 보장 | Queue 내 FIFO | 파티션 내 Key 기반 |
| 처리량 | 중간 | 매우 높음 |
| 확장 단위 | Consumer 수 증가 | 파티션 수 증가 |
| 실패 처리 | DLQ 자동 격리 | 오프셋 기반 재처리 |
| 관리 UI | 내장 (포트 15672) | 별도 도구 필요 |

---

## Step_Final — 구현 중

> ⚠️ **현재 미완성 상태입니다.** 설계 및 골격 코드만 작성되어 있으며, 실제 동작하지 않습니다.

### 구현 목표

실제 티켓팅 서비스(코레일, 인터파크 등)처럼 동작하는 **토큰 기반 대기열 시스템**입니다.

```
사용자 대기열 진입
      ↓
순번 발급 (대기 중 상태 폴링 가능)
      ↓
N명씩 입장 허용 → 액세스 토큰 발급 (TTL: 5분)
      ↓
토큰 보유자만 좌석 선점 가능 (선점 TTL: 3분)
      ↓
선점 시간 내 결제 완료 → 예약 확정
      ↓
모든 상태 변화를 Kafka 이벤트로 발행
(USER_ENTER_QUEUE → TOKEN_ISSUED → USER_ADMITTED → SEAT_HOLD → RESERVATION_COMPLETED)
```

### 현재 구현된 것

| 분류 | 파일 | 상태 |
|------|------|------|
| DTO | `QueueEnterRequest`, `QueueStatusResponse`, `TokenResponse`, `SeatHoldRequest`, `QueueEvent` | ✅ 완성 |
| Entity | `Reservation` (HOLD/RESERVED/CANCELLED 상태, 선점 만료시간 포함) | ✅ 완성 |
| Config | `RedisConfig` | ✅ 완성 |
| Controller | `QueueController`, `SeatController`, `TrainController` | ❌ 미구현 |
| Service | `QueueService`, `TokenService`, `SeatService`, `PaymentService`, `KafkaEventService` | ❌ 미구현 |
| Config | `KafkaConfig`, `QueueInitRunner` | ❌ 미구현 |
| Repository | — | ❌ 미작성 |

---

## 로컬 실행 방법

### 사전 요구사항

- Java 21+
- Docker & Docker Compose
- (선택) Apache JMeter 5.6+

### 1. 인프라 실행

```bash
# 전체 서비스 한 번에 실행 (PostgreSQL, Redis, RabbitMQ, Kafka)
docker-compose up -d

# 또는 실행할 Step에 필요한 서비스만 선택
docker-compose up -d postgres redis             # Step1 ~ Step3
docker-compose up -d postgres redis rabbitmq    # Step4-1
docker-compose up -d postgres redis zookeeper kafka  # Step4-2
```

| 서비스 | 포트 |
|--------|------|
| PostgreSQL | 5432 |
| Redis | 6379 |
| RabbitMQ (AMQP) | 5672 |
| RabbitMQ Management UI | 15672 |
| Kafka | 9092 |
| Zookeeper | 2181 |

### 2. 실행할 Step 선택

`TrainReservationsJmeterTestApplication.java`의 어노테이션 3개를 실행할 Step으로 변경합니다.

```java
// Step1
@SpringBootApplication(scanBasePackages = "com.shl.trainreservationsjmetertest.step1")
@EntityScan(basePackages = "com.shl.trainreservationsjmetertest.step1.domain")
@EnableJpaRepositories(basePackages = "com.shl.trainreservationsjmetertest.step1.repository")

// Step2
@SpringBootApplication(scanBasePackages = "com.shl.trainreservationsjmetertest.step2")
@EntityScan(basePackages = "com.shl.trainreservationsjmetertest.step2.entity")
@EnableJpaRepositories(basePackages = "com.shl.trainreservationsjmetertest.step2.repository")

// Step3 (기본값)
@SpringBootApplication(scanBasePackages = "com.shl.trainreservationsjmetertest.step3")
@EntityScan(basePackages = "com.shl.trainreservationsjmetertest.step3.entity")
@EnableJpaRepositories(basePackages = "com.shl.trainreservationsjmetertest.step3.repository")

// Step4-1
@SpringBootApplication(scanBasePackages = "com.shl.trainreservationsjmetertest.step4_1")
@EntityScan(basePackages = "com.shl.trainreservationsjmetertest.step4_1.entity")
@EnableJpaRepositories(basePackages = "com.shl.trainreservationsjmetertest.step4_1.repository")

// Step4-2
@SpringBootApplication(scanBasePackages = "com.shl.trainreservationsjmetertest.step4_2")
@EntityScan(basePackages = "com.shl.trainreservationsjmetertest.step4_2.entity")
@EnableJpaRepositories(basePackages = "com.shl.trainreservationsjmetertest.step4_2.repository")
```

### 3. 애플리케이션 실행

```bash
./gradlew bootRun
```

`dev` 프로파일이 기본 활성화되며 `application-dev.yaml`의 로컬 설정이 적용됩니다.  
DB 초기화 시 `init-data.sql`이 자동 실행되어 좌석 1,000건이 삽입됩니다.

---

## JMeter 테스트

### 테스트 플랜 구성

| 항목 | 값 |
|------|-----|
| 파일 | `Test Plan.jmx` |
| 스레드 수 | 5개 |
| 루프 횟수 | 100회 |
| 총 요청 수 | 500건 |
| Ramp-up | 1초 |
| 기본 대상 | `POST http://localhost:8080/api3/reservation/{seatId}` |
| seatId 생성 방식 | Groovy 스크립트로 1~100 순환 |

### 실행 방법

```bash
# GUI 모드 (테스트 확인 / 수정 시)
jmeter -t "Test Plan.jmx"

# CLI 모드 (실제 부하 측정 시 권장)
jmeter -n -t "Test Plan.jmx" -l result.jtl -e -o report/
```

다른 Step 테스트 시 JMeter 플랜의 HTTP Request URL을 해당 Step API 경로로 수정하세요.  
예) `/api3/reservation/{seatId}` → `/api4-1/reservation/{seatId}`

---

## 환경 설정

### 로컬 개발 (`dev` 프로파일)

`application-dev.yaml`에 로컬 환경 기본값이 설정되어 있습니다.  
DB 비밀번호 등 민감한 값은 환경 변수로 오버라이드하거나 `.env` 파일을 활용하세요.

```bash
# 환경 변수 오버라이드 예시
export SPRING_DATASOURCE_PASSWORD=your_local_password
./gradlew bootRun
```

### 프로덕션 (`prod` 프로파일)

운영 환경은 모든 민감 정보를 **환경 변수**로 주입합니다. 파일에 자격증명이 포함되지 않습니다.

```bash
export DB_URL=jdbc:postgresql://db-host:5432/reservation_db
export DB_USERNAME=your_db_user
export DB_PASSWORD=your_db_password
export REDIS_HOST=your_redis_host
export KAFKA_BOOTSTRAP_SERVERS=kafka-broker:9092

java -jar app.jar --spring.profiles.active=prod
```

---

## 프로젝트 구조

```
train-reservations-jmeter-test/
├── src/main/java/com/shl/trainreservationsjmetertest/
│   ├── step1/          # JPA + 비관적 락
│   ├── step2/          # Redis Queue + Lua 스크립트
│   ├── step3/          # Redis Cluster + 배치/캐싱 강화
│   ├── step4_1/        # RabbitMQ 메시지 큐
│   ├── step4_2/        # Kafka 메시지 스트리밍
│   └── step_final/     # 토큰 기반 대기열 (구현 중)
├── src/main/resources/
│   ├── application.yaml          # 공통 설정
│   ├── application-dev.yaml      # 로컬 개발 설정
│   ├── application-prod.yaml     # 운영 환경 설정 (환경 변수 기반)
│   └── init-data.sql             # 초기 데이터 (좌석 1,000건)
├── Test Plan.jmx                 # JMeter 테스트 플랜
└── docker-compose.yml            # 로컬 인프라 구성
```
