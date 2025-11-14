# Step1
- JPA + 비관적 락 + Jmeter 사용해서 기차 예약 시스템 테스트!


# Step2
- JPA + Queue(Redis & Lua 스크립트) + Jmeter 사용해서 동시성 충돌 문제 해결 & 예약 순서 보장

# Step3
- Step2로 동시성 문제는 해결 but 트래픽 폭주시 성능 유지용
- JPA + Redis Cluster(분산서버로 확장) + 비동기/배치 처리 강화 + 캐싱/읽기 최적화