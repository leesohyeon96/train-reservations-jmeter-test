package com.shl.trainreservationsjmetertest.step2.service;

import com.shl.trainreservationsjmetertest.step2.entity.Reservation;
import com.shl.trainreservationsjmetertest.step2.repository.ReservationRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReservationService {
    private final RedisService redisService;
    private final ReservationRepository reservationRepository;

    public String reserve(Long seatId) {
        boolean success = redisService.tryReservationSeat(seatId);
        return success ? "예약성공!" : "예약 실패ㅜ 좌석 없엉";
    }

    // worker 쓰레드 : 큐에서 꺼내서 DB 저장하는 메소드임
    // 단건 처리(loop형)
//    @Scheduled(fixedDelay = 100)
//    public void processQueue() {
//        String seatIdStr = redisService.popQueue();
//        if (seatIdStr != null) {
//            Long seatId = Long.parseLong(seatIdStr);
//            Reservation r = new Reservation();
//            r.setSeatId(seatId);
//            r.setUserId(0L); // 테스트
//            reservationRepository.save(r);
//            System.out.println("DB 저장 완료: seatId=" + seatId);
//        }
//    }

    // 배치 형태 처리(batch)
    // CPU + DB connection + 메모리
    // 1. TaskScheduler 만들어서 별도 스케줄링 전용 스레드 운영 > 큐에 데이터 없으면 거의 sleep 상태라 CPU 점유율 매우 낮음
    // 2. 큐 > 데이터 꺼내 List<> 임시 생성시 최대 20건 정도 객체 생성되나 매우 미미한 상태 > GC 부담 거의X
    // 3. Redis I/O/DB Connection 은 popQueue 호출시 redis 네트워크 1회, saveAll로 JPA+DB Connection 1회
    //    500ms 마다 1번 >> 1초당 최대 5번 DB 쓰기 트랜젝션 일어남
    //    1,2,3중 가장 큰 리소스 소모 지점이나 batch로 20개씩 처리하니 효율적인편!

    // 내부적으로 ThreadPoolTaskScheduler(위에서 언급함) 라는 스케쥴러 풀에서 실행되는데, 기본값은 스레드 1개
    // 따라서, 설정해주는 것이 좋음 (트래픽이 많거나, 큐에 쌓이는 속도가 빠르다면 worker thread 수 늘리는 설정) > config 참고
    @Scheduled(fixedDelay = 500) // 애플리케이션 실행 이후 500ms(0.5초) 마다 주기적으로 실행됨
    @Transactional // 배치 저장 시 트랜잭션 경계 명시: 전체 배치가 하나의 트랜잭션으로 처리됨
    public void processQueue() {
        long startTime = System.currentTimeMillis();
        Long queueLengthBefore = redisService.getQueueLength(); // 처리 전 큐 길이 (모니터링)
        
        List<Reservation> batchList = new ArrayList<>();
        List<String> failedSeatIds = new ArrayList<>(); // 예외 발생한 항목 추적

        try {
            // 한 번에 최대 100개까지 꺼냄 (큐가 비면 중단)
            for (int i = 0; i < 100; i++) {
                String seatIdStr = redisService.popQueue();
                if (seatIdStr == null) break;

                try {
                    Long seatId = Long.parseLong(seatIdStr);
                    Reservation r = new Reservation();
                    r.setSeatId(seatId);
                    r.setUserId(0L);
                    batchList.add(r);
                } catch (NumberFormatException e) {
                    // 잘못된 데이터 형식인 경우 로그만 남기고 스킵
                    log.error("큐에서 잘못된 데이터 형식 발견: {}", seatIdStr, e);
                    failedSeatIds.add(seatIdStr);
                }
            }

            // 큐에서 모은 데이터가 있다면 한 번에 저장
            if (!batchList.isEmpty()) {
                reservationRepository.saveAll(batchList);
                long processingTime = System.currentTimeMillis() - startTime;
                Long queueLengthAfter = redisService.getQueueLength(); // 처리 후 큐 길이 (모니터링)
                
                log.info("DB 배치 저장 완료: {}건, 처리 시간: {}ms, 큐 길이: {} -> {}", 
                    batchList.size(), processingTime, queueLengthBefore, queueLengthAfter);
            }
        } catch (Exception e) {
            // 예외 발생 시: 실패한 항목들을 다시 큐에 넣어서 재시도 가능하도록 함
            log.error("큐 처리 중 예외 발생. 실패한 {}건을 다시 큐에 넣습니다.", batchList.size(), e);
            
            // 이미 큐에서 꺼낸 항목들을 다시 큐에 넣기 (롤백)
            for (Reservation reservation : batchList) {
                redisService.pushQueue(reservation.getSeatId().toString());
            }
            
            // 파싱 실패한 항목들도 다시 넣기
            for (String failedSeatId : failedSeatIds) {
                redisService.pushQueue(failedSeatId);
            }
            
            // 예외를 다시 던져서 트랜잭션 롤백 보장
            throw new RuntimeException("큐 처리 실패", e);
        }
    }




}
