package com.shl.trainreservationsjmetertest.step1.repository;

import com.shl.trainreservationsjmetertest.step1.domain.Seat;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface SeatRepository extends JpaRepository<Seat, Long> {
    // OPTIMISTIC : 낙관적 락 > 조회 후 저장시 버전 체크 실패하면 예외 발생!
    // OPTIMISTIC_FORCE_INCREMENT : 낙관적 락 + 버전 컬럼 강제 증가 > 트랜잭션 동안 다른 쓰기와 충돌 방지
    // PESSIMISTIC_READ : 공유 락(=읽기 락) > 다른 트랜잭션에서 쓰기 막음 !
    // PESSIMISTIC_WRITE : 배타적 락(=쓰기 락) > 다른 트랜잭션에서 읽기/쓰기 막음 !
    // PESSIMISTIC_FORCE_INCREMENT : 쓰기 락 + 버전 컬럼 강제 증가 > 트랜잭션 동안 다른 쓰기 완전히 차단 + 버전 증가

    @Lock(LockModeType.PESSIMISTIC_WRITE) // 즉, 베타적 락이고 > 다른 트랜잭션에서 읽기/쓰기 막음!
    @Query("select s from Seat s where s.id = :id")
    Seat findByIdForUpdate(@Param("id") Long id);
}
