package com.nilm.monitoring;

import static org.assertj.core.api.Assertions.assertThat;

import com.nilm.monitoring.config.enums.RiskLevel;
import com.nilm.monitoring.config.enums.StateChangeTrigger;
import com.nilm.monitoring.domain.Subject;
import com.nilm.monitoring.dto.kafka.AnalysisSnapshotMessage;
import com.nilm.monitoring.repository.SubjectRepository;
import com.nilm.monitoring.service.ApplianceActivityService;
import com.nilm.monitoring.service.RiskAssessmentService;
import com.nilm.monitoring.service.RiskAssessmentService.EvaluationTarget;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 진짜 PostgreSQL에서 확인하는 대상자 행 잠금.
 *
 * <p>H2로는 {@code select ... for update}가 두 트랜잭션을 실제로 직렬화하는지 확인할 수 없다.
 * 여기서 확인하는 성질은 전부 잠금이 있어야만 성립하는 것들이다.
 *
 * <ul>
 *   <li>타이머와 이벤트가 겹쳐도 최신 이벤트 등급이 사라지지 않는다</li>
 *   <li>두 평가가 같은 재발송 시각을 읽어 알림을 둘 만들지 않는다</li>
 *   <li>가전 OFF 해제와 타이머가 겹쳐도 상태가 일관된다</li>
 *   <li>서로 다른 가구는 서로를 기다리지 않는다</li>
 * </ul>
 *
 * <p>DB 접속 정보를 환경변수로 주지 않으면 통째로 건너뛴다. 기본 {@code ./gradlew test}는
 * H2로 돌고, 이 테스트는 일회용 PostgreSQL을 띄운 뒤에만 켜진다.
 *
 * <pre>
 * docker run -d --name monitoring-lock-test -p 55433:5432 \
 *     -e POSTGRES_DB=monitoring_db -e POSTGRES_USER=nilm_admin \
 *     -e POSTGRES_PASSWORD=1234 postgres:16
 * MONITORING_PG_URL=jdbc:postgresql://localhost:55433/monitoring_db \
 * MONITORING_PG_USER=nilm_admin MONITORING_PG_PASSWORD=1234 \
 *     ./gradlew test --tests '*RiskAssessmentLockPostgresTest'
 * </pre>
 */
@SpringBootTest
@EnabledIfEnvironmentVariable(named = "MONITORING_PG_URL", matches = ".+")
class RiskAssessmentLockPostgresTest {

    private static final OffsetDateTime NOW = OffsetDateTime.parse("2026-09-21T12:10:00+09:00");

    /** 잠금을 쥔 트랜잭션이 붙들고 있는 시간. 기다림이 관측될 만큼 길게 둔다. */
    private static final long HOLD_MILLIS = 800;

    /** 같은 가구에 두 평가를 동시에 던지는 라운드 수. 한 번으로는 겹치지 않을 수 있다. */
    private static final int ROUNDS = 20;

    @DynamicPropertySource
    static void postgres(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> System.getenv("MONITORING_PG_URL"));
        registry.add("spring.datasource.username",
                () -> env("MONITORING_PG_USER", "nilm_admin"));
        registry.add("spring.datasource.password",
                () -> env("MONITORING_PG_PASSWORD", "1234"));
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        // 잠금이 풀리지 않으면 테스트가 멈추는 대신 실패하게 둔다.
        registry.add("spring.datasource.hikari.connection-init-sql",
                () -> "set lock_timeout = '15s'");
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> "10");
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    @Autowired RiskAssessmentService assessments;
    @Autowired ApplianceActivityService activities;
    @Autowired SubjectRepository subjects;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager transactionManager;

    private ExecutorService pool;
    private TransactionTemplate transactions;

    @BeforeEach
    void setup() {
        pool = Executors.newFixedThreadPool(4);
        transactions = new TransactionTemplate(transactionManager);

        jdbc.update("delete from notifications");
        jdbc.update("delete from risk_assessments");
        jdbc.update("delete from analysis_events");
        jdbc.update("delete from appliance_usage_episodes");
        jdbc.update("delete from appliance_states");
        jdbc.update("delete from household_daily_appliance_usage");
        jdbc.update("delete from household_observations");
        jdbc.update("delete from subjects");
    }

    @AfterEach
    void teardown() {
        pool.shutdownNow();
    }

    private void insertSubject(String householdId) {
        jdbc.update("""
                insert into subjects(household_id, auth_sub, birth_date, name, phone, address)
                values (?, ?, DATE '1950-01-01', 'test', '010', 'test')
                """, householdId, "auth-" + householdId);
    }

    private Long subjectId(String householdId) {
        return jdbc.queryForObject(
                "select id from subjects where household_id = ?", Long.class, householdId);
    }

    private EvaluationTarget target(String householdId) {
        return new EvaluationTarget(subjectId(householdId), householdId);
    }

    private String column(String householdId, String column) {
        return jdbc.queryForObject(
                "select cast(" + column + " as varchar) from subjects where household_id = ?",
                String.class, householdId);
    }

    private int notificationCount(String householdId) {
        return jdbc.queryForObject("""
                select count(*) from notifications n
                join subjects s on s.id = n.subject_id
                where s.household_id = ?
                """, Integer.class, householdId);
    }

    /** 이벤트 경로가 세운 등급. 해제 조건은 아직 오지 않은 상태다. */
    private void seedEventRisk(String householdId, RiskLevel level, int score) {
        jdbc.update("""
                update subjects
                set event_risk_level = ?, event_risk_score = ?, event_risk_appliance = 'KETTLE',
                    event_risk_set_at = ?, current_risk_level = ?, current_risk_score = ?
                where household_id = ?
                """, level.name(), score, NOW, level.name(), score, householdId);
    }

    /** 모든 작업을 같은 출발선에서 동시에 놓는다. */
    private void runTogether(List<Runnable> tasks) throws Exception {
        CyclicBarrier start = new CyclicBarrier(tasks.size());
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        List<Future<?>> futures = new ArrayList<>();

        for (Runnable task : tasks) {
            futures.add(pool.submit(() -> {
                try {
                    start.await(20, TimeUnit.SECONDS);
                    task.run();
                } catch (Throwable t) {
                    failures.add(t);
                }
            }));
        }
        for (Future<?> future : futures) {
            future.get(60, TimeUnit.SECONDS);
        }
        assertThat(failures).isEmpty();
    }

    @Test
    void 두_평가가_같은_재발송_시각을_읽어_중복_알림을_만들지_않는다() throws Exception {
        for (int round = 0; round < ROUNDS; round++) {
            String household = "house-dup-" + round;
            insertSubject(household);
            seedEventRisk(household, RiskLevel.WARNING, 75);
            EvaluationTarget target = target(household);

            runTogether(List.of(
                    () -> assessments.evaluateSubject(
                            target, NOW, StateChangeTrigger.ASSESSMENT),
                    () -> assessments.evaluateSubject(
                            target, NOW, StateChangeTrigger.ASSESSMENT)));

            // 잠금이 없으면 두 평가가 같은 lastAlertAt(null)을 읽고 각자 알림을 만든다.
            assertThat(notificationCount(household))
                    .as("round %d", round)
                    .isEqualTo(1);
        }
    }

    @Test
    void 타이머는_이벤트_트랜잭션이_커밋될_때까지_기다리고_그_등급을_지우지_않는다() throws Exception {
        insertSubject("house-race");
        EvaluationTarget target = target("house-race");

        CountDownLatch locked = new CountDownLatch(1);
        AtomicLong eventCommittedAt = new AtomicLong();
        AtomicLong timerFinishedAt = new AtomicLong();

        Future<?> event = pool.submit(() -> transactions.executeWithoutResult(status -> {
            // 이벤트 경로와 같은 잠금을 잡고, 같은 슬롯을 세운다.
            Subject subject = subjects.findHouseholdForUpdate("house-race").get(0);
            subject.applyEventRisk(RiskLevel.DANGER, 95, UUID.randomUUID(), "KETTLE", NOW);
            locked.countDown();
            sleep(HOLD_MILLIS);
        }));

        assertThat(locked.await(20, TimeUnit.SECONDS)).isTrue();
        Future<?> timer = pool.submit(() -> {
            assessments.evaluateSubject(target, NOW, StateChangeTrigger.ASSESSMENT);
            timerFinishedAt.set(System.nanoTime());
        });

        event.get(60, TimeUnit.SECONDS);
        eventCommittedAt.set(System.nanoTime());
        timer.get(60, TimeUnit.SECONDS);

        // 타이머는 상태를 처음 읽을 때부터 같은 잠금을 잡으므로 커밋을 기다려야 한다.
        assertThat(timerFinishedAt.get())
                .as("타이머가 이벤트 트랜잭션의 커밋을 기다렸는지")
                .isGreaterThan(eventCommittedAt.get());

        // 잠금 이전에 읽은 Subject로 덮어쓰면 이 슬롯이 null로 지워진다.
        assertThat(column("house-race", "event_risk_level")).isEqualTo("DANGER");
        assertThat(column("house-race", "current_risk_level")).isEqualTo("DANGER");
        assertThat(column("house-race", "event_risk_appliance")).isEqualTo("KETTLE");
    }

    @Test
    void 서로_다른_가구는_서로를_기다리지_않는다() throws Exception {
        insertSubject("house-slow");
        insertSubject("house-fast");
        EvaluationTarget fast = target("house-fast");

        CountDownLatch locked = new CountDownLatch(1);
        AtomicLong slowCommittedAt = new AtomicLong();
        AtomicLong fastFinishedAt = new AtomicLong();

        Future<?> slow = pool.submit(() -> transactions.executeWithoutResult(status -> {
            subjects.findHouseholdForUpdate("house-slow");
            locked.countDown();
            sleep(HOLD_MILLIS * 2);
        }));

        assertThat(locked.await(20, TimeUnit.SECONDS)).isTrue();
        Future<?> other = pool.submit(() -> {
            assessments.evaluateSubject(fast, NOW, StateChangeTrigger.ASSESSMENT);
            fastFinishedAt.set(System.nanoTime());
        });

        other.get(60, TimeUnit.SECONDS);
        slow.get(60, TimeUnit.SECONDS);
        slowCommittedAt.set(System.nanoTime());

        // 잠금 범위가 가구다. 전역 잠금이면 여기서 house-slow의 커밋을 기다리게 된다.
        assertThat(fastFinishedAt.get())
                .as("다른 가구의 평가가 먼저 끝났는지")
                .isLessThan(slowCommittedAt.get());
        assertThat(column("house-fast", "assessment_status")).isEqualTo("LEARNING");
    }

    @Test
    void 가전_OFF_해제와_타이머가_겹쳐도_상태가_일관된다() throws Exception {
        insertSubject("house-off");
        EvaluationTarget target = target("house-off");

        OffsetDateTime observedAt = OffsetDateTime.now(ZoneOffset.UTC);
        // 첫 스냅샷은 전환으로 보지 않는다. 켜진 상태를 먼저 심는다.
        activities.handle(snapshot("house-off", observedAt, true));
        seedEventRisk("house-off", RiskLevel.DANGER, 95);

        runTogether(List.of(
                () -> activities.handle(
                        snapshot("house-off", observedAt.plusMinutes(5), false)),
                () -> assessments.evaluateSubject(
                        target, NOW, StateChangeTrigger.ASSESSMENT)));

        // OFF 전환이 슬롯을 비운다. 타이머가 잠금 이전 상태를 들고 있으면 되살아난다.
        assertThat(column("house-off", "event_risk_level")).isNull();
        assertThat(column("house-off", "event_risk_appliance")).isNull();
        // 유효 등급은 두 슬롯에서 다시 계산한 값과 어긋나면 안 된다.
        assertThat(column("house-off", "current_risk_level")).isEqualTo("NORMAL");
        assertThat(column("house-off", "current_risk_score")).isEqualTo("0");
    }

    private AnalysisSnapshotMessage snapshot(
            String householdId,
            OffsetDateTime observedAt,
            boolean kettleOn
    ) {
        return new AnalysisSnapshotMessage(
                1,
                UUID.randomUUID().toString(),
                householdId,
                observedAt,
                observedAt,
                new AnalysisSnapshotMessage.Measurement(null, null, null, null),
                List.of(new AnalysisSnapshotMessage.Appliance("KETTLE", kettleOn))
        );
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
