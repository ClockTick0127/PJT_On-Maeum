package com.nilm.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.nilm.monitoring.config.enums.StateChangeTrigger;
import com.nilm.monitoring.dto.kafka.HouseholdProfileMessage;
import com.nilm.monitoring.service.HouseholdProfileService;
import com.nilm.monitoring.service.ResolvedProfile;
import com.nilm.monitoring.service.SubjectStateChanged;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.event.ApplicationEvents;
import org.springframework.test.context.event.RecordApplicationEvents;

@SpringBootTest
@RecordApplicationEvents
class HouseholdProfileFlowTest {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    /**
     * 평가 기준일. 프로필은 하루 전까지의 데이터로 만들어져 있어야 한다.
     * 발효 시각(PENDING/ACTIVE) 판정은 서비스가 실제 시계로 하므로 날짜를 고정하면 안 된다.
     */
    private static final LocalDate TODAY = LocalDate.now(KST);

    private static final OffsetDateTime EVALUATION_TIME =
            TODAY.atTime(10, 0).atZone(KST).toOffsetDateTime();

    @Autowired HouseholdProfileService service;
    @Autowired JdbcTemplate jdbc;
    @Autowired ApplicationEvents applicationEvents;

    @BeforeEach
    void setup() {
        jdbc.update("delete from household_routine_baselines");
        jdbc.update("delete from household_profile_statistics");
        jdbc.update("delete from household_profiles");
        jdbc.update("delete from notifications");
        jdbc.update("delete from analysis_events");
        jdbc.update("delete from subjects");
        jdbc.update("""
                insert into subjects(household_id, auth_sub, birth_date, name, phone, address)
                values ('H001', 'test-subject-3', DATE '1950-01-01', 'test', '010', 'test')
                """);
        applicationEvents.clear();
    }

    // --- 메시지 만들기 -----------------------------------------------------

    private HouseholdProfileMessage profile(
            String householdId,
            String profileVersion,
            LocalDate asOfDate,
            String qualityStatus
    ) {
        return profile(
                householdId,
                profileVersion,
                asOfDate,
                asOfDate.plusDays(1).atStartOfDay(KST).toOffsetDateTime(),
                qualityStatus);
    }

    private HouseholdProfileMessage profile(
            String householdId,
            String profileVersion,
            LocalDate asOfDate,
            OffsetDateTime effectiveFrom,
            String qualityStatus
    ) {
        return new HouseholdProfileMessage(
                1,
                householdId,
                profileVersion,
                Long.parseLong(profileVersion.substring(1)),
                "ACTIVE",
                asOfDate,
                asOfDate.minusDays(27),
                asOfDate,
                effectiveFrom,
                effectiveFrom.minusHours(1),
                "snapshot-" + profileVersion,
                "gold-profile-v1",
                "household-statistics-v1-nearest-rank",
                qualityStatus,
                List.of(
                        baseline("KETTLE", "OVERALL", null),
                        baseline("KETTLE", "WEEKDAY", "MON"),
                        baseline("MICROWAVE", "OVERALL", null)
                ),
                List.of(
                        statistic("CUMULATIVE_ACTIVITY_START_COUNT", null, "09:30"),
                        statistic("LOGICAL_USE_ACTIVE_DURATION", "KETTLE", null)
                )
        );
    }

    private HouseholdProfileMessage.RoutineBaseline baseline(
            String applianceType,
            String scope,
            String weekday
    ) {
        return new HouseholdProfileMessage.RoutineBaseline(
                applianceType, scope, weekday, 26, 22,
                new BigDecimal("0.8462"), new BigDecimal("1.0000"),
                30600, 33000, 27000, 33000, "READY", true);
    }

    private HouseholdProfileMessage deliveredAs(
            HouseholdProfileMessage message,
            String deliveryMode
    ) {
        return new HouseholdProfileMessage(
                message.schemaVersion(),
                message.householdId(),
                message.profileVersion(),
                message.profileRevision(),
                deliveryMode,
                message.asOfDate(),
                message.windowStartDate(),
                message.windowEndDate(),
                message.effectiveFrom(),
                message.publishedAt(),
                message.inputSnapshotId(),
                message.ruleVersion(),
                message.statisticRuleVersion(),
                message.qualityStatus(),
                message.routineBaselines(),
                message.statistics()
        );
    }

    private HouseholdProfileMessage.Statistic statistic(
            String metricName,
            String applianceType,
            String timeBucket
    ) {
        return new HouseholdProfileMessage.Statistic(
                metricName, applianceType, "ALL", timeBucket,
                20L, 20L, 1.0, 3.0, 0.5, "count", "READY");
    }

    // --- 조회 도우미 -------------------------------------------------------

    private int count(String table) {
        return jdbc.queryForObject("select count(*) from " + table, Integer.class);
    }

    private String statusOf(String profileVersion) {
        return jdbc.queryForObject(
                "select status from household_profiles where profile_version = ?",
                String.class, profileVersion);
    }

    private String activeVersion() {
        return jdbc.queryForObject(
                "select profile_version from household_profiles where status = 'ACTIVE'",
                String.class);
    }

    private String activeVersion(String householdId) {
        return jdbc.queryForObject(
                "select profile_version from household_profiles "
                        + "where household_id = ? and status = 'ACTIVE'",
                String.class, householdId);
    }

    private long stateVersion() {
        return jdbc.queryForObject("select state_version from subjects", Long.class);
    }

    private long profileUpdatedEvents() {
        return applicationEvents.stream(SubjectStateChanged.class)
                .filter(event -> event.trigger() == StateChangeTrigger.PROFILE_UPDATED)
                .count();
    }

    // --- 수신 ---------------------------------------------------------------

    @Test
    void firstProfileBecomesActiveAndNotifiesTheDashboard() {
        service.receive(profile("H001", "v1", TODAY.minusDays(1), "READY"));

        assertThat(count("household_profiles")).isEqualTo(1);
        assertThat(activeVersion()).isEqualTo("v1");
        assertThat(count("household_routine_baselines")).isEqualTo(3);
        assertThat(count("household_profile_statistics")).isEqualTo(2);
        // 프로필 반영에서 한 번, 뒤이어 도는 첫 위험 평가에서 한 번 올라간다.
        assertThat(stateVersion()).isEqualTo(3L);
        assertThat(profileUpdatedEvents()).isEqualTo(1);
    }

    @Test
    void replayedProfileChangesNothing() {
        var message = profile("H001", "v1", TODAY.minusDays(1), "READY");
        service.receive(message);
        applicationEvents.clear();

        service.receive(message);

        assertThat(count("household_profiles")).isEqualTo(1);
        assertThat(count("household_routine_baselines")).isEqualTo(3);
        assertThat(count("household_profile_statistics")).isEqualTo(2);
        // 같은 버전을 다시 받으면 반영도 평가도 일어나지 않는다.
        assertThat(stateVersion()).isEqualTo(3L);
        assertThat(profileUpdatedEvents()).isZero();
    }

    @Test
    void shadowThenActiveUsesANewVersionAndIsNotBlockedAsDuplicate() {
        service.receive(deliveredAs(
                profile("H001", "v1", TODAY.minusDays(1), "READY"), "SHADOW"));
        applicationEvents.clear();

        service.receive(profile("H001", "v2", TODAY.minusDays(1), "READY"));

        assertThat(statusOf("v1")).isEqualTo("SHADOW");
        assertThat(statusOf("v2")).isEqualTo("ACTIVE");
        assertThat(activeVersion()).isEqualTo("v2");
        assertThat(profileUpdatedEvents()).isEqualTo(1);
    }

    @Test
    void lateShadowNeverReplacesTheActiveProfile() {
        service.receive(profile("H001", "v2", TODAY.minusDays(1), "READY"));
        applicationEvents.clear();

        service.receive(deliveredAs(
                profile("H001", "v3", TODAY.minusDays(1), "READY"), "SHADOW"));

        assertThat(statusOf("v3")).isEqualTo("SHADOW");
        assertThat(activeVersion()).isEqualTo("v2");
        assertThat(profileUpdatedEvents()).isZero();
    }

    @Test
    void newerProfileTakesOverAndSupersedesThePreviousActive() {
        service.receive(profile("H001", "v1", TODAY.minusDays(2), "READY"));
        applicationEvents.clear();

        service.receive(profile("H001", "v2", TODAY.minusDays(1), "READY"));

        assertThat(statusOf("v1")).isEqualTo("SUPERSEDED");
        assertThat(statusOf("v2")).isEqualTo("ACTIVE");
        assertThat(activeVersion()).isEqualTo("v2");
        assertThat(profileUpdatedEvents()).isEqualTo(1);
    }

    @Test
    void lateOlderProfileIsKeptAsHistoryWithoutReplacingTheActive() {
        service.receive(profile("H001", "v2", TODAY.minusDays(1), "READY"));
        applicationEvents.clear();

        // backfill이나 재처리로 구버전이 뒤늦게 도착한 상황.
        service.receive(profile("H001", "v1", TODAY.minusDays(2), "READY"));

        assertThat(statusOf("v1")).isEqualTo("SUPERSEDED");
        assertThat(activeVersion()).isEqualTo("v2");
        // 구버전은 이력으로만 남고 평가를 다시 돌리지 않는다.
        assertThat(stateVersion()).isEqualTo(3L);
        assertThat(profileUpdatedEvents()).isZero();
    }

    @Test
    void higherRevisionOnSameDateReplacesTheActive() {
        service.receive(profile("H001", "v1", TODAY.minusDays(1), "READY"));
        applicationEvents.clear();

        service.receive(profile("H001", "v2", TODAY.minusDays(1), "READY"));

        assertThat(activeVersion()).isEqualTo("v2");
        assertThat(statusOf("v1")).isEqualTo("SUPERSEDED");
        assertThat(profileUpdatedEvents()).isEqualTo(1);
    }

    @Test
    void lowerRevisionArrivingLaterCannotRollBackSameDate() {
        service.receive(profile("H001", "v2", TODAY.minusDays(1), "READY"));
        applicationEvents.clear();

        service.receive(profile("H001", "v1", TODAY.minusDays(1), "READY"));

        assertThat(activeVersion()).isEqualTo("v2");
        assertThat(statusOf("v1")).isEqualTo("SUPERSEDED");
        assertThat(profileUpdatedEvents()).isZero();
    }

    @Test
    void incompleteProfileIsRejectedAndLeavesTheActiveAlone() {
        service.receive(profile("H001", "v1", TODAY.minusDays(2), "READY"));
        applicationEvents.clear();

        service.receive(profile("H001", "v2", TODAY.minusDays(1), "INPUT_INCOMPLETE"));

        assertThat(statusOf("v2")).isEqualTo("REJECTED");
        assertThat(jdbc.queryForObject(
                "select rejection_reason from household_profiles where profile_version = 'v2'",
                String.class)).contains("INPUT_INCOMPLETE");
        assertThat(activeVersion()).isEqualTo("v1");
        // 품질 미달 프로필은 머리말만 남기고 본문은 저장하지 않는다.
        assertThat(count("household_routine_baselines")).isEqualTo(3);
        assertThat(count("household_profile_statistics")).isEqualTo(2);
        assertThat(profileUpdatedEvents()).isZero();
    }

    @Test
    void emptyProfileIsRejected() {
        var empty = new HouseholdProfileMessage(
                2, "H001", "v1", 1L, "ACTIVE", TODAY.minusDays(1),
                TODAY.minusDays(28), TODAY.minusDays(1),
                TODAY.atStartOfDay(KST).toOffsetDateTime(),
                TODAY.atStartOfDay(KST).toOffsetDateTime(),
                "snapshot-v1", "gold-profile-v1",
                "household-statistics-v1-nearest-rank",
                "READY", List.of(), List.of());

        service.receive(empty);

        assertThat(statusOf("v1")).isEqualTo("REJECTED");
        assertThat(count("household_routine_baselines")).isZero();
        assertThat(profileUpdatedEvents()).isZero();
    }

    @Test
    void profileOfUnknownHouseholdIsIgnored() {
        assertThatCode(() -> service.receive(
                profile("H999", "v1", TODAY.minusDays(1), "READY")))
                .doesNotThrowAnyException();

        assertThat(count("household_profiles")).isZero();
        assertThat(count("household_routine_baselines")).isZero();
        assertThat(stateVersion()).isEqualTo(1L);
    }

    @Test
    void profileMissingRequiredFieldsIsIgnored() {
        var broken = new HouseholdProfileMessage(
                2, "H001", null, null, null, null, null, null, null, null,
                null, null, null, "READY", List.of(), List.of());

        assertThatCode(() -> service.receive(broken)).doesNotThrowAnyException();

        assertThat(count("household_profiles")).isZero();
    }

    @Test
    void profilesForDifferentHouseholdsAreOrderedIndependently() {
        jdbc.update("""
                insert into subjects(household_id, auth_sub, birth_date, name, phone, address)
                values ('H002', 'test-subject-4', DATE '1955-01-01', 'test2', '010', 'test2')
                """);

        service.receive(profile("H001", "v2", TODAY.minusDays(1), "READY"));
        service.receive(profile("H002", "v1", TODAY.minusDays(1), "READY"));

        assertThat(activeVersion("H001")).isEqualTo("v2");
        assertThat(activeVersion("H002")).isEqualTo("v1");
    }

    // --- 선택 ---------------------------------------------------------------

    @Test
    void resolvesTheActiveProfileForEvaluation() {
        service.receive(profile("H001", "v1", TODAY.minusDays(1), "READY"));

        Optional<ResolvedProfile> resolved = service.resolveActive("H001", EVALUATION_TIME);

        assertThat(resolved).isPresent();
        ResolvedProfile value = resolved.get();
        assertThat(value.profileVersion()).isEqualTo("v1");
        assertThat(value.asOfDate()).isEqualTo(TODAY.minusDays(1));
        assertThat(value.stale()).isFalse();
        assertThat(value.baselines()).hasSize(3);
        assertThat(value.statistic(
                "CUMULATIVE_ACTIVITY_START_COUNT", null, "ALL", "09:30"))
                .get()
                .extracting(ResolvedProfile.Statistic::p90)
                .isEqualTo(3.0);
        assertThat(value.statistic(
                "LOGICAL_USE_ACTIVE_DURATION", "KETTLE", "ALL", null)).isPresent();
    }

    @Test
    void resolvesNothingWhileTheProfileHasNotTakenEffect() {
        // 자료는 충분히 과거지만 발효 시각이 아직 오지 않았다.
        service.receive(profile(
                "H001", "v1", TODAY.minusDays(2),
                TODAY.plusDays(1).atStartOfDay(KST).toOffsetDateTime(),
                "READY"));

        assertThat(service.resolveActive("H001", EVALUATION_TIME)).isEmpty();
    }

    @Test
    void futureProfileDoesNotHideCurrentlyUsableProfile() {
        service.receive(profile("H001", "v1", TODAY.minusDays(2), "READY"));
        applicationEvents.clear();
        service.receive(profile(
                "H001", "v2", TODAY.minusDays(1),
                TODAY.plusDays(1).atStartOfDay(KST).toOffsetDateTime(), "READY"));

        assertThat(statusOf("v2")).isEqualTo("PENDING");
        assertThat(activeVersion()).isEqualTo("v1");
        assertThat(service.resolveActive("H001", EVALUATION_TIME))
                .get().extracting(ResolvedProfile::profileVersion).isEqualTo("v1");
        assertThat(profileUpdatedEvents()).isZero();
    }

    @Test
    void resolvesNothingWhenTheProfileIncludesTheEvaluationDay() {
        // 오늘 자료가 섞인 프로필로 오늘을 평가하면 자기 자신과 비교하게 된다.
        service.receive(profile(
                "H001", "v1", TODAY,
                TODAY.atStartOfDay(KST).toOffsetDateTime(),
                "READY"));

        assertThat(service.resolveActive("H001", EVALUATION_TIME)).isEmpty();
    }

    @Test
    void marksTheProfileStaleWhenItIsOlderThanTheMaxAge() {
        service.receive(profile("H001", "v1", TODAY.minusDays(5), "READY"));

        Optional<ResolvedProfile> resolved = service.resolveActive("H001", EVALUATION_TIME);

        // 버리지 않는다. 쓸지 말지는 평가 로직이 정한다.
        assertThat(resolved).isPresent();
        assertThat(resolved.get().stale()).isTrue();
        assertThat(resolved.get().baselines()).hasSize(3);
    }

    @Test
    void resolvesNothingWithoutAnyProfile() {
        assertThat(service.resolveActive("H001", EVALUATION_TIME)).isEmpty();
        assertThat(service.resolveActive("H999", EVALUATION_TIME)).isEmpty();
    }
}
