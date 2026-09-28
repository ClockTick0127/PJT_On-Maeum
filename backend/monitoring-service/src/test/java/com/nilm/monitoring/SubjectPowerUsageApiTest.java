package com.nilm.monitoring;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.nilm.monitoring.dto.kafka.AnalysisSnapshotMessage;
import com.nilm.monitoring.service.ApplianceActivityService;
import com.nilm.monitoring.service.InMemoryLatestSnapshotStore;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
class SubjectPowerUsageApiTest {

    private static final ZoneId SEOUL = ZoneId.of("Asia/Seoul");
    private static final String ENDPOINT = "/api/monitoring/subjects/{id}/power-usage";

    @Autowired MockMvc mockMvc;
    @Autowired JdbcTemplate jdbc;
    @Autowired ApplianceActivityService applianceActivity;
    @Autowired InMemoryLatestSnapshotStore snapshots;

    @BeforeEach
    void cleanDatabase() {
        jdbc.update("delete from hourly_appliance_power_usage");
        jdbc.update("delete from hourly_power_usage");
        jdbc.update("delete from notifications");
        jdbc.update("delete from analysis_events");
        jdbc.update("delete from appliance_usage_episodes");
        jdbc.update("delete from appliance_states");
        jdbc.update("delete from household_observations");
        jdbc.update("delete from subjects");
        jdbc.update("delete from managers");
        snapshots.clear();
    }

    @Test
    void returnsTwentyFourBucketsForAPastDay() throws Exception {
        long managerId = insertManager("manager-a");
        long subjectId = insertSubject(managerId, "H001", "subject-H001");
        LocalDate date = LocalDate.now(SEOUL).minusDays(1);
        insertTotal("H001", date, 1, "180.5");
        insertTotal("H001", date, 2, "120.0");
        insertAppliance("H001", date, 1, "KETTLE", "60.0");
        insertAppliance("H001", date, 1, "MICROWAVE", "20.25");

        mockMvc.perform(get(ENDPOINT, subjectId)
                        .param("date", date.toString())
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.subjectId").value(Long.toString(subjectId)))
                .andExpect(jsonPath("$.date").value(date.toString()))
                .andExpect(jsonPath("$.timezone").value("Asia/Seoul"))
                .andExpect(jsonPath("$.intervalMinutes").value(60))
                .andExpect(jsonPath("$.unit").value("Wh"))
                .andExpect(jsonPath("$.totalUsage").value(300.5))
                .andExpect(jsonPath("$.hourlyUsage.length()").value(24))
                .andExpect(jsonPath("$.hourlyUsage[0].hour").value(0))
                // 관측 기록이 없는 구간이다. 0을 내려 "안 썼다"로 보이게 하지 않는다.
                .andExpect(jsonPath("$.hourlyUsage[0].usage").doesNotExist())
                .andExpect(jsonPath("$.hourlyUsage[0].status").value("NO_DATA"))
                .andExpect(jsonPath("$.hourlyUsage[1].usage").value(180.5))
                .andExpect(jsonPath("$.hourlyUsage[1].status").value("COMPLETE"))
                .andExpect(jsonPath("$.hourlyUsage[2].usage").value(120.0))
                .andExpect(jsonPath("$.hourlyUsage[23].status").value("NO_DATA"))
                // 그날 쓰지 않은 가전도 포함해 6종이 언제나 가전 코드 순으로 온다
                .andExpect(jsonPath("$.appliances.length()").value(6))
                .andExpect(jsonPath("$.appliances[0].applianceType").value("HAIR_DRYER"))
                .andExpect(jsonPath("$.appliances[1].applianceType").value("INDUCTION"))
                .andExpect(jsonPath("$.appliances[2].applianceType").value("IRON"))
                .andExpect(jsonPath("$.appliances[3].applianceType").value("KETTLE"))
                .andExpect(jsonPath("$.appliances[4].applianceType").value("MICROWAVE"))
                .andExpect(jsonPath("$.appliances[5].applianceType").value("VACUUM_CLEANER"))
                .andExpect(jsonPath("$.appliances[3].totalUsage").value(60.0))
                .andExpect(jsonPath("$.appliances[3].hourlyUsage.length()").value(24))
                .andExpect(jsonPath("$.appliances[3].hourlyUsage[1].usage").value(60.0))
                .andExpect(jsonPath("$.appliances[4].totalUsage").value(20.3))
                // 쓰지 않은 가전은 계열만 서고 값은 0이다
                .andExpect(jsonPath("$.appliances[0].totalUsage").value(0))
                .andExpect(jsonPath("$.appliances[0].hourlyUsage[1].usage").value(0.0))
                .andExpect(jsonPath("$.updatedAt").exists());
    }

    @Test
    void marksTheCurrentHourPartialAndLaterHoursNotYet() throws Exception {
        long managerId = insertManager("manager-a");
        long subjectId = insertSubject(managerId, "H001", "subject-H001");
        LocalDate today = LocalDate.now(SEOUL);
        int currentHour = OffsetDateTime.now().atZoneSameInstant(SEOUL).getHour();
        // 진행 중인 구간은 관측이 있어야 PARTIAL이다. 없으면 NO_DATA가 맞다.
        insertTotal("H001", today, currentHour, "12.0", 60);

        var result = mockMvc.perform(get(ENDPOINT, subjectId)
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.date").value(today.toString()))
                .andExpect(jsonPath("$.hourlyUsage.length()").value(24))
                .andExpect(jsonPath("$.hourlyUsage[" + currentHour + "].status")
                        .value("PARTIAL"))
                .andExpect(jsonPath("$.appliances.length()").value(6))
                .andExpect(jsonPath("$.updatedAt").exists());

        if (currentHour < 23) {
            result.andExpect(jsonPath("$.hourlyUsage[23].status").value("NOT_YET"))
                    .andExpect(jsonPath("$.hourlyUsage[23].usage").doesNotExist());
        }
        if (currentHour > 0) {
            // 지나갔지만 본 적 없는 구간이다. NOT_YET과도, COMPLETE와도 다르다.
            result.andExpect(jsonPath("$.hourlyUsage[0].status").value("NO_DATA"));
        }
    }

    @Test
    void rejectsOtherManagersAndUnknownSubjects() throws Exception {
        long managerId = insertManager("manager-a");
        insertManager("manager-b");
        long subjectId = insertSubject(managerId, "H001", "subject-H001");

        mockMvc.perform(get(ENDPOINT, subjectId))
                .andExpect(status().isUnauthorized());

        mockMvc.perform(get(ENDPOINT, subjectId)
                        .with(jwt().jwt(token -> token.subject("manager-b"))))
                .andExpect(status().isForbidden());

        mockMvc.perform(get(ENDPOINT, subjectId + 999)
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isNotFound());

        mockMvc.perform(get(ENDPOINT, subjectId)
                        .param("date", "2026-13-40")
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isBadRequest());
    }

    @Test
    void subjectCanReadTheirOwnGraph() throws Exception {
        long managerId = insertManager("manager-a");
        long subjectId = insertSubject(managerId, "H001", "subject-H001");

        mockMvc.perform(get(ENDPOINT, subjectId)
                        .with(jwt().jwt(token -> token.subject("subject-H001"))))
                .andExpect(status().isOk());
    }

    /**
     * 적산이 만든 구간과 그래프가 읽는 구간이 같은 칸을 가리키는지 본다.
     * 저장은 UTC 정각, 화면은 한국 시간 0~23시라 이 둘이 어긋나면 값이 옆 칸에 찍힌다.
     */
    @Test
    void graphShowsWhatTheSnapshotConsumerAccumulated() throws Exception {
        long managerId = insertManager("manager-a");
        long subjectId = insertSubject(managerId, "H001", "subject-H001");
        LocalDate date = LocalDate.now(SEOUL).minusDays(1);
        OffsetDateTime tenAmSeoul = bucketStart(date, 10);

        applianceActivity.handle(snapshot(tenAmSeoul, "1200"));
        applianceActivity.handle(snapshot(tenAmSeoul.plusSeconds(60), "1200"));

        mockMvc.perform(get(ENDPOINT, subjectId)
                        .param("date", date.toString())
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isOk())
                // 1200W × 60초 = 20Wh가 한국 시간 10시 칸에 들어간다
                .andExpect(jsonPath("$.hourlyUsage[10].usage").value(20.0))
                // 한 시간 중 60초만 봤으므로 확정할 수 없다
                .andExpect(jsonPath("$.hourlyUsage[10].status").value("PARTIAL"))
                .andExpect(jsonPath("$.hourlyUsage[9].status").value("NO_DATA"))
                .andExpect(jsonPath("$.hourlyUsage[11].status").value("NO_DATA"))
                .andExpect(jsonPath("$.totalUsage").value(20.0));
    }

    /**
     * 같은 빈칸이라도 뜻이 다르다. 전기를 안 쓴 시간과 관측이 끊긴 시간을 가른다.
     * 화면에서 이 둘이 같아 보이면 담당자가 정반대로 읽는다.
     */
    @Test
    void separatesAQuietHourFromAnUnobservedHour() throws Exception {
        long managerId = insertManager("manager-a");
        long subjectId = insertSubject(managerId, "H001", "subject-H001");
        LocalDate date = LocalDate.now(SEOUL).minusDays(1);
        // 5시: 한 시간 내내 봤고 쓴 것이 없다. 6시: 아예 보지 못했다.
        insertTotal("H001", date, 5, "0.0", 3600);

        mockMvc.perform(get(ENDPOINT, subjectId)
                        .param("date", date.toString())
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.hourlyUsage[5].status").value("COMPLETE"))
                .andExpect(jsonPath("$.hourlyUsage[5].usage").value(0.0))
                .andExpect(jsonPath("$.hourlyUsage[6].status").value("NO_DATA"))
                .andExpect(jsonPath("$.hourlyUsage[6].usage").doesNotExist());
    }

    @Test
    void marksAnUnderObservedHourPartial() throws Exception {
        long managerId = insertManager("manager-a");
        long subjectId = insertSubject(managerId, "H001", "subject-H001");
        LocalDate date = LocalDate.now(SEOUL).minusDays(1);
        // 절반만 본 구간. 값이 낮은 것이 실제인지 덜 본 탓인지 알 수 없다.
        insertTotal("H001", date, 7, "40.0", 1800);
        // 기준(90%)을 넘긴 구간은 확정으로 본다.
        insertTotal("H001", date, 8, "80.0", 3400);

        mockMvc.perform(get(ENDPOINT, subjectId)
                        .param("date", date.toString())
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.hourlyUsage[7].status").value("PARTIAL"))
                .andExpect(jsonPath("$.hourlyUsage[7].usage").value(40.0))
                .andExpect(jsonPath("$.hourlyUsage[8].status").value("COMPLETE"));
    }

    /** 가전별 계열도 가구의 관측 커버리지를 따른다. 같은 시간의 칸이 서로 다르면 안 된다. */
    @Test
    void applianceSeriesFollowsTheHouseholdCoverage() throws Exception {
        long managerId = insertManager("manager-a");
        long subjectId = insertSubject(managerId, "H001", "subject-H001");
        LocalDate date = LocalDate.now(SEOUL).minusDays(1);
        insertTotal("H001", date, 3, "100.0", 1800);
        insertAppliance("H001", date, 3, "KETTLE", "60.0");

        mockMvc.perform(get(ENDPOINT, subjectId)
                        .param("date", date.toString())
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isOk())
                // KETTLE은 가전 코드 순 네 번째다
                .andExpect(jsonPath("$.appliances[3].applianceType").value("KETTLE"))
                .andExpect(jsonPath("$.appliances[3].hourlyUsage[3].status").value("PARTIAL"))
                .andExpect(jsonPath("$.appliances[3].hourlyUsage[0].status").value("NO_DATA"))
                .andExpect(jsonPath("$.appliances[3].hourlyUsage[0].usage").doesNotExist());
    }

    /** 기록이 하나도 없는 날도 6종 계열이 선다. 화면이 가전 행을 직접 채우지 않아도 된다. */
    @Test
    void alwaysReturnsEverySeriesEvenWithNoRecordsAtAll() throws Exception {
        long managerId = insertManager("manager-a");
        long subjectId = insertSubject(managerId, "H001", "subject-H001");
        LocalDate date = LocalDate.now(SEOUL).minusDays(1);

        mockMvc.perform(get(ENDPOINT, subjectId)
                        .param("date", date.toString())
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.appliances.length()").value(6))
                .andExpect(jsonPath("$.appliances[3].applianceType").value("KETTLE"))
                .andExpect(jsonPath("$.appliances[3].hourlyUsage.length()").value(24))
                // 관측 자체가 없는 하루다. 0이 아니라 NO_DATA여야 한다.
                .andExpect(jsonPath("$.appliances[3].hourlyUsage[0].status").value("NO_DATA"))
                .andExpect(jsonPath("$.appliances[3].totalUsage").value(0));
    }

    private AnalysisSnapshotMessage snapshot(OffsetDateTime observedAt, String activePower) {
        List<AnalysisSnapshotMessage.Appliance> appliances = List.of(
                new AnalysisSnapshotMessage.Appliance("KETTLE", false),
                new AnalysisSnapshotMessage.Appliance("INDUCTION", false),
                new AnalysisSnapshotMessage.Appliance("IRON", false),
                new AnalysisSnapshotMessage.Appliance("MICROWAVE", false),
                new AnalysisSnapshotMessage.Appliance("HAIR_DRYER", false),
                new AnalysisSnapshotMessage.Appliance("VACUUM_CLEANER", false)
        );
        return new AnalysisSnapshotMessage(
                1,
                UUID.randomUUID().toString(),
                "H001",
                observedAt,
                observedAt,
                new AnalysisSnapshotMessage.Measurement(
                        new BigDecimal(activePower), null, null, null),
                appliances
        );
    }

    private long insertManager(String authSub) {
        jdbc.update("""
                insert into managers(auth_sub, name, organization)
                values (?, '김담당', '중구 복지관')
                """, authSub);
        return jdbc.queryForObject(
                "select id from managers where auth_sub = ?", Long.class, authSub);
    }

    private long insertSubject(long managerId, String householdId, String authSub) {
        jdbc.update("""
                insert into subjects(
                    household_id, auth_sub, birth_date, name, phone, address, manager_id
                ) values (?, ?, cast('1950-01-01' as date), '김철수', '01012345678',
                          '서울특별시 중구', ?)
                """, householdId, authSub, managerId);
        return jdbc.queryForObject(
                "select id from subjects where household_id = ?", Long.class, householdId);
    }

    private void insertTotal(String householdId, LocalDate date, int hour, String energyWh) {
        insertTotal(householdId, date, hour, energyWh, 3600);
    }

    /** 구간을 온전히 관측한 경우가 기본이다. 커버리지를 따지는 테스트만 직접 넘긴다. */
    private void insertTotal(
            String householdId,
            LocalDate date,
            int hour,
            String energyWh,
            int observedSeconds
    ) {
        jdbc.update("""
                insert into hourly_power_usage(
                    household_id, bucket_start_at, energy_wh, observed_seconds, updated_at
                ) values (?, ?, cast(? as numeric), ?, ?)
                """,
                householdId,
                bucketStart(date, hour),
                energyWh,
                observedSeconds,
                OffsetDateTime.now());
    }

    private void insertAppliance(
            String householdId,
            LocalDate date,
            int hour,
            String applianceType,
            String energyWh
    ) {
        jdbc.update("""
                insert into hourly_appliance_power_usage(
                    household_id, bucket_start_at, appliance_type, energy_wh, updated_at
                ) values (?, ?, ?, cast(? as numeric), ?)
                """,
                householdId,
                bucketStart(date, hour),
                applianceType,
                energyWh,
                OffsetDateTime.now());
    }

    private OffsetDateTime bucketStart(LocalDate date, int hour) {
        return date.atStartOfDay(SEOUL).plusHours(hour).toOffsetDateTime();
    }
}
