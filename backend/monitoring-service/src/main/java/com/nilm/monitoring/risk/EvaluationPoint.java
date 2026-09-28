package com.nilm.monitoring.risk;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * 평가 한 번이 현재 값과 프로필 통계를 맞대는 기준 시각.
 *
 * <p>프로필의 시간대별 통계는 구간 <b>끝 시각</b>에서 잰 값이다. Gold는 영업일 자정에
 * 구간 폭을 더해 평가 시점을 만들고, 그 시점까지의 누적 활동량과 그 시점의 무활동을 적는다
 * ({@code batch/gold_profile_service/src/gold_profile/statistical_profile.py}의
 * {@code evaluation_epoch}). 그래서 "12:30" 통계는 12:30 정각의 값이지 12:00~12:30 어딘가의
 * 값이 아니다.
 *
 * <p>12:10에 "12:30" 통계를 꺼내 쓰면 아직 오지 않은 20분치 활동을 이미 했어야 하는 것으로
 * 비교하게 된다. 그래서 기준 시각을 <b>지금 이하의 가장 최근 구간 경계</b>로 내리고,
 * 현재 관측값도 그 시각 기준으로 계산한다.
 *
 * <p>정책은 다음과 같다.
 *
 * <ul>
 *   <li><b>구간 경계</b>: 매시 00분과 30분. 12:00~12:29:59는 "12:00", 12:30 정각부터 "12:30"이다.
 *       경계 정각은 그 경계에 속한다(포함).</li>
 *   <li><b>하루 끝</b>: 자정 경계는 그 전날의 마지막 구간인 "24:00"이다. Gold도 전날 자정에
 *       48번째 구간 폭을 더해 같은 순간을 가리킨다.</li>
 *   <li><b>하루 시작</b>: 00:00~00:29:59에는 오늘 끝난 구간이 아직 없다. 기준 시각은 오늘 자정,
 *       곧 <b>전날의 "24:00"</b>이고 누적 활동량의 영업일도 전날이다. 전날 하루치 누적과
 *       전날 하루치 분포를 비교하므로 기준이 어긋나지 않는다.</li>
 *   <li><b>타이머 평가</b>: 타이머는 1분마다 돌지만 기준 시각은 30분마다 움직인다.
 *       같은 구간 안에서는 같은 기준 시각과 같은 통계를 쓰고, 경계를 넘는 순간 함께 넘어간다.</li>
 * </ul>
 *
 * <p>구간 사이를 보간하지 않는다. 지금과 경계 사이에 일어난 활동은 다음 경계에서 반영된다.
 * 근사로 메우면 어떤 값이 관측이고 어떤 값이 추정인지 이력에서 구분할 수 없다.
 *
 * @param at 비교 기준 시각. 항상 {@code now} 이하다
 * @param bucket 프로필 통계를 찾을 구간 이름
 * @param businessDate 누적 활동량이 쌓이는 영업일
 * @param businessDayStart 그 영업일이 시작한 시각
 */
public record EvaluationPoint(
        OffsetDateTime at,
        String bucket,
        LocalDate businessDate,
        OffsetDateTime businessDayStart
) {

    /** 프로필의 시간대 구간 폭(분). 배치의 bucket_minutes와 같은 값이다. */
    public static final int BUCKET_MINUTES = 30;

    private static final int BUCKETS_PER_DAY = 24 * 60 / BUCKET_MINUTES;

    /** 지금 이하의 가장 최근 구간 경계를 고른다. */
    public static EvaluationPoint of(OffsetDateTime now, ZoneId zone) {
        ZonedDateTime local = now.atZoneSameInstant(zone);
        ZonedDateTime dayStart = local.toLocalDate().atStartOfDay(zone);
        int index = local.toLocalTime().toSecondOfDay() / 60 / BUCKET_MINUTES;
        ZonedDateTime at = dayStart.plusMinutes((long) index * BUCKET_MINUTES);
        if (index == 0) {
            // 오늘 끝난 구간이 아직 없다. 자정은 전날의 마지막 구간이다.
            ZonedDateTime previousDayStart = dayStart.minusDays(1);
            return new EvaluationPoint(
                    at.toOffsetDateTime(),
                    label(BUCKETS_PER_DAY),
                    previousDayStart.toLocalDate(),
                    previousDayStart.toOffsetDateTime());
        }
        return new EvaluationPoint(
                at.toOffsetDateTime(),
                label(index),
                dayStart.toLocalDate(),
                dayStart.toOffsetDateTime());
    }

    /** 배치 표기를 따라 구간의 끝 시각만 적는다. 하루의 마지막 구간은 "24:00"이다. */
    private static String label(int index) {
        int minutes = index * BUCKET_MINUTES;
        return String.format("%02d:%02d", minutes / 60, minutes % 60);
    }
}
