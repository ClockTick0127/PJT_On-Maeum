package com.nilm.monitoring.risk;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

/**
 * 비교 기준 시각 정책. 구간 경계, 하루의 시작과 끝, 1분마다 도는 타이머를 모두 확인한다.
 *
 * <p>프로필 통계는 구간 끝 시각에서 잰 값이다. 기준 시각이 통계보다 앞서면 아직 오지 않은
 * 활동을 이미 했어야 하는 것으로 비교하게 된다.
 */
class EvaluationPointTest {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    private EvaluationPoint at(String kst) {
        return EvaluationPoint.of(OffsetDateTime.parse(kst), KST);
    }

    @Test
    void 구간_중간에는_직전_경계를_쓴다() {
        EvaluationPoint point = at("2026-09-21T12:10:00+09:00");

        assertThat(point.bucket()).isEqualTo("12:00");
        assertThat(point.at()).isEqualTo(OffsetDateTime.parse("2026-09-21T12:00:00+09:00"));
        assertThat(point.businessDate()).isEqualTo(LocalDate.of(2026, 9, 21));
    }

    @Test
    void 경계_정각은_그_경계에_속한다() {
        assertThat(at("2026-09-21T12:30:00+09:00").bucket()).isEqualTo("12:30");
        assertThat(at("2026-09-21T12:29:59+09:00").bucket()).isEqualTo("12:00");
        assertThat(at("2026-09-21T12:30:00+09:00").at())
                .isEqualTo(OffsetDateTime.parse("2026-09-21T12:30:00+09:00"));
    }

    @Test
    void 하루의_마지막_구간은_24시로_적고_다음날_자정을_가리킨다() {
        EvaluationPoint point = at("2026-09-22T00:00:00+09:00");

        assertThat(point.bucket()).isEqualTo("24:00");
        assertThat(point.at()).isEqualTo(OffsetDateTime.parse("2026-09-22T00:00:00+09:00"));
        // 그 시각까지의 누적은 전날의 하루치다.
        assertThat(point.businessDate()).isEqualTo(LocalDate.of(2026, 9, 21));
        assertThat(point.businessDayStart())
                .isEqualTo(OffsetDateTime.parse("2026-09-21T00:00:00+09:00"));
    }

    @Test
    void 하루가_시작한_뒤_30분_동안은_전날의_마지막_구간을_쓴다() {
        EvaluationPoint point = at("2026-09-22T00:29:00+09:00");

        assertThat(point.bucket()).isEqualTo("24:00");
        assertThat(point.at()).isEqualTo(OffsetDateTime.parse("2026-09-22T00:00:00+09:00"));
        assertThat(point.businessDate()).isEqualTo(LocalDate.of(2026, 9, 21));

        // 00:30이 되면 오늘의 첫 구간이 끝난다.
        EvaluationPoint next = at("2026-09-22T00:30:00+09:00");
        assertThat(next.bucket()).isEqualTo("00:30");
        assertThat(next.businessDate()).isEqualTo(LocalDate.of(2026, 9, 22));
    }

    @Test
    void 타이머가_매분_돌아도_구간_안에서는_기준_시각이_움직이지_않는다() {
        OffsetDateTime start = OffsetDateTime.parse("2026-09-21T12:00:00+09:00");
        for (int minute = 0; minute < 30; minute++) {
            EvaluationPoint point = EvaluationPoint.of(start.plusMinutes(minute), KST);
            assertThat(point.at()).isEqualTo(start);
            assertThat(point.bucket()).isEqualTo("12:00");
        }
        assertThat(EvaluationPoint.of(start.plusMinutes(30), KST).bucket()).isEqualTo("12:30");
    }

    @Test
    void 같은_순간이면_어느_시간대로_들어와도_같은_기준_시각이_나온다() {
        // 저장·전달은 UTC로 한다. 영업일과 구간은 언제나 KST로 센다.
        EvaluationPoint kst = at("2026-09-21T12:10:00+09:00");
        EvaluationPoint utc = EvaluationPoint.of(
                OffsetDateTime.parse("2026-09-21T03:10:00Z"), KST);

        assertThat(utc.at()).isEqualTo(kst.at());
        assertThat(utc.bucket()).isEqualTo(kst.bucket());
        assertThat(utc.businessDate()).isEqualTo(kst.businessDate());
    }
}
