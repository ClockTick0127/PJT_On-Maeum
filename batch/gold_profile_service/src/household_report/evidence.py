from pyspark.sql import functions as F


def build_evidence(usage_summary, assessment_detail):
    if "assessment_mode" not in assessment_detail.columns:
        assessment_detail = assessment_detail.withColumn("assessment_mode", F.lit("UNKNOWN_ORIGIN"))
    usage = (
        usage_summary.withColumn("evidence_type", F.lit("USAGE_FREQUENCY"))
        .withColumn(
            "evidence_id",
            F.sha2(
                F.concat_ws(
                    "|", "household_id", "appliance_type", F.lit("USAGE_FREQUENCY")
                ),
                256,
            ),
        )
        .withColumn(
            "statement",
            F.when(
                F.col("eligible_days") > 0,
                F.concat(
                    F.col("eligible_days").cast("string"),
                    F.lit("개 유효 표본일 중 "),
                    F.col("active_days").cast("string"),
                    F.lit("일 사용했습니다."),
                ),
            ).otherwise(F.lit("사용 비율을 계산할 유효 표본이 없습니다.")),
        )
        .withColumn(
            "values_json",
            F.to_json(
                F.struct(
                    "eligible_days",
                    "active_days",
                    "use_probability",
                    "unknown_days",
                    "missing_days",
                )
            ),
        )
        .select(
            "household_id",
            "evidence_id",
            "evidence_type",
            "appliance_type",
            "statement",
            "values_json",
            "quality_status",
        )
    )
    assessment = (
        assessment_detail.withColumn("evidence_type", F.lit("RECORDED_ASSESSMENT"))
        .withColumn(
            "evidence_id",
            F.concat(F.lit("assessment:"), F.col("assessment_id").cast("string")),
        )
        .withColumn("appliance_type", F.lit(None).cast("string"))
        .withColumn("statement", F.when(F.col("assessment_mode") == "EVENT_TIME_REASSESSMENT",
                                       "과거 관측 시점 기준으로 다시 계산한 평가입니다.")
                    .when(F.col("assessment_mode") == "LIVE_RECORDED", "모니터링 DB에 저장된 평가입니다.")
                    .otherwise("평가 출처가 확인되지 않은 기록입니다."))
        .withColumn(
            "values_json",
            F.to_json(
                F.struct(
                    "assessment_id",
                    "assessment_mode",
                    "assessed_at",
                    "assessment_status",
                    "risk_score",
                    "risk_level",
                    "profile_version",
                    "policy_version",
                    "score_version",
                    "indicators",
                )
            ),
        )
        .withColumn(
            "quality_status",
            F.when(F.col("indicators").isNull(), "MISSING_INDICATOR_EVIDENCE").otherwise(
                "RECORDED"
            ),
        )
        .select(
            "household_id",
            "evidence_id",
            "evidence_type",
            "appliance_type",
            "statement",
            "values_json",
            "quality_status",
        )
    )
    return usage.unionByName(assessment)
