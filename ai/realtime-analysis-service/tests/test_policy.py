from realtime_analysis.policy import PolicyRepository


def test_bootstrap_policy_file_contains_four_event_types() -> None:
    repository = PolicyRepository.from_json_file(
        "config/analysis_policies.json"
    )

    assert {policy.event_type for policy in repository.policies} == {
        "ROUTINE_MISSED",
        "PROLONGED_INACTIVITY",
        "PROLONGED_APPLIANCE_USE",
        "ROUTINE_CHANGED",
    }
    assert all("score" not in policy.model_dump() for policy in repository.policies)
    assert all("severity" not in policy.model_dump() for policy in repository.policies)

    inactivity = next(
        policy
        for policy in repository.policies
        if policy.event_type == "PROLONGED_INACTIVITY"
    )
    assert inactivity.algorithm_type == "AWAKE_INACTIVITY_ELAPSED"
    assert inactivity.parameters["inactivity_hours"] == 2
    assert inactivity.parameters["sleep_window"] == {
        "start": "23:00",
        "end": "07:00",
    }

    appliance_use = next(
        policy
        for policy in repository.policies
        if policy.event_type == "PROLONGED_APPLIANCE_USE"
    )
    assert appliance_use.parameters["limits_minutes"] == {
        "INDUCTION": 2,
        "IRON": 2,
    }
