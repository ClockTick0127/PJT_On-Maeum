from realtime_analysis.config import Settings


def test_default_topic_contracts() -> None:
    settings = Settings(_env_file=None)

    assert settings.kafka_input_topic == "power.raw.v1"
    assert settings.kafka_analysis_event_topic == "analysis.event.v1"
    assert settings.consumer_config()["enable.auto.commit"] is False


def test_fake_appliance_setting_is_parsed() -> None:
    settings = Settings(
        _env_file=None,
        fake_on_appliances=" microwave, hair_dryer ",
    )

    assert settings.fake_on_appliance_types == ("MICROWAVE", "HAIR_DRYER")
