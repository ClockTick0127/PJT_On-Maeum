from aggregation_service.__main__ import main


def test_aggregation_entrypoint_is_importable() -> None:
    assert callable(main)
