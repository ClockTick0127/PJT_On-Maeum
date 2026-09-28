import asyncio
from datetime import datetime, timezone
from pathlib import Path
import ssl

import aiomqtt
import pytest
from realtime_analysis.fixture_publisher import mqtt_tls, prepare, replay_mqtt

ASSETS = Path(__file__).resolve().parents[2] / 'assets/nilm_r3'


def prepared():
    if not ASSETS.exists(): pytest.skip('Fixture assets are outside the service-only build context')
    return prepare(ASSETS, 'kettle', 'test-house', 'fixture-test', datetime(2026, 9, 21, tzinfo=timezone.utc),
                   'v1/power/demo/{household_id}/main')


def test_tls_verifies_host_and_certificate():
    context = mqtt_tls()
    assert context.check_hostname and context.verify_mode == ssl.CERT_REQUIRED
    assert context.minimum_version >= ssl.TLSVersion.TLSv1_2
    assert mqtt_tls(True) is None
    with pytest.raises(ValueError): mqtt_tls(True, 'ca.crt')
    with pytest.raises(FileNotFoundError): mqtt_tls(False, 'missing-ca.crt')


def test_preflight_and_stable_retries():
    topic, rows, profile = prepared()
    assert topic == 'v1/power/demo/test-house/main'
    assert len(rows) == profile['expected_source_rows'] == 592
    assert [r.model_dump_json() for r in rows] == [r.model_dump_json() for r in prepared()[1]]
    assert all((b.measured_at-a.measured_at).total_seconds() == 1 for a,b in zip(rows,rows[1:]))


@pytest.mark.parametrize('house,topic', [('bad/house','x/{household_id}'), ('house','x/#/{household_id}'), ('house','x')])
def test_invalid_topic_rejected_before_connection(house, topic):
    with pytest.raises(ValueError):
        prepare(ASSETS,'kettle',house,'run',datetime.now(timezone.utc),topic)


def test_ack_is_not_backend_completion_and_retry_ids_are_same():
    topic, rows, _ = prepared()
    sent, calls = [], []
    class Client:
        def __init__(self, **kwargs): calls.append(kwargs)
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def publish(self, topic, payload, qos):
            sent.append(payload)
            assert qos == 1
            if len(calls) == 1: raise aiomqtt.MqttError('disconnect')
    report = asyncio.run(replay_mqtt(rows[:2], topic, hostname='localhost', port=1883,
        tls_context=None, interval=0, retries=1, client_factory=Client))
    assert sent[0] == sent[1]
    assert report['attempts'] == 2 and not report['backend_completion_verified']


def test_permanent_transport_failure_is_not_success():
    topic, rows, _ = prepared()
    class Client:
        def __init__(self, **kwargs): pass
        async def __aenter__(self): raise aiomqtt.MqttError('failed')
        async def __aexit__(self, *args): pass
    with pytest.raises(aiomqtt.MqttError):
        asyncio.run(replay_mqtt(rows,topic,hostname='localhost',port=1883,tls_context=None,retries=0,client_factory=Client))
