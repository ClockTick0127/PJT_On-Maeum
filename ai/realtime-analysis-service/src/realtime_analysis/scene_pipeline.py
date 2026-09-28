"""Durable selected-scene inference, isolated from production risk/session logic."""

from datetime import datetime, timezone
import hashlib
import json
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import JSON, String, BigInteger, select
from sqlalchemy.orm import Mapped, mapped_column

from realtime_analysis.database import Base
from realtime_analysis.predictor import APPLIANCE_ORDER
from realtime_analysis.schemas import ProcessingSource
from realtime_analysis.selected_scene import SelectedSceneDecoder
from realtime_analysis.scene_contracts import Measurement, Runtime, Snapshot
from realtime_analysis.pipeline_timing import stage

FEATURES = ("active_power", "reactive_power", "power_factor", "current")


class SceneMeasurement(Measurement):
    """The shared strict scene input contract."""


class SceneEvidence(Base):
    __tablename__ = "selected_scene_evidence"
    household_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    profile_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    source_index: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    input_sha256: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict] = mapped_column(JSON)
    checkpoint: Mapped[dict] = mapped_column(JSON)


class SceneRepository:
    def __init__(self, session_factory, projector=None, event_publisher=None, final_index=None):
        self._sessions = session_factory
        self.projector, self.event_publisher, self.final_index = projector, event_publisher, final_index

    @property
    def projection_config(self):
        return self.projector.config if self.projector else None

    def flush_events(self, measurement):
        if self.projector is not None:
            from realtime_analysis.scene_events import flush_outbox
            flush_outbox(self._sessions, self.event_publisher, measurement.household_id,
                         measurement.run_id, measurement.profile_id)

    def get(self, measurement):
        with self._sessions() as session:
            row = session.get(SceneEvidence, (measurement.household_id, measurement.run_id,
                                             measurement.profile_id, measurement.source_index))
            return (row.input_sha256, row.payload) if row else None

    def latest(self, measurement):
        with self._sessions() as session:
            row = session.scalar(select(SceneEvidence).where(
                SceneEvidence.household_id == measurement.household_id,
                SceneEvidence.run_id == measurement.run_id,
                SceneEvidence.profile_id == measurement.profile_id,
            ).order_by(SceneEvidence.source_index.desc()).limit(1))
            return row.checkpoint if row else None

    def save(self, measurement, digest, payload, checkpoint):
        # All volatile inference state and its output commit atomically. A failed
        # publish retries this same payload before Kafka stores its input offset.
        with self._sessions.begin() as session:
            session.add(SceneEvidence(household_id=measurement.household_id,
                run_id=measurement.run_id, profile_id=measurement.profile_id,
                source_index=measurement.source_index, input_sha256=digest,
                payload=payload, checkpoint=checkpoint))
            if self.projector is not None:
                self.projector.apply(session, payload, self.final_index)


class SceneHandler:
    def __init__(self, predictor, repository, publisher, run_id: str, household_id: str | None = None):
        self.predictor, self.repository, self.publisher = predictor, repository, publisher
        self.run_id = run_id
        self.household_id = household_id
        self.lease_check = lambda: None
        Runtime.model_validate_json(json.dumps(predictor.runtime, allow_nan=False))

    def reset_household(self, household_id: str) -> None:
        # No process-local window survives a call. The next owner restores the
        # committed window from the database, including its UNKNOWN state.
        pass

    def __call__(self, measurement: SceneMeasurement, source: ProcessingSource | None = None):
        self.lease_check()
        profile = self.predictor.profile
        if self.household_id is not None and measurement.household_id != self.household_id:
            raise ValueError("Input household does not match configured selected scene")
        if measurement.run_id != self.run_id or measurement.profile_id != profile["profile_id"]:
            raise ValueError("Input run/profile does not match configured selected scene")
        canonical = measurement.model_dump(mode="json", exclude={"message_id"})
        digest = hashlib.sha256(json.dumps(canonical, sort_keys=True).encode()).hexdigest()
        existing = self.repository.get(measurement)
        if existing:
            if existing[0] != digest:
                raise ValueError("Conflicting duplicate source_index")
            previous = self.repository.latest(measurement)
            if existing[1]['runtime'] != self.predictor.runtime or previous.get('projection_config') != self.repository.projection_config:
                raise ValueError('Runtime or event policy changed; use a new run_id')
            self.publisher.publish(existing[1])
            self.repository.flush_events(measurement)
            return
        previous = self.repository.latest(measurement)
        if previous:
            if measurement.source_index != previous["source_index"] + 1:
                raise ValueError("Require chronological input with explicit missing seconds")
            if (measurement.measured_at - datetime.fromisoformat(previous["measured_at"])).total_seconds() != 1:
                raise ValueError("Scene measurement time must advance exactly one second")
            if previous["runtime"] != self.predictor.runtime:
                raise ValueError("Runtime changed; use a new run_id")
            if previous.get('projection_config') != self.repository.projection_config:
                raise ValueError('Event policy changed; use a new run_id')
            window = list(previous["window"])
        else:
            if measurement.source_index != profile["source_prefix_start_index"]:
                raise ValueError("New run must start at the frozen prefix start")
            window = []
        if measurement.source_index >= profile["source_clip_end_exclusive"]:
            raise ValueError("Input outside frozen scene")
        window.append([getattr(measurement, key) for key in FEATURES] if measurement.valid
                      else list(self.predictor.missing_feature_row))
        window = window[-255:]
        ready = len(window) == 255 and measurement.context
        with stage("scene_model_forward"):
            score = self.predictor.predict(window)[0].probability if ready else None
        decoder = SelectedSceneDecoder(**{k: profile["threshold"][k] for k in ("on", "off", "confirm")})
        if previous:
            decoder.state = previous["state"]
        decision = decoder.step(score)
        identity = [measurement.household_id, self.run_id, profile["profile_id"], measurement.source_index]
        target = profile["appliance_type"].upper()
        payload = {
            "schema_version": 2, "scope": "selected_scene",
            "snapshot_id": str(uuid5(NAMESPACE_URL, json.dumps(identity))),
            "household_id": measurement.household_id, "run_id": self.run_id,
            "profile_id": profile["profile_id"], "source_index": measurement.source_index,
            "observed_at": measurement.measured_at.isoformat(),
            "published_at": datetime.now(timezone.utc).isoformat(),
            "target_appliance": target, "ready": ready, "transition": decision.transition,
            "measurement": {key: getattr(measurement, key) for key in FEATURES} if measurement.valid else None,
            "source": source.model_dump() if source else None,
            "runtime": self.predictor.runtime,
            "appliances": [{"appliance_type": name, "inferred": name == target and ready,
                "probability": score if name == target else None,
                "state": decision.state if name == target else "UNKNOWN",
                "is_on": (decision.state == "ON" if decision.state != "UNKNOWN" else None)
                         if name == target else None} for name in APPLIANCE_ORDER],
        }
        Snapshot.model_validate_json(json.dumps(payload, allow_nan=False))
        checkpoint = {"source_index": measurement.source_index,
            "measured_at": measurement.measured_at.isoformat(), "window": window,
            "state": decision.state, "runtime": self.predictor.runtime}
        checkpoint['projection_config'] = self.repository.projection_config
        self.repository.save(measurement, digest, payload, checkpoint)
        self.publisher.publish(payload)
        self.repository.flush_events(measurement)


def run_selected_scene(settings, stop_event):
    from confluent_kafka.admin import AdminClient
    from realtime_analysis.consumer import AnalysisConsumer
    from realtime_analysis.database import create_session_factory
    from realtime_analysis.dlq import DlqPublisher
    from realtime_analysis.health_server import ObservabilityServer
    from realtime_analysis.readiness import ReadinessProbe, database_readiness_check, kafka_readiness_check
    from realtime_analysis.real_predictor import SelectedScenePredictor
    from realtime_analysis.snapshot_publisher import AnalysisSnapshotPublisher
    import torch
    torch.set_num_threads(settings.model_threads)
    predictor = SelectedScenePredictor(settings.model_asset_root, settings.model_appliance,
                                       device=settings.model_device, dtype=settings.model_dtype)
    sessions = create_session_factory(settings)
    publisher = AnalysisSnapshotPublisher(settings, topic=settings.kafka_scene_snapshot_topic)
    if not settings.model_household_id:
        raise ValueError("MODEL_HOUSEHOLD_ID is required for a deployed selected-scene worker")
    from realtime_analysis.scene_events import SceneProjector, SceneEventPublisher, flush_outbox
    projector = (SceneProjector(settings.scene_risk_threshold_seconds, settings.scene_test_household_id,
                                settings.scene_policy_id) if settings.scene_events_enabled else None)
    event_publisher = SceneEventPublisher(settings) if projector else None
    repository = SceneRepository(sessions, projector, event_publisher, predictor.profile['source_clip_end_exclusive'] - 1)
    handler = SceneHandler(predictor, repository, publisher, settings.analysis_run_id,
                           settings.model_household_id)
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    checks = {
        "model": lambda: predictor is not None,
        "database": database_readiness_check(sessions),
        "kafka_input": kafka_readiness_check(admin, settings.kafka_input_topic, settings.readiness_timeout_seconds),
        "kafka_scene_output": kafka_readiness_check(admin, settings.kafka_scene_snapshot_topic, settings.readiness_timeout_seconds),
    }
    if projector:
        from realtime_analysis.scene_events import SESSION_TOPIC, RISK_TOPIC
        checks['kafka_sessions'] = kafka_readiness_check(admin, SESSION_TOPIC, settings.readiness_timeout_seconds)
        checks['kafka_risks'] = kafka_readiness_check(admin, RISK_TOPIC, settings.readiness_timeout_seconds)
    readiness = ReadinessProbe(checks)
    health = ObservabilityServer(settings.http_host, settings.http_port, readiness)
    consumer = AnalysisConsumer(settings, DlqPublisher(settings), handler)
    from realtime_analysis.scene_lease import household_lease
    with household_lease(sessions, settings.model_household_id) as check_alive:
        handler.lease_check = check_alive
        if projector:
            flush_outbox(sessions, event_publisher, settings.model_household_id, settings.analysis_run_id,
                         predictor.profile['profile_id'])
        health.start()
        try:
            consumer.run(stop_event)
        finally:
            health.stop()
