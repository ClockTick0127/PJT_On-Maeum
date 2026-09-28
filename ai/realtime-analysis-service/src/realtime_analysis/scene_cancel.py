"""Explicitly interrupt one stopped scene; never synthesizes an OFF transition."""
from realtime_analysis.config import get_settings
from realtime_analysis.database import create_session_factory
from realtime_analysis.real_predictor import load_profile
from realtime_analysis.scene_events import SceneProjector, SceneEventPublisher, flush_outbox
from realtime_analysis.scene_lease import household_lease


def main():
    settings = get_settings()
    if settings.model_backend != 'selected_scene' or not settings.model_household_id or not settings.scene_events_enabled:
        raise ValueError('Cancellation requires explicit selected-scene run/household configuration and events')
    sessions = create_session_factory(settings)
    scope = dict(household_id=settings.model_household_id, run_id=settings.analysis_run_id,
                 profile_id=load_profile(settings.model_appliance)['profile_id'])
    with household_lease(sessions, settings.model_household_id):
        with sessions.begin() as db: SceneProjector().cancel(db, scope)
        flush_outbox(sessions, SceneEventPublisher(settings), *scope.values())
    print('Scene cancelled; physical OFF was not inferred.')


if __name__ == '__main__': main()
