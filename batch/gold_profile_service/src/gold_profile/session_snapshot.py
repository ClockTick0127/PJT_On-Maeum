"""Guard against combining daily slices prepared from different session states."""

from __future__ import annotations

from dataclasses import dataclass
import re


_SESSION_TOKEN = re.compile(r"(?:^|;)sessions=([^;]+)")


def session_state_token(ref) -> str | None:
    """Return the session manifest-set token embedded by analysis-usage-daily."""

    match = _SESSION_TOKEN.search(ref.config_version or "")
    return match.group(1) if match else None


@dataclass(frozen=True)
class SessionSnapshotCheck:
    aligned: bool
    token: str | None
    missing_dates: tuple
    missing_usage_dates: tuple
    missing_slice_dates: tuple
    provenance_missing_dates: tuple
    version_mismatch_dates: tuple
    different_token_dates: tuple
    tokens_by_date: dict


def inspect_session_snapshot(
    usage_refs,
    slice_refs,
    expected_dates,
    *,
    expected_token: str | None = None,
) -> SessionSnapshotCheck:
    """Check the exact condition Gold relies on for session consistency.

    Both outputs for a date must exist, come from the same analysis run, carry
    the same session provenance, and every date must carry one global token.
    An empty session day still has a zero-row slice *version*, so it passes.
    """

    usage = {ref.target_date: ref for ref in usage_refs}
    slices = {ref.target_date: ref for ref in slice_refs}
    expected = tuple(expected_dates)
    missing_usage = tuple(day for day in expected if day not in usage)
    missing_slices = tuple(day for day in expected if day not in slices)
    missing = tuple(
        day for day in expected if day not in usage or day not in slices
    )
    provenance_missing = []
    version_mismatch = []
    tokens_by_date = {}
    for day in expected:
        usage_ref = usage.get(day)
        slice_ref = slices.get(day)
        if usage_ref is None or slice_ref is None:
            continue
        usage_token = session_state_token(usage_ref)
        slice_token = session_state_token(slice_ref)
        if usage_token is None or slice_token is None:
            provenance_missing.append(day)
            continue
        if usage_ref.run_id != slice_ref.run_id or usage_token != slice_token:
            version_mismatch.append(day)
            continue
        tokens_by_date[day] = usage_token

    token = expected_token
    if token is None and tokens_by_date:
        unique = set(tokens_by_date.values())
        token = next(iter(unique)) if len(unique) == 1 else None
    different = tuple(
        day
        for day in expected
        if day in tokens_by_date and (
            token is None or tokens_by_date[day] != token
        )
    )
    aligned = not (
        missing
        or provenance_missing
        or version_mismatch
        or different
        or token is None
    )
    return SessionSnapshotCheck(
        aligned=aligned,
        token=token,
        missing_dates=missing,
        missing_usage_dates=missing_usage,
        missing_slice_dates=missing_slices,
        provenance_missing_dates=tuple(provenance_missing),
        version_mismatch_dates=tuple(version_mismatch),
        different_token_dates=different,
        tokens_by_date=tokens_by_date,
    )


def require_one_session_snapshot(refs, expected_dates) -> str:
    """Require every selected date to be rebuilt from one global session state.

    A delete produces no slice, so looking only at slice rows cannot distinguish
    deletion from a date that has not yet been rebuilt.  The upstream daily job
    embeds its global session-manifest-set digest in ``config_version``.  Gold
    must wait until all dates in its window carry the same digest.
    """

    refs = list(refs)
    expected_dates = tuple(expected_dates)
    if not refs and not expected_dates:
        return "NO_SESSION_INPUT"
    if len(refs) != len(expected_dates):
        raise ValueError("session slices are missing for part of the profile window")
    tokens = {session_state_token(ref) for ref in refs}
    if None in tokens:
        raise ValueError("session slice version lacks sessions=<manifest-set> provenance")
    if len(tokens) != 1:
        raise ValueError(
            "session slice dates were prepared from different session snapshots: "
            + ", ".join(sorted(tokens))
        )
    return next(iter(tokens))


def require_matching_session_outputs(
    usage_refs,
    slice_refs,
    expected_dates,
    *,
    expected_token: str | None = None,
) -> str:
    """Require usage summaries and slices to prove the same selected input."""

    check = inspect_session_snapshot(
        usage_refs, slice_refs, expected_dates, expected_token=expected_token
    )
    if check.missing_dates:
        raise ValueError("usage or session slice versions are missing for the profile window")
    if check.provenance_missing_dates:
        raise ValueError("analysis version lacks sessions=<manifest-set> provenance")
    if check.version_mismatch_dates:
        raise ValueError("daily usage and session slices do not share one analysis run")
    if check.different_token_dates or check.token is None:
        raise ValueError(
            "profile dates were prepared from different session snapshots"
        )
    return check.token
