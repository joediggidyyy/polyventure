"""Tests for the locked CANDIDATES panel contract — SC-CP1 through SC-CP9.

Validation classes:
- pytest (source inspection): SC-CP1, SC-CP2, SC-CP4, SC-CP5, SC-CP9
- pytest (DB fixture): SC-CP3
"""
from __future__ import annotations

import inspect
import json
import os
import sqlite3
import tempfile
from typing import Any

from polyventure import web_app
from polyventure.persistence import (
  open_database,
  persist_candidate_review_candidates,
  persist_candidate_review_run,
)
from polyventure.web_app import _fetch_stage_columns, _STAGE_COLUMNS_EMPTY


# ---------------------------------------------------------------------------
# SC-CP1: candidates found: 0 removed from summary strip
# ---------------------------------------------------------------------------

def test_summary_strip_silent_when_no_review_candidates() -> None:
    src = inspect.getsource(web_app)
    assert "candidates found: 0" not in src, (
        "SC-CP1 regression: 'candidates found: 0' string still present in web_app source"
    )


# ---------------------------------------------------------------------------
# SC-CP2: 3 metric rows removed from review card; closes row retained
# ---------------------------------------------------------------------------

def test_review_card_has_no_detail_metric_rows() -> None:
    src = inspect.getsource(web_app)
    # The three metric labels must not appear in card template context
    # (they may still exist in comments, but not as metric-label strings in template)
    # We check the specific template strings that would appear inside candidateSectionMarkup
    for label in ('edge/density', 'market activity', 'threshold'):
        # Search for it as a metric-label value in the card template
        marker = f'candidate-metric-label">{label}'
        assert marker not in src, (
            f"SC-CP2 regression: metric row '{label}' still present in review card template"
        )


def test_review_card_retains_closes_row() -> None:
    src = inspect.getsource(web_app)
    assert 'closes' in src, "SC-CP2: closes row absent — should be retained on review card"
    assert 'formatCandidateTimeContext' in src, (
        "SC-CP2: formatCandidateTimeContext call absent — closes row likely removed"
    )


# ---------------------------------------------------------------------------
# SC-CP3: backend enriches _fetch_stage_columns items to dicts
# ---------------------------------------------------------------------------

_LANE_SESSION_ID = 'live-20260615T000000Z-testfix'


def _make_db_with_candidates(candidates: list[dict[str, Any]]) -> tuple[str, str]:
    """Create a temp sqlite3 DB with candidate rows; return (db_path, lane_session_id)."""
    run_id = 'test-run-001'
    fd, tmp = tempfile.mkstemp(suffix='.sqlite3')
    import os as _os
    _os.close(fd)
    conn = open_database(tmp)
    persist_candidate_review_run(
        conn,
        run_id=run_id,
        recorded_at_utc='2026-06-15T00:00:00Z',
        operation_lane='live',
        lane_session_id=_LANE_SESSION_ID,
        candidate_signature='sig',
        candidate_count=len(candidates),
        source_action='scan',
    )
    for idx, c in enumerate(candidates):
        uid = c.get('candidate_key', f'key{idx}')
        detail_dict = json.loads(c.get('detail_json') or '{}')
        persist_candidate_review_candidates(
            conn,
            run_id=run_id,
            recorded_at_utc='2026-06-15T00:00:01Z',
            operation_lane='live',
            candidates=[{
                'candidate_uid': uid,
                'candidate_key': uid,
                'ticker': c['ticker'],
                'qualifier_tier': c.get('qualifier_tier', ''),
                'review_row_origin': 'current',
                **detail_dict,
            }],
        )
        lifecycle_stage = c.get('lifecycle_stage', 'discovered')
        if lifecycle_stage != 'discovered':
            conn.execute(
                'UPDATE candidate_review_candidates SET lifecycle_stage = ?, terminal_cause = ? '
                'WHERE candidate_uid = ? AND run_id = ?',
                (lifecycle_stage, c.get('terminal_cause', ''), uid, run_id),
            )
            conn.commit()
    return tmp, _LANE_SESSION_ID


def test_fetch_stage_columns_items_are_enriched_objects() -> None:
    detail = json.dumps({'close_time_utc': '2026-06-12T18:00:00Z'})
    db_path, lane_session_id = _make_db_with_candidates([
        {'ticker': 'KXBTCD24', 'lifecycle_stage': 'in_flight', 'qualifier_tier': 'live_qualifying', 'detail_json': detail},
    ])
    payload: dict[str, Any] = {
        'review_selection': {'persisted_lane_session_id': lane_session_id},
        'settings': {'state_db_path': db_path},
    }
    result = _fetch_stage_columns(payload)
    queued = result['stage_columns'][0]['items']
    assert len(queued) == 1, f"Expected 1 queued item, got {len(queued)}"
    item = queued[0]
    assert isinstance(item, dict), f"Expected dict item, got {type(item)}: {item!r}"
    assert 'ticker' in item
    assert 'qualifier_tier' in item
    assert 'close_time' in item
    assert item['ticker'] == 'KXBTCD24'
    assert item['qualifier_tier'] == 'live_qualifying'


def test_fetch_stage_columns_cancelled_items_have_terminal_cause() -> None:
    db_path, lane_session_id = _make_db_with_candidates([
        {'ticker': 'BTCD24', 'lifecycle_stage': 'terminal', 'terminal_cause': 'expired_unfilled',
         'qualifier_tier': 'sandbox_extended', 'detail_json': '{}'},
    ])
    payload: dict[str, Any] = {
        'review_selection': {'persisted_lane_session_id': lane_session_id},
        'settings': {'state_db_path': db_path},
    }
    result = _fetch_stage_columns(payload)
    cancelled = result['stage_columns'][2]['items']
    assert len(cancelled) == 1
    item = cancelled[0]
    assert isinstance(item, dict)
    assert item.get('terminal_cause') == 'expired_unfilled'
    assert item.get('ticker') == 'BTCD24'


# ---------------------------------------------------------------------------
# SC-CP4: stage card template uses new article format; backward compat
# ---------------------------------------------------------------------------

def test_stage_card_renders_qualifier_from_object_item() -> None:
    src = inspect.getsource(web_app)
    assert 'stage-candidate-card' in src, (
        "SC-CP4: stage-candidate-card class missing from source"
    )
    assert 'candidate-pill' in src, (
        "SC-CP4: candidate-pill missing from stage card template area"
    )
    assert 'qualLabel' in src or 'qual_label' in src.lower() or 'qualifierTier' in src, (
        "SC-CP4: qualifier label logic absent from stage card template"
    )


def test_stage_card_fallback_for_string_items() -> None:
    src = inspect.getsource(web_app)
    assert "typeof item === 'string'" in src, (
        "SC-CP4: string-item backward-compat check absent — string fallback will break"
    )


def test_stage_card_has_click_handler_when_candidate_key_present() -> None:
    """W5: stage cards open the shared candidate detail modal when a candidate_key is available."""
    src = inspect.getsource(web_app)
    assert 'stageCandidateKey' in src, (
        "W5: stage card template does not derive an opener key from the stage item"
    )
    open_attrs_block = src.split('const stageOpenAttrs')[1][:400]
    assert 'data-candidate-open' in open_attrs_block, (
        "W5: stageOpenAttrs does not set data-candidate-open — click-to-open contract not wired"
    )
    assert "role=\"button\"" in open_attrs_block, (
        "W5: stageOpenAttrs missing role=button for the open affordance"
    )
    assert 'tabindex="0"' in open_attrs_block, (
        "W5: stageOpenAttrs missing tabindex for keyboard open"
    )
    stage_card_block = src.split('class="stage-candidate-card"')[1].split('stage-card-dismiss')[0]
    assert '${stageOpenAttrs}' in stage_card_block, (
        "W5: stage card article tag does not apply stageOpenAttrs"
    )


def test_stage_card_reuses_existing_candidate_open_listener() -> None:
    """W5: no duplicate click/keyboard wiring — the Found/Saved [data-candidate-open] listener is shared."""
    src = inspect.getsource(web_app)
    assert src.count("querySelectorAll('[data-candidate-open]')") == 1, (
        "W5: expected a single [data-candidate-open] listener wiring shared by review and stage cards"
    )


def test_stage_open_guard_does_not_close_on_next_payload() -> None:
    """W5: the detailKey auto-clear guard must also recognize stage-column keys, not only Found/Saved keys."""
    src = inspect.getsource(web_app)
    assert 'findStageCandidateItem(state.candidateSelection.detailKey, payload)' in src, (
        "W5: applyBackendCandidateSelection guard does not check stage-column membership — "
        "a stage-opened popup would self-close on the next payload refresh"
    )


# ---------------------------------------------------------------------------
# SC-CP5: candidateReviewShellVisible gate removed
# ---------------------------------------------------------------------------

def test_review_shell_always_rendered_when_no_candidates() -> None:
    src = inspect.getsource(web_app)
    # The ternary gate pattern must be gone
    assert "candidateReviewShellVisible(candidateView, reviewSelection) ? (" not in src, (
        "SC-CP5 regression: candidateReviewShellVisible gate still present — review shell not always rendered"
    )


def test_review_shell_empty_state_message_when_no_rows() -> None:
    src = inspect.getsource(web_app)
    # candidateReviewCurrentEmptyMessage should still be called (provides the empty state message)
    assert 'candidateReviewCurrentEmptyMessage' in src, (
        "SC-CP5: candidateReviewCurrentEmptyMessage absent — empty state message path broken"
    )


# ---------------------------------------------------------------------------
# SC-CP7/8: Cancelled card has terminal_cause badge and dismiss button
# ---------------------------------------------------------------------------

def test_cancelled_stage_card_has_terminal_cause_badge() -> None:
    src = inspect.getsource(web_app)
    assert 'terminal-cause' in src, (
        "SC-CP7/8: terminal-cause pill class absent from source"
    )
    assert 'causeLabel' in src or 'termCause' in src, (
        "SC-CP7/8: terminal cause label logic absent"
    )


def test_cancelled_stage_card_has_dismiss_button() -> None:
    src = inspect.getsource(web_app)
    assert 'stage-card-dismiss' in src, (
        "SC-CP7/8: stage-card-dismiss class absent — dismiss button missing"
    )
    assert 'data-dismiss-ticker' in src, (
        "SC-CP7/8: data-dismiss-ticker attribute absent — dismiss listener cannot find ticker"
    )


# ---------------------------------------------------------------------------
# SC-CP9: Dismiss is session-scoped (no API call / fetch)
# ---------------------------------------------------------------------------

def test_cancelled_dismiss_no_api_call() -> None:
    src = inspect.getsource(web_app)
    # Find the dismiss handler block and confirm no fetch/xhr inside it
    dismiss_idx = src.find('stage-card-dismiss')
    assert dismiss_idx >= 0, "SC-CP9: dismiss handler not found in source"
    # Extract a window around the dismiss handler
    handler_window = src[dismiss_idx:dismiss_idx + 600]
    assert 'fetch(' not in handler_window, (
        "SC-CP9: fetch() call found inside dismiss handler — dismiss must be session-scoped only"
    )
    assert 'XMLHttpRequest' not in handler_window, (
        "SC-CP9: XMLHttpRequest found inside dismiss handler — dismiss must be session-scoped only"
    )


def test_dismissed_state_is_on_state_object() -> None:
    src = inspect.getsource(web_app)
    assert 'dismissedCancelledTickers' in src, (
        "SC-CP9: dismissedCancelledTickers absent — dismiss state not tracked on state object"
    )


def test_cancelled_dismiss_stops_propagation_and_does_not_open_modal() -> None:
    """W5: dismiss must not bubble into the card's open handler."""
    src = inspect.getsource(web_app)
    dismiss_idx = src.find("querySelectorAll('.stage-card-dismiss')")
    assert dismiss_idx >= 0, "W5: dismiss listener wiring not found in source"
    handler_window = src[dismiss_idx:dismiss_idx + 400]
    assert 'event.stopPropagation()' in handler_window, (
        "W5: dismiss handler no longer calls stopPropagation — dismiss clicks would open the modal"
    )
    assert 'data-candidate-open' not in handler_window, (
        "W5: dismiss handler block must not itself set the modal-open attribute"
    )


# ---------------------------------------------------------------------------
# W5: stage-aware modal resolver and read-only field rendering
# ---------------------------------------------------------------------------

def test_resolve_candidate_detail_checks_review_then_stage() -> None:
    src = inspect.getsource(web_app)
    assert 'function resolveCandidateDetail(' in src, (
        "W5: resolveCandidateDetail resolver missing"
    )
    assert 'function findStageCandidateItem(' in src, (
        "W5: findStageCandidateItem lookup missing"
    )
    resolver_src = src.split('function resolveCandidateDetail(')[1][:900]
    assert 'findCandidateRow(candidateKey)' in resolver_src, (
        "W5: resolver must check Found/Saved rows first when not explicitly opened from a stage card"
    )
    assert 'findStageCandidateItem(candidateKey)' in resolver_src, (
        "W5: resolver must fall back to stage-column items"
    )


def test_resolve_candidate_detail_forces_stage_view_when_opened_from_stage() -> None:
    """W5: a card opened from a stage column must never resolve to the mutable review view,
    even when its candidate_key still happens to appear in the Found/Saved list (e.g. a Queued
    item not yet de-listed there) — the click origin decides the view, not keyset priority."""
    src = inspect.getsource(web_app)
    resolver_src = src.split('function resolveCandidateDetail(')[1][:900]
    assert "openedFrom === 'stage'" in resolver_src, (
        "W5: resolver does not branch on the stage-card open origin"
    )
    stage_branch = resolver_src.split("openedFrom === 'stage'")[1].split('const reviewCandidate')[0]
    assert 'findCandidateRow' not in stage_branch, (
        "W5: stage-origin branch must not fall back to the Found/Saved review lookup"
    )
    assert 'findStageCandidateItem(candidateKey)' in stage_branch, (
        "W5: stage-origin branch must resolve strictly via the stage-column item"
    )


def test_stage_card_carries_open_origin_and_listener_passes_it_through() -> None:
    """W5: stage cards tag themselves as the open origin so the shared listener and the
    resolver can force the read-only view regardless of Found/Saved keyset membership."""
    src = inspect.getsource(web_app)
    open_attrs_block = src.split('const stageOpenAttrs')[1][:400]
    assert 'data-candidate-source="stage"' in open_attrs_block, (
        "W5: stageOpenAttrs does not tag the card with its open origin"
    )
    listener_src = src.split("querySelectorAll('[data-candidate-open]')")[1][:300]
    assert 'card.dataset.candidateSource' in listener_src, (
        "W5: shared open listener does not forward the card's open-origin dataset attribute"
    )
    assert 'function openCandidateDetail(candidateKey, openedFrom' in src, (
        "W5: openCandidateDetail does not accept/store the open origin"
    )


def test_stage_source_modal_is_read_only() -> None:
    src = inspect.getsource(web_app)
    modal_src = src.split('function renderCandidateDetailModal(')[1][:2000]
    stage_branch = modal_src.split("resolution.source === 'stage'")[1].split('return;')[0]
    assert 'data-candidate-toggle' not in stage_branch, (
        "W5: stage modal branch must not set a selection-toggle attribute"
    )
    assert "toggle.hidden = true" in stage_branch, (
        "W5: stage modal branch must hide the selection toggle"
    )
    assert "toggle.dataset.candidateDetailKey = ''" in stage_branch, (
        "W5: stage modal branch must not leave a mutable selection key on the toggle"
    )
    for forbidden_route in ('/api/review-selection', '/api/runtime-overlay'):
        assert forbidden_route not in stage_branch, (
            f"W5: stage modal branch must not reference mutation route {forbidden_route}"
        )


# ---------------------------------------------------------------------------
# Regression guard: _STAGE_COLUMNS_EMPTY still has 3 entries
# ---------------------------------------------------------------------------

def test_stage_columns_empty_has_three_entries() -> None:
    assert len(_STAGE_COLUMNS_EMPTY) == 3
    stage_ids = {s['stage_id'] for s in _STAGE_COLUMNS_EMPTY}
    assert stage_ids == {'queued', 'filled', 'cancelled'}
