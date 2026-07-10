from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
import sqlite3
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import ROUND_FLOOR, Decimal, InvalidOperation
import logging
from pathlib import Path
from typing import Any, Callable, Literal
from urllib.parse import urlparse
from uuid import uuid4

from . import signed_evidence
from .auth import load_private_key
from .candidate_identity import canonical_candidate_key, canonical_candidate_uid
from .config import Settings, load_settings, resolve_private_key_path, safe_settings_summary, websocket_url_is_valid
from .execution import cancel_pair, compute_locked_pnl, reconcile_pair, simulate_cancel_pair, simulate_partial_fill, simulate_submit_pair
from .http_client import KalshiHttpClient, KalshiHttpError, kalshi_error_safe_detail, submitted_order_from_payload
from .kalshi_units import (
  KalshiUnitError,
  count_contracts_to_int,
  group_limit_to_wire,
  outbound_leg_price_dollars,
  price_dollars_to_fp4,
  restore_leg_price_dollars,
)
from .market_data import fetch_open_markets
from .persistence import (
  dismiss_all_operator_notifications,
  dismiss_operator_notification,
  fetch_candidate_saved_set_for_handoff,
  fetch_operator_notifications,
  fetch_pair_state_history,
  fetch_latest_candidate_saved_set,
  fetch_saved_set_evaluation_history,
  fetch_saved_set_history,
  open_database,
  persist_account_limits,
  load_latest_dynamic_sizing_snapshot,
  persist_analytical_snapshot,
  persist_dynamic_sizing_snapshot,
  persist_candidate_review_candidates,
  persist_candidate_review_run,
  persist_fill,
  persist_known_non_binary_market,
  persist_operator_action,
  persist_order_statuses,
  persist_pair_plan,
  persist_pnl_snapshot,
  persist_pair_state_transition,
  persist_pair_liquidity_observation,
  persist_runtime_event,
  persist_service_heartbeat,
  persist_operator_notification,
  promote_order_id,
  record_market_seen,
  resolve_active_profile_token,
  summarize_persistence,
)
from .risk import (
  CoverabilityGuardResult,
  can_open_new_pair,
  evaluate_flow_coverability,
  evaluate_pre_submit_coverability_static,
  evaluate_pre_submit_coverability_static_prices,
  validate_pair_plan,
)
from .strategy import (
  build_pair_order_plan,
  compute_candidate_density_weight,
  compute_candidate_liquidity_score,
  compute_dynamic_max_contracts,
  compute_dynamic_pair_notional_cap_dollars,
  compute_dynamic_pair_notional_pct,
  compute_effective_qualifying_density,
  compute_instantaneous_qualifying_density,
  classify_binary_suitability,
  find_candidates,
  summarize_depth_within_band,
  reprice_candidate,
)
from .types import AccountBucketLimit, AccountLimits, CandidatePair, FillEvent, OrderbookSnapshot, PairOrderPlan, PairPnlSnapshot, PairRuntimeState, SubmittedOrder
from .websocket_client import KalshiWebSocketClient, WebSocketAuthError, WebSocketError, apply_orderbook_delta, normalize_orderbook_snapshot


ClientFactory = Callable[[Settings, object], Any]


@dataclass(frozen=True)
class KalshiAlignmentTruth:
  ticker: str
  market: dict[str, Any]
  positions: list[dict[str, Any]]
  resting_orders: list[dict[str, Any]]
  readback_status: str
  error_family: str | None = None


@dataclass(frozen=True)
class KalshiAlignmentChange:
  pair_id: str
  ticker: str
  state_before: str
  state_after: str
  reason: str


@dataclass(frozen=True)
class AlignmentResult:
  aligned_pairs: list[dict[str, Any]]
  truth_by_ticker: dict[str, KalshiAlignmentTruth]
  terminalized: list[KalshiAlignmentChange]
  preserved: list[KalshiAlignmentChange]
  readback_status: dict[str, str]
  degraded: bool


@dataclass(frozen=True)
class AcceptedPairSettlementInput:
  dispatch_index: int
  plan: PairOrderPlan
  order_group_id: str
  yes_order: SubmittedOrder
  no_order: SubmittedOrder
  sizing_summary: dict[str, Any]
  saved_set_snapshot: dict[str, Any] | None
  submit_mode: Literal['single_create_v2', 'batch_create_v2']


@dataclass(frozen=True)
class BatchPairAcceptanceClassification:
  pair_id: str
  ticker: str
  yes_order: SubmittedOrder | None
  no_order: SubmittedOrder | None
  classification: Literal['both_accepted', 'none_accepted', 'partial_or_ambiguous', 'global_ambiguous']
  missing_client_order_ids: tuple[str, ...]
  malformed_order_count: int
  duplicate_client_order_ids: tuple[str, ...]
  duplicate_remote_order_ids: tuple[str, ...]
  unknown_client_order_ids: tuple[str, ...]
  classification_reasons: tuple[str, ...]

ScanProgressCallback = Callable[[str, str, dict[str, Any] | None, float | None], None]
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOGGER = logging.getLogger(__name__)
PAIR_STATE_PRIORITY = {
  'ERROR': 0,
  'RECONCILE_REQUIRED': 0,
  'PARTIAL_ONE_SIDE': 1,
  'ASYMMETRIC_EXPOSURE': 1,
  'REPAIR_LIVE': 1,
  'EXPOSURE_CAPPED': 1,
  'PARTIAL_BOTH': 2,
  'RESTING_BOTH': 3,
  'PLANNED': 4,
  'LOCKED': 5,
  'FILLED': 6,
  'SETTLED_EXPOSURE': 6,
  'CANCELED': 7,
}

TRANCHE_F_EXECUTION_EVENT_CONTRACT_VERSION = 'tranche_f_execution_event_packet.v1'

NOTIFICATION_EVENT_COPY: dict[str, dict[str, str]] = {
  'eligibility_blocked': {
    'level': 'info',
    'title': 'Eligibility blocked',
    'body': 'We skipped one candidate because its eligibility window was missing.',
  },
  'eligibility_revoked_selected': {
    'level': 'warn',
    'title': 'Eligibility revoked',
    'body': 'One saved candidate lost eligibility and was removed before submit.',
  },
  'eligibility_revoked_in_flight': {
    'level': 'warn',
    'title': 'In-flight eligibility revoked',
    'body': 'An in-flight order was canceled after its eligibility window was revoked.',
  },
  'datapack_overwrite_prompted': {
    'level': 'warn',
    'title': 'Overwrite confirmation needed',
    'body': 'This load will orphan current lane records unless you extract first.',
  },
  'datapack_load_overwrite_completed': {
    'level': 'info',
    'title': 'Datapack loaded',
    'body': 'Datapack loaded, and previous lane records were moved out of active view.',
  },
  'datapack_extract_started': {
    'level': 'info',
    'title': 'Extract started',
    'body': 'Extract started. Please wait while we package current lane records.',
  },
  'datapack_extract_completed': {
    'level': 'info',
    'title': 'Extract finished',
    'body': 'Extract finished. Your current lane records were saved to the datapack store.',
  },
  'datapack_extract_failed': {
    'level': 'error',
    'title': 'Extract failed',
    'body': 'Extract did not finish, and the current lane records are still in place.',
  },
  'lane_changed': {
    'level': 'info',
    'title': 'Lane changed',
    'body': 'You are now working in {lane_label}.',
  },
  'connection_dropped': {
    'level': 'warn',
    'title': 'Connection dropped',
    'body': 'Connection dropped. Actions that need a live link may be delayed.',
  },
  'connection_restored': {
    'level': 'info',
    'title': 'Connection restored',
    'body': 'Connection restored. You can continue.',
  },
  'active_datapack_closed_cli': {
    'level': 'warn',
    'title': 'Active datapack closed',
    'body': "A CLI action closed this lane's active datapack. Reload when ready.",
  },
}


class ScanCanceledError(RuntimeError):
  """Raised when an in-flight candidate scan is cooperatively canceled."""


class SubmitHandoffValidationError(ValueError):
  """Raised when backend auto-dispatch cannot prove its saved-set handoff."""


# Backward-compatible alias for legacy callers/tests using the historical
# double-l spelling.
ScanCancelledError = ScanCanceledError


def _notification_now_iso() -> str:
  return datetime.now(UTC).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def _notification_event_copy(event_key: str) -> dict[str, str]:
  normalized_event_key = str(event_key or '').strip().lower()
  if normalized_event_key not in NOTIFICATION_EVENT_COPY:
    raise KeyError(f'Unknown notification event: {event_key}')
  return dict(NOTIFICATION_EVENT_COPY[normalized_event_key])


def emit_notification(
  connection: sqlite3.Connection,
  *,
  event_key: str,
  operation_lane: str,
  profile_token: str,
  source: str,
  detail: dict[str, Any] | None = None,
  related_candidate_id: str | None = None,
  created_at_utc: str | None = None,
  notification_id: str | None = None,
) -> dict[str, Any]:
  spec = _notification_event_copy(event_key)
  payload_detail = dict(detail or {})
  created_at = created_at_utc or _notification_now_iso()
  title = str(spec['title']).format(**payload_detail)
  body = str(spec['body']).format(**payload_detail)
  emitted_id = persist_operator_notification(
    connection,
    notification_id=notification_id,
    created_at_utc=created_at,
    operation_lane=operation_lane,
    profile_token=profile_token,
    level=spec['level'],
    title=title,
    body=body,
    source=str(source or event_key),
    related_candidate_id=related_candidate_id,
  )
  return {
    'notification_id': emitted_id,
    'event_key': str(event_key),
    'created_at_utc': created_at,
    'operation_lane': str(operation_lane),
    'profile_token': str(profile_token),
    'level': spec['level'],
    'title': title,
    'body': body,
    'source': str(source or event_key),
    'related_candidate_id': related_candidate_id,
  }


def notify_active_datapack_closed(
  connection: sqlite3.Connection,
  *,
  operation_lane: str,
  profile_token: str,
  detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
  return emit_notification(
    connection,
    event_key='active_datapack_closed_cli',
    operation_lane=operation_lane,
    profile_token=profile_token,
    source='datapack',
    detail=detail,
  )


def notify_eligibility_blocked(
  connection: sqlite3.Connection,
  *,
  operation_lane: str,
  profile_token: str,
  candidate_uid: str | None = None,
  detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
  return emit_notification(
    connection,
    event_key='eligibility_blocked',
    operation_lane=operation_lane,
    profile_token=profile_token,
    source='eligibility',
    related_candidate_id=candidate_uid,
    detail=detail,
  )


def notify_eligibility_revoked_selected(
  connection: sqlite3.Connection,
  *,
  operation_lane: str,
  profile_token: str,
  candidate_uid: str | None = None,
  detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
  return emit_notification(
    connection,
    event_key='eligibility_revoked_selected',
    operation_lane=operation_lane,
    profile_token=profile_token,
    source='eligibility',
    related_candidate_id=candidate_uid,
    detail=detail,
  )


def notify_eligibility_revoked_in_flight(
  connection: sqlite3.Connection,
  *,
  operation_lane: str,
  profile_token: str,
  candidate_uid: str | None = None,
  detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
  return emit_notification(
    connection,
    event_key='eligibility_revoked_in_flight',
    operation_lane=operation_lane,
    profile_token=profile_token,
    source='eligibility',
    related_candidate_id=candidate_uid,
    detail=detail,
  )


def notify_datapack_overwrite_prompted(
  connection: sqlite3.Connection,
  *,
  operation_lane: str,
  profile_token: str,
  detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
  return emit_notification(
    connection,
    event_key='datapack_overwrite_prompted',
    operation_lane=operation_lane,
    profile_token=profile_token,
    source='datapack',
    detail=detail,
  )


def notify_datapack_load_overwrite_completed(
  connection: sqlite3.Connection,
  *,
  operation_lane: str,
  profile_token: str,
  detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
  return emit_notification(
    connection,
    event_key='datapack_load_overwrite_completed',
    operation_lane=operation_lane,
    profile_token=profile_token,
    source='datapack',
    detail=detail,
  )


def notify_datapack_extract_started(
  connection: sqlite3.Connection,
  *,
  operation_lane: str,
  profile_token: str,
  detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
  return emit_notification(
    connection,
    event_key='datapack_extract_started',
    operation_lane=operation_lane,
    profile_token=profile_token,
    source='datapack',
    detail=detail,
  )


def notify_datapack_extract_completed(
  connection: sqlite3.Connection,
  *,
  operation_lane: str,
  profile_token: str,
  detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
  return emit_notification(
    connection,
    event_key='datapack_extract_completed',
    operation_lane=operation_lane,
    profile_token=profile_token,
    source='datapack',
    detail=detail,
  )


def notify_datapack_extract_failed(
  connection: sqlite3.Connection,
  *,
  operation_lane: str,
  profile_token: str,
  detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
  return emit_notification(
    connection,
    event_key='datapack_extract_failed',
    operation_lane=operation_lane,
    profile_token=profile_token,
    source='datapack',
    detail=detail,
  )


def notify_lane_changed(
  connection: sqlite3.Connection,
  *,
  operation_lane: str,
  profile_token: str,
  detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
  return emit_notification(
    connection,
    event_key='lane_changed',
    operation_lane=operation_lane,
    profile_token=profile_token,
    source='system',
    detail=detail,
  )


def notify_connection_dropped(
  connection: sqlite3.Connection,
  *,
  operation_lane: str,
  profile_token: str,
  detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
  return emit_notification(
    connection,
    event_key='connection_dropped',
    operation_lane=operation_lane,
    profile_token=profile_token,
    source='connection',
    detail=detail,
  )


def notify_connection_restored(
  connection: sqlite3.Connection,
  *,
  operation_lane: str,
  profile_token: str,
  detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
  return emit_notification(
    connection,
    event_key='connection_restored',
    operation_lane=operation_lane,
    profile_token=profile_token,
    source='connection',
    detail=detail,
  )


def _apply_eligibility_event(
  connection: sqlite3.Connection,
  *,
  run_id: str,
  candidate_uid: str,
  event_key: str,
  operation_lane: str,
  profile_token: str,
  lane_session_id: str | None = None,
  recorded_at_utc: str | None = None,
  detail: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
  normalized_event = str(event_key or '').strip().lower()
  row = connection.execute(
    '''
    SELECT *
    FROM candidate_review_candidates
    WHERE run_id = ? AND candidate_uid = ?
    LIMIT 1
    ''',
    (run_id, candidate_uid),
  ).fetchone()
  if row is None:
    return None

  candidate = dict(row)
  now_iso = recorded_at_utc or _notification_now_iso()
  lifecycle_stage = str(candidate.get('lifecycle_stage') or 'discovered')
  update_values: dict[str, Any] = {}

  if normalized_event == 'eligibility_blocked':
    update_values.update(
      {
        'eligibility_status': 'missing_blocked',
        'lifecycle_stage': 'discovered',
        'terminal_cause': None,
        'terminal_subcause': None,
        'terminal_at_utc': None,
      }
    )
    notify_eligibility_blocked(
      connection,
      operation_lane=operation_lane,
      profile_token=profile_token,
      candidate_uid=candidate_uid,
      detail=detail,
    )
  elif normalized_event == 'eligibility_revoked_selected':
    update_values.update(
      {
        'eligibility_status': 'revoked_post_select',
        'lifecycle_stage': 'discovered',
        'terminal_cause': None,
        'terminal_subcause': None,
        'terminal_at_utc': None,
      }
    )
    notify_eligibility_revoked_selected(
      connection,
      operation_lane=operation_lane,
      profile_token=profile_token,
      candidate_uid=candidate_uid,
      detail=detail,
    )
  elif normalized_event == 'eligibility_revoked_in_flight':
    update_values.update(
      {
        'eligibility_status': 'revoked_in_flight',
        'lifecycle_stage': 'terminal',
        'terminal_cause': 'canceled',
        'terminal_subcause': 'eligibility_revoked',
        'terminal_at_utc': now_iso,
        'expires_at_utc': now_iso,
      }
    )
    notify_eligibility_revoked_in_flight(
      connection,
      operation_lane=operation_lane,
      profile_token=profile_token,
      candidate_uid=candidate_uid,
      detail=detail,
    )
  else:
    raise KeyError(f'Unknown eligibility event: {event_key}')

  if update_values:
    assignments = ', '.join(f'{column} = ?' for column in update_values)
    connection.execute(
      f'''
      UPDATE candidate_review_candidates
      SET {assignments}
      WHERE run_id = ? AND candidate_uid = ?
      ''',
      (*update_values.values(), run_id, candidate_uid),
    )

  candidate.update(update_values)
  candidate['lane_session_id'] = lane_session_id
  candidate['recorded_at_utc'] = now_iso
  candidate['lifecycle_stage_before'] = lifecycle_stage
  candidate['event_key'] = normalized_event
  return candidate


def _write_saved_set_candidates_in_flight(
  connection: sqlite3.Connection,
  *,
  saved_set: dict[str, Any],
) -> None:
  run_id = str(saved_set.get('run_id') or '').strip()
  operation_lane = str(saved_set.get('operation_lane') or '').strip()
  members = saved_set.get('members') if isinstance(saved_set.get('members'), list) else []
  if not run_id or not members:
    return
  with connection:
    for member in members:
      if not isinstance(member, dict):
        continue
      candidate_uid = str(member.get('candidate_uid') or '').strip()
      if not candidate_uid:
        continue
      candidate_key = str(member.get('candidate_key') or candidate_uid).strip()
      detail = member.get('detail') if isinstance(member.get('detail'), dict) else {}
      ticker = str(detail.get('ticker') or '').strip()
      qualifier_tier = str(detail.get('qualifier_tier') or '').strip()
      review_row_origin = str(detail.get('review_row_origin') or 'current').strip()
      recorded_at_utc = str(member.get('recorded_at_utc') or saved_set.get('recorded_at_utc') or '').strip()
      if not recorded_at_utc:
        recorded_at_utc = datetime.now(UTC).isoformat()
      connection.execute(
        '''
        INSERT INTO candidate_review_candidates (
          run_id,
          candidate_uid,
          candidate_key,
          ticker,
          qualifier_tier,
          review_row_origin,
          detail_json,
          recorded_at_utc,
          operation_lane,
          lifecycle_stage
        )
        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, 'in_flight'
        WHERE NOT EXISTS (
          SELECT 1
          FROM candidate_review_candidates
          WHERE run_id = ? AND candidate_uid = ?
        )
        ''',
        (
          run_id,
          candidate_uid,
          candidate_key,
          ticker,
          qualifier_tier,
          review_row_origin,
          json.dumps(detail, sort_keys=True),
          recorded_at_utc,
          operation_lane,
          run_id,
          candidate_uid,
        ),
      )
      connection.execute(
        '''
        UPDATE candidate_review_candidates
        SET lifecycle_stage = 'in_flight'
        WHERE run_id = ? AND candidate_uid = ? AND lifecycle_stage NOT IN ('terminal')
        ''',
        (run_id, candidate_uid),
      )


VISUAL_WINDOW_CONFIG: dict[str, dict[str, Any]] = {
  'current': {'label': 'CURRENT', 'bucket': 'snapshot', 'lookback': None},
  '1h': {'label': '1H', 'bucket': '5m', 'lookback': timedelta(hours=1)},
  '24h': {'label': '24H', 'bucket': '15m', 'lookback': timedelta(hours=24)},
  '7d': {'label': '7D', 'bucket': '6h', 'lookback': timedelta(days=7)},
  'all': {'label': 'ALL', 'bucket': '1d', 'lookback': None},
}

ANALYSIS_ACTIVATION_MIN_CANDIDATES = 2
OV_U5A_CANDIDATE_MATH_CONTRACT_VERSION = 'ov-u5a-candidate-math-contract.v1'
OV_U5A_CANDIDATE_SCORE_MODEL_VERSION = 'candidate-linear-evidence-score.v1'
OV_U5A_COMPONENT_WEIGHTS = {
  'edge_strength': Decimal('0.34'),
  'liquidity_depth': Decimal('0.24'),
  'density_weight': Decimal('0.18'),
  'timing_pressure': Decimal('0.12'),
  'sizing_capacity': Decimal('0.12'),
}

VISUAL_SCOPE_CATALOG: dict[str, dict[str, Any]] = {
  'runtime_posture': {
    'title': 'Runtime',
    'default_view': 'pair_state_distribution',
    'default_window': 'current',
    'window_ids': ['current', '1h', '24h', '7d', 'all'],
  },
  'performance': {
    'title': 'Performance',
    'default_view': 'performance_waterfall',
    'default_window': 'all',
    'window_ids': ['1h', '24h', '7d', 'all'],
  },
  'candidate_landscape': {
    'title': 'Candidates',
    'default_view': 'candidate_density_curve',
    'default_window': 'current',
    'window_ids': ['current'],
  },
  'analysis': {
    'title': 'Analysis',
    'default_view': 'analysis_linear_diagnostics',
    'default_window': 'current',
    'window_ids': ['current'],
  },
}

VISUAL_DETAIL_CONFIG: dict[str, dict[str, Any]] = {
  'low': {'label': 'Low', 'glyph': 'Γûü'},
  'med': {'label': 'Med', 'glyph': 'ΓûüΓûé'},
  'high': {'label': 'High', 'glyph': 'ΓûüΓûéΓûâ'},
}

BALANCE_STALENESS_GRACE_MS = 10000
AUTO_CANCEL_RECOMMENDED_SEC = 15
AUTO_CANCEL_ARMED_SEC = 30
AUTO_CANCEL_DISPATCH_SEC = 45
ACTION_VOCABULARY = (
  'WAIT',
  'RECONCILE',
  'RETRY_SUBMIT',
  'CANCEL_PAIR',
  'RESAVE_AND_RETRY',
  'ESCALATE_OPERATOR',
  'NOOP_EXISTING_PAIR',
  'AUTO_CANCEL',
)

VISUAL_DETAIL_BUCKET_COUNT: dict[str, int] = {
  'low': 3,
  'med': 5,
  'high': 9,
}

VISUAL_VIEW_CATALOG: dict[str, dict[str, Any]] = {
  'pair_state_distribution': {
    'title': 'Pair states',
    'family': 'runtime',
    'scope_id': 'runtime_posture',
    'table_supported': True,
    'report_supported': False,
    'source_contracts': ['pair_states'],
  },
  'runtime_cadence': {
    'title': 'Runtime',
    'family': 'runtime',
    'scope_id': 'runtime_posture',
    'table_supported': True,
    'report_supported': False,
    'source_contracts': ['service_heartbeats', 'operator_actions', 'runtime_events'],
  },
  'cycle_outcomes': {
    'title': 'Cycle',
    'family': 'runtime',
    'scope_id': 'runtime_posture',
    'table_supported': True,
    'report_supported': False,
    'source_contracts': ['service_heartbeats', 'runtime_events'],
  },
  'freshness_latency': {
    'title': 'Freshness',
    'family': 'freshness',
    'scope_id': 'runtime_posture',
    'table_supported': True,
    'report_supported': False,
    'source_contracts': ['service_heartbeats', 'runtime_events'],
  },
  'performance_total': {
    'title': 'Total',
    'family': 'performance',
    'scope_id': 'performance',
    'table_supported': True,
    'report_supported': True,
    'source_contracts': ['pair_states', 'pair_runtime_summary'],
  },
  'performance_delta': {
    'title': '+/-',
    'family': 'performance',
    'scope_id': 'performance',
    'table_supported': True,
    'report_supported': True,
    'source_contracts': ['pair_states', 'pair_runtime_summary'],
  },
  'performance_total_out': {
    'title': 'Total out',
    'family': 'performance',
    'scope_id': 'performance',
    'table_supported': True,
    'report_supported': True,
    'source_contracts': ['pair_states', 'pair_runtime_summary'],
  },
  'performance_total_in': {
    'title': 'Total in',
    'family': 'performance',
    'scope_id': 'performance',
    'table_supported': True,
    'report_supported': True,
    'source_contracts': ['pair_states', 'pair_runtime_summary'],
  },
  'performance_fees': {
    'title': 'Fees',
    'family': 'performance',
    'scope_id': 'performance',
    'table_supported': True,
    'report_supported': True,
    'source_contracts': ['pair_states', 'pair_runtime_summary'],
  },
  'performance_waterfall': {
    'title': 'Bridge',
    'family': 'performance',
    'scope_id': 'performance',
    'table_supported': True,
    'report_supported': True,
    'source_contracts': ['pair_states', 'pair_runtime_summary'],
  },
  'candidate_density_curve': {
    'title': 'Density',
    'family': 'candidate_landscape',
    'scope_id': 'candidate_landscape',
    'table_supported': True,
    'report_supported': False,
    'source_contracts': ['runtime_events', 'service_heartbeats'],
  },
  'candidate_decision_boundary': {
    'title': 'Decision',
    'family': 'candidate_landscape',
    'scope_id': 'candidate_landscape',
    'table_supported': True,
    'report_supported': True,
    'source_contracts': ['runtime_events', 'service_heartbeats', 'candidate_review_runs', 'candidate_review_candidates'],
  },
  'candidate_frontier_scatter': {
    'title': 'Frontier',
    'family': 'candidate_landscape',
    'scope_id': 'candidate_landscape',
    'table_supported': True,
    'report_supported': True,
    'source_contracts': ['runtime_events', 'service_heartbeats', 'candidate_review_runs', 'candidate_review_candidates'],
  },
  'threshold_boundary_marker': {
    'title': 'Thresholds',
    'family': 'candidate_landscape',
    'scope_id': 'candidate_landscape',
    'table_supported': True,
    'report_supported': False,
    'source_contracts': ['runtime_events', 'service_heartbeats'],
  },
  'comparative_ranking_snapshot': {
    'title': 'Rankings',
    'family': 'candidate_landscape',
    'scope_id': 'candidate_landscape',
    'table_supported': True,
    'report_supported': False,
    'source_contracts': ['runtime_events', 'service_heartbeats'],
  },
  'analysis_threshold_progress': {
    'title': 'Threshold progress',
    'family': 'analysis',
    'scope_id': 'analysis',
    'table_supported': True,
    'report_supported': False,
    'source_contracts': ['runtime_events', 'service_heartbeats'],
  },
  'factor_contribution': {
    'title': 'Factors',
    'family': 'analysis',
    'scope_id': 'analysis',
    'table_supported': True,
    'report_supported': True,
    'source_contracts': ['runtime_events', 'service_heartbeats'],
  },
  'factors_timeseries': {
    'title': 'Factors',
    'family': 'analysis',
    'scope_id': 'analysis',
    'table_supported': True,
    'report_supported': True,
    'source_contracts': ['runtime_events', 'service_heartbeats', 'candidate_review_runs', 'candidate_review_candidates'],
  },
  'parameter_sensitivity_delta': {
    'title': 'Sensitivity',
    'family': 'analysis',
    'scope_id': 'analysis',
    'table_supported': True,
    'report_supported': True,
    'source_contracts': ['runtime_events', 'service_heartbeats'],
  },
  'analysis_linear_diagnostics': {
    'title': 'Diagnostics',
    'family': 'analysis',
    'scope_id': 'analysis',
    'table_supported': True,
    'report_supported': True,
    'source_contracts': ['runtime_events', 'service_heartbeats', 'candidate_review_runs', 'candidate_review_candidates'],
  },
  'saved_set_carry_forward': {
    'title': 'Carry-forward',
    'family': 'candidate_landscape',
    'scope_id': 'candidate_landscape',
    'table_supported': True,
    'report_supported': False,
    'source_contracts': ['candidate_saved_sets', 'candidate_saved_set_members'],
  },
  'actionability_status_distribution': {
    'title': 'Actionability',
    'family': 'analysis',
    'scope_id': 'analysis',
    'table_supported': True,
    'report_supported': False,
    'source_contracts': ['candidate_saved_set_evaluations', 'candidate_saved_sets'],
  },
}

PARAMETER_SURFACE_PAGE_CATALOG: tuple[dict[str, Any], ...] = (
  {
    'page_id': 'info',
    'title': 'Info',
    'summary': 'Current runtime truth for the active shell state, including current values, overlay visibility, and derived sizing outputs.',
    'lane_kind': 'informational_only',
    'group_catalog': (
      {
        'group_id': 'scan_cadence_posture',
        'title': 'Scan cadence',
        'summary': 'Current scan timing and automation execution values for the active shell state.',
        'field_ids': (
          'scan_interval_ms',
          'auto_find_candidates_cadence_ms',
          'cancel_on_pause',
          'max_unhedged_sec',
          'post_submit_processing_buffer_sec',
        ),
      },
      {
        'group_id': 'entry_window_posture',
        'title': 'Entry window',
        'summary': 'Current entry window boundaries that govern which markets are in scope for candidate review.',
        'field_ids': (
          'entry_window_start_sec',
          'entry_window_end_sec',
        ),
      },
      {
        'group_id': 'threshold_and_reserve_posture',
        'title': 'Threshold and reserve',
        'summary': 'Current threshold and reserve values that shape candidate qualification posture.',
        'field_ids': (
          'min_edge_dollars',
          'min_profit_dollars',
          'fee_reserve_dollars',
          'max_divergence',
          'flow_participation_k',
        ),
      },
      {
        'group_id': 'sizing_and_density_posture',
        'title': 'Sizing and density',
        'summary': 'Configured sizing values that shape pair-level notional and density tuning.',
        'field_ids': (
          'max_pair_contracts',
          'max_open_pairs',
          'submit_prep_top_k',
          'min_pair_notional_pct',
          'max_pair_notional_pct',
          'target_deployment_pct',
          'density_alpha',
        ),
      },
      {
        'group_id': 'optimization_runtime_context',
        'title': 'Runtime context',
        'summary': 'Current backend-derived sizing outputs that frame the advisory analysis packets.',
        'field_ids': (
          'effective_density',
          'dynamic_pair_notional_pct',
          'dynamic_max_contracts',
          'binding_limiter',
        ),
      },
    ),
  },
  {
    'page_id': 'set',
    'title': 'Set',
    'summary': 'Bounded session-overlay controls only. This page stages allowlisted runtime-setting changes without changing backend-owned derived or advisory truth.',
    'lane_kind': 'manual_mutation',
    'group_catalog': (
      {
        'group_id': 'manual_scan_cadence',
        'title': 'Scan cadence',
        'summary': 'Session-overlay controls for scan timing and automation execution parameters.',
        'field_ids': ('scan_interval_ms', 'auto_find_candidates_cadence_ms', 'cancel_on_pause', 'max_unhedged_sec', 'post_submit_processing_buffer_sec'),
      },
      {
        'group_id': 'manual_entry_window',
        'title': 'Entry window',
        'summary': 'Session-overlay controls for the entry window boundaries that govern market scope.',
        'field_ids': ('entry_window_start_sec', 'entry_window_end_sec'),
      },
      {
        'group_id': 'manual_thresholds_and_reserves',
        'title': 'Threshold and reserve',
        'summary': 'Session-overlay controls for threshold and reserve settings that shape candidate qualification posture.',
        'field_ids': ('min_edge_dollars', 'min_profit_dollars', 'fee_reserve_dollars', 'max_divergence', 'flow_participation_k'),
      },
      {
        'group_id': 'manual_sizing_and_density',
        'title': 'Sizing and density',
        'summary': 'Session-overlay controls for bounded sizing posture and density tuning.',
        'field_ids': (
          'max_pair_contracts',
          'max_open_pairs',
          'submit_prep_top_k',
          'min_pair_notional_pct',
          'max_pair_notional_pct',
          'target_deployment_pct',
          'density_alpha',
        ),
      },
    ),
  },
  {
    'page_id': 'analysis',
    'title': 'Analysis',
    'summary': 'Advisory-only action analysis. Sensitivity and adjustment packets remain visible here without gaining direct apply authority.',
    'lane_kind': 'algorithmic_optimization',
    'group_catalog': (
      {
        'group_id': 'optimization_runtime_context',
        'title': 'Runtime context',
        'summary': 'Current backend-derived sizing outputs that frame the advisory analysis packets.',
        'field_ids': (
          'effective_density',
          'dynamic_pair_notional_pct',
          'dynamic_max_contracts',
          'binding_limiter',
        ),
      },
      {
        'group_id': 'optimization_advisory_packets',
        'title': 'Advisory packets',
        'summary': 'Retained bounded analytical outputs remain advisory-only and no-auto-apply in this page.',
        'field_ids': (),
        'includes_advisory_cards': True,
      },
    ),
  },
)

PARAMETER_SURFACE_FIELD_CATALOG: dict[str, dict[str, Any]] = {
  'scan_interval_ms': {
    'label': 'Scan interval',
    'source_env_var': 'KALSHI_SCAN_INTERVAL_MS',
    'value_class': 'setting',
    'info_detail': 'How often the shell starts a new candidate-review cycle. Lower intervals refresh opportunity discovery more often, while higher intervals slow review cadence and reduce churn.',
  },
  'auto_find_candidates_cadence_ms': {
    'label': 'Auto-find-candidates cadence',
    'source_env_var': 'KALSHI_AUTO_FIND_CANDIDATES_CADENCE_MS',
    'value_class': 'setting',
    'info_detail': 'How long the shell waits between bounded client auto-forward passes when auto-find-candidates is armed for this browser session. Lower values keep the shell moving more aggressively, while higher values leave more review time between passes.',
  },
  'cancel_on_pause': {
    'label': 'Cancel on pause',
    'source_env_var': 'KALSHI_CANCEL_ON_PAUSE',
    'value_class': 'setting',
    'info_detail': 'Whether open resting orders are canceled when the shell pauses or stops automation. When true, any unfilled orders are withdrawn on stop or pause. When false, resting orders remain open and continue toward fill or natural expiry.',
  },
  'max_unhedged_sec': {
    'label': 'Shelter Window',
    'source_env_var': 'KALSHI_MAX_UNHEDGED_SEC',
    'value_class': 'setting',
    'info_detail': 'Seconds before market close when the shell may shelter an unresolved pair. Shelter action preserves the original opposite repair order when valid and caps only the ahead side, rather than using this value as an order-age timeout.',
  },
  'post_submit_processing_buffer_sec': {
    'label': 'Post-submit processing buffer',
    'source_env_var': 'KALSHI_POST_SUBMIT_PROCESSING_BUFFER_SEC',
    'value_class': 'setting',
    'info_detail': 'Additional scheduler lease backstop time reserved for submit processing after the entry window. Normal submit completion still releases through the event-driven path; this value bounds abandoned submit ownership.',
  },
  'entry_window_start_sec': {
    'label': 'Entry window start',
    'source_env_var': 'KALSHI_ENTRY_WINDOW_START_SEC',
    'value_class': 'setting',
    'info_detail': 'The earliest seconds-to-close point where a market can begin qualifying for entry review. Markets stay out of scope until they enter the time band between this boundary and the entry-window end.',
  },
  'entry_window_end_sec': {
    'label': 'Entry window end',
    'source_env_var': 'KALSHI_ENTRY_WINDOW_END_SEC',
    'value_class': 'setting',
    'info_detail': 'The latest seconds-to-close point where a market is still eligible for a new entry. After a market crosses this boundary, new-entry qualification stops even if other thresholds still pass.',
  },
  'min_edge_dollars': {
    'label': 'Minimum edge',
    'source_env_var': 'KALSHI_MIN_EDGE_DOLLARS',
    'value_class': 'setting',
    'info_detail': 'The minimum per-contract pricing edge a candidate must show before the shell treats it as actionable. This is a hard qualification gate: values below it prevent candidate admission regardless of density or sizing.',
  },
  'min_profit_dollars': {
    'label': 'Minimum profit',
    'source_env_var': 'KALSHI_MIN_PROFIT_DOLLARS',
    'value_class': 'setting',
    'info_detail': 'The minimum projected net dollars a qualifying pair must retain after costs and reserves. This is the post-cost, post-reserve profitability floor that must remain before a pair can qualify.',
  },
  'fee_reserve_dollars': {
    'label': 'Fee reserve',
    'source_env_var': 'KALSHI_FEE_RESERVE_DOLLARS',
    'value_class': 'setting',
    'info_detail': 'The per-pair fee cushion held back before the shell counts projected profit as usable. It reduces usable profit and affordable size because it is included before final qualification and sizing math.',
  },
  'max_divergence': {
    'label': 'Max divergence',
    'source_env_var': 'KALSHI_MAX_DIVERGENCE',
    'value_class': 'setting',
    'info_detail': 'The maximum allowed live yes/no bid divergence at the pre-submit boundary. It has no operating default: unset or invalid values block submit before any order is placed.',
  },
  'flow_participation_k': {
    'label': 'Flow participation',
    'source_env_var': 'KALSHI_FLOW_PARTICIPATION_K',
    'value_class': 'setting',
    'info_detail': 'Required per-side recent traded flow as a multiple of intended pair size. It has no operating default: unset or invalid values block submit before any order is placed.',
  },
  'max_pair_contracts': {
    'label': 'Max pair contracts',
    'source_env_var': 'KALSHI_MAX_PAIR_CONTRACTS',
    'value_class': 'setting',
    'info_detail': 'The hard per-pair contract ceiling before dynamic sizing applies any tighter limit. This cap is non-negotiable: derived sizing can tighten below it, but never exceed it.',
  },
  'max_open_pairs': {
    'label': 'Max open pairs',
    'source_env_var': 'KALSHI_MAX_OPEN_PAIRS',
    'value_class': 'setting',
    'info_detail': 'The maximum number of live pair positions the shell may carry at the same time. This limits portfolio breadth, not the size of any single pair.',
  },
  'submit_prep_top_k': {
    'label': 'Submit prep top K',
    'source_env_var': 'KALSHI_SUBMIT_PREP_TOP_K',
    'value_class': 'setting',
    'info_detail': 'How many top-ranked saved-set candidates enter the serial per-candidate pre-submit readbacks. A work bound that keeps final coverability checks fresh, never a risk gate: every entering candidate still passes every pre-submit guard, and 0 disables the cap.',
  },
  'min_pair_notional_pct': {
    'label': 'Minimum pair notional',
    'source_env_var': 'KALSHI_MIN_PAIR_NOTIONAL_PCT',
    'value_class': 'setting',
    'info_detail': 'The floor share of available balance the shell is willing to allocate to one pair. This is the lower clamp on density-aware pair sizing when the computed cap would otherwise fall below the configured floor.',
  },
  'max_pair_notional_pct': {
    'label': 'Maximum pair notional',
    'source_env_var': 'KALSHI_MAX_PAIR_NOTIONAL_PCT',
    'value_class': 'setting',
    'info_detail': 'The ceiling share of available balance the shell may allocate to one pair before tighter limits apply. This is the upper clamp before density, pricing, and other caps tighten the result further.',
  },
  'target_deployment_pct': {
    'label': 'Target deployment',
    'source_env_var': 'KALSHI_TARGET_DEPLOYMENT_PCT',
    'value_class': 'setting',
    'info_detail': 'The share of available balance the shell aims to keep deployed across qualifying pairs. This is the starting deployment target that density later compresses into the current dynamic pair notional cap.',
  },
  'density_alpha': {
    'label': 'Density alpha',
    'source_env_var': 'KALSHI_DENSITY_ALPHA',
    'value_class': 'setting',
    'info_detail': 'The weighting strength that controls how strongly qualifying-candidate density changes sizing. Higher alpha makes current crowding influence sizing more strongly, while lower alpha keeps sizing closer to prior density posture.',
  },
  'effective_density': {
    'label': 'Effective density',
    'source_family': 'strategy',
    'value_class': 'derived',
    'info_detail': 'The current weighted crowding level of viable candidates after the shell blends edge and liquidity into one density reading. Higher density compresses pair-level notional sizing, while lower density leaves more room before clamps apply.',
  },
  'dynamic_pair_notional_pct': {
    'label': 'Dynamic pair notional cap',
    'source_family': 'strategy',
    'value_class': 'derived',
    'info_detail': 'The current share of available balance the shell can allocate to one pair after density-aware sizing is applied. This is the live result of applying density-aware sizing to target deployment and then clamping it within the configured min/max pair-notional bounds.',
  },
  'dynamic_max_contracts': {
    'label': 'Dynamic max contracts',
    'source_family': 'strategy',
    'value_class': 'derived',
    'info_detail': 'The current per-pair contract ceiling implied by the active notional cap at current pricing. This converts the current dynamic notional cap into a contract count using the present per-contract spend, including the fee reserve.',
  },
  'binding_limiter': {
    'label': 'Binding limiter',
    'source_family': 'service',
    'value_class': 'derived',
    'info_detail': 'The derived label that identifies which limit currently sets the tightest cap on pair size.\n\ncash limit\n  Available cash is the tightest cap on pair size.\n\nconfigured contract cap\n  The configured max-pair-contract ceiling is the tightest cap on pair size.\n\ncandidate size cap\n  The candidate max-size ceiling is the tightest cap on pair size.\n\ndynamic cap\n  The density-adjusted dynamic notional cap is the tightest cap on pair size.',
  },
}

PARAMETER_SURFACE_OVERLAY_FIELD_IDS = frozenset(
  {
    'scan_interval_ms',
    'auto_find_candidates_cadence_ms',
    'cancel_on_pause',
    'max_unhedged_sec',
    'post_submit_processing_buffer_sec',
    'entry_window_start_sec',
    'entry_window_end_sec',
    'min_edge_dollars',
    'min_profit_dollars',
    'fee_reserve_dollars',
    'max_divergence',
    'flow_participation_k',
    'max_pair_contracts',
    'max_open_pairs',
    'submit_prep_top_k',
    'min_pair_notional_pct',
    'max_pair_notional_pct',
    'target_deployment_pct',
    'density_alpha',
  }
)


def _resolve_settings(
  settings: Settings | None,
  *,
  env_override: str | None = None,
  subaccount_override: int | None = None,
) -> Settings:
  resolved = settings or load_settings()
  if env_override is not None:
    resolved = replace(resolved, kalshi_env=env_override)
  if subaccount_override is not None:
    resolved = replace(resolved, subaccount=subaccount_override)
  return resolved


def _sandbox_relaxed_settings(settings: Settings) -> tuple[Settings, Decimal]:
  factor = Decimal(str(getattr(settings, 'sandbox_edge_relaxation_factor', 0.80)))
  if factor < 0:
    factor = Decimal('0')
  if factor > 1:
    factor = Decimal('1')
  relaxed = replace(
    settings,
    min_edge_dollars=float(Decimal(str(settings.min_edge_dollars)) * factor),
    min_profit_dollars=float(Decimal(str(settings.min_profit_dollars)) * factor),
  )
  return relaxed, factor


# Near-miss widening factor is always fixed at 0.50, independent of sandbox policy.
# This captures markets that almost qualify (edge/profit >= 50% of threshold) as
# evidence-only frontier rows, decoupled from the active sandbox relaxation scope.
_NEAR_MISS_EVIDENCE_FACTOR = Decimal('0.50')


def _near_miss_widened_settings(settings: Settings) -> Settings:
  """Return a Settings copy with thresholds relaxed by the fixed near-miss factor."""
  return replace(
    settings,
    min_edge_dollars=float(Decimal(str(settings.min_edge_dollars)) * _NEAR_MISS_EVIDENCE_FACTOR),
    min_profit_dollars=float(Decimal(str(settings.min_profit_dollars)) * _NEAR_MISS_EVIDENCE_FACTOR),
  )


def _near_miss_candidate_projection(
  near_miss_raw: list[Any],
  already_captured_tickers: set[str],
  *,
  market_by_ticker: dict[str, Any],
  settings: Settings,
) -> list[dict[str, Any]]:
  """Project near-miss candidates, excluding tickers already surfaced in live/sandbox tiers."""
  projected: list[dict[str, Any]] = []
  for rank, candidate in enumerate(near_miss_raw, start=1):
    if candidate.ticker in already_captured_tickers:
      continue
    projected.append(
      _candidate_projection_record(
        candidate,
        rank=rank,
        qualifier_tier='near_miss',
        market_by_ticker=market_by_ticker,
        settings=settings,
      )
    )
  return projected


def _sandbox_candidate_projection(
  candidates: list[Any],
  live_tickers: set[str],
  *,
  market_by_ticker: dict[str, Any],
  settings: Settings,
) -> tuple[list[dict[str, Any]], int | None]:
  limit = max(0, int(getattr(settings, 'sandbox_scan_return_limit', 50) or 50))
  projected: list[dict[str, Any]] = []
  transition_rank: int | None = None
  for rank, candidate in enumerate(candidates, start=1):
    qualifier_tier = (
      'live_qualifying'
      if (
        candidate.ticker in live_tickers
        and candidate.target_yes_bid > 0
        and candidate.target_no_bid > 0
      )
      else 'sandbox_extended'
    )
    if qualifier_tier == 'live_qualifying':
      transition_rank = rank
    projected.append(
      _candidate_projection_record(
        candidate,
        rank=rank,
        qualifier_tier=qualifier_tier,
        market_by_ticker=market_by_ticker,
        settings=settings,
      )
    )
  return projected[:limit], transition_rank


def _decimal_text(value: Any) -> str:
  return str(value) if value is not None else '0'


def _iso_text(value: Any) -> str | None:
  if value is None:
    return None
  if hasattr(value, 'isoformat'):
    return str(value.isoformat())
  return str(value)


def _candidate_projection_record(
  candidate: Any,
  *,
  rank: int,
  qualifier_tier: str,
  market_by_ticker: dict[str, Any],
  settings: Settings,
) -> dict[str, Any]:
  record = asdict(candidate)
  market = market_by_ticker.get(candidate.ticker)
  record['density_weight'] = str(compute_candidate_density_weight(candidate, settings))
  record['liquidity_score'] = str(compute_candidate_liquidity_score(candidate))
  record['qualifier_tier'] = qualifier_tier
  record['rank'] = rank
  min_edge = Decimal(str(settings.min_edge_dollars))
  min_profit = Decimal(str(settings.min_profit_dollars))
  edge_gross = Decimal(str(record.get('edge_gross_per_contract') or '0'))
  edge_net = Decimal(str(record.get('edge_net_per_contract') or '0'))
  record['min_edge_dollars'] = str(min_edge)
  record['min_profit_dollars'] = str(min_profit)
  record['gross_edge_margin_to_min_edge'] = str(edge_gross - min_edge)
  record['net_profit_margin_to_min_profit'] = str(edge_net - min_profit)
  record['edge_threshold_pass'] = edge_gross >= min_edge
  record['profit_threshold_pass'] = edge_net >= min_profit
  record['threshold_outcome'] = (
    'pass'
    if edge_gross >= min_edge and edge_net >= min_profit
    else 'near_miss'
    if qualifier_tier == 'near_miss'
    else 'below_threshold'
  )
  if market is not None:
    record['title'] = str(getattr(market, 'title', '') or '')
    record['event_ticker'] = str(getattr(market, 'event_ticker', '') or '')
    record['yes_sub_title'] = str(getattr(market, 'yes_sub_title', '') or '')
    record['no_sub_title'] = str(getattr(market, 'no_sub_title', '') or '')
    record['volume_24h_fp'] = _decimal_text(getattr(market, 'volume_24h_fp', None))
    record['open_interest_fp'] = _decimal_text(getattr(market, 'open_interest_fp', None))
    record['volume_fp'] = _decimal_text(getattr(market, 'volume_fp', None))
    record['yes_bid_size_fp'] = _decimal_text(getattr(market, 'yes_bid_size_fp', None))
    record['yes_ask_size_fp'] = _decimal_text(getattr(market, 'yes_ask_size_fp', None))
    record['market_status'] = str(getattr(market, 'status', '') or '')
    record['close_time_utc'] = _iso_text(getattr(market, 'close_time', None))
    record['market_close_time_utc'] = _iso_text(getattr(market, 'close_time', None))
    record['binary_suitability'] = {
      'status': str(getattr(market, 'binary_suitability_status', '') or ''),
      'reason': str(getattr(market, 'binary_suitability_reason', '') or ''),
      'event_ticker': str(getattr(market, 'binary_suitability_event_ticker', '') or ''),
      'series_ticker': str(getattr(market, 'binary_suitability_series_ticker', '') or ''),
      'category': str(getattr(market, 'binary_suitability_category', '') or ''),
      'market_count': int(getattr(market, 'binary_suitability_market_count', 0) or 0),
      'sibling_tickers': list(getattr(market, 'binary_suitability_sibling_tickers', ()) or ()),
    }
  else:
    record['binary_suitability'] = {
      'status': str(getattr(candidate, 'binary_suitability_status', '') or ''),
      'reason': str(getattr(candidate, 'binary_suitability_reason', '') or ''),
      'event_ticker': str(getattr(candidate, 'binary_suitability_event_ticker', '') or ''),
      'series_ticker': str(getattr(candidate, 'binary_suitability_series_ticker', '') or ''),
      'category': str(getattr(candidate, 'binary_suitability_category', '') or ''),
      'market_count': int(getattr(candidate, 'binary_suitability_market_count', 0) or 0),
      'sibling_tickers': list(getattr(candidate, 'binary_suitability_sibling_tickers', ()) or ()),
    }
  return record


def _candidate_evidence_preview(candidates: list[dict[str, Any]], *, limit: int = 3) -> list[dict[str, Any]]:
  preview: list[dict[str, Any]] = []
  for candidate in candidates[:max(limit, 0)]:
    preview.append(
      {
        'ticker': candidate.get('ticker'),
        'rank': candidate.get('rank'),
        'qualifier_tier': candidate.get('qualifier_tier'),
        'liquidity_score': candidate.get('liquidity_score'),
        'density_weight': candidate.get('density_weight'),
        'volume_24h_fp': candidate.get('volume_24h_fp'),
        'open_interest_fp': candidate.get('open_interest_fp'),
        'volume_fp': candidate.get('volume_fp'),
        'yes_bid_size_fp': candidate.get('yes_bid_size_fp'),
        'yes_ask_size_fp': candidate.get('yes_ask_size_fp'),
        'seconds_to_close': candidate.get('seconds_to_close'),
        'target_yes_bid': _decimal_text(candidate.get('target_yes_bid')),
        'target_no_bid': _decimal_text(candidate.get('target_no_bid')),
        'edge_gross_per_contract': _decimal_text(candidate.get('edge_gross_per_contract')),
        'fee_reserve_per_contract': _decimal_text(candidate.get('fee_reserve_per_contract')),
        'edge_net_per_contract': _decimal_text(candidate.get('edge_net_per_contract')),
        'min_edge_dollars': _decimal_text(candidate.get('min_edge_dollars')),
        'min_profit_dollars': _decimal_text(candidate.get('min_profit_dollars')),
        'gross_edge_margin_to_min_edge': _decimal_text(candidate.get('gross_edge_margin_to_min_edge')),
        'net_profit_margin_to_min_profit': _decimal_text(candidate.get('net_profit_margin_to_min_profit')),
        'edge_threshold_pass': candidate.get('edge_threshold_pass'),
        'profit_threshold_pass': candidate.get('profit_threshold_pass'),
        'threshold_outcome': candidate.get('threshold_outcome'),
        'asymmetry': _decimal_text(candidate.get('asymmetry')),
      }
    )
  return preview


def _analysis_scope_label(candidates: list[dict[str, Any]]) -> str:
  qualifier_tiers = {str(candidate.get('qualifier_tier') or '') for candidate in candidates}
  if 'sandbox_extended' in qualifier_tiers and 'live_qualifying' in qualifier_tiers:
    return 'sandbox_extended_with_transition'
  if 'sandbox_extended' in qualifier_tiers:
    return 'sandbox_extended_only'
  if 'near_miss' in qualifier_tiers:
    return 'near_miss_only'
  return 'live_qualifying_only'


def _analysis_packet_context(
  candidates: list[dict[str, Any]],
  *,
  recorded_at: datetime,
  operation_lane: str,
  lane_session_id: str,
  transition_rank: int | None,
  near_miss_candidates: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
  live_qualifying_count = sum(1 for candidate in candidates if candidate.get('qualifier_tier') == 'live_qualifying')
  sandbox_extended_count = sum(1 for candidate in candidates if candidate.get('qualifier_tier') == 'sandbox_extended')
  near_miss_count = len(near_miss_candidates) if near_miss_candidates is not None else 0
  return {
    'generated_at_utc': _iso_utc(recorded_at),
    'operation_lane': operation_lane,
    'lane_session_id': lane_session_id,
    'source_population_scope': _analysis_scope_label(candidates),
    'candidate_row_count': len(candidates),
    'live_qualifying_count': live_qualifying_count,
    'sandbox_extended_count': sandbox_extended_count,
    'near_miss_count': near_miss_count,
    'transition_rank': transition_rank,
  }


def _analysis_candidate_row(candidate: dict[str, Any]) -> dict[str, Any]:
  return {
    'ticker': candidate.get('ticker'),
    'rank': candidate.get('rank'),
    'qualifier_tier': candidate.get('qualifier_tier'),
    'density_weight': candidate.get('density_weight'),
    'liquidity_score': candidate.get('liquidity_score'),
    'edge_net_per_contract': _decimal_text(candidate.get('edge_net_per_contract')),
    'seconds_to_close': candidate.get('seconds_to_close'),
  }


def _settings_reference(settings: Settings) -> dict[str, Any]:
  return {
    'density_alpha': settings.density_alpha,
    'density_edge_ref': settings.density_edge_ref,
    'density_liquidity_ref': settings.density_liquidity_ref,
    'target_deployment_pct': settings.target_deployment_pct,
    'min_pair_notional_pct': settings.min_pair_notional_pct,
    'max_pair_notional_pct': settings.max_pair_notional_pct,
    'entry_window_start_sec': settings.entry_window_start_sec,
    'entry_window_end_sec': settings.entry_window_end_sec,
    'min_edge_dollars': settings.min_edge_dollars,
    'min_profit_dollars': settings.min_profit_dollars,
  }


def _build_candidate_density_curve(
  candidates: list[dict[str, Any]],
  *,
  context: dict[str, Any],
) -> dict[str, Any]:
  return {
    **context,
    'view_id': 'candidate_density_curve',
    'series': [
      {
        'x': candidate.get('rank'),
        'y': candidate.get('density_weight'),
        'ticker': candidate.get('ticker'),
        'qualifier_tier': candidate.get('qualifier_tier'),
      }
      for candidate in candidates
    ],
  }


def _build_threshold_boundary_marker(
  candidates: list[dict[str, Any]],
  *,
  context: dict[str, Any],
  sizing_summary: dict[str, Any],
  settings: Settings,
  near_miss_candidates: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
  live_rows = [candidate for candidate in candidates if candidate.get('qualifier_tier') == 'live_qualifying']
  boundary_row: dict[str, Any] | None = None
  next_row: dict[str, Any] | None = None
  transition_rank = context.get('transition_rank')
  if transition_rank is not None:
    boundary_row = next((candidate for candidate in candidates if candidate.get('rank') == transition_rank), None)
    next_row = next((candidate for candidate in candidates if candidate.get('rank') == transition_rank + 1), None)
  if boundary_row is None:
    boundary_row = live_rows[-1] if live_rows else (candidates[-1] if candidates else None)
  edge_floor = min(
    (Decimal(str(candidate.get('edge_net_per_contract'))) for candidate in live_rows),
    default=None,
  )
  min_seconds = min((int(candidate.get('seconds_to_close') or 0) for candidate in candidates), default=None)
  max_seconds = max((int(candidate.get('seconds_to_close') or 0) for candidate in candidates), default=None)
  near_miss_edge_range: dict[str, Any] | None = None
  if near_miss_candidates:
    nm_edges = [Decimal(str(c.get('edge_net_per_contract') or '0')) for c in near_miss_candidates]
    if nm_edges:
      near_miss_edge_range = {
        'min': str(min(nm_edges)),
        'max': str(max(nm_edges)),
        'count': len(nm_edges),
        'evidence_factor': str(_NEAR_MISS_EVIDENCE_FACTOR),
      }
  boundary_notes = (
    ['Near-miss frontier evidence available; see near_miss_rows for below-threshold candidates.']
    if near_miss_candidates
    else [
      'Current threshold markers are limited to surfaced qualifying and sandbox-extended rows.',
      'Near-miss frontier evidence not available in this scan.',
    ]
  )
  result = {
    **context,
    'view_id': 'threshold_boundary_marker',
    'settings_reference': _settings_reference(settings),
    'current_live_floor_edge_net_per_contract': str(edge_floor) if edge_floor is not None else None,
    'current_surface_seconds_to_close_range': {
      'min': min_seconds,
      'max': max_seconds,
    },
    'tier_transition': {
      'transition_rank': transition_rank,
      'boundary_ticker': boundary_row.get('ticker') if boundary_row is not None else None,
      'boundary_qualifier_tier': boundary_row.get('qualifier_tier') if boundary_row is not None else None,
      'next_ticker': next_row.get('ticker') if next_row is not None else None,
      'next_qualifier_tier': next_row.get('qualifier_tier') if next_row is not None else None,
    },
    'sizing_posture': {
      'effective_density': sizing_summary.get('effective_density'),
      'dynamic_pair_notional_pct': sizing_summary.get('dynamic_pair_notional_pct'),
      'dynamic_pair_notional_cap_dollars': sizing_summary.get('dynamic_pair_notional_cap_dollars'),
      'dynamic_max_contracts': sizing_summary.get('dynamic_max_contracts'),
      'binding_limiter': sizing_summary.get('binding_limiter'),
    },
    'boundary_notes': boundary_notes,
  }
  if near_miss_edge_range is not None:
    result['near_miss_edge_range'] = near_miss_edge_range
  return result


def _build_comparative_ranking_snapshot(
  candidates: list[dict[str, Any]],
  *,
  context: dict[str, Any],
  near_miss_candidates: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
  transition_rank = context.get('transition_rank')
  transition_row: dict[str, Any] | None = None
  next_row: dict[str, Any] | None = None
  if transition_rank is not None:
    transition_row = next((candidate for candidate in candidates if candidate.get('rank') == transition_rank), None)
    next_row = next((candidate for candidate in candidates if candidate.get('rank') == transition_rank + 1), None)
  result: dict[str, Any] = {
    **context,
    'view_id': 'comparative_ranking_snapshot',
    'top_rows': [_analysis_candidate_row(candidate) for candidate in candidates[:3]],
    'transition_rows': [
      _analysis_candidate_row(candidate)
      for candidate in (transition_row, next_row)
      if candidate is not None
    ],
  }
  if near_miss_candidates:
    result['near_miss_rows'] = [_analysis_candidate_row(candidate) for candidate in near_miss_candidates[:3]]
    result['near_miss_evidence_class'] = 'near_miss'
    result['near_miss_evidence_factor'] = str(_NEAR_MISS_EVIDENCE_FACTOR)
  else:
    result['near_miss_rows'] = []
    result['near_miss_evidence_class'] = 'not_available'
  return result


def _build_factor_contribution(
  candidates: list[dict[str, Any]],
  raw_candidates_by_ticker: dict[str, Any],
  *,
  context: dict[str, Any],
  sizing_summary: dict[str, Any],
  settings: Settings,
) -> dict[str, Any]:
  candidate_rows: list[dict[str, Any]] = []
  edge_ref = Decimal(str(settings.density_edge_ref))
  liquidity_ref = Decimal(str(settings.density_liquidity_ref))
  for candidate in candidates[:5]:
    raw_candidate = raw_candidates_by_ticker.get(str(candidate.get('ticker') or ''))
    edge_net = Decimal(str(candidate.get('edge_net_per_contract') or '0'))
    liquidity_score = Decimal(str(candidate.get('liquidity_score') or '0'))
    if raw_candidate is not None:
      edge_net = raw_candidate.edge_net_per_contract
      liquidity_score = compute_candidate_liquidity_score(raw_candidate)
    edge_ratio = edge_net / edge_ref if edge_ref > 0 else Decimal('1')
    liquidity_ratio = liquidity_score / liquidity_ref if liquidity_ref > 0 else Decimal('1')
    edge_weight = min(Decimal('1.25'), max(Decimal('0.75'), edge_ratio))
    liquidity_weight = min(Decimal('1.25'), max(Decimal('0.75'), liquidity_ratio))
    candidate_rows.append(
      {
        'ticker': candidate.get('ticker'),
        'rank': candidate.get('rank'),
        'qualifier_tier': candidate.get('qualifier_tier'),
        'edge_net_per_contract': str(edge_net),
        'asymmetry': _decimal_text(candidate.get('asymmetry')),
        'liquidity_score': str(liquidity_score),
        'density_weight': candidate.get('density_weight'),
        'density_components': {
          'edge_reference': str(edge_ref),
          'edge_ratio': str(edge_ratio),
          'edge_weight': str(edge_weight),
          'liquidity_reference': str(liquidity_ref),
          'liquidity_ratio': str(liquidity_ratio),
          'liquidity_weight': str(liquidity_weight),
        },
      }
    )
  return {
    **context,
    'view_id': 'factor_contribution',
    'settings_reference': _settings_reference(settings),
    'candidate_rows': candidate_rows,
    'sizing_context': {
      'qualifying_candidate_count': sizing_summary.get('qualifying_candidate_count'),
      'instantaneous_density': sizing_summary.get('instantaneous_density'),
      'effective_density': sizing_summary.get('effective_density'),
      'dynamic_pair_notional_pct': sizing_summary.get('dynamic_pair_notional_pct'),
      'dynamic_pair_notional_cap_dollars': sizing_summary.get('dynamic_pair_notional_cap_dollars'),
      'dynamic_max_contracts': sizing_summary.get('dynamic_max_contracts'),
      'binding_limiter': sizing_summary.get('binding_limiter'),
    },
    'advisory_only': True,
    'authority_boundary': 'explanation_only_not_workflow_authority',
  }


def _bounded_target_deployment_settings(settings: Settings) -> dict[str, Settings]:
  baseline = Decimal(str(settings.target_deployment_pct))
  increase_value = min(Decimal('0.95'), baseline + Decimal('0.05'))
  decrease_value = max(Decimal('0.05'), baseline - Decimal('0.05'))
  return {
    'increase_target_deployment_pct': replace(settings, target_deployment_pct=float(increase_value)),
    'decrease_target_deployment_pct': replace(settings, target_deployment_pct=float(decrease_value)),
  }


def _sensitivity_snapshot(
  candidates: list[Any],
  *,
  settings: Settings,
  balance: Decimal,
) -> dict[str, Any]:
  summary = _build_dynamic_sizing_summary(candidates, balance=balance, settings=settings)
  return {
    'settings': {
      'target_deployment_pct': settings.target_deployment_pct,
      'density_alpha': settings.density_alpha,
      'density_edge_ref': settings.density_edge_ref,
      'density_liquidity_ref': settings.density_liquidity_ref,
    },
    'derived': {
      'qualifying_candidate_count': summary.get('qualifying_candidate_count'),
      'instantaneous_density': summary.get('instantaneous_density'),
      'effective_density': summary.get('effective_density'),
      'dynamic_pair_notional_pct': summary.get('dynamic_pair_notional_pct'),
      'dynamic_pair_notional_cap_dollars': summary.get('dynamic_pair_notional_cap_dollars'),
      'dynamic_max_contracts': summary.get('dynamic_max_contracts'),
      'binding_limiter': summary.get('binding_limiter'),
    },
  }


def _build_parameter_sensitivity_delta(
  raw_candidates: list[Any],
  *,
  context: dict[str, Any],
  balance: Decimal,
  settings: Settings,
) -> dict[str, Any]:
  baseline_snapshot = _sensitivity_snapshot(raw_candidates, settings=settings, balance=balance)
  scenario_rows: list[dict[str, Any]] = []
  for label, scenario_settings in _bounded_target_deployment_settings(settings).items():
    scenario_snapshot = _sensitivity_snapshot(raw_candidates, settings=scenario_settings, balance=balance)
    baseline_contracts = Decimal(str(baseline_snapshot['derived']['dynamic_max_contracts'] or '0'))
    scenario_contracts = Decimal(str(scenario_snapshot['derived']['dynamic_max_contracts'] or '0'))
    baseline_notional_pct = Decimal(str(baseline_snapshot['derived']['dynamic_pair_notional_pct'] or '0'))
    scenario_notional_pct = Decimal(str(scenario_snapshot['derived']['dynamic_pair_notional_pct'] or '0'))
    scenario_rows.append(
      {
        'scenario_id': label,
        'parameter': 'target_deployment_pct',
        'baseline_value': baseline_snapshot['settings']['target_deployment_pct'],
        'scenario_value': scenario_snapshot['settings']['target_deployment_pct'],
        'delta_value': round(
          float(scenario_snapshot['settings']['target_deployment_pct'] - baseline_snapshot['settings']['target_deployment_pct']),
          6,
        ),
        'derived_delta': {
          'effective_density': scenario_snapshot['derived']['effective_density'],
          'dynamic_pair_notional_pct': scenario_snapshot['derived']['dynamic_pair_notional_pct'],
          'dynamic_pair_notional_pct_delta': str(scenario_notional_pct - baseline_notional_pct),
          'dynamic_max_contracts': scenario_snapshot['derived']['dynamic_max_contracts'],
          'dynamic_max_contracts_delta': str(scenario_contracts - baseline_contracts),
          'binding_limiter': scenario_snapshot['derived']['binding_limiter'],
        },
      }
    )
  return {
    **context,
    'view_id': 'parameter_sensitivity_delta',
    'advisory_only': True,
    'no_auto_apply': True,
    'baseline_settings': baseline_snapshot['settings'],
    'baseline_derived': baseline_snapshot['derived'],
    'scenarios': scenario_rows,
    'parameter_scope': 'bounded_current_state_density_and_sizing_only',
  }


def _candidate_status_label(candidate: dict[str, Any]) -> str:
  qualifier_tier = str(candidate.get('qualifier_tier') or '')
  if qualifier_tier == 'live_qualifying':
    return 'selected'
  if qualifier_tier == 'near_miss':
    return 'near_miss'
  return 'rejected'


def _candidate_uid(candidate: dict[str, Any]) -> str:
  # Lane 0 (CANDIDATE_PERSISTENCE_UNIFORM_CONTRACT_BMAP_2026-06-19): delegate to the
  # single canonical, display-stable identity so candidate-math rows share identity
  # with the card / save_selection path. (Was ticker:tier:rank — unstable across rerank.)
  return canonical_candidate_uid(candidate)


def _candidate_key(candidate: dict[str, Any]) -> str:
  # One identity, one key (was sha256(ticker:tier:rank)).
  return canonical_candidate_key(candidate)


def _score_weight_vector_id() -> str:
  payload = json.dumps(
    {key: str(value) for key, value in sorted(OV_U5A_COMPONENT_WEIGHTS.items())},
    sort_keys=True,
  )
  return 'weights-{digest}'.format(digest=hashlib.sha256(payload.encode('utf-8')).hexdigest()[:12])


def _bounded_decimal_ratio(numerator: Decimal, denominator: Decimal, *, fallback: Decimal = Decimal('0')) -> Decimal:
  if denominator <= 0:
    return fallback
  return min(Decimal('2'), max(Decimal('0'), numerator / denominator))


def _timing_pressure(candidate: dict[str, Any], settings: Settings) -> Decimal:
  seconds = Decimal(str(candidate.get('seconds_to_close') or '0'))
  window_start = Decimal(str(settings.entry_window_start_sec))
  window_end = Decimal(str(settings.entry_window_end_sec))
  span = max(Decimal('1'), window_start - window_end)
  return min(Decimal('1'), max(Decimal('0'), (window_start - seconds) / span))


def _candidate_feature_vector(candidate: dict[str, Any], *, settings: Settings) -> dict[str, Any]:
  edge_net = Decimal(str(candidate.get('edge_net_per_contract') or '0'))
  edge_gross = Decimal(str(candidate.get('edge_gross_per_contract') or '0'))
  liquidity_score = Decimal(str(candidate.get('liquidity_score') or '0'))
  density_weight = Decimal(str(candidate.get('density_weight') or '0'))
  fee_reserve = Decimal(str(candidate.get('fee_reserve_per_contract') or settings.fee_reserve_dollars or '0'))
  max_size = Decimal(str(candidate.get('max_size_contracts') or settings.max_pair_contracts or '0'))
  target_yes = Decimal(str(candidate.get('target_yes_bid') or '0'))
  target_no = Decimal(str(candidate.get('target_no_bid') or '0'))
  projected_profit = edge_net * max_size
  fee_drag = fee_reserve * max_size
  per_contract_spend = target_yes + target_no + fee_reserve
  sizing_reference = Decimal(str(settings.max_pair_contracts or 1))
  sizing_pressure = _bounded_decimal_ratio(max_size, sizing_reference, fallback=Decimal('1'))
  return {
    'edge_gross_per_contract': str(edge_gross),
    'edge_net_per_contract': str(edge_net),
    'liquidity_score': str(liquidity_score),
    'density_weight': str(density_weight),
    'projected_profit_dollars': str(projected_profit),
    'fee_drag_dollars': str(fee_drag),
    'seconds_to_close': candidate.get('seconds_to_close'),
    'timing_pressure': str(_timing_pressure(candidate, settings)),
    'sizing_pressure': str(sizing_pressure),
    'per_contract_spend': str(per_contract_spend),
    'qualifier_tier': candidate.get('qualifier_tier'),
    'selection_status': _candidate_status_label(candidate),
  }


def _candidate_score_components(feature_vector: dict[str, Any], *, settings: Settings) -> dict[str, Any]:
  edge_component = _bounded_decimal_ratio(
    Decimal(str(feature_vector['edge_net_per_contract'])),
    Decimal(str(settings.density_edge_ref)),
  )
  liquidity_component = _bounded_decimal_ratio(
    Decimal(str(feature_vector['liquidity_score'])),
    Decimal(str(settings.density_liquidity_ref)),
  )
  density_component = _bounded_decimal_ratio(
    Decimal(str(feature_vector['density_weight'])),
    Decimal('1'),
  )
  timing_component = Decimal(str(feature_vector['timing_pressure']))
  sizing_component = _bounded_decimal_ratio(
    Decimal(str(feature_vector['sizing_pressure'])),
    Decimal('1'),
  )
  components = {
    'edge_strength': edge_component,
    'liquidity_depth': liquidity_component,
    'density_weight': density_component,
    'timing_pressure': timing_component,
    'sizing_capacity': sizing_component,
  }
  return {key: str(value) for key, value in components.items()}


def _weighted_candidate_score(score_components: dict[str, Any]) -> Decimal:
  score = Decimal('0')
  for component_id, weight in OV_U5A_COMPONENT_WEIGHTS.items():
    score += Decimal(str(score_components.get(component_id) or '0')) * weight
  return score


def _candidate_threshold_outcome(feature_vector: dict[str, Any], *, settings: Settings) -> dict[str, Any]:
  gross_margin = Decimal(str(feature_vector['edge_gross_per_contract'])) - Decimal(str(settings.min_edge_dollars))
  net_margin = Decimal(str(feature_vector['edge_net_per_contract'])) - Decimal(str(settings.min_profit_dollars))
  selected = feature_vector.get('selection_status') == 'selected'
  return {
    'selection_status': feature_vector.get('selection_status'),
    'gross_edge_margin': str(gross_margin),
    'net_profit_margin': str(net_margin),
    'threshold_margin': str(min(gross_margin, net_margin)),
    'passes_current_thresholds': gross_margin >= 0 and net_margin >= 0,
    'selected_by_current_policy': selected,
  }


def _build_candidate_math_evidence_contract(
  candidates: list[dict[str, Any]],
  *,
  context: dict[str, Any],
  settings: Settings,
  near_miss_candidates: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
  evidence_rows: list[dict[str, Any]] = []
  score_values: list[Decimal] = []
  surface_candidates = list(candidates) + list(near_miss_candidates or [])
  for candidate in surface_candidates:
    feature_vector = _candidate_feature_vector(candidate, settings=settings)
    score_components = _candidate_score_components(feature_vector, settings=settings)
    weighted_score = _weighted_candidate_score(score_components)
    score_values.append(weighted_score)
    evidence_rows.append(
      {
        # Lane 0 (CANDIDATE_PERSISTENCE_UNIFORM_CONTRACT_BMAP_2026-06-19): spread the full
        # candidate display dict so every persisted candidate-math row is card-renderable
        # (same display fields the save_selection path persists). The explicit identity /
        # tier / scoring keys below override any display-dict collisions; scoring fields
        # remain top-level (unchanged) so downstream scoring consumers are unaffected.
        **candidate,
        'candidate_uid': _candidate_uid(candidate),
        'candidate_key': _candidate_key(candidate),
        'ticker': candidate.get('ticker'),
        'rank': candidate.get('rank'),
        'qualifier_tier': candidate.get('qualifier_tier'),
        'review_row_origin': 'current',
        'feature_vector': feature_vector,
        'score_components': score_components,
        'composite_score': {
          'weighted_score': str(weighted_score),
          'normalized_score': '0',
          'threshold_margin': None,
          'rank': candidate.get('rank'),
          'score_model_version': OV_U5A_CANDIDATE_SCORE_MODEL_VERSION,
          'weight_vector_reference': _score_weight_vector_id(),
        },
        'threshold_outcome': _candidate_threshold_outcome(feature_vector, settings=settings),
      }
    )
  max_score = max(score_values, default=Decimal('0'))
  selected_scores = [
    Decimal(str(row['composite_score']['weighted_score']))
    for row in evidence_rows
    if row['threshold_outcome']['selected_by_current_policy']
  ]
  score_threshold = min(selected_scores, default=Decimal('1'))
  for row in evidence_rows:
    weighted_score = Decimal(str(row['composite_score']['weighted_score']))
    normalized_score = weighted_score / max_score if max_score > 0 else Decimal('0')
    row['composite_score']['normalized_score'] = str(normalized_score)
    row['composite_score']['threshold_margin'] = str(weighted_score - score_threshold)
  return {
    **context,
    'view_id': 'candidate_math_evidence_contract',
    'contract_version': OV_U5A_CANDIDATE_MATH_CONTRACT_VERSION,
    'model_reference': {
      'score_model_version': OV_U5A_CANDIDATE_SCORE_MODEL_VERSION,
      'weight_vector_reference': _score_weight_vector_id(),
      'component_weights': {key: str(value) for key, value in OV_U5A_COMPONENT_WEIGHTS.items()},
      'formula': 'weighted_score=sum(component_value*component_weight); normalized_score=weighted_score/max_observed_weighted_score',
    },
    'feature_vector_schema': [
      'edge_gross_per_contract',
      'edge_net_per_contract',
      'liquidity_score',
      'density_weight',
      'projected_profit_dollars',
      'fee_drag_dollars',
      'seconds_to_close',
      'timing_pressure',
      'sizing_pressure',
      'per_contract_spend',
      'qualifier_tier',
      'selection_status',
    ],
    'composite_score_schema': [
      'weighted_score',
      'normalized_score',
      'threshold_margin',
      'rank',
      'score_model_version',
      'weight_vector_reference',
    ],
    'retention_schema': {
      'run_session_fields': ['run_id', 'recorded_at_utc', 'operation_lane', 'lane_session_id'],
      'candidate_fields': ['candidate_uid', 'candidate_key', 'ticker', 'feature_vector', 'composite_score', 'threshold_outcome'],
      'storage_tables': ['candidate_review_runs', 'candidate_review_candidates', 'runtime_events.detail_json.analytical_outputs'],
    },
    'seeded_fixture_family': ['sparse', 'crowded', 'elbow', 'flat', 'noisy'],
    'candidate_evidence_rows': evidence_rows,
    'authority_boundary': 'explanation_only_not_workflow_authority',
  }


def _build_advisory_parameter_adjustment(
  sensitivity_delta: dict[str, Any],
  *,
  context: dict[str, Any],
  settings: Settings,
) -> dict[str, Any]:
  baseline = sensitivity_delta.get('baseline_derived', {})
  binding_limiter = str(baseline.get('binding_limiter') or '')
  increase_scenario = next(
    (
      scenario
      for scenario in sensitivity_delta.get('scenarios', [])
      if scenario.get('scenario_id') == 'increase_target_deployment_pct'
    ),
    None,
  )
  recommendation_status = 'no_change_recommended'
  recommended_value: float | None = None
  reason_summary = (
    'Current sizing is well-matched to conditions; no deployment-target change indicated.'
  )
  if binding_limiter == 'dynamic_notional_cap' and increase_scenario is not None:
    contract_delta = Decimal(str(increase_scenario['derived_delta'].get('dynamic_max_contracts_delta') or '0'))
    if contract_delta >= 1:
      recommendation_status = 'review_increase'
      recommended_value = float(increase_scenario.get('scenario_value'))
      reason_summary = (
        'Sizing is capped by the per-pair budget. Raising the deployment target would lift max contracts '
        f'per pair by {contract_delta}.'
      )
  elif binding_limiter:
    reason_summary = (
      'Sizing is currently limited by another factor, so adjusting the deployment target would not be '
      'the main lever.'
    )
  return {
    **context,
    'view_id': 'advisory_parameter_adjustment',
    'advisory_only': True,
    'no_auto_apply': True,
    'recommendation_status': recommendation_status,
    'parameter': 'target_deployment_pct',
    'current_value': settings.target_deployment_pct,
    'recommended_value': recommended_value,
    'reason_summary': reason_summary,
    'provenance_sources': [
      'parameter_sensitivity_delta',
      'threshold_boundary_marker',
      'factor_contribution',
    ],
    'freshness_reference': {
      'generated_at_utc': context.get('generated_at_utc'),
      'lane_session_id': context.get('lane_session_id'),
      'source_population_scope': context.get('source_population_scope'),
    },
  }


def _build_analytical_outputs(
  candidates: list[dict[str, Any]],
  raw_candidates: list[Any],
  *,
  recorded_at: datetime,
  operation_lane: str,
  lane_session_id: str,
  transition_rank: int | None,
  sizing_summary: dict[str, Any],
  balance: Decimal,
  settings: Settings,
  near_miss_candidates: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
  context = _analysis_packet_context(
    candidates,
    recorded_at=recorded_at,
    operation_lane=operation_lane,
    lane_session_id=lane_session_id,
    transition_rank=transition_rank,
    near_miss_candidates=near_miss_candidates,
  )
  raw_candidates_by_ticker = {candidate.ticker: candidate for candidate in raw_candidates}
  parameter_sensitivity_delta = _build_parameter_sensitivity_delta(
    raw_candidates,
    context=context,
    balance=balance,
    settings=settings,
  )
  return {
    'candidate_math_evidence_contract': _build_candidate_math_evidence_contract(
      candidates,
      context=context,
      settings=settings,
      near_miss_candidates=near_miss_candidates,
    ),
    'candidate_density_curve': _build_candidate_density_curve(candidates, context=context),
    'threshold_boundary_marker': _build_threshold_boundary_marker(
      candidates,
      context=context,
      sizing_summary=sizing_summary,
      settings=settings,
      near_miss_candidates=near_miss_candidates,
    ),
    'comparative_ranking_snapshot': _build_comparative_ranking_snapshot(
      candidates,
      context=context,
      near_miss_candidates=near_miss_candidates,
    ),
    'factor_contribution': _build_factor_contribution(
      candidates,
      raw_candidates_by_ticker,
      context=context,
      sizing_summary=sizing_summary,
      settings=settings,
    ),
    'parameter_sensitivity_delta': parameter_sensitivity_delta,
    'advisory_parameter_adjustment': _build_advisory_parameter_adjustment(
      parameter_sensitivity_delta,
      context=context,
      settings=settings,
    ),
    'dependency_group_recommendations': _build_dependency_group_recommendations(
      candidates,
      context=context,
      sizing_summary=sizing_summary,
      settings=settings,
      near_miss_candidates=near_miss_candidates,
      parameter_sensitivity_delta=parameter_sensitivity_delta,
    ),
  }


def _candidate_math_contract_signature(contract: dict[str, Any]) -> str:
  rows = contract.get('candidate_evidence_rows') if isinstance(contract, dict) else []
  payload = json.dumps(rows if isinstance(rows, list) else [], sort_keys=True, default=str)
  return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def _lane_session_stopped_after(
  connection: Any,
  *,
  lane_session_id: str,
  operation_lane: str,
  cycle_recorded_at: datetime,
) -> bool:
  # STOP-2: True when the most recent automation transition for this session is an
  # operator STOP that is MORE RECENT than this cycle's start time -- i.e. this cycle
  # began before the stop and is now persisting after it (a straggler). Its candidate
  # writes must be skipped so a fresh 'discovered' row cannot mask the halt's
  # auto_cancel and leave a committed candidate stuck in the 'queued' position. A
  # MANUAL run started AFTER a stop has a later cycle time than the stop and is NOT
  # skipped. Fails toward NOT skipping on any ambiguity, so a real scan is never
  # silently dropped.
  if not lane_session_id:
    return False
  try:
    row = connection.execute(
      '''
      SELECT detail_json, recorded_at_utc
      FROM runtime_events
      WHERE event_type = 'automation_policy_transition'
        AND lane_session_id = ?
        AND operation_lane = ?
      ORDER BY id DESC LIMIT 1
      ''',
      (lane_session_id, operation_lane),
    ).fetchone()
  except Exception:
    return False
  if row is None:
    return False
  try:
    detail = json.loads(row['detail_json']) if row['detail_json'] else {}
  except Exception:
    detail = {}
  is_stop = (
    str(detail.get('automation_state_id') or '') == 'stopped'
    or str(detail.get('transition_reason') or '') == 'operator_stop'
  )
  if not is_stop:
    return False
  try:
    stop_at = _parse_recorded_at(str(row['recorded_at_utc'] or ''))
    if stop_at.tzinfo is None:
      stop_at = stop_at.replace(tzinfo=UTC)
    cycle_at = cycle_recorded_at if cycle_recorded_at.tzinfo else cycle_recorded_at.replace(tzinfo=UTC)
    return stop_at.astimezone(UTC) > cycle_at.astimezone(UTC)
  except Exception:
    return False


def _persist_candidate_math_contract(
  connection: Any,
  *,
  operation_lane: str,
  lane_session_id: str,
  operator_lane_session_id: str | None = None,
  recorded_at: datetime,
  source_action: str,
  analytical_outputs: dict[str, Any],
) -> None:
  contract = analytical_outputs.get('candidate_math_evidence_contract') if isinstance(analytical_outputs, dict) else None
  if not isinstance(contract, dict):
    return
  rows = contract.get('candidate_evidence_rows')
  if not isinstance(rows, list):
    rows = []
  # STOP-2: if an operator stop landed after this cycle started, this is a straggler
  # persisting after the halt -- skip the candidate write so it cannot mask the halt's
  # auto_cancel (the operator-reported 'queued candidate not cancelling' on STOP).
  if _lane_session_stopped_after(
    connection,
    lane_session_id=operator_lane_session_id or lane_session_id,
    operation_lane=operation_lane,
    cycle_recorded_at=recorded_at,
  ):
    return
  # run_id stays per-cycle (unique across automation cycles); lane_session_id
  # in the DB row uses the stable operator session so the panel can query
  # all cycles at once.
  run_id = '{lane_session_id}:{source_action}:candidate-math'.format(
    lane_session_id=lane_session_id,
    source_action=source_action,
  )
  persist_candidate_review_run(
    connection,
    run_id=run_id,
    recorded_at_utc=recorded_at.isoformat(),
    operation_lane=operation_lane,
    lane_session_id=operator_lane_session_id or lane_session_id,
    candidate_signature=_candidate_math_contract_signature(contract),
    candidate_count=len(rows),
    source_action=source_action,
    detail={
      'contract_version': contract.get('contract_version'),
      'model_reference': contract.get('model_reference'),
      'retention_schema': contract.get('retention_schema'),
      'seeded_fixture_family': contract.get('seeded_fixture_family'),
    },
  )
  persist_candidate_review_candidates(
    connection,
    run_id=run_id,
    recorded_at_utc=recorded_at.isoformat(),
    operation_lane=operation_lane,
    candidates=rows,
  )


def _build_qualification_threshold_recommendation(
  candidates: list[dict[str, Any]],
  *,
  context: dict[str, Any],
  settings: Settings,
  near_miss_candidates: list[dict[str, Any]] | None,
) -> dict[str, Any]:
  """Advisory recommendation for min_edge_dollars and min_profit_dollars."""
  near_miss_count = len(near_miss_candidates) if near_miss_candidates else 0
  live_count = sum(1 for c in candidates if c.get('qualifier_tier') == 'live_qualifying')
  recommendation_status = 'no_change_recommended'
  direction: str | None = None
  rationale = (
    'Current qualification thresholds are producing live-qualifying candidates. No threshold adjustment indicated.'
  )
  if live_count == 0 and near_miss_count > 0:
    recommendation_status = 'review_decrease'
    direction = 'decrease'
    rationale = (
      f'No live-qualifying candidates present but {near_miss_count} near-miss candidates exist within '
      f'{_NEAR_MISS_EVIDENCE_FACTOR * 100:.0f}% of current thresholds. Consider relaxing qualification thresholds.'
    )
  elif live_count == 0 and near_miss_count == 0:
    recommendation_status = 'insufficient_evidence'
    rationale = 'No live-qualifying or near-miss candidates. Cannot recommend a threshold direction without broader evidence.'
  return {
    'group_id': 'qualification_thresholds',
    'advisory_only': True,
    'no_auto_apply': True,
    'parameters': ['min_edge_dollars', 'min_profit_dollars'],
    'current_settings': {
      'min_edge_dollars': settings.min_edge_dollars,
      'min_profit_dollars': settings.min_profit_dollars,
    },
    'recommendation_status': recommendation_status,
    'suggested_direction': direction,
    'rationale': rationale,
    'evidence': {
      'live_qualifying_count': live_count,
      'near_miss_count': near_miss_count,
      'near_miss_evidence_factor': str(_NEAR_MISS_EVIDENCE_FACTOR),
    },
    'freshness_reference': {
      'generated_at_utc': context.get('generated_at_utc'),
      'lane_session_id': context.get('lane_session_id'),
    },
  }


def _build_entry_window_recommendation(
  candidates: list[dict[str, Any]],
  *,
  context: dict[str, Any],
  settings: Settings,
  near_miss_candidates: list[dict[str, Any]] | None,
) -> dict[str, Any]:
  """Advisory recommendation for entry_window_start_sec and entry_window_end_sec."""
  all_surface = list(candidates) + (near_miss_candidates or [])
  seconds_values = [int(c.get('seconds_to_close') or 0) for c in all_surface if c.get('seconds_to_close') is not None]
  recommendation_status = 'no_change_recommended'
  direction: str | None = None
  rationale = 'Entry window covers the observed candidate distribution. No window adjustment indicated.'
  if seconds_values:
    observed_min = min(seconds_values)
    observed_max = max(seconds_values)
    window_start = int(settings.entry_window_start_sec)
    window_end = int(settings.entry_window_end_sec)
    if observed_min < window_start * 0.8:
      recommendation_status = 'review_decrease_start'
      direction = 'widen_start'
      rationale = (
        f'Candidates are appearing at {observed_min}s to close, below the current entry window start '
        f'({window_start}s). Consider decreasing entry_window_start_sec to capture more candidates.'
      )
    elif observed_max > window_end * 1.2:
      recommendation_status = 'review_increase_end'
      direction = 'widen_end'
      rationale = (
        f'Candidates are appearing at {observed_max}s to close, above the current entry window end '
        f'({window_end}s). Consider increasing entry_window_end_sec to widen the observation window.'
      )
  else:
    recommendation_status = 'insufficient_evidence'
    rationale = 'No candidate timing data available for entry window analysis.'
  return {
    'group_id': 'entry_window_posture',
    'advisory_only': True,
    'no_auto_apply': True,
    'parameters': ['entry_window_start_sec', 'entry_window_end_sec'],
    'current_settings': {
      'entry_window_start_sec': settings.entry_window_start_sec,
      'entry_window_end_sec': settings.entry_window_end_sec,
    },
    'recommendation_status': recommendation_status,
    'suggested_direction': direction,
    'rationale': rationale,
    'evidence': {
      'observed_seconds_min': min(seconds_values) if seconds_values else None,
      'observed_seconds_max': max(seconds_values) if seconds_values else None,
      'surface_candidate_count': len(all_surface),
    },
    'freshness_reference': {
      'generated_at_utc': context.get('generated_at_utc'),
      'lane_session_id': context.get('lane_session_id'),
    },
  }


def _build_density_sizing_recommendation(
  *,
  context: dict[str, Any],
  settings: Settings,
  sizing_summary: dict[str, Any],
  parameter_sensitivity_delta: dict[str, Any],
) -> dict[str, Any]:
  """Advisory recommendation for density/deployment sizing parameters."""
  binding_limiter = str(sizing_summary.get('binding_limiter') or '')
  recommendation_status = 'no_change_recommended'
  direction: str | None = None
  rationale = 'Current density and sizing posture is operating within expected parameters.'
  if binding_limiter == 'dynamic_notional_cap':
    recommendation_status = 'review_increase_target_deployment'
    direction = 'increase_target_deployment_pct'
    rationale = (
      'Sizing is bound by the dynamic notional cap. Increasing target_deployment_pct may '
      'allow higher contract utilization. Review parameter_sensitivity_delta for delta impact.'
    )
  elif binding_limiter == 'min_pair_notional':
    recommendation_status = 'review_density_alpha'
    direction = 'adjust_density_alpha'
    rationale = (
      'Sizing is bound by min_pair_notional. Reviewing density_alpha relative to current '
      'qualifying candidate density may help unlock deployment capacity.'
    )
  return {
    'group_id': 'density_deployment_sizing',
    'advisory_only': True,
    'no_auto_apply': True,
    'parameters': ['target_deployment_pct', 'density_alpha', 'min_pair_notional_pct', 'max_pair_notional_pct'],
    'current_settings': {
      'target_deployment_pct': settings.target_deployment_pct,
      'density_alpha': settings.density_alpha,
      'min_pair_notional_pct': settings.min_pair_notional_pct,
      'max_pair_notional_pct': settings.max_pair_notional_pct,
    },
    'recommendation_status': recommendation_status,
    'suggested_direction': direction,
    'rationale': rationale,
    'evidence': {
      'binding_limiter': binding_limiter,
      'effective_density': sizing_summary.get('effective_density'),
      'dynamic_pair_notional_pct': sizing_summary.get('dynamic_pair_notional_pct'),
      'dynamic_max_contracts': sizing_summary.get('dynamic_max_contracts'),
    },
    'sensitivity_delta_reference': parameter_sensitivity_delta.get('view_id'),
    'freshness_reference': {
      'generated_at_utc': context.get('generated_at_utc'),
      'lane_session_id': context.get('lane_session_id'),
    },
  }


def _build_hard_caps_recommendation(
  candidates: list[dict[str, Any]],
  *,
  context: dict[str, Any],
  settings: Settings,
  sizing_summary: dict[str, Any],
) -> dict[str, Any]:
  """Advisory recommendation for hard cap / operational limit parameters."""
  dynamic_max_contracts = sizing_summary.get('dynamic_max_contracts')
  max_pair_contracts = int(getattr(settings, 'max_pair_contracts', 0) or 0)
  binding_limiter = str(sizing_summary.get('binding_limiter') or '')
  recommendation_status = 'no_change_recommended'
  direction: str | None = None
  rationale = 'Hard caps and operational limits appear consistent with current deployment posture.'
  if max_pair_contracts > 0 and dynamic_max_contracts is not None:
    try:
      dmc = int(dynamic_max_contracts)
      if dmc >= max_pair_contracts:
        recommendation_status = 'review_increase_max_pair_contracts'
        direction = 'increase_max_pair_contracts'
        rationale = (
          f'Dynamic max contracts ({dmc}) has reached the max_pair_contracts hard cap ({max_pair_contracts}). '
          'Consider increasing max_pair_contracts if deployment capacity should be higher.'
        )
    except (ValueError, TypeError):
      pass
  return {
    'group_id': 'hard_caps_operational_limits',
    'advisory_only': True,
    'no_auto_apply': True,
    'parameters': ['max_pair_contracts', 'max_open_pairs', 'fee_reserve_dollars'],
    'current_settings': {
      'max_pair_contracts': getattr(settings, 'max_pair_contracts', None),
      'max_open_pairs': getattr(settings, 'max_open_pairs', None),
      'fee_reserve_dollars': getattr(settings, 'fee_reserve_dollars', None),
    },
    'recommendation_status': recommendation_status,
    'suggested_direction': direction,
    'rationale': rationale,
    'evidence': {
      'dynamic_max_contracts': dynamic_max_contracts,
      'binding_limiter': binding_limiter,
    },
    'freshness_reference': {
      'generated_at_utc': context.get('generated_at_utc'),
      'lane_session_id': context.get('lane_session_id'),
    },
  }


def _build_dependency_group_recommendations(
  candidates: list[dict[str, Any]],
  *,
  context: dict[str, Any],
  sizing_summary: dict[str, Any],
  settings: Settings,
  near_miss_candidates: list[dict[str, Any]] | None,
  parameter_sensitivity_delta: dict[str, Any],
) -> dict[str, Any]:
  """Assemble 4-group dependency recommendation packets."""
  return {
    'view_id': 'dependency_group_recommendations',
    'advisory_only': True,
    'no_auto_apply': True,
    'groups': [
      _build_qualification_threshold_recommendation(
        candidates,
        context=context,
        settings=settings,
        near_miss_candidates=near_miss_candidates,
      ),
      _build_entry_window_recommendation(
        candidates,
        context=context,
        settings=settings,
        near_miss_candidates=near_miss_candidates,
      ),
      _build_density_sizing_recommendation(
        context=context,
        settings=settings,
        sizing_summary=sizing_summary,
        parameter_sensitivity_delta=parameter_sensitivity_delta,
      ),
      _build_hard_caps_recommendation(
        candidates,
        context=context,
        settings=settings,
        sizing_summary=sizing_summary,
      ),
    ],
    'freshness_reference': {
      'generated_at_utc': context.get('generated_at_utc'),
      'lane_session_id': context.get('lane_session_id'),
      'source_population_scope': context.get('source_population_scope'),
    },
  }


def _parse_recorded_at(value: str) -> datetime:
  return datetime.fromisoformat(value.replace('Z', '+00:00'))


def _decimal_number(value: Any) -> float:
  try:
    return float(Decimal(str(value)))
  except Exception:
    return 0.0


def _iso_utc(value: datetime) -> str:
  return value.astimezone(UTC).isoformat().replace('+00:00', 'Z')


def _db_tail(path: str) -> str:
  return Path(path).name or path


def _websocket_label(value: str) -> str:
  text = str(value or '').strip()
  if not text:
    return ''
  parsed = urlparse(text)
  if not parsed.netloc:
    return text
  path = parsed.path or ''
  return '{host}{path}'.format(host=parsed.netloc, path=path)


def _lane_session_id(operation_lane: str) -> str:
  timestamp = datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')
  return '{lane}-{timestamp}-{suffix}'.format(
    lane=str(operation_lane or 'sandbox').strip().lower() or 'sandbox',
    timestamp=timestamp,
    suffix=uuid4().hex[:8],
  )


def _lane_runtime_posture(
  settings: Settings,
  *,
  lane_session_id: str | None = None,
  connection_state: str = 'waiting',
  websocket_connected: bool = False,
) -> dict[str, Any]:
  return {
    'operation_lane': settings.operation_lane,
    'lane_session_id': lane_session_id,
    'active_websocket_url_tail': _websocket_label(settings.active_websocket_url),
    'available_websocket_urls': {
      'sandbox': _websocket_label(settings.sandbox_websocket_url) or 'unconfigured',
      'live': _websocket_label(settings.live_websocket_url) or 'unconfigured',
    },
    'connection_state': {
      'status': connection_state,
      'websocket_connected': websocket_connected,
    },
  }


def _settings_summary_payload(settings: Settings | dict[str, Any] | None) -> dict[str, Any]:
  if settings is None:
    return {}
  if isinstance(settings, Settings):
    return safe_settings_summary(settings)
  return dict(settings)


PARAMETER_SURFACE_SIZING_FIELD_IDS = (
  'effective_density',
  'dynamic_pair_notional_pct',
  'dynamic_max_contracts',
  'binding_limiter',
)


def _parameter_surface_sizing_packet(
  source: Any,
  *,
  recorded_at_utc: str | None = None,
  source_name: str | None = None,
) -> dict[str, Any]:
  if not isinstance(source, dict):
    return {}
  packet = {
    key: source.get(key)
    for key in PARAMETER_SURFACE_SIZING_FIELD_IDS
    if source.get(key) not in {None, ''}
  }
  if not packet:
    return {}
  if recorded_at_utc:
    packet['recorded_at_utc'] = recorded_at_utc
  if source_name:
    packet['source_name'] = source_name
  return packet


def _merge_parameter_surface_sizing_packets(
  packets: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> dict[str, Any]:
  merged: dict[str, Any] = {}
  for packet in packets:
    if not isinstance(packet, dict) or not packet:
      continue
    if 'recorded_at_utc' not in merged and packet.get('recorded_at_utc') not in {None, ''}:
      merged['recorded_at_utc'] = packet['recorded_at_utc']
    if 'source_name' not in merged and packet.get('source_name') not in {None, ''}:
      merged['source_name'] = packet['source_name']
    for key in PARAMETER_SURFACE_SIZING_FIELD_IDS:
      if key not in merged and packet.get(key) not in {None, ''}:
        merged[key] = packet[key]
    if all(key in merged for key in PARAMETER_SURFACE_SIZING_FIELD_IDS):
      break
  return merged


def _load_latest_sizing_posture(
  connection: Any,
  *,
  operation_lane: str,
) -> dict[str, Any]:
  rows = connection.execute(
    '''
    SELECT recorded_at_utc, source_name, detail_json
    FROM (
      SELECT recorded_at_utc, 'runtime_event' AS source_name, detail_json, id
      FROM runtime_events
      WHERE operation_lane = ?
      UNION ALL
      SELECT recorded_at_utc, 'service_heartbeat' AS source_name, detail_json, id
      FROM service_heartbeats
      WHERE operation_lane = ?
    ) combined
    ORDER BY recorded_at_utc DESC, id DESC
    LIMIT 24
    ''',
    (operation_lane, operation_lane),
  ).fetchall()
  packets: list[dict[str, Any]] = []
  for row in rows:
    detail = json.loads(row['detail_json']) if row['detail_json'] else {}
    packet = _parameter_surface_sizing_packet(
      detail,
      recorded_at_utc=str(row['recorded_at_utc'] or '') or None,
      source_name=str(row['source_name'] or '') or None,
    )
    if packet:
      packets.append(packet)
  # C1 rehydrate: append the last ready dynamic-sizing snapshot as a lowest-priority,
  # gap-filling source. The merge is fresh-wins-first, so a live scan's runtime_event
  # still overrides it; at session start (runtime sources lack sizing) it carries the
  # values over instead of the panel cold-starting at "needs more data".
  carried = load_latest_dynamic_sizing_snapshot(connection, operation_lane=operation_lane)
  if carried and isinstance(carried.get('values'), dict):
    carried_packet = _parameter_surface_sizing_packet(
      carried['values'],
      recorded_at_utc=str(carried.get('recorded_at_utc') or '') or None,
      source_name='computed:sizing_last_ready',
    )
    if carried_packet:
      packets.append(carried_packet)
  return _merge_parameter_surface_sizing_packets(packets)


def _parameter_surface_runtime_summary(report_payload: dict[str, Any] | None) -> dict[str, Any]:
  if not isinstance(report_payload, dict):
    return {}
  packets: list[dict[str, Any]] = []

  explicit_latest = _parameter_surface_sizing_packet(
    report_payload.get('latest_sizing_posture'),
    recorded_at_utc=(report_payload.get('latest_sizing_posture') or {}).get('recorded_at_utc') if isinstance(report_payload.get('latest_sizing_posture'), dict) else None,
    source_name=(report_payload.get('latest_sizing_posture') or {}).get('source_name') if isinstance(report_payload.get('latest_sizing_posture'), dict) else None,
  )
  if explicit_latest:
    packets.append(explicit_latest)

  direct_payload = _parameter_surface_sizing_packet(
    report_payload,
    recorded_at_utc=str(report_payload.get('recorded_at_utc') or report_payload.get('generated_at_utc') or '') or None,
    source_name='report_payload',
  )
  if direct_payload:
    packets.append(direct_payload)

  pair_monitor = report_payload.get('pair_monitor') if isinstance(report_payload.get('pair_monitor'), dict) else {}
  monitor_sizing = _parameter_surface_sizing_packet(
    pair_monitor.get('sizing_overview') if isinstance(pair_monitor.get('sizing_overview'), dict) else {},
    recorded_at_utc=str(pair_monitor.get('recorded_at_utc') or report_payload.get('recorded_at_utc') or '') or None,
    source_name='pair_monitor',
  )
  if monitor_sizing:
    packets.append(monitor_sizing)

  latest_heartbeat = report_payload.get('latest_heartbeat') if isinstance(report_payload.get('latest_heartbeat'), dict) else {}
  heartbeat_detail = latest_heartbeat.get('detail') if isinstance(latest_heartbeat.get('detail'), dict) else {}
  heartbeat_sizing = _parameter_surface_sizing_packet(
    heartbeat_detail,
    recorded_at_utc=str(latest_heartbeat.get('recorded_at_utc') or '') or None,
    source_name='latest_heartbeat',
  )
  if heartbeat_sizing:
    packets.append(heartbeat_sizing)

  retained_packets = report_payload.get('retained_sizing_packets')
  if isinstance(retained_packets, list):
    for packet in retained_packets:
      if not isinstance(packet, dict):
        continue
      normalized_packet = _parameter_surface_sizing_packet(
        packet,
        recorded_at_utc=str(packet.get('recorded_at_utc') or '') or None,
        source_name=str(packet.get('source_name') or '') or None,
      )
      if normalized_packet:
        packets.append(normalized_packet)

  planned_pairs = report_payload.get('planned_pairs')
  if isinstance(planned_pairs, list):
    for row in planned_pairs:
      packet = _parameter_surface_sizing_packet(row, source_name='planned_pairs')
      if packet:
        packets.append(packet)

  runtime_rows = report_payload.get('pair_runtime_summary')
  if isinstance(runtime_rows, list):
    for row in runtime_rows:
      packet = _parameter_surface_sizing_packet(row, source_name='pair_runtime_summary')
      if packet:
        packets.append(packet)

  return _merge_parameter_surface_sizing_packets(packets)


def _parameter_surface_setting_row(
  field_id: str,
  *,
  current_settings: dict[str, Any],
  default_settings: dict[str, Any],
  working_defaults: dict[str, Any],
  durable_defaults: dict[str, Any],
  durable_default_available: bool,
  durable_default_reason: str | None,
  runtime_overlay: dict[str, Any],
) -> dict[str, Any]:
  field_meta = PARAMETER_SURFACE_FIELD_CATALOG[field_id]
  overlay_supported = field_id in PARAMETER_SURFACE_OVERLAY_FIELD_IDS
  overlay_active = overlay_supported and field_id in runtime_overlay
  baseline_value = default_settings.get(field_id, current_settings.get(field_id))
  applied_value = current_settings.get(field_id)
  working_default_value = working_defaults.get(field_id, baseline_value)
  durable_default_value = durable_defaults.get(field_id) if durable_default_available else None
  overlay_status = (
    'session_overlay_active'
    if overlay_active
    else 'config_baseline'
    if overlay_supported
    else 'deferred_overlay_contract'
  )
  return {
    'parameter_id': field_id,
    'label': field_meta['label'],
    'detail_copy': field_meta.get('info_detail', ''),
    'row_kind': 'editable' if overlay_supported else 'read_only',
    'value_class': 'current_default_overlay',
    'current_value': applied_value,
    'applied_value': applied_value,
    'default_value': baseline_value,
    'baseline_value': baseline_value,
    'working_default_value': working_default_value,
    'working_default_source': 'session_working_default' if working_default_value != baseline_value else 'configured_baseline',
    'durable_default_value': durable_default_value,
    'durable_default_available': durable_default_available,
    'durable_default_status': 'available' if durable_default_available else 'unavailable',
    'durable_default_reason': durable_default_reason,
    'overlay_value': runtime_overlay.get(field_id) if overlay_active else None,
    'overlay_supported': overlay_supported,
    'overlay_active': overlay_active,
    'overlay_status': overlay_status,
    'effective_source': 'session_overlay' if overlay_active else 'config_baseline',
    'source_env_var': field_meta.get('source_env_var'),
    'source_family': 'config',
    'advisory_only': False,
    'no_auto_apply': True,
  }


def _parameter_surface_derived_row(
  field_id: str,
  *,
  runtime_summary: dict[str, Any],
  generated_at_utc: str | None,
) -> dict[str, Any]:
  field_meta = PARAMETER_SURFACE_FIELD_CATALOG[field_id]
  return {
    'parameter_id': field_id,
    'label': field_meta['label'],
    'detail_copy': field_meta.get('info_detail', ''),
    'row_kind': 'read_only',
    'value_class': 'derived',
    'derived_value': runtime_summary.get(field_id),
    'row_status': 'available' if runtime_summary.get(field_id) is not None else 'pending',
    'effective_source': 'backend_derived',
    'source_family': field_meta.get('source_family', 'service'),
    'advisory_only': False,
    'no_auto_apply': True,
    'freshness_reference': {
      'generated_at_utc': generated_at_utc,
    },
  }


def _parameter_surface_advisory_cards(
  analytical_outputs: dict[str, Any] | None,
  *,
  analytical_captured_at: str | None,
) -> list[dict[str, Any]]:
  if not isinstance(analytical_outputs, dict):
    return [
      {
        'card_id': 'parameter_sensitivity_delta',
        'title': 'Deployment sizing sensitivity',
        'value_class': 'advisory',
        'status': 'unavailable',
        'advisory_only': True,
        'no_auto_apply': True,
        'reason_summary': 'No sizing sensitivity data available yet.',
        'provenance_sources': ['report_runtime'],
        'freshness_reference': {'generated_at_utc': analytical_captured_at},
      },
      {
        'card_id': 'advisory_parameter_adjustment',
        'title': 'Sizing adjustment suggestion',
        'value_class': 'advisory',
        'status': 'unavailable',
        'advisory_only': True,
        'no_auto_apply': True,
        'reason_summary': 'No sizing adjustment suggestion available yet.',
        'provenance_sources': ['report_runtime'],
        'freshness_reference': {'generated_at_utc': analytical_captured_at},
      },
    ]

  sensitivity_packet = analytical_outputs.get('parameter_sensitivity_delta') if isinstance(analytical_outputs.get('parameter_sensitivity_delta'), dict) else {}
  advisory_packet = analytical_outputs.get('advisory_parameter_adjustment') if isinstance(analytical_outputs.get('advisory_parameter_adjustment'), dict) else {}
  sensitivity_generated_at = str(sensitivity_packet.get('generated_at_utc') or analytical_captured_at or '') or None
  advisory_freshness = advisory_packet.get('freshness_reference') if isinstance(advisory_packet.get('freshness_reference'), dict) else {}
  if 'generated_at_utc' not in advisory_freshness and analytical_captured_at is not None:
    advisory_freshness = {
      **advisory_freshness,
      'generated_at_utc': analytical_captured_at,
    }
  return [
    {
      'card_id': 'parameter_sensitivity_delta',
      'title': 'Deployment sizing sensitivity',
      'value_class': 'advisory',
      'status': 'available' if sensitivity_packet else 'unavailable',
      'advisory_only': True,
      'no_auto_apply': True,
      'baseline_value': (sensitivity_packet.get('baseline_settings') or {}).get('target_deployment_pct') if isinstance(sensitivity_packet.get('baseline_settings'), dict) else None,
      'scenario_count': len(sensitivity_packet.get('scenarios', [])) if isinstance(sensitivity_packet.get('scenarios'), list) else 0,
      'reason_summary': (
        'Compares deployment-target scenarios and their effect on sizing.'
        if sensitivity_packet
        else 'No sizing sensitivity data available yet.'
      ),
      'provenance_sources': ['parameter_sensitivity_delta'],
      'apply_guidance': 'To change this, open Set and adjust the deployment target.',
      'freshness_reference': {'generated_at_utc': sensitivity_generated_at},
      'packet': sensitivity_packet,
    },
    {
      'card_id': 'advisory_parameter_adjustment',
      'title': 'Sizing adjustment suggestion',
      'value_class': 'advisory',
      'status': str(advisory_packet.get('recommendation_status') or 'unavailable'),
      'advisory_only': True,
      'no_auto_apply': True,
      'current_value': advisory_packet.get('current_value'),
      'recommended_value': advisory_packet.get('recommended_value'),
      'apply_guidance': 'To apply, open Set and adjust the deployment target.',
      'reason_summary': str(advisory_packet.get('reason_summary') or 'No sizing adjustment suggestion available yet.'),
      'provenance_sources': list(advisory_packet.get('provenance_sources', [])) if isinstance(advisory_packet.get('provenance_sources'), list) else ['advisory_parameter_adjustment'],
      'freshness_reference': advisory_freshness,
      'packet': advisory_packet,
    },
    {
      'card_id': 'dependency_group_recommendations',
      'title': 'Parameter group recommendations',
      'value_class': 'advisory',
      'status': 'available' if isinstance(analytical_outputs.get('dependency_group_recommendations'), dict) else 'unavailable',
      'advisory_only': True,
      'no_auto_apply': True,
      'group_count': len((analytical_outputs.get('dependency_group_recommendations') or {}).get('groups', [])) if isinstance(analytical_outputs.get('dependency_group_recommendations'), dict) else 0,
      'reason_summary': (
        'Covers qualification thresholds, entry window, sizing, and cap limits.'
        if isinstance(analytical_outputs.get('dependency_group_recommendations'), dict)
        else 'No parameter group recommendations available yet.'
      ),
      'provenance_sources': ['dependency_group_recommendations'],
      'apply_guidance': 'To apply, open Set and adjust the relevant parameters.',
      'freshness_reference': {
        'generated_at_utc': (analytical_outputs.get('dependency_group_recommendations') or {}).get('freshness_reference', {}).get('generated_at_utc') if isinstance(analytical_outputs.get('dependency_group_recommendations'), dict) else analytical_captured_at,
      },
      'packet': analytical_outputs.get('dependency_group_recommendations') if isinstance(analytical_outputs.get('dependency_group_recommendations'), dict) else {},
    },
  ]


def _parameter_surface_group_payload(
  group_meta: dict[str, Any],
  *,
  current_settings: dict[str, Any],
  default_settings: dict[str, Any],
  working_defaults: dict[str, Any],
  durable_defaults: dict[str, Any],
  durable_default_available: bool,
  durable_default_reason: str | None,
  runtime_overlay: dict[str, Any],
  runtime_summary: dict[str, Any],
  analytical_outputs: dict[str, Any] | None,
  analytical_captured_at: str | None,
  generated_at_utc: str | None,
) -> dict[str, Any]:
  rows: list[dict[str, Any]] = []
  for field_id in group_meta['field_ids']:
    field_meta = PARAMETER_SURFACE_FIELD_CATALOG[field_id]
    if field_meta['value_class'] == 'setting':
      rows.append(
        _parameter_surface_setting_row(
          field_id,
          current_settings=current_settings,
          default_settings=default_settings,
          working_defaults=working_defaults,
          durable_defaults=durable_defaults,
          durable_default_available=durable_default_available,
          durable_default_reason=durable_default_reason,
          runtime_overlay=runtime_overlay,
        )
      )
    else:
      rows.append(
        _parameter_surface_derived_row(
          field_id,
          runtime_summary=runtime_summary,
          generated_at_utc=generated_at_utc,
        )
      )
  group_payload = {
    'group_id': group_meta['group_id'],
    'title': group_meta['title'],
    'summary': group_meta['summary'],
    'rows': rows,
  }
  if group_meta.get('includes_advisory_cards'):
    group_payload['advisory_cards'] = _parameter_surface_advisory_cards(
      analytical_outputs,
      analytical_captured_at=analytical_captured_at,
    )
  return group_payload


def build_parameter_surface_payload(
  current_settings: Settings | dict[str, Any] | None,
  *,
  default_settings: Settings | dict[str, Any] | None = None,
  working_defaults: Settings | dict[str, Any] | None = None,
  durable_defaults: Settings | dict[str, Any] | None = None,
  durable_default_available: bool = False,
  durable_default_reason: str | None = None,
  runtime_overlay: dict[str, Any] | None = None,
  report_payload: dict[str, Any] | None = None,
  analytical_outputs: dict[str, Any] | None = None,
  analytical_captured_at: datetime | str | None = None,
) -> dict[str, Any]:
  current_settings_payload = _settings_summary_payload(current_settings)
  default_settings_payload = _settings_summary_payload(default_settings) or dict(current_settings_payload)
  working_defaults_payload = _settings_summary_payload(working_defaults) or dict(default_settings_payload)
  durable_defaults_payload = _settings_summary_payload(durable_defaults)
  runtime_overlay_payload = dict(runtime_overlay or {})
  runtime_summary = _parameter_surface_runtime_summary(report_payload)
  latest_heartbeat_payload = (
    report_payload.get('latest_heartbeat')
    if isinstance(report_payload, dict) and isinstance(report_payload.get('latest_heartbeat'), dict)
    else {}
  )
  analytical_generated_at = (
    _iso_utc(analytical_captured_at)
    if isinstance(analytical_captured_at, datetime)
    else str(analytical_captured_at or '') or None
  )

  row_generated_at = (
    str(runtime_summary.get('recorded_at_utc') or '') or None
    or analytical_generated_at
    or str(latest_heartbeat_payload.get('recorded_at_utc') or '') or None
  )
  pages: list[dict[str, Any]] = []
  groups: list[dict[str, Any]] = []
  for page_meta in PARAMETER_SURFACE_PAGE_CATALOG:
    page_groups: list[dict[str, Any]] = []
    for group_meta in page_meta['group_catalog']:
      group_payload = {
        **_parameter_surface_group_payload(
          group_meta,
          current_settings=current_settings_payload,
          default_settings=default_settings_payload,
          working_defaults=working_defaults_payload,
          durable_defaults=durable_defaults_payload,
          durable_default_available=durable_default_available,
          durable_default_reason=durable_default_reason,
          runtime_overlay=runtime_overlay_payload,
          runtime_summary=runtime_summary,
          analytical_outputs=analytical_outputs,
          analytical_captured_at=analytical_generated_at,
          generated_at_utc=row_generated_at,
        ),
        'page_id': page_meta['page_id'],
      }
      page_groups.append(group_payload)
      groups.append(group_payload)
    pages.append(
      {
        'page_id': page_meta['page_id'],
        'title': page_meta['title'],
        'summary': page_meta['summary'],
        'lane_kind': page_meta['lane_kind'],
        'groups': page_groups,
      }
    )

  return {
    'surface_id': 'weights_and_parameters',
    'contract_version': 'tranche_e_parameter_surface.v2',
    'mode': 'page_owned_info_set_analysis',
    'surface_mode': 'page_owned_info_set_analysis',
    'authority_boundary': 'backend_owned_parameter_posture_only',
    'active_page_id': 'info',
    'page_order': [page['page_id'] for page in pages],
    'overlay_summary': {
      'active': bool(runtime_overlay_payload),
      'status': 'session_overlay_active' if runtime_overlay_payload else 'config_baseline_only',
      'applied_parameter_count': len(runtime_overlay_payload),
      'staged_parameter_ids': sorted(runtime_overlay_payload.keys()),
    },
    'default_management': {
      'configured_baseline_parameter_ids': sorted(
        field_id for field_id in PARAMETER_SURFACE_OVERLAY_FIELD_IDS if field_id in default_settings_payload
      ),
      'working_default_status': (
        'session_working_default_active'
        if any(working_defaults_payload.get(field_id) != default_settings_payload.get(field_id) for field_id in PARAMETER_SURFACE_OVERLAY_FIELD_IDS)
        else 'configured_baseline_only'
      ),
      'working_default_parameter_ids': sorted(
        field_id
        for field_id in PARAMETER_SURFACE_OVERLAY_FIELD_IDS
        if working_defaults_payload.get(field_id) != default_settings_payload.get(field_id)
      ),
      'durable_default_available': durable_default_available,
      'durable_default_status': 'available' if durable_default_available else 'unavailable',
      'durable_default_parameter_ids': sorted(
        field_id for field_id in PARAMETER_SURFACE_OVERLAY_FIELD_IDS if field_id in durable_defaults_payload
      ) if durable_default_available else [],
      'durable_default_reason': durable_default_reason,
    },
    'pages': pages,
    'groups': groups,
    'advisory_only': False,
    'manual_overlay_only': True,
    'no_auto_apply': True,
    'generated_at_utc': _iso_utc(datetime.now(UTC)),
  }


def _tail_if_path_like(value: object) -> str:
  text = str(value)
  lowered = text.lower()
  if '://' in lowered:
    return text
  if '\\' in text or '/' in text:
    return Path(text).name or text
  return text


def _format_detail_lines(detail: dict[str, Any], *, limit: int = 3) -> list[str]:
  lines: list[str] = []

  def _detail_line_value(value: Any) -> str:
    if isinstance(value, dict):
      nested_keys = sorted(str(key) for key in value.keys() if str(key))
      if not nested_keys:
        return '{dict}'
      preview = ','.join(nested_keys[:3])
      if len(nested_keys) > 3:
        preview += ',...'
      return '{dict:' + preview + '}'
    if isinstance(value, list):
      return '[list:{count}]'.format(count=len(value))
    return _tail_if_path_like(value)

  candidates = detail.get('candidates')
  if isinstance(candidates, list) and candidates:
    candidate_count = detail.get('candidate_count')
    if candidate_count not in {None, ''}:
      lines.append('{key}={value}'.format(key='candidate_count', value=candidate_count))

    remaining_slots = max(limit - len(lines), 0)
    preview_limit = min(len(candidates), max(remaining_slots, 0))
    for index, candidate in enumerate(candidates[:preview_limit], start=1):
      if not isinstance(candidate, dict):
        continue
      path_tail = (
        str(candidate.get('path_tail') or '')
        or _tail_if_path_like(candidate.get('resolved_path') or '')
        or '--'
      )
      env_label = str(
        candidate.get('environment_inferred')
        or candidate.get('environment')
        or candidate.get('source_label')
        or 'unknown'
      )
      lines.append('candidate_{idx}={path} [{env}]'.format(idx=index, path=path_tail, env=env_label))
      if len(lines) >= limit:
        break

    overflow = len(candidates) - preview_limit
    if overflow > 0 and len(lines) < limit:
      lines.append('candidates_more=+{count}'.format(count=overflow))

  for key, value in detail.items():
    if key == 'candidates':
      continue
    if value is None or value == '':
      continue
    lines.append('{key}={value}'.format(key=key, value=_detail_line_value(value)))
    if len(lines) >= limit:
      break
  return lines


def _normalize_system_log_entry(row: Any) -> dict[str, Any]:
  source = str(row['source'])
  detail = json.loads(row['detail_json']) if row['detail_json'] else {}
  operation_lane = str(row['operation_lane'] or 'sandbox')
  lane_prefix = '[{lane}]'.format(lane=operation_lane.upper())
  if source == 'service_heartbeat':
    headline = '[HEARTBEAT]{lane} {component} -> {status}'.format(
      lane=lane_prefix,
      component=row['field_a'],
      status=row['field_b'],
    )
  elif source == 'operator_action':
    headline = '[ACTION]{lane} {action}'.format(lane=lane_prefix, action=row['field_a'])
    if row['pair_id']:
      headline += ' :: {pair_id}'.format(pair_id=row['pair_id'])
  else:
    headline = '[RUNTIME]{lane} {event_type}'.format(lane=lane_prefix, event_type=row['field_a'])
    if row['pair_id']:
      headline += ' :: {pair_id}'.format(pair_id=row['pair_id'])

  detail_lines = _format_detail_lines(detail)
  if row['lane_session_id']:
    detail_lines.insert(0, 'lane_session_id={value}'.format(value=row['lane_session_id']))
  message_lines = [headline] + detail_lines
  return {
    'key': '{source}:{id}'.format(source=source, id=row['id']),
    'source': source,
    'action': str(row['field_a'] or ''),
    'detail': detail,
    'operation_lane': operation_lane,
    'lane_session_id': row['lane_session_id'],
    'recorded_at_utc': row['recorded_at_utc'],
    'message': '\n'.join(message_lines),
  }


def _bucket_start(recorded_at: datetime, bucket: str) -> datetime:
  moment = recorded_at.astimezone(UTC)
  if bucket == '5m':
    minute = (moment.minute // 5) * 5
    return moment.replace(minute=minute, second=0, microsecond=0)
  if bucket == '15m':
    minute = (moment.minute // 15) * 15
    return moment.replace(minute=minute, second=0, microsecond=0)
  if bucket == '6h':
    hour = (moment.hour // 6) * 6
    return moment.replace(hour=hour, minute=0, second=0, microsecond=0)
  if bucket == '1d':
    return moment.replace(hour=0, minute=0, second=0, microsecond=0)
  return moment.replace(second=0, microsecond=0)


def _bucket_delta(bucket: str) -> timedelta:
  if bucket == '5m':
    return timedelta(minutes=5)
  if bucket == '15m':
    return timedelta(minutes=15)
  if bucket == '6h':
    return timedelta(hours=6)
  if bucket == '1d':
    return timedelta(days=1)
  return timedelta(minutes=1)


def _window_cutoff(now: datetime, window_id: str) -> datetime | None:
  config = VISUAL_WINDOW_CONFIG.get(window_id, VISUAL_WINDOW_CONFIG['24h'])
  lookback = config.get('lookback')
  if lookback is None or window_id == 'current':
    return None
  return now - lookback


def _load_runtime_history_rows(
  connection: Any,
  *,
  operation_lane: str,
  limit: int = 400,
  include_all_lanes: bool = False,
) -> list[dict[str, Any]]:
  lane_clause = '' if include_all_lanes else 'WHERE operation_lane = ?'
  lane_params: tuple[Any, ...] = () if include_all_lanes else (operation_lane,)
  rows = connection.execute(
    f'''
    SELECT *
    FROM (
      SELECT id, recorded_at_utc, operation_lane, lane_session_id, 'service_heartbeat' AS source, component AS field_a, status AS field_b, NULL AS pair_id, detail_json
      FROM service_heartbeats
      {lane_clause}
      UNION ALL
      SELECT id, recorded_at_utc, operation_lane, lane_session_id, 'operator_action' AS source, action AS field_a, NULL AS field_b, pair_id, detail_json
      FROM operator_actions
      {lane_clause}
      UNION ALL
      SELECT id, recorded_at_utc, operation_lane, lane_session_id, 'runtime_event' AS source, event_type AS field_a, level AS field_b, pair_id, detail_json
      FROM runtime_events
      {lane_clause}
    ) combined
    ORDER BY recorded_at_utc DESC, id DESC
    LIMIT ?
    ''',
    (*lane_params, *lane_params, *lane_params, max(limit, 1)),
  ).fetchall()
  normalized: list[dict[str, Any]] = []
  for row in reversed(rows):
    normalized.append(
      {
        'source': str(row['source']),
        'field_a': row['field_a'],
        'field_b': row['field_b'],
        'pair_id': row['pair_id'],
        'operation_lane': row['operation_lane'],
        'lane_session_id': row['lane_session_id'],
        'detail': json.loads(row['detail_json']) if row['detail_json'] else {},
        'recorded_at': _parse_recorded_at(row['recorded_at_utc']),
      }
    )
  return normalized


def _filter_rows_for_window(rows: list[dict[str, Any]], *, now: datetime, window_id: str) -> list[dict[str, Any]]:
  if window_id == 'current':
    current_bucket_start = _bucket_start(now, 'snapshot')
    return [row for row in rows if row['recorded_at'] >= current_bucket_start]
  cutoff = _window_cutoff(now, window_id)
  if cutoff is None:
    return rows
  return [row for row in rows if row['recorded_at'] >= cutoff]


def _window_bucket_ids(now: datetime, window_id: str, *, rows: list[dict[str, Any]] | None = None) -> list[str]:
  window_meta = VISUAL_WINDOW_CONFIG.get(window_id, VISUAL_WINDOW_CONFIG['24h'])
  bucket = str(window_meta.get('bucket') or 'snapshot')
  end_bucket = _bucket_start(now, bucket)
  if window_id == 'current':
    return [_iso_utc(end_bucket)]

  lookback = window_meta.get('lookback')
  if lookback is not None:
    start_bucket = _bucket_start(end_bucket - lookback + _bucket_delta(bucket), bucket)
  else:
    retained_rows = rows or []
    if retained_rows:
      earliest_record = min(row['recorded_at'] for row in retained_rows)
      start_bucket = _bucket_start(earliest_record, bucket)
    else:
      start_bucket = end_bucket

  bucket_ids: list[str] = []
  cursor = start_bucket
  step = _bucket_delta(bucket)
  while cursor <= end_bucket:
    bucket_ids.append(_iso_utc(cursor))
    cursor = cursor + step
  return bucket_ids or [_iso_utc(end_bucket)]


def _visual_detail_bucket_count(detail_mode: str) -> int:
  return VISUAL_DETAIL_BUCKET_COUNT.get(detail_mode, VISUAL_DETAIL_BUCKET_COUNT['med'])


def _window_bucket_endpoints(
  now: datetime,
  window_id: str,
  *,
  detail_mode: str,
  rows: list[dict[str, Any]] | None = None,
) -> list[datetime]:
  window_meta = VISUAL_WINDOW_CONFIG.get(window_id, VISUAL_WINDOW_CONFIG['24h'])
  if window_id == 'current':
    return [_bucket_start(now, 'snapshot')]
  target_count = _visual_detail_bucket_count(detail_mode)
  bucket = str(window_meta.get('bucket') or 'snapshot')
  end_moment = _bucket_start(now, bucket)
  lookback = window_meta.get('lookback')
  if lookback is None:
    retained_rows = rows or []
    if retained_rows:
      earliest_record = min(row['recorded_at'] for row in retained_rows)
      span = max(end_moment - earliest_record, _bucket_delta(bucket) * target_count)
    else:
      span = _bucket_delta(bucket) * target_count
  else:
    span = lookback
  step = span / max(target_count, 1)
  start_moment = end_moment - span
  return [start_moment + (step * index) for index in range(1, target_count + 1)]


def _trailing_bucket_id(recorded_at: datetime, bucket_endpoints: list[datetime]) -> str:
  if not bucket_endpoints:
    return _iso_utc(recorded_at)
  for endpoint in bucket_endpoints:
    if recorded_at <= endpoint:
      return _iso_utc(endpoint)
  return _iso_utc(bucket_endpoints[-1])


def _resolve_visual_scope_id(view_id: str | None, scope_id: str | None) -> str:
  requested_scope = str(scope_id or '').strip().lower()
  if requested_scope in VISUAL_SCOPE_CATALOG:
    return requested_scope
  if view_id and view_id in VISUAL_VIEW_CATALOG:
    return str(VISUAL_VIEW_CATALOG[view_id].get('scope_id') or 'runtime_posture')
  return 'runtime_posture'


def _resolve_visual_detail_mode(detail_mode: str | None) -> str:
  normalized = str(detail_mode or '').strip().lower()
  if normalized == 'medium':
    normalized = 'med'
  return normalized if normalized in VISUAL_DETAIL_CONFIG else 'med'


def _visual_available_modes(*, table_supported: bool, report_supported: bool) -> list[str]:
  modes = ['plot']
  if table_supported:
    modes.append('table')
  if report_supported:
    modes.append('report')
  return modes


def _visual_axis_contract(view_id: str) -> dict[str, str]:
  if view_id in {'runtime_cadence', 'cycle_outcomes'}:
    return {'x': {'kind': 'temporal_bucket', 'spacing': 'uniform_bucket'}}
  if view_id in {'performance_total', 'performance_delta', 'performance_total_out', 'performance_total_in', 'performance_fees'}:
    return {'x': {'kind': 'temporal_bucket', 'spacing': 'uniform_bucket'}}
  if view_id in {'freshness_latency', 'saved_set_carry_forward'}:
    return {'x': {'kind': 'temporal_event', 'spacing': 'event_time'}}
  if view_id in {'candidate_density_curve', 'candidate_decision_boundary', 'comparative_ranking_snapshot'}:
    return {'x': {'kind': 'ordinal_rank', 'spacing': 'ordinal'}}
  if view_id == 'candidate_frontier_scatter':
    return {'x': {'kind': 'numeric_feature', 'spacing': 'feature_value'}}
  if view_id == 'analysis_linear_diagnostics':
    return {'x': {'kind': 'numeric_feature', 'spacing': 'feature_value'}}
  if view_id in {'parameter_sensitivity_delta'}:
    return {'x': {'kind': 'scenario', 'spacing': 'uniform_category'}}
  if view_id in {'actionability_status_distribution'}:
    return {'x': {'kind': 'status', 'spacing': 'uniform_category'}}
  if view_id in {'analysis_threshold_progress'}:
    return {'x': {'kind': 'progress', 'spacing': 'uniform_category'}}
  if view_id == 'performance_waterfall':
    return {'x': {'kind': 'financial_bridge', 'spacing': 'uniform_category'}}
  return {'x': {'kind': 'category', 'spacing': 'uniform_category'}}


def _visual_bucket_contract(view_id: str, *, window_id: str, detail_mode: str) -> dict[str, Any]:
  if view_id in {'runtime_cadence', 'cycle_outcomes'}:
    endpoints = _window_bucket_endpoints(datetime.now(UTC), window_id, detail_mode=detail_mode)
    interval_sec = None
    if len(endpoints) >= 2:
      interval_sec = max((endpoints[1] - endpoints[0]).total_seconds(), 0.0)
    return {
      'mode': 'trailing_aggregate',
      'interval_sec': interval_sec,
      'label_policy': 'bucket_end',
    }
  if view_id in {'performance_total', 'performance_delta', 'performance_total_out', 'performance_total_in', 'performance_fees'}:
    endpoints = _window_bucket_endpoints(datetime.now(UTC), window_id, detail_mode=detail_mode)
    interval_sec = None
    if len(endpoints) >= 2:
      interval_sec = max((endpoints[1] - endpoints[0]).total_seconds(), 0.0)
    return {
      'mode': 'trailing_aggregate',
      'interval_sec': interval_sec,
      'label_policy': 'bucket_end',
    }
  if view_id in {'freshness_latency', 'saved_set_carry_forward'}:
    return {'mode': 'event_sequence', 'interval_sec': None, 'label_policy': 'event_time'}
  if view_id == 'pair_state_distribution':
    return {'mode': 'snapshot', 'interval_sec': None, 'label_policy': 'category_label'}
  return {'mode': 'none', 'interval_sec': None, 'label_policy': 'category_label'}


def _visual_density_contract(view_id: str) -> dict[str, Any]:
  if view_id in {'runtime_cadence', 'cycle_outcomes'}:
    return {'mode': 'backend_bucket_count', 'enabled': True, 'label': 'Bucket density'}
  if view_id in {'candidate_density_curve', 'candidate_decision_boundary', 'candidate_frontier_scatter', 'analysis_linear_diagnostics'}:
    return {'mode': 'rank_detail', 'enabled': True, 'label': 'Rank detail'}
  return {'mode': 'not_applicable', 'enabled': False, 'label': 'Visual density'}


def _visual_controls_contract(view_id: str) -> dict[str, Any]:
  window_enabled = view_id in {
    'runtime_cadence',
    'cycle_outcomes',
    'freshness_latency',
    'performance_total',
    'performance_delta',
    'performance_total_out',
    'performance_total_in',
    'performance_fees',
  }
  density = _visual_density_contract(view_id)
  return {
    'window': {
      'enabled': window_enabled,
      'label': 'Window' if window_enabled else 'Current only',
    },
    'density': {
      'enabled': bool(density.get('enabled')),
      'label': density.get('label', 'Visual density'),
      'mode': density.get('mode', 'not_applicable'),
    },
  }


def _visual_window_enabled(view_id: str) -> bool:
  return bool(_visual_controls_contract(view_id)['window']['enabled'])


def _metric_number(value: Any) -> float:
  try:
    return float(Decimal(str(value)))
  except Exception:
    return 0.0


def _performance_view_config(view_id: str) -> dict[str, str]:
  configs = {
    'performance_total': {'metric_key': 'gross_dollars', 'title': 'Total'},
    'performance_delta': {'metric_key': 'net_projected_dollars', 'title': '+/-'},
    'performance_total_out': {'metric_key': 'total_cost_dollars', 'title': 'Total out'},
    'performance_total_in': {'metric_key': 'total_in_dollars', 'title': 'Total in'},
    'performance_fees': {'metric_key': 'settled_fees_dollars', 'title': 'Fees'},
  }
  return configs[view_id]


def _performance_shared_metric_configs() -> list[dict[str, str]]:
  return [
    {'id': 'performance_total', 'metric_key': 'gross_dollars', 'title': 'Total'},
    {'id': 'performance_delta', 'metric_key': 'net_projected_dollars', 'title': '+/-'},
    {'id': 'performance_total_out', 'metric_key': 'total_cost_dollars', 'title': 'Total out'},
    {'id': 'performance_total_in', 'metric_key': 'total_in_dollars', 'title': 'Total in'},
    {'id': 'performance_fees', 'metric_key': 'settled_fees_dollars', 'title': 'Fees'},
  ]


def _performance_default_visible_metric_ids() -> list[str]:
  return ['performance_total', 'performance_delta']


def _load_pair_runtime_rows(connection: Any, *, operation_lane: str, fee_reserve_dollars: Decimal) -> list[dict[str, Any]]:
  return [
    _pair_runtime_summary(
      pair,
      fee_reserve_dollars=fee_reserve_dollars,
    )
    for pair in _latest_pair_snapshots(connection, operation_lane=operation_lane)
  ]


def _performance_metric_window_id(window_id: str) -> str:
  normalized = str(window_id or '').strip().lower()
  return normalized if normalized in {'1h', '24h', '7d', 'all'} else 'all'


def _load_pair_runtime_history_snapshots(connection: Any, *, operation_lane: str) -> dict[str, list[dict[str, Any]]]:
  latest_pairs = _latest_pair_snapshots(connection, operation_lane=operation_lane)
  history: dict[str, list[dict[str, Any]]] = {}
  for pair in latest_pairs:
    pair_id = str(pair.get('pair_id') or '').strip()
    if not pair_id:
      continue
    pair_history = fetch_pair_state_history(connection, pair_id=pair_id, operation_lane=operation_lane)
    if not pair_history:
      continue
    history[pair_id] = [
      {
        'pair_id': pair_id,
        'ticker': pair.get('ticker'),
        'contract_count': pair.get('contract_count'),
        'state': snapshot['state'],
        'detail': snapshot.get('detail') or {},
        'recorded_at_utc': snapshot['recorded_at_utc'],
      }
      for snapshot in pair_history
    ]
  return history


def _build_performance_metric_timeseries(
  connection: Any,
  *,
  operation_lane: str,
  fee_reserve_dollars: Decimal,
  window_id: str,
  detail_mode: str,
  now: datetime,
) -> tuple[str, list[dict[str, Any]], datetime]:
  effective_window_id = _performance_metric_window_id(window_id)
  history_by_pair = _load_pair_runtime_history_snapshots(connection, operation_lane=operation_lane)
  history_rows = [
    {
      'pair_id': snapshot['pair_id'],
      'recorded_at': _parse_recorded_at(snapshot['recorded_at_utc']),
    }
    for snapshots in history_by_pair.values()
    for snapshot in snapshots
  ]
  bucket_endpoints = _window_bucket_endpoints(now, effective_window_id, detail_mode=detail_mode, rows=history_rows)
  ordered_bucket_ids = [_iso_utc(endpoint) for endpoint in bucket_endpoints]
  series_by_metric = {
    config['id']: {bucket_id: 0.0 for bucket_id in ordered_bucket_ids}
    for config in _performance_shared_metric_configs()
  }
  captured_at = now
  # `{total}` is the account-gross aggregate, not a per-pair sum: it is the Kalshi
  # cash balance plus the value of still-in-flight positions, less their estimated
  # fees. It is therefore built separately from the shared per-pair accumulation.
  # Only in-flight pairs count toward the position value; terminal pairs (settled,
  # canceled, failed) have already resolved into the cash balance.
  in_flight_gross_by_bucket = {bucket_id: 0.0 for bucket_id in ordered_bucket_ids}
  in_flight_fees_by_bucket = {bucket_id: 0.0 for bucket_id in ordered_bucket_ids}
  for snapshots in history_by_pair.values():
    ordered_snapshots = sorted(snapshots, key=lambda item: _parse_recorded_at(item['recorded_at_utc']))
    snapshot_index = 0
    current_summary: dict[str, Any] | None = None
    for endpoint, bucket_id in zip(bucket_endpoints, ordered_bucket_ids):
      while snapshot_index < len(ordered_snapshots):
        candidate = ordered_snapshots[snapshot_index]
        candidate_recorded_at = _parse_recorded_at(candidate['recorded_at_utc'])
        if candidate_recorded_at > endpoint:
          break
        current_summary = _pair_runtime_summary(candidate, fee_reserve_dollars=fee_reserve_dollars)
        captured_at = max(captured_at, candidate_recorded_at)
        snapshot_index += 1
      if current_summary is None:
        continue
      for config in _performance_shared_metric_configs():
        if config['id'] == 'performance_total':
          continue
        series_by_metric[config['id']][bucket_id] += _metric_number(current_summary.get(config['metric_key']))
      if not str(current_summary.get('terminal_state') or '').strip():
        in_flight_gross_by_bucket[bucket_id] += _metric_number(current_summary.get('gross_dollars'))
        in_flight_fees_by_bucket[bucket_id] += _metric_number(current_summary.get('fees_dollars'))
  for endpoint, bucket_id in zip(bucket_endpoints, ordered_bucket_ids):
    balance = float(_heartbeat_balance_at(connection, operation_lane=operation_lane, at_utc=endpoint))
    series_by_metric['performance_total'][bucket_id] = (
      balance + in_flight_gross_by_bucket[bucket_id] - in_flight_fees_by_bucket[bucket_id]
    )
  series = [
    {
      'id': config['id'],
      'label': config['title'],
      'kind': 'line',
      'unit': 'dollars',
      'points': [
        {'x': bucket_id, 'y': round(series_by_metric[config['id']].get(bucket_id, 0.0), 2)}
        for bucket_id in ordered_bucket_ids
      ],
    }
    for config in _performance_shared_metric_configs()
  ]
  return effective_window_id, series, captured_at


def _analysis_activation_state(analytical_outputs: dict[str, Any] | None) -> dict[str, Any]:
  candidate_packet = analytical_outputs.get('candidate_density_curve') if isinstance(analytical_outputs, dict) else None
  threshold_known = isinstance(candidate_packet, dict) and candidate_packet.get('candidate_row_count') is not None
  candidate_row_count = int(candidate_packet.get('candidate_row_count') or 0) if threshold_known else 0
  threshold = ANALYSIS_ACTIVATION_MIN_CANDIDATES
  ready = (
    threshold_known
    and isinstance(analytical_outputs.get('factor_contribution'), dict)
    and isinstance(analytical_outputs.get('parameter_sensitivity_delta'), dict)
    and candidate_row_count >= threshold
  )
  status = 'analysis_ready' if ready else ('threshold_progress' if threshold_known else 'threshold_undetermined')
  return {
    'ready': ready,
    'status': status,
    'threshold_known': threshold_known,
    'current_count': candidate_row_count,
    'threshold': threshold,
    'remaining_count': max(threshold - candidate_row_count, 0),
  }


def _analysis_recalculation_generated_at(
  analytical_outputs: dict[str, Any] | None,
  analytical_captured_at: datetime | str | None,
) -> str | None:
  if isinstance(analytical_captured_at, datetime):
    return _iso_utc(analytical_captured_at)
  if analytical_captured_at not in {None, ''}:
    return str(analytical_captured_at)
  advisory_packet = analytical_outputs.get('advisory_parameter_adjustment') if isinstance(analytical_outputs, dict) else None
  if isinstance(advisory_packet, dict):
    freshness_reference = advisory_packet.get('freshness_reference') if isinstance(advisory_packet.get('freshness_reference'), dict) else {}
    generated_at = str(freshness_reference.get('generated_at_utc') or '').strip()
    if generated_at:
      return generated_at
  sensitivity_packet = analytical_outputs.get('parameter_sensitivity_delta') if isinstance(analytical_outputs, dict) else None
  if isinstance(sensitivity_packet, dict):
    generated_at = str(sensitivity_packet.get('generated_at_utc') or '').strip()
    if generated_at:
      return generated_at
  return None


def _analysis_recalculation_baseline_result(
  analytical_outputs: dict[str, Any] | None,
  *,
  analytical_captured_at: datetime | str | None,
) -> dict[str, Any] | None:
  if not isinstance(analytical_outputs, dict):
    return None
  sensitivity_packet = analytical_outputs.get('parameter_sensitivity_delta') if isinstance(analytical_outputs.get('parameter_sensitivity_delta'), dict) else None
  advisory_packet = analytical_outputs.get('advisory_parameter_adjustment') if isinstance(analytical_outputs.get('advisory_parameter_adjustment'), dict) else None
  if not isinstance(sensitivity_packet, dict) or not isinstance(advisory_packet, dict):
    return None
  baseline_settings = sensitivity_packet.get('baseline_settings') if isinstance(sensitivity_packet.get('baseline_settings'), dict) else {}
  baseline_derived = sensitivity_packet.get('baseline_derived') if isinstance(sensitivity_packet.get('baseline_derived'), dict) else {}
  generated_at = _analysis_recalculation_generated_at(analytical_outputs, analytical_captured_at)
  return {
    'result_id': 'retained-current',
    'status': 'retained_current',
    'parameter_id': 'target_deployment_pct',
    'parameter_value': baseline_settings.get('target_deployment_pct'),
    'effective_density': baseline_derived.get('effective_density'),
    'dynamic_pair_notional_pct': baseline_derived.get('dynamic_pair_notional_pct'),
    'dynamic_max_contracts': baseline_derived.get('dynamic_max_contracts'),
    'binding_limiter': baseline_derived.get('binding_limiter'),
    'reason_summary': str(advisory_packet.get('reason_summary') or ''),
    'generated_at_utc': generated_at,
    'source_population_scope': advisory_packet.get('freshness_reference', {}).get('source_population_scope') if isinstance(advisory_packet.get('freshness_reference'), dict) else None,
  }


def _analysis_recalculation_proposed_result(
  analytical_outputs: dict[str, Any],
  *,
  analytical_captured_at: datetime | str | None,
) -> dict[str, Any] | None:
  sensitivity_packet = analytical_outputs.get('parameter_sensitivity_delta') if isinstance(analytical_outputs.get('parameter_sensitivity_delta'), dict) else None
  advisory_packet = analytical_outputs.get('advisory_parameter_adjustment') if isinstance(analytical_outputs.get('advisory_parameter_adjustment'), dict) else None
  if not isinstance(sensitivity_packet, dict) or not isinstance(advisory_packet, dict):
    return None
  increase_scenario = next(
    (
      scenario
      for scenario in sensitivity_packet.get('scenarios', [])
      if isinstance(scenario, dict) and scenario.get('scenario_id') == 'increase_target_deployment_pct'
    ),
    None,
  )
  recommended_value = advisory_packet.get('recommended_value')
  if not isinstance(increase_scenario, dict) or recommended_value in {None, ''}:
    return None
  derived_delta = increase_scenario.get('derived_delta') if isinstance(increase_scenario.get('derived_delta'), dict) else {}
  generated_at = _analysis_recalculation_generated_at(analytical_outputs, analytical_captured_at)
  return {
    'result_id': 'recalculated-proposal',
    'status': 'validation_succeeded',
    'parameter_id': 'target_deployment_pct',
    'parameter_value': recommended_value,
    'effective_density': derived_delta.get('effective_density'),
    'dynamic_pair_notional_pct': derived_delta.get('dynamic_pair_notional_pct'),
    'dynamic_max_contracts': derived_delta.get('dynamic_max_contracts'),
    'binding_limiter': derived_delta.get('binding_limiter'),
    'reason_summary': str(advisory_packet.get('reason_summary') or ''),
    'generated_at_utc': generated_at,
    'source_population_scope': advisory_packet.get('freshness_reference', {}).get('source_population_scope') if isinstance(advisory_packet.get('freshness_reference'), dict) else None,
  }


def _analysis_recalculation_comparison_rows(
  current_result: dict[str, Any] | None,
  proposed_result: dict[str, Any] | None,
) -> list[dict[str, Any]]:
  current_payload = current_result if isinstance(current_result, dict) else {}
  proposed_payload = proposed_result if isinstance(proposed_result, dict) else {}
  return [
    {
      'field_id': 'target_deployment_pct',
      'label': 'Target deployment',
      'current_value': current_payload.get('parameter_value'),
      'proposed_value': proposed_payload.get('parameter_value'),
    },
    {
      'field_id': 'effective_density',
      'label': 'Effective density',
      'current_value': current_payload.get('effective_density'),
      'proposed_value': proposed_payload.get('effective_density'),
    },
    {
      'field_id': 'dynamic_pair_notional_pct',
      'label': 'Dynamic pair notional cap',
      'current_value': current_payload.get('dynamic_pair_notional_pct'),
      'proposed_value': proposed_payload.get('dynamic_pair_notional_pct'),
    },
    {
      'field_id': 'dynamic_max_contracts',
      'label': 'Dynamic max contracts',
      'current_value': current_payload.get('dynamic_max_contracts'),
      'proposed_value': proposed_payload.get('dynamic_max_contracts'),
    },
    {
      'field_id': 'binding_limiter',
      'label': 'Binding limiter',
      'current_value': current_payload.get('binding_limiter'),
      'proposed_value': proposed_payload.get('binding_limiter'),
    },
  ]


def evaluate_analysis_recalculation(
  analytical_outputs: dict[str, Any] | None,
  *,
  analytical_captured_at: datetime | str | None = None,
  active_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
  activation = _analysis_activation_state(analytical_outputs)
  current_result = dict(active_result) if isinstance(active_result, dict) else _analysis_recalculation_baseline_result(
    analytical_outputs,
    analytical_captured_at=analytical_captured_at,
  )
  generated_at = _analysis_recalculation_generated_at(analytical_outputs, analytical_captured_at)
  if not activation['ready']:
    return {
      'status': 'insufficient_data',
      'tone': 'warn',
      'message': 'The retained analysis packet is not yet sufficient for recalculation. Run Find candidates to refresh the candidate evidence first.',
      'next_action': 'Run Find candidates, then retry the analysis recalculation request.',
      'threshold_state': activation,
      'generated_at_utc': generated_at,
      'current_result': current_result,
      'proposed_result': None,
      'comparison_rows': [],
      'apply_allowed': False,
      'retry_allowed': False,
    }

  advisory_packet = analytical_outputs.get('advisory_parameter_adjustment') if isinstance(analytical_outputs, dict) else None
  if not isinstance(advisory_packet, dict):
    return {
      'status': 'validation_failed',
      'tone': 'warn',
      'message': 'The retained analysis packet is missing the bounded advisory adjustment required for recalculation.',
      'next_action': 'Retry after a fresh retained analysis packet is available.',
      'threshold_state': activation,
      'generated_at_utc': generated_at,
      'current_result': current_result,
      'proposed_result': None,
      'comparison_rows': [],
      'apply_allowed': False,
      'retry_allowed': True,
    }

  recommendation_status = str(advisory_packet.get('recommendation_status') or '').strip().lower()
  proposed_result = _analysis_recalculation_proposed_result(
    analytical_outputs,
    analytical_captured_at=analytical_captured_at,
  ) if isinstance(analytical_outputs, dict) else None
  if recommendation_status != 'review_increase' or proposed_result is None:
    return {
      'status': 'validation_failed',
      'tone': 'warn',
      'message': str(
        advisory_packet.get('reason_summary')
        or 'The retained analysis packet did not validate a bounded adjustment that can be applied in this shell state.'
      ),
      'next_action': 'Retry when fresher candidate evidence is retained, or cancel and continue with the current values.',
      'threshold_state': activation,
      'generated_at_utc': generated_at,
      'current_result': current_result,
      'proposed_result': None,
      'comparison_rows': [],
      'apply_allowed': False,
      'retry_allowed': True,
    }

  return {
    'status': 'validation_succeeded',
    'tone': 'ok',
    'message': 'A bounded target deployment adjustment validated against the retained analysis packet and is ready for manual apply.',
    'next_action': 'Review the comparison and apply it only if this recalculated result should become the active analysis result for this shell session.',
    'threshold_state': activation,
    'generated_at_utc': generated_at,
    'current_result': current_result,
    'proposed_result': proposed_result,
    'comparison_rows': _analysis_recalculation_comparison_rows(current_result, proposed_result),
    'apply_allowed': True,
    'retry_allowed': True,
  }


def _available_visual_view_ids(
  scope_id: str,
  *,
  active_run_count: int,
  analytical_outputs: dict[str, Any] | None,
) -> list[str]:
  ordered: dict[str, list[str]] = {
    'runtime_posture': ['runtime_cadence', 'cycle_outcomes', 'freshness_latency', 'pair_state_distribution'],
    'performance': ['performance_total', 'performance_delta', 'performance_total_out', 'performance_total_in', 'performance_fees', 'performance_waterfall'],
    'candidate_landscape': ['candidate_density_curve', 'candidate_decision_boundary', 'candidate_frontier_scatter', 'threshold_boundary_marker', 'comparative_ranking_snapshot', 'saved_set_carry_forward'],
    'analysis': ['analysis_linear_diagnostics', 'actionability_status_distribution', 'analysis_threshold_progress'],
  }
  if scope_id == 'analysis' and _analysis_activation_state(analytical_outputs)['ready']:
    return ['analysis_linear_diagnostics', 'actionability_status_distribution', 'parameter_sensitivity_delta', 'factors_timeseries', 'factor_contribution']
  return ordered.get(scope_id, [])


def _build_visual_report(title: str, sections: list[dict[str, Any]]) -> dict[str, Any]:
  return {
    'title': title,
    'sections': sections,
  }


def _visual_packet(
  *,
  view_id: str,
  scope_id: str,
  window_id: str,
  render_mode: str,
  detail_mode: str,
  now: datetime,
  headline: str,
  next_action: str,
  series: list[dict[str, Any]],
  table: dict[str, Any] | None,
  report: dict[str, Any] | None = None,
  source_contracts: list[str],
  status: str = 'ready',
  empty_reason: str | None = None,
  captured_at: datetime | None = None,
) -> dict[str, Any]:
  view_meta = VISUAL_VIEW_CATALOG[view_id]
  scope_meta = VISUAL_SCOPE_CATALOG[scope_id]
  window_meta = VISUAL_WINDOW_CONFIG.get(window_id, VISUAL_WINDOW_CONFIG['24h'])
  captured_moment = captured_at or now
  available_modes = _visual_available_modes(
    table_supported=bool(view_meta.get('table_supported')),
    report_supported=bool(view_meta.get('report_supported')) and report is not None,
  )
  resolved_render_mode = render_mode if render_mode in available_modes else available_modes[0]
  return {
    'decision': 'planned',
    'scope': {
      'id': scope_id,
      'title': scope_meta['title'],
    },
    'view': {
      'id': view_id,
      'title': view_meta['title'],
      'family': view_meta['family'],
      'scope_id': scope_id,
      'render_mode': resolved_render_mode,
      'available_modes': available_modes,
    },
    'window': {
      'id': window_id,
      'label': window_meta['label'],
      'bucket': window_meta['bucket'],
    },
    'detail': {
      'mode': detail_mode,
      'label': VISUAL_DETAIL_CONFIG[detail_mode]['label'],
      'glyph': VISUAL_DETAIL_CONFIG[detail_mode]['glyph'],
      'contract_type': 'visual_only',
    },
    'axis': _visual_axis_contract(view_id),
    'bucket': _visual_bucket_contract(view_id, window_id=window_id, detail_mode=detail_mode),
    'density': _visual_density_contract(view_id),
    'controls': _visual_controls_contract(view_id),
    'status': status,
    'generated_at_utc': _iso_utc(now),
    'freshness': {
      'captured_at_utc': _iso_utc(captured_moment),
      'lag_sec': max((now - captured_moment).total_seconds(), 0.0),
    },
    'summary': {
      'headline': headline,
      'next_action': next_action,
    },
    'series': series,
    'categories': [],
    'table': table,
    'report': report,
    'available_modes': available_modes,
    'source_contracts': source_contracts,
    'empty_reason': empty_reason,
  }


VISUAL_EMPTY_COPY: dict[str, dict[str, str]] = {
  'pair_state_distribution': {
    'headline': 'When local pair-state rows are present, this view will show the current pair-state distribution here, with exact counts available in the table.',
    'next_action': 'Run a dry-run cycle or reconcile after the next pair plan appears to populate pair-state distribution.',
    'empty_reason': 'When local pair-state rows are present, this view will show the current pair-state distribution here.',
  },
  'runtime_cadence': {
    'headline': 'When heartbeat, operator, and runtime activity accumulate in the selected window, this view will show lane activity cadence here, with matching bucket counts in the table.',
    'next_action': 'Run scan, report, or dry-run actions to establish recent activity history for this view.',
    'empty_reason': 'When recent heartbeat, operator, and runtime activity exists, this cadence view will populate here.',
  },
  'cycle_outcomes': {
    'headline': 'When completed runtime cycles exist in the selected window, this view will show planned, blocked, and no-candidate outcome history here, with matching bucket counts in the table.',
    'next_action': 'Run a dry-run cycle to begin building completed outcome history for this view.',
    'empty_reason': 'When completed runtime cycles exist in the selected window, their outcome history will appear here.',
  },
  'freshness_latency': {
    'headline': 'When heartbeat history has accumulated, this view will show freshness age and heartbeat-gap drift here, with exact timestamps available in the table.',
    'next_action': 'Run shell actions that persist heartbeats to begin building freshness history for this view.',
    'empty_reason': 'When heartbeat history has accumulated, freshness age and heartbeat-gap drift will appear here.',
  },
  'performance_metric': {
    'headline': 'When retained live-lane monetary history is available, the selected money timeline will appear here and the matching table/report will populate below this surface.',
    'next_action': 'Create or reconcile live-lane pair state to begin building this history.',
    'empty_reason': 'When retained live-lane monetary history is available, the selected money timeline will appear here.',
  },
  'performance_waterfall': {
    'headline': 'When live-lane pair-runtime monetary rows are available, this bridge will show how total out, fees, and total in reconcile to net projected value, with matching table/report detail.',
    'next_action': 'Create or reconcile live-lane pair state before using the bridge as a money check.',
    'empty_reason': 'When live-lane pair-runtime monetary rows are available, this bridge will show the money reconciliation here.',
  },
  'candidate_density_curve': {
    'headline': 'This view displays the score-and-margin density shape of the retained candidate set when a candidate review packet is available.',
    'next_action': 'It populates after Find candidates or a dry-run cycle retains that packet.',
    'empty_reason': 'This view displays the score-and-margin density shape of the retained candidate set when a candidate review packet is available. It populates after Find candidates or a dry-run cycle retains that packet.',
  },
  'candidate_decision_boundary': {
    'headline': 'This view displays the retained decision-boundary reading for surfaced candidates, with weighted score, threshold, and score-margin context.',
    'next_action': 'It populates after Find candidates or a dry-run cycle retains the decision packet.',
    'empty_reason': 'This view displays the retained decision-boundary reading for surfaced candidates, with weighted score, threshold, and score-margin context. It populates after Find candidates or a dry-run cycle retains the decision packet.',
  },
  'threshold_boundary_marker': {
    'headline': 'This view displays each surfaced candidate\'s distance from the active qualification gates when retained threshold evidence is available.',
    'next_action': 'It populates after Find candidates or a dry-run cycle retains the threshold packet.',
    'empty_reason': 'This view displays each surfaced candidate\'s distance from the active qualification gates when retained threshold evidence is available. It populates after Find candidates or a dry-run cycle retains the threshold packet.',
  },
  'candidate_frontier_scatter': {
    'headline': 'This view displays the opportunity shape across edge and liquidity, with selected, near-miss, and rejected cohorts separated in the same surface.',
    'next_action': 'It populates after Find candidates or a dry-run cycle retains the frontier packet.',
    'empty_reason': 'This view displays the opportunity shape across edge and liquidity, with selected, near-miss, and rejected cohorts separated in the same surface. It populates after Find candidates or a dry-run cycle retains the frontier packet.',
  },
  'comparative_ranking_snapshot': {
    'headline': 'This view displays the retained ordinal ranking snapshot for the current surfaced leaders when a candidate ranking packet is available.',
    'next_action': 'It populates after Find candidates or a dry-run cycle retains the ranking snapshot.',
    'empty_reason': 'This view displays the retained ordinal ranking snapshot for the current surfaced leaders when a candidate ranking packet is available. It populates after Find candidates or a dry-run cycle retains the ranking snapshot.',
  },
  'saved_set_carry_forward': {
    'headline': 'This view displays how saved candidate selections carry forward over time when saved-set history exists.',
    'next_action': 'It populates after at least one candidate selection is saved.',
    'empty_reason': 'This view displays how saved candidate selections carry forward over time when saved-set history exists. It populates after at least one candidate selection is saved.',
  },
  'analysis_threshold_progress': {
    'headline': 'When enough retained candidate rows are available, this view will show threshold progress here and keep the current-versus-target count visible until analysis activation is reached.',
    'next_action': 'Run Find candidates to continue building the retained row count for this view.',
    'empty_reason': 'When enough retained candidate rows are available, threshold progress will appear here.',
  },
  'analysis_linear_diagnostics': {
    'headline': 'When retained candidate feature vectors are available, this view will project the current diagnostic shape here and keep matching table/report detail available.',
    'next_action': 'Run Find candidates to retain candidate math evidence for this view.',
    'empty_reason': 'When retained candidate feature vectors are available, the diagnostic projection will appear here.',
  },
  'factors_timeseries': {
    'headline': 'When retained candidate run history is available, this view will show factor-weight history here and keep exact run values available in the table.',
    'next_action': 'Run Find candidates to begin building factor run history for this view.',
    'empty_reason': 'When retained candidate run history is available, factor-weight history will appear here.',
  },
  'factor_contribution': {
    'headline': 'When retained factor evidence is available, this view will show the current factor contribution mix here and keep matching table/report detail available.',
    'next_action': 'Run Find candidates to retain the factor contribution inputs for this view.',
    'empty_reason': 'When retained factor evidence is available, the current factor contribution mix will appear here.',
  },
  'parameter_sensitivity_delta': {
    'headline': 'When retained bounded sensitivity scenarios are available, this view will show contract-impact deltas here and keep exact scenario values available in the table and report.',
    'next_action': 'Run Find candidates to retain the bounded sensitivity scenarios for this view.',
    'empty_reason': 'When retained bounded sensitivity scenarios are available, scenario deltas will appear here.',
  },
  'actionability_status_distribution': {
    'headline': 'When saved candidate selections have been evaluated, this view will show sandbox and live actionability history here, with exact counts available in the table.',
    'next_action': 'Save and evaluate a candidate selection to begin building this history.',
    'empty_reason': 'When saved candidate selections have been evaluated, actionability history will appear here.',
  },
}


def _visual_empty_copy(view_id: str) -> dict[str, str]:
  if view_id in {'performance_total', 'performance_delta', 'performance_total_out', 'performance_total_in', 'performance_fees'}:
    return VISUAL_EMPTY_COPY['performance_metric']
  return VISUAL_EMPTY_COPY[view_id]


def _build_pair_state_distribution_visual(
  connection: Any,
  *,
  operation_lane: str,
  window_id: str,
  mode: str,
  detail_mode: str,
  now: datetime,
) -> dict[str, Any]:
  pairs = _latest_pair_snapshots(connection, operation_lane=operation_lane)
  if not pairs:
    empty_copy = _visual_empty_copy('pair_state_distribution')
    return _visual_packet(
      view_id='pair_state_distribution',
      scope_id='runtime_posture',
      window_id=window_id,
      render_mode='plot',
      detail_mode=detail_mode,
      now=now,
      headline=empty_copy['headline'],
      next_action=empty_copy['next_action'],
      series=[],
      table=None,
      source_contracts=['pair_states'],
      status='empty',
      empty_reason=empty_copy['empty_reason'],
    )

  state_counts: dict[str, int] = {}
  for pair in pairs:
    state_name = str(pair['state']).upper()
    state_counts[state_name] = state_counts.get(state_name, 0) + 1
  ordered_states = sorted(state_counts.keys(), key=lambda name: (PAIR_STATE_PRIORITY.get(name, 99), name))
  points = [{'x': state_name, 'y': state_counts[state_name]} for state_name in ordered_states]
  table = {
    'columns': ['State', 'Count'],
    'rows': [[state_name, state_counts[state_name]] for state_name in ordered_states],
  }
  table_payload = table if mode == 'table' else table
  return _visual_packet(
    view_id='pair_state_distribution',
    scope_id='runtime_posture',
    window_id=window_id,
    render_mode='table' if mode == 'table' else 'plot',
    detail_mode=detail_mode,
    now=now,
    headline=f'Current pair attention is concentrated across {len(ordered_states)} visible states.',
    next_action='Review reconcile if error or partial states begin to dominate the active load.',
    series=[{'id': 'pair_state_count', 'label': 'Pairs', 'kind': 'bar', 'unit': 'count', 'points': points}],
    table=table_payload,
    source_contracts=['pair_states'],
    captured_at=max((_parse_recorded_at(pair['recorded_at_utc']) for pair in pairs), default=now),
  )


def _build_runtime_cadence_visual(
  connection: Any,
  *,
  operation_lane: str,
  window_id: str,
  mode: str,
  detail_mode: str,
  now: datetime,
) -> dict[str, Any]:
  rows = _filter_rows_for_window(
    _load_runtime_history_rows(
      connection,
      operation_lane=operation_lane,
      include_all_lanes=True,
    ),
    now=now,
    window_id=window_id,
  )
  if not rows:
    empty_copy = _visual_empty_copy('runtime_cadence')
    return _visual_packet(
      view_id='runtime_cadence',
      scope_id='runtime_posture',
      window_id=window_id,
      render_mode='plot',
      detail_mode=detail_mode,
      now=now,
      headline=empty_copy['headline'],
      next_action=empty_copy['next_action'],
      series=[],
      table=None,
      source_contracts=['service_heartbeats', 'operator_actions', 'runtime_events'],
      status='empty',
      empty_reason=empty_copy['empty_reason'],
    )

  bucket = VISUAL_WINDOW_CONFIG.get(window_id, VISUAL_WINDOW_CONFIG['24h'])['bucket']
  bucket_endpoints = _window_bucket_endpoints(now, window_id, detail_mode=detail_mode, rows=rows)
  series_by_source: dict[str, dict[str, int]] = {
    'service_heartbeat': {},
    'operator_action': {},
    'runtime_event': {},
  }
  for row in rows:
    bucket_id = _trailing_bucket_id(row['recorded_at'], bucket_endpoints)
    source_id = str(row.get('source') or '')
    if source_id not in series_by_source:
      continue
    source_counts = series_by_source[source_id]
    source_counts[bucket_id] = source_counts.get(bucket_id, 0) + 1
  ordered_buckets = [_iso_utc(endpoint) for endpoint in bucket_endpoints]
  source_labels = {
    'service_heartbeat': 'Heartbeats',
    'operator_action': 'Actions',
    'runtime_event': 'Runtime',
  }
  source_chip_labels = {
    'service_heartbeat': 'HEARTBEAT',
    'operator_action': 'ACTION',
    'runtime_event': 'RUNTIME',
  }
  source_tooltips = {
    'service_heartbeat': 'Service heartbeat entries recorded per time bucket',
    'operator_action': 'Operator actions recorded per time bucket',
    'runtime_event': 'Runtime events recorded per time bucket',
  }
  chart_series = [
    {
      'id': source,
      'label': source_labels[source],
      'chip_label': source_chip_labels[source],
      'tooltip': source_tooltips[source],
      'kind': 'line',
      'unit': 'count',
      'points': [{'x': bucket_id, 'y': series_by_source[source].get(bucket_id, 0)} for bucket_id in ordered_buckets],
    }
    for source in ('service_heartbeat', 'operator_action', 'runtime_event')
  ]
  table = {
    'columns': ['Bucket', 'Heartbeats', 'Actions', 'Runtime'],
    'rows': [
      [
        bucket_id,
        series_by_source['service_heartbeat'].get(bucket_id, 0),
        series_by_source['operator_action'].get(bucket_id, 0),
        series_by_source['runtime_event'].get(bucket_id, 0),
      ]
      for bucket_id in ordered_buckets
    ],
  }
  return _visual_packet(
    view_id='runtime_cadence',
    scope_id='runtime_posture',
    window_id=window_id,
    render_mode='table' if mode == 'table' else 'plot',
    detail_mode=detail_mode,
    now=now,
    headline='Heartbeat, operator, and runtime activity remain visible across the selected window.',
    next_action='Review reconcile if heartbeat continuity drops or action/runtime cadence diverges across buckets.',
    series=chart_series,
    table=table,
    source_contracts=['service_heartbeats', 'operator_actions', 'runtime_events'],
    captured_at=max((row['recorded_at'] for row in rows), default=now),
  )


def _build_cycle_outcomes_visual(
  connection: Any,
  *,
  operation_lane: str,
  window_id: str,
  mode: str,
  detail_mode: str,
  now: datetime,
) -> dict[str, Any]:
  rows = _filter_rows_for_window(
    _load_runtime_history_rows(connection, operation_lane=operation_lane),
    now=now,
    window_id=window_id,
  )
  heartbeat_rows = [row for row in rows if row['source'] == 'service_heartbeat' and row['field_b'] == 'cycle-complete']
  if not heartbeat_rows:
    empty_copy = _visual_empty_copy('cycle_outcomes')
    return _visual_packet(
      view_id='cycle_outcomes',
      scope_id='runtime_posture',
      window_id=window_id,
      render_mode='plot',
      detail_mode=detail_mode,
      now=now,
      headline=empty_copy['headline'],
      next_action=empty_copy['next_action'],
      series=[],
      table=None,
      source_contracts=['service_heartbeats', 'runtime_events'],
      status='empty',
      empty_reason=empty_copy['empty_reason'],
    )

  bucket_endpoints = _window_bucket_endpoints(now, window_id, detail_mode=detail_mode, rows=heartbeat_rows)
  outcome_counts: dict[str, dict[str, int]] = {'planned': {}, 'blocked': {}, 'no_candidate': {}}
  for row in heartbeat_rows:
    detail = row['detail']
    bucket_id = _trailing_bucket_id(row['recorded_at'], bucket_endpoints)
    if int(detail.get('planned_pair_count', 0) or 0) > 0:
      outcome = 'planned'
    elif str(detail.get('blocked_reason') or '').lower() in {'no_viable_candidates', 'no_candidate'}:
      outcome = 'no_candidate'
    elif detail.get('blocked_reason'):
      outcome = 'blocked'
    else:
      outcome = 'blocked'
    outcome_counts[outcome][bucket_id] = outcome_counts[outcome].get(bucket_id, 0) + 1
  ordered_buckets = [_iso_utc(endpoint) for endpoint in bucket_endpoints]
  labels = {'planned': 'Planned', 'blocked': 'Blocked', 'no_candidate': 'No candidate'}
  chart_series = [
    {
      'id': outcome,
      'label': labels[outcome],
      'kind': 'bar',
      'unit': 'count',
      'points': [{'x': bucket_id, 'y': outcome_counts[outcome].get(bucket_id, 0)} for bucket_id in ordered_buckets],
    }
    for outcome in ('planned', 'blocked', 'no_candidate')
  ]
  table = {
    'columns': ['Bucket', 'Planned', 'Blocked', 'No candidate'],
    'rows': [
      [bucket_id, outcome_counts['planned'].get(bucket_id, 0), outcome_counts['blocked'].get(bucket_id, 0), outcome_counts['no_candidate'].get(bucket_id, 0)]
      for bucket_id in ordered_buckets
    ],
  }
  return _visual_packet(
    view_id='cycle_outcomes',
    scope_id='runtime_posture',
    window_id=window_id,
    render_mode='table' if mode == 'table' else 'plot',
    detail_mode=detail_mode,
    now=now,
    headline='Cycle outcomes show how often dry-run work ends planned, blocked, or candidate-empty.',
    next_action='Review reconcile or scan cadence if blocked or no-candidate density starts to rise.',
    series=chart_series,
    table=table,
    source_contracts=['service_heartbeats', 'runtime_events'],
    captured_at=max((row['recorded_at'] for row in heartbeat_rows), default=now),
  )


def _build_freshness_latency_visual(
  connection: Any,
  *,
  operation_lane: str,
  window_id: str,
  mode: str,
  detail_mode: str,
  now: datetime,
) -> dict[str, Any]:
  rows = _filter_rows_for_window(
    [
      row
      for row in _load_runtime_history_rows(connection, operation_lane=operation_lane)
      if row['source'] == 'service_heartbeat'
    ],
    now=now,
    window_id=window_id,
  )
  if not rows:
    empty_copy = _visual_empty_copy('freshness_latency')
    return _visual_packet(
      view_id='freshness_latency',
      scope_id='runtime_posture',
      window_id=window_id,
      render_mode='plot',
      detail_mode=detail_mode,
      now=now,
      headline=empty_copy['headline'],
      next_action=empty_copy['next_action'],
      series=[],
      table=None,
      source_contracts=['service_heartbeats', 'runtime_events'],
      status='empty',
      empty_reason=empty_copy['empty_reason'],
    )

  gap_points: list[dict[str, Any]] = []
  age_points: list[dict[str, Any]] = []
  table_rows: list[list[Any]] = []
  previous: datetime | None = None
  for row in rows:
    recorded_at = row['recorded_at']
    gap_sec = 0.0 if previous is None else max((recorded_at - previous).total_seconds(), 0.0)
    age_sec = max((now - recorded_at).total_seconds(), 0.0)
    x_value = _iso_utc(recorded_at)
    gap_points.append({'x': x_value, 'y': round(gap_sec, 2)})
    age_points.append({'x': x_value, 'y': round(age_sec, 2)})
    table_rows.append([x_value, round(gap_sec, 2), round(age_sec, 2), row['field_b']])
    previous = recorded_at

  packet = _visual_packet(
    view_id='freshness_latency',
    scope_id='runtime_posture',
    window_id=window_id,
    render_mode='table' if mode == 'table' else 'plot',
    detail_mode=detail_mode,
    now=now,
    headline='Freshness history shows current data age and heartbeat gap drift.',
    next_action='Review runtime cadence if heartbeat gaps widen or freshness age trends upward.',
    series=[
      {'id': 'heartbeat_gap_sec', 'label': 'Heartbeat gap', 'chip_label': 'GAP', 'tooltip': 'Elapsed time between consecutive heartbeat pulses (seconds)', 'kind': 'line', 'unit': 'sec', 'points': gap_points},
      {'id': 'data_age_sec', 'label': 'Data age', 'chip_label': 'AGE', 'tooltip': 'Time since the most recent heartbeat signal was received (seconds)', 'kind': 'line', 'unit': 'sec', 'points': age_points},
    ],
    table={'columns': ['Recorded at', 'Gap sec', 'Age sec', 'Status'], 'rows': table_rows},
    source_contracts=['service_heartbeats', 'runtime_events'],
    captured_at=max((row['recorded_at'] for row in rows), default=now),
  )
  packet['shared_graph_mode'] = True
  packet['shared_series_contract'] = {
    'scope_id': 'runtime_posture',
    'toggle_semantics': 'independent_series_visibility',
    'metric_ids': ['heartbeat_gap_sec', 'data_age_sec'],
    'default_visible_metric_ids': ['heartbeat_gap_sec', 'data_age_sec'],
  }
  return packet


def _build_performance_metric_visual(
  connection: Any,
  *,
  operation_lane: str,
  window_id: str,
  mode: str,
  detail_mode: str,
  now: datetime,
  settings: Settings,
  view_id: str,
) -> dict[str, Any]:
  metric_config = _performance_view_config(view_id)
  metric_key = metric_config['metric_key']
  metric_title = metric_config['title']
  effective_window_id = _performance_metric_window_id(window_id)
  rows = _load_pair_runtime_rows(
    connection,
    operation_lane=operation_lane,
    fee_reserve_dollars=Decimal(str(settings.fee_reserve_dollars)),
  )
  if not rows:
    empty_copy = _visual_empty_copy(view_id)
    packet = _visual_packet(
      view_id=view_id,
      scope_id='performance',
      window_id=effective_window_id,
      render_mode=mode,
      detail_mode=detail_mode,
      now=now,
      headline=empty_copy['headline'],
      next_action=empty_copy['next_action'],
      series=[],
      table=None,
      report=None,
      source_contracts=['pair_states', 'pair_runtime_summary'],
      status='empty',
      empty_reason=empty_copy['empty_reason'],
    )
    packet['shared_graph_mode'] = True
    packet['shared_series_contract'] = {
      'scope_id': 'performance',
      'toggle_semantics': 'independent_series_visibility',
      'metric_ids': [config['id'] for config in _performance_shared_metric_configs()],
      'default_visible_metric_ids': _performance_default_visible_metric_ids(),
      'financial_story': 'same_pair_set_all_metrics',
    }
    packet['no_workflow_authority'] = True
    return packet

  if view_id == 'performance_total':
    # Headline matches the series: account gross = cash + in-flight position value
    # less in-flight estimated fees, never a per-pair sum.
    in_flight_rows = [row for row in rows if not str(row.get('terminal_state') or '').strip()]
    in_flight_gross = sum((Decimal(str(row.get('gross_dollars') or '0')) for row in in_flight_rows), Decimal('0'))
    in_flight_fees = sum((Decimal(str(row.get('fees_dollars') or '0')) for row in in_flight_rows), Decimal('0'))
    total_metric = _heartbeat_balance_at(connection, operation_lane=operation_lane, at_utc=now) + in_flight_gross - in_flight_fees
  else:
    total_metric = sum((Decimal(str(row.get(metric_key) or '0')) for row in rows), Decimal('0'))
  effective_window_id, series, captured_at = _build_performance_metric_timeseries(
    connection,
    operation_lane=operation_lane,
    fee_reserve_dollars=Decimal(str(settings.fee_reserve_dollars)),
    window_id=window_id,
    detail_mode=detail_mode,
    now=now,
  )
  report = _build_visual_report(
    '{title} report'.format(title=metric_title),
    [
      {
        'heading': 'Current posture',
        'lines': [
          '{title}: {value}'.format(title=metric_title, value=str(total_metric)),
          'Pairs included: {count}'.format(count=len(rows)),
          'Performance uses live-lane monetary data only; heartbeat and freshness stay in Runtime.',
        ],
      },
      {
        'heading': 'Largest contributors',
        'lines': [
          '{ticker} :: {state} :: {value}'.format(
            ticker=str(row.get('ticker') or '--'),
            state=str(row.get('state') or '--'),
            value=str(row.get(metric_key) or '--'),
          )
          for row in sorted(rows, key=lambda row: abs(_metric_number(row.get(metric_key))), reverse=True)[:3]
        ],
      },
    ],
  )
  packet = _visual_packet(
    view_id=view_id,
    scope_id='performance',
    window_id=effective_window_id,
    render_mode=mode,
    detail_mode=detail_mode,
    now=now,
    headline='Performance supporting metrics now show the live-lane money story over time instead of per-pair snapshot bars.',
    next_action='Toggle Total, +/-, Total out, Total in, and Fees to compare the same live-lane financial timeline across the selected window.',
    series=series,
    table={
      'columns': ['Pair', 'State', 'P&L', '+/-', 'Total out', 'Total in', 'Fees'],
      'rows': [
        [
          row.get('ticker') or row.get('pair_id'),
          row.get('state'),
          row.get('gross_dollars'),
          row.get('net_projected_dollars'),
          row.get('total_cost_dollars'),
          row.get('total_in_dollars'),
          row.get('settled_fees_dollars'),
        ]
        for row in rows
      ],
    },
    report=report,
    source_contracts=['pair_states', 'pair_runtime_summary'],
    captured_at=captured_at,
  )
  packet['shared_graph_mode'] = True
  packet['shared_series_contract'] = {
    'scope_id': 'performance',
    'toggle_semantics': 'independent_series_visibility',
    'metric_ids': [config['id'] for config in _performance_shared_metric_configs()],
    'default_visible_metric_ids': _performance_default_visible_metric_ids(),
    'financial_story': 'same_pair_set_all_metrics',
  }
  packet['no_workflow_authority'] = True
  return packet


def _build_performance_waterfall_visual(
  connection: Any,
  *,
  operation_lane: str,
  window_id: str,
  mode: str,
  detail_mode: str,
  now: datetime,
  settings: Settings,
) -> dict[str, Any]:
  del window_id
  rows = _load_pair_runtime_rows(
    connection,
    operation_lane=operation_lane,
    fee_reserve_dollars=Decimal(str(settings.fee_reserve_dollars)),
  )
  if not rows:
    empty_copy = _visual_empty_copy('performance_waterfall')
    packet = _visual_packet(
      view_id='performance_waterfall',
      scope_id='performance',
      window_id='current',
      render_mode=mode,
      detail_mode=detail_mode,
      now=now,
      headline=empty_copy['headline'],
      next_action=empty_copy['next_action'],
      series=[],
      table=None,
      report=None,
      source_contracts=['pair_states', 'pair_runtime_summary'],
      status='empty',
      empty_reason=empty_copy['empty_reason'],
    )
    packet['waterfall_bridge'] = {
      'formula': 'total_in_dollars - total_cost_dollars - projected_fee_reserve_dollars = net_projected_dollars',
      'status': 'empty',
      'pair_count': 0,
    }
    packet['no_workflow_authority'] = True
    return packet

  total_out = sum((Decimal(str(row.get('total_cost_dollars') or '0')) for row in rows), Decimal('0'))
  total_in = sum((Decimal(str(row.get('total_in_dollars') or '0')) for row in rows), Decimal('0'))
  net_projected = sum((Decimal(str(row.get('net_projected_dollars') or '0')) for row in rows), Decimal('0'))
  gross_total = sum((Decimal(str(row.get('gross_dollars') or '0')) for row in rows), Decimal('0'))
  realized_fees = sum((Decimal(str(row.get('fees_dollars') or '0')) for row in rows), Decimal('0'))
  projected_fee_reserve = total_in - total_out - net_projected
  after_out = -total_out
  after_fees = after_out - projected_fee_reserve
  after_in = after_fees + total_in
  reconciliation_delta = after_in - net_projected
  points = [
    {
      'x': 'Total out',
      'y': float(-total_out),
      'start': 0.0,
      'end': float(after_out),
      'role': 'outflow',
    },
    {
      'x': 'Projected fees',
      'y': float(-projected_fee_reserve),
      'start': float(after_out),
      'end': float(after_fees),
      'role': 'fee_drag',
    },
    {
      'x': 'Total in',
      'y': float(total_in),
      'start': float(after_fees),
      'end': float(after_in),
      'role': 'inflow',
    },
    {
      'x': 'Net projected',
      'y': float(net_projected),
      'start': 0.0,
      'end': float(net_projected),
      'role': 'net_projected_total',
      'is_total': True,
    },
  ]
  report = _build_visual_report(
    'Performance bridge report',
    [
      {
        'heading': 'Financial bridge',
        'lines': [
          'Total out: {value}'.format(value=str(total_out)),
          'Projected fee reserve: {value}'.format(value=str(projected_fee_reserve)),
          'Total in: {value}'.format(value=str(total_in)),
          'Net projected: {value}'.format(value=str(net_projected)),
        ],
      },
      {
        'heading': 'Reconciliation',
        'lines': [
          'Formula: total in - total out - projected fee reserve = net projected.',
          'Reconciliation delta: {value}'.format(value=str(reconciliation_delta)),
          'Live-lane rows included: {count}'.format(count=len(rows)),
        ],
      },
    ],
  )
  packet = _visual_packet(
    view_id='performance_waterfall',
    scope_id='performance',
    window_id='current',
    render_mode=mode,
    detail_mode=detail_mode,
    now=now,
    headline='Performance bridge shows how live-lane total out, projected fee drag, and total in reconcile to net projected value.',
    next_action='Use the bridge to verify the financial story before moving to deeper trend diagnostics.',
    series=[
      {
        'id': 'performance_waterfall_bridge',
        'label': 'Financial bridge',
        'kind': 'waterfall',
        'unit': 'dollars',
        'points': points,
      }
    ],
    table={
      'columns': ['Step', 'Delta', 'Start', 'End'],
      'rows': [[point['x'], point['y'], point['start'], point['end']] for point in points],
    },
    report=report,
    source_contracts=['pair_states', 'pair_runtime_summary'],
    captured_at=now,
  )
  packet['waterfall_bridge'] = {
    'formula': 'total_in_dollars - total_cost_dollars - projected_fee_reserve_dollars = net_projected_dollars',
    'source_fields': ['total_cost_dollars', 'total_in_dollars', 'net_projected_dollars', 'gross_dollars', 'fees_dollars'],
    'total_out_dollars': str(total_out),
    'projected_fee_reserve_dollars': str(projected_fee_reserve),
    'realized_fees_dollars': str(realized_fees),
    'total_in_dollars': str(total_in),
    'gross_dollars': str(gross_total),
    'net_projected_dollars': str(net_projected),
    'computed_net_projected_dollars': str(after_in),
    'reconciliation_delta_dollars': str(reconciliation_delta),
    'pair_count': len(rows),
    'trend_story': 'current_slice_bridge_ready_for_history_extension',
  }
  packet['no_workflow_authority'] = True
  return packet


def _load_latest_analytical_outputs(
  connection: Any,
  *,
  operation_lane: str,
) -> tuple[dict[str, Any] | None, datetime | None]:
  rows = connection.execute(
    '''
    SELECT recorded_at_utc, detail_json
    FROM (
      SELECT recorded_at_utc, detail_json, id
      FROM runtime_events
      WHERE operation_lane = ? AND event_type = 'scan_complete'
      UNION ALL
      SELECT recorded_at_utc, detail_json, id
      FROM service_heartbeats
      WHERE operation_lane = ? AND component = 'runtime-loop' AND status = 'cycle-complete'
    ) combined
    ORDER BY recorded_at_utc DESC, id DESC
    LIMIT 12
    ''',
    (operation_lane, operation_lane),
  ).fetchall()
  for row in rows:
    detail = json.loads(row['detail_json']) if row['detail_json'] else {}
    analytical_outputs = detail.get('analytical_outputs')
    if isinstance(analytical_outputs, dict) and analytical_outputs:
      return analytical_outputs, _parse_recorded_at(row['recorded_at_utc'])
  return None, None


def _candidate_visual_empty_packet(
  *,
  view_id: str,
  detail_mode: str,
  now: datetime,
) -> dict[str, Any]:
  empty_copy = _visual_empty_copy(view_id)
  return _visual_packet(
    view_id=view_id,
    scope_id='candidate_landscape',
    window_id='current',
    render_mode='plot',
    detail_mode=detail_mode,
    now=now,
    headline=empty_copy['headline'],
    next_action=empty_copy['next_action'],
    series=[],
    table=None,
    report=None,
    source_contracts=VISUAL_VIEW_CATALOG[view_id]['source_contracts'],
    status='empty',
    empty_reason=empty_copy['empty_reason'],
  )


def _analysis_visual_empty_packet(
  *,
  view_id: str,
  detail_mode: str,
  now: datetime,
  captured_at: datetime | None,
) -> dict[str, Any]:
  empty_copy = _visual_empty_copy(view_id)
  packet = _visual_packet(
    view_id=view_id,
    scope_id='analysis',
    window_id='current',
    render_mode='plot',
    detail_mode=detail_mode,
    now=now,
    headline=empty_copy['headline'],
    next_action=empty_copy['next_action'],
    series=[],
    table=None,
    report=None,
    source_contracts=VISUAL_VIEW_CATALOG[view_id]['source_contracts'],
    status='empty',
    empty_reason=empty_copy['empty_reason'],
    captured_at=captured_at or now,
  )
  packet['advisory_only'] = True
  packet['no_workflow_authority'] = True
  return packet


def _candidate_math_contract(analytical_outputs: dict[str, Any] | None) -> dict[str, Any] | None:
  packet = analytical_outputs.get('candidate_math_evidence_contract') if isinstance(analytical_outputs, dict) else None
  return packet if isinstance(packet, dict) else None


def _candidate_math_rows(analytical_outputs: dict[str, Any] | None) -> list[dict[str, Any]]:
  contract = _candidate_math_contract(analytical_outputs)
  rows = contract.get('candidate_evidence_rows') if isinstance(contract, dict) else []
  return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _candidate_weighted_score(row: dict[str, Any]) -> Decimal:
  score = row.get('composite_score') if isinstance(row.get('composite_score'), dict) else {}
  return Decimal(str(score.get('weighted_score') or '0'))


def _candidate_score_threshold(rows: list[dict[str, Any]]) -> Decimal:
  selected_scores = [
    _candidate_weighted_score(row)
    for row in rows
    if (row.get('threshold_outcome') or {}).get('selected_by_current_policy') is True
  ]
  return min(selected_scores, default=Decimal('1'))


def _candidate_row_rank(row: dict[str, Any], index: int) -> int:
  try:
    return int(row.get('rank') or index)
  except (TypeError, ValueError):
    return index


def _candidate_elbow_breakpoints(rows: list[dict[str, Any]], *, limit: int = 3) -> list[dict[str, Any]]:
  ordered = sorted(rows, key=lambda row: _candidate_row_rank(row, 9999))
  scores = [_candidate_weighted_score(row) for row in ordered]
  breakpoints: list[dict[str, Any]] = []
  for index in range(1, len(scores) - 1):
    first_difference = scores[index] - scores[index - 1]
    second_difference = (scores[index + 1] - scores[index]) - first_difference
    breakpoints.append(
      {
        'rank': _candidate_row_rank(ordered[index], index + 1),
        'ticker': ordered[index].get('ticker'),
        'weighted_score': str(scores[index]),
        'first_difference': str(first_difference),
        'second_difference': str(second_difference),
        'elbow_strength': str(abs(second_difference)),
      }
    )
  return sorted(breakpoints, key=lambda row: Decimal(str(row.get('elbow_strength') or '0')), reverse=True)[:limit]


def _candidate_outcome_history_timeline(connection: Any, *, operation_lane: str) -> list[dict[str, Any]]:
  rows = connection.execute(
    '''
    SELECT r.recorded_at_utc, r.operation_lane, c.detail_json
    FROM candidate_review_runs r
    INNER JOIN candidate_review_candidates c ON c.run_id = r.run_id
    WHERE r.operation_lane = ?
    ORDER BY r.recorded_at_utc ASC, c.id ASC
    ''',
    (operation_lane,),
  ).fetchall()
  buckets: dict[str, dict[str, Any]] = {}
  for row in rows:
    recorded_at = str(row['recorded_at_utc'] or '')
    bucket = buckets.setdefault(
      recorded_at,
      {
        'recorded_at_utc': recorded_at,
        'operation_lane': str(row['operation_lane'] or operation_lane),
        'selected': 0,
        'near_miss': 0,
        'rejected': 0,
      },
    )
    try:
      detail = json.loads(row['detail_json']) if row['detail_json'] else {}
    except (TypeError, json.JSONDecodeError):
      detail = {}
    feature_vector = detail.get('feature_vector') if isinstance(detail.get('feature_vector'), dict) else {}
    status = str(feature_vector.get('selection_status') or '').strip().lower()
    if status == 'selected':
      bucket['selected'] += 1
    elif status == 'near_miss':
      bucket['near_miss'] += 1
    else:
      bucket['rejected'] += 1
  return list(buckets.values())


def _candidate_history_series(history_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
  return [
    {
      'id': 'historical_selected_count',
      'label': 'Selected history',
      'kind': 'line',
      'unit': 'count',
      'points': [{'x': row['recorded_at_utc'], 'y': row['selected']} for row in history_rows],
    },
    {
      'id': 'historical_near_miss_count',
      'label': 'Near-miss history',
      'kind': 'line',
      'unit': 'count',
      'points': [{'x': row['recorded_at_utc'], 'y': row['near_miss']} for row in history_rows],
    },
    {
      'id': 'historical_rejected_count',
      'label': 'Rejected history',
      'kind': 'line',
      'unit': 'count',
      'points': [{'x': row['recorded_at_utc'], 'y': row['rejected']} for row in history_rows],
    },
  ]


def _build_candidate_density_curve_visual(
  analytical_outputs: dict[str, Any] | None,
  *,
  mode: str,
  detail_mode: str,
  now: datetime,
  captured_at: datetime | None,
) -> dict[str, Any]:
  source_packet = analytical_outputs.get('candidate_density_curve') if analytical_outputs else None
  if not isinstance(source_packet, dict):
    packet = _candidate_visual_empty_packet(view_id='candidate_density_curve', detail_mode=detail_mode, now=now)
    packet['shared_graph_mode'] = True
    packet['shared_series_contract'] = {
      'scope_id': 'candidate_landscape',
      'toggle_semantics': 'independent_series_visibility',
      'metric_ids': ['score', 'margin'],
      'default_visible_metric_ids': ['score', 'margin'],
    }
    return packet
  density_series = list(source_packet.get('series', []))
  table_rows = [
    [point.get('x'), point.get('ticker'), point.get('qualifier_tier'), point.get('y')]
    for point in density_series
  ]
  math_rows = _candidate_math_rows(analytical_outputs)
  ordered_math = sorted(math_rows, key=lambda row: _candidate_row_rank(row, 9999))
  threshold_score = _candidate_score_threshold(ordered_math) if ordered_math else Decimal('1')
  score_points = [
    {
      'x': _candidate_row_rank(row, index),
      'y': float(_candidate_weighted_score(row)),
      'ticker': row.get('ticker'),
    }
    for index, row in enumerate(ordered_math, start=1)
  ]
  threshold_points = [{'x': p['x'], 'y': float(threshold_score)} for p in score_points]
  margin_points = [
    {
      'x': _candidate_row_rank(row, index),
      'y': float(_candidate_weighted_score(row) - threshold_score),
      'ticker': row.get('ticker'),
    }
    for index, row in enumerate(ordered_math, start=1)
  ]
  packet = _visual_packet(
    view_id='candidate_density_curve',
    scope_id='candidate_landscape',
    window_id='current',
    render_mode=mode,
    detail_mode=detail_mode,
    now=now,
    headline='Candidate landscape highlights the current surfaced density curve without widening beyond retained tranche-C truth.',
    next_action='Use threshold and ranking companions to inspect where the current surfaced set tightens or drops away.',
    series=[
      {
        'id': 'curve',
        'label': 'Density weight',
        'tooltip': 'Base density weighting across the candidate distribution',
        'kind': 'line',
        'line_style': 'long_dash',
        'color': '#7B2D52',
        'toggleable': False,
        'unit': 'weight',
        'points': [{'x': point.get('x'), 'y': _decimal_number(point.get('y'))} for point in density_series],
      },
      {
        'id': 'score',
        'label': 'Weighted score',
        'chip_label': 'SCORE',
        'tooltip': 'Composite OV-U5A score weighted by density factors',
        'kind': 'line',
        'unit': 'score',
        'points': score_points,
      },
      {
        'id': 'margin',
        'label': 'Score margin',
        'chip_label': 'MARGIN',
        'tooltip': 'Distance between each candidate\'s weighted score and the threshold',
        'kind': 'line',
        'unit': 'score',
        'points': margin_points,
      },
      {
        'id': 'threshold',
        'label': 'Score threshold',
        'tooltip': 'Score cutoff separating selected from near-miss candidates',
        'kind': 'line',
        'line_style': 'dotted',
        'color': '#6B7E9E',
        'toggleable': False,
        'unit': 'score',
        'points': threshold_points,
      },
    ],
    table={
      'columns': ['Rank', 'Ticker', 'Tier', 'Density'],
      'rows': table_rows,
    },
    report=None,
    source_contracts=VISUAL_VIEW_CATALOG['candidate_density_curve']['source_contracts'],
    captured_at=captured_at or now,
  )
  packet['shared_graph_mode'] = True
  packet['shared_series_contract'] = {
    'scope_id': 'candidate_landscape',
    'toggle_semantics': 'independent_series_visibility',
    'metric_ids': ['score', 'margin'],
    'default_visible_metric_ids': ['score', 'margin'],
  }
  return packet


def _build_candidate_decision_boundary_visual(
  connection: Any,
  analytical_outputs: dict[str, Any] | None,
  *,
  operation_lane: str,
  mode: str,
  detail_mode: str,
  now: datetime,
  captured_at: datetime | None,
) -> dict[str, Any]:
  rows = _candidate_math_rows(analytical_outputs)
  if not rows:
    return _candidate_visual_empty_packet(view_id='candidate_decision_boundary', detail_mode=detail_mode, now=now)
  ordered_rows = sorted(rows, key=lambda row: _candidate_row_rank(row, 9999))
  threshold_score = _candidate_score_threshold(ordered_rows)
  score_points = [
    {'x': _candidate_row_rank(row, index), 'y': float(_candidate_weighted_score(row)), 'ticker': row.get('ticker')}
    for index, row in enumerate(ordered_rows, start=1)
  ]
  threshold_points = [
    {'x': point['x'], 'y': float(threshold_score)}
    for point in score_points
  ]
  margin_points = [
    {'x': _candidate_row_rank(row, index), 'y': float(_candidate_weighted_score(row) - threshold_score), 'ticker': row.get('ticker')}
    for index, row in enumerate(ordered_rows, start=1)
  ]
  history_rows = _candidate_outcome_history_timeline(connection, operation_lane=operation_lane)
  selected_count = sum(1 for row in ordered_rows if (row.get('feature_vector') or {}).get('selection_status') == 'selected')
  near_miss_count = sum(1 for row in ordered_rows if (row.get('feature_vector') or {}).get('selection_status') == 'near_miss')
  rejected_count = max(len(ordered_rows) - selected_count - near_miss_count, 0)
  table_rows = []
  for index, row in enumerate(ordered_rows, start=1):
    feature_vector = row.get('feature_vector') if isinstance(row.get('feature_vector'), dict) else {}
    composite_score = row.get('composite_score') if isinstance(row.get('composite_score'), dict) else {}
    threshold_outcome = row.get('threshold_outcome') if isinstance(row.get('threshold_outcome'), dict) else {}
    table_rows.append(
      [
        _candidate_row_rank(row, index),
        row.get('ticker'),
        feature_vector.get('selection_status'),
        composite_score.get('weighted_score'),
        composite_score.get('normalized_score'),
        composite_score.get('threshold_margin'),
        threshold_outcome.get('threshold_margin'),
      ]
    )
  report = _build_visual_report(
    'Candidate decision report',
    [
      {
        'heading': 'Decision boundary',
        'lines': [
          'Selected candidates: {count}'.format(count=selected_count),
          'Near-miss candidates: {count}'.format(count=near_miss_count),
          'Rejected or extended candidates: {count}'.format(count=rejected_count),
          'Score threshold: {score}'.format(score=str(threshold_score)),
        ],
      },
      {
        'heading': 'Interpretation boundary',
        'lines': [
          'The score curve explains why candidates sit above or below the current selection boundary.',
          'Elbow annotations are descriptive breakpoints, not autonomous trading authority.',
        ],
      },
    ],
  )
  packet = _visual_packet(
    view_id='candidate_decision_boundary',
    scope_id='candidate_landscape',
    window_id='current',
    render_mode=mode,
    detail_mode=detail_mode,
    now=now,
    headline='Decision boundary shows weighted candidate scores against the current selection threshold.',
    next_action='Use the threshold and ranking companions to inspect margins before treating a boundary zone as stable.',
    series=[
      {'id': 'weighted_score_curve', 'label': 'Weighted score', 'kind': 'line', 'unit': 'score', 'points': score_points},
      {'id': 'score_threshold_overlay', 'label': 'Score threshold', 'kind': 'line', 'unit': 'score', 'points': threshold_points},
      {'id': 'margin_to_threshold_curve', 'label': 'Score margin', 'kind': 'line', 'unit': 'score', 'points': margin_points},
    ],
    table={
      'columns': ['Rank', 'Ticker', 'Status', 'Weighted score', 'Normalized score', 'Score margin', 'Threshold margin'],
      'rows': table_rows,
    },
    report=report,
    source_contracts=VISUAL_VIEW_CATALOG['candidate_decision_boundary']['source_contracts'],
    captured_at=captured_at or now,
  )
  packet['decision_boundary'] = {
    'score_model_version': ((_candidate_math_contract(analytical_outputs) or {}).get('model_reference') or {}).get('score_model_version'),
    'weight_vector_reference': ((_candidate_math_contract(analytical_outputs) or {}).get('model_reference') or {}).get('weight_vector_reference'),
    'score_threshold': str(threshold_score),
    'selected_count': selected_count,
    'near_miss_count': near_miss_count,
    'rejected_count': rejected_count,
    'current_run_overlay': True,
    'certainty_boundary': 'descriptive_not_autonomous',
  }
  packet['elbow_breakpoints'] = _candidate_elbow_breakpoints(ordered_rows)
  packet['near_miss_boundary_band'] = {
    'status': 'available' if near_miss_count else 'not_available',
    'near_miss_count': near_miss_count,
    'selection_status_field': 'feature_vector.selection_status',
  }
  packet['historical_outcome_timeline'] = {
    'rows': history_rows,
    'series': _candidate_history_series(history_rows),
    'source': 'candidate_review_candidates.feature_vector.selection_status',
  }
  return packet


def _build_candidate_frontier_scatter_visual(
  analytical_outputs: dict[str, Any] | None,
  *,
  settings: Settings,
  mode: str,
  detail_mode: str,
  now: datetime,
  captured_at: datetime | None,
) -> dict[str, Any]:
  rows = _candidate_math_rows(analytical_outputs)
  if not rows:
    return _candidate_visual_empty_packet(view_id='candidate_frontier_scatter', detail_mode=detail_mode, now=now)
  ordered_rows = sorted(rows, key=lambda row: _candidate_row_rank(row, 9999))
  points_by_status: dict[str, list[dict[str, Any]]] = {'selected': [], 'near_miss': [], 'rejected': []}
  table_rows: list[list[Any]] = []
  for index, row in enumerate(ordered_rows, start=1):
    feature_vector = row.get('feature_vector') if isinstance(row.get('feature_vector'), dict) else {}
    status = str(feature_vector.get('selection_status') or 'rejected')
    if status not in points_by_status:
      status = 'rejected'
    liquidity = _decimal_number(feature_vector.get('liquidity_score'))
    edge = _decimal_number(feature_vector.get('edge_net_per_contract'))
    density_weight = _decimal_number(feature_vector.get('density_weight'))
    point = {
      'x': liquidity,
      'y': edge,
      'ticker': row.get('ticker'),
      'rank': _candidate_row_rank(row, index),
      'status': status,
      'density_weight': density_weight,
      'radius': max(3.8, min(8.2, (4.2 if status == 'selected' else (5.0 if status == 'near_miss' else 3.6)) + density_weight)),
      'color': '#7cf7ab' if status == 'selected' else ('#f2a654' if status == 'near_miss' else '#72859a'),
    }
    points_by_status[status].append(point)
    table_rows.append([
      point['rank'],
      row.get('ticker'),
      status,
      feature_vector.get('liquidity_score'),
      feature_vector.get('edge_net_per_contract'),
      feature_vector.get('density_weight'),
      feature_vector.get('projected_profit_dollars'),
      feature_vector.get('fee_drag_dollars'),
    ])
  series = [
    {'id': 'selected', 'label': 'Selected', 'chip_label': 'SELECTED', 'tooltip': 'Candidates that passed all threshold gates in the current scoring run', 'kind': 'scatter', 'marker_shape': 'circle', 'unit': 'edge_liquidity', 'color': '#7cf7ab', 'points': points_by_status['selected']},
    {'id': 'near_miss', 'label': 'Near miss', 'chip_label': 'NEAR MISS', 'tooltip': 'Candidates that narrowly missed qualification thresholds', 'kind': 'scatter', 'marker_shape': 'circle', 'unit': 'edge_liquidity', 'color': '#f2a654', 'points': points_by_status['near_miss']},
    {'id': 'rejected', 'label': 'Rejected', 'chip_label': 'REJECTED', 'tooltip': 'Candidates that did not meet threshold qualification criteria', 'kind': 'scatter', 'marker_shape': 'circle', 'unit': 'edge_liquidity', 'color': '#72859a', 'points': points_by_status['rejected']},
  ]
  report = _build_visual_report(
    'Candidate frontier report',
    [
      {
        'heading': 'Frontier shape',
        'lines': [
          'Selected points: {count}'.format(count=len(points_by_status['selected'])),
          'Near-miss points: {count}'.format(count=len(points_by_status['near_miss'])),
          'Rejected or extended points: {count}'.format(count=len(points_by_status['rejected'])),
          'X-axis is liquidity score; Y-axis is edge net per contract.',
        ],
      },
      {
        'heading': 'Interpretation boundary',
        'lines': [
          'The frontier is explanatory and shows opportunity shape; it does not submit, save, or select candidates.',
          'Decision-threshold overlays are metadata only when mathematically supported by the retained scoring contract.',
        ],
      },
    ],
  )
  packet = _visual_packet(
    view_id='candidate_frontier_scatter',
    scope_id='candidate_landscape',
    window_id='current',
    render_mode=mode,
    detail_mode=detail_mode,
    now=now,
    headline='Frontier plots edge against liquidity so the opportunity shape is visible without replaying the candidate table.',
    next_action='Use frontier clusters to inspect opportunity shape; use Decision for cutoff math before interpreting boundary certainty.',
    series=series,
    table={
      'columns': ['Rank', 'Ticker', 'Status', 'Liquidity', 'Edge net', 'Density', 'Projected profit', 'Fee drag'],
      'rows': table_rows,
    },
    report=report,
    source_contracts=VISUAL_VIEW_CATALOG['candidate_frontier_scatter']['source_contracts'],
    captured_at=captured_at or now,
  )
  packet['frontier'] = {
    'x_feature': 'feature_vector.liquidity_score',
    'y_feature': 'feature_vector.edge_net_per_contract',
    'marker_size_feature': 'feature_vector.density_weight',
    'status_feature': 'feature_vector.selection_status',
    'x_label': 'Liquidity score',
    'y_label': 'Edge net per contract',
    'decision_boundary_overlay': {
      'min_profit_dollars': settings.min_profit_dollars,
      'min_edge_dollars': settings.min_edge_dollars,
      'support_level': 'threshold_metadata_only',
    },
    'certainty_boundary': 'descriptive_not_autonomous',
  }
  packet['shared_graph_mode'] = True
  packet['shared_series_contract'] = {
    'scope_id': 'candidate_landscape',
    'toggle_semantics': 'independent_series_visibility',
    'metric_ids': ['selected', 'near_miss', 'rejected'],
    'default_visible_metric_ids': ['selected', 'rejected'],
  }
  packet['advisory_only'] = True
  packet['no_workflow_authority'] = True
  return packet


def _build_threshold_boundary_marker_visual(
  analytical_outputs: dict[str, Any] | None,
  *,
  mode: str,
  detail_mode: str,
  now: datetime,
  captured_at: datetime | None,
) -> dict[str, Any]:
  source_packet = analytical_outputs.get('threshold_boundary_marker') if analytical_outputs else None
  if not isinstance(source_packet, dict):
    return _candidate_visual_empty_packet(view_id='threshold_boundary_marker', detail_mode=detail_mode, now=now)
  math_rows = _candidate_math_rows(analytical_outputs)
  if math_rows:
    ordered_rows = sorted(math_rows, key=lambda row: _candidate_row_rank(row, 9999))
    points_by_status: dict[str, list[dict[str, Any]]] = {'selected': [], 'near_miss': [], 'rejected': []}
    table_rows = []
    for index, row in enumerate(ordered_rows, start=1):
      feature_vector = row.get('feature_vector') if isinstance(row.get('feature_vector'), dict) else {}
      threshold_outcome = row.get('threshold_outcome') if isinstance(row.get('threshold_outcome'), dict) else {}
      rank = _candidate_row_rank(row, index)
      margin = threshold_outcome.get('threshold_margin')
      status = str(feature_vector.get('selection_status') or 'rejected')
      if status not in points_by_status:
        status = 'rejected'
      base_radius = 4.4 if status == 'selected' else (5.2 if status == 'near_miss' else 3.8)
      points_by_status[status].append(
        {
          'x': rank,
          'y': _decimal_number(margin),
          'ticker': row.get('ticker'),
          'status': status,
          'radius': base_radius,
          'color': '#7cf7ab' if status == 'selected' else ('#f2a654' if status == 'near_miss' else '#8da1b4'),
        }
      )
      table_rows.append([rank, row.get('ticker'), feature_vector.get('selection_status'), margin, threshold_outcome.get('passes_current_thresholds')])
    rank_points = [
      point
      for status in ('selected', 'near_miss', 'rejected')
      for point in points_by_status[status]
    ]
    packet = _visual_packet(
      view_id='threshold_boundary_marker',
      scope_id='candidate_landscape',
      window_id='current',
      render_mode=mode,
      detail_mode=detail_mode,
      now=now,
      headline='Thresholds now show each surfaced candidate as a boundary scatter against the zero qualification line so the decision fringe stays visible.',
      next_action='Treat candidates near zero margin as the current boundary zone before making any stability claim.',
      series=[
        {
          'id': 'zero_threshold_line',
          'label': 'Threshold',
          'kind': 'line',
          'line_style': 'dashed',
          'toggleable': False,
          'unit': 'margin',
          'points': [{'x': point['x'], 'y': 0} for point in rank_points],
        },
        {
          'id': 'selected',
          'label': 'Selected',
          'kind': 'scatter',
          'unit': 'margin',
          'color': '#7cf7ab',
          'points': points_by_status['selected'],
        },
        {
          'id': 'near_miss',
          'label': 'Near miss',
          'kind': 'scatter',
          'unit': 'margin',
          'color': '#f2a654',
          'points': points_by_status['near_miss'],
        },
        {
          'id': 'rejected',
          'label': 'Rejected',
          'kind': 'scatter',
          'unit': 'margin',
          'color': '#72859a',
          'points': points_by_status['rejected'],
        },
      ],
      table={
        'columns': ['Rank', 'Ticker', 'Status', 'Threshold margin', 'Passes thresholds'],
        'rows': table_rows,
      },
      report=None,
      source_contracts=VISUAL_VIEW_CATALOG['threshold_boundary_marker']['source_contracts'],
      captured_at=captured_at or now,
    )
    packet['boundary_band'] = {
      'zero_margin_label': 'qualification_threshold',
      'near_miss_count': sum(1 for row in ordered_rows if (row.get('feature_vector') or {}).get('selection_status') == 'near_miss'),
      'margin_field': 'threshold_outcome.threshold_margin',
      'near_zero_band': {'min_margin': '-0.05', 'max_margin': '0.05'},
    }
    packet['shared_graph_mode'] = True
    packet['shared_series_contract'] = {
      'scope_id': 'candidate_landscape',
      'toggle_semantics': 'independent_series_visibility',
      'metric_ids': ['selected', 'near_miss', 'rejected'],
      'default_visible_metric_ids': ['selected', 'near_miss', 'rejected'],
    }
    packet['advisory_only'] = True
    packet['no_workflow_authority'] = True
    return packet
  transition = source_packet.get('tier_transition', {}) if isinstance(source_packet.get('tier_transition'), dict) else {}
  boundary_rank = int(transition.get('transition_rank') or source_packet.get('candidate_row_count') or 0)
  next_rank = boundary_rank + 1 if boundary_rank > 0 else 0
  table_rows = [
    ['Boundary ticker', transition.get('boundary_ticker') or '--'],
    ['Boundary tier', transition.get('boundary_qualifier_tier') or '--'],
    ['Transition rank', boundary_rank or '--'],
    ['Current live floor edge', source_packet.get('current_live_floor_edge_net_per_contract') or '--'],
    ['Binding limiter', ((source_packet.get('sizing_posture') or {}).get('binding_limiter') if isinstance(source_packet.get('sizing_posture'), dict) else '--') or '--'],
  ]
  return _visual_packet(
    view_id='threshold_boundary_marker',
    scope_id='candidate_landscape',
    window_id='current',
    render_mode=mode,
    detail_mode=detail_mode,
    now=now,
    headline='Threshold boundary keeps the live surfaced cut line visible without implying rejected-row frontier access.',
    next_action='Compare the boundary view with the ranking snapshot before widening any interpretation beyond the surfaced set.',
    series=[
      {
        'id': 'threshold_boundary_marker',
        'label': 'Boundary ranks',
        'kind': 'bar',
        'unit': 'rank',
        'points': [
          {'x': 'Boundary', 'y': boundary_rank},
          {'x': 'Next', 'y': next_rank},
        ],
      }
    ],
    table={
      'columns': ['Boundary field', 'Value'],
      'rows': table_rows,
    },
    report=None,
    source_contracts=VISUAL_VIEW_CATALOG['threshold_boundary_marker']['source_contracts'],
    captured_at=captured_at or now,
  )


def _build_comparative_ranking_snapshot_visual(
  analytical_outputs: dict[str, Any] | None,
  *,
  mode: str,
  detail_mode: str,
  now: datetime,
  captured_at: datetime | None,
) -> dict[str, Any]:
  source_packet = analytical_outputs.get('comparative_ranking_snapshot') if analytical_outputs else None
  if not isinstance(source_packet, dict):
    return _candidate_visual_empty_packet(view_id='comparative_ranking_snapshot', detail_mode=detail_mode, now=now)
  top_rows = list(source_packet.get('top_rows', []))
  transition_rows = list(source_packet.get('transition_rows', []))
  near_miss_rows = list(source_packet.get('near_miss_rows', []))

  combined_rows: list[dict[str, Any]] = []
  seen_rows: set[tuple[Any, Any]] = set()

  def _append_rows(rows: list[dict[str, Any]], role: str) -> None:
    for row in rows:
      if not isinstance(row, dict):
        continue
      rank = row.get('rank')
      ticker = row.get('ticker')
      key = (rank, ticker)
      if key in seen_rows:
        continue
      seen_rows.add(key)
      combined_rows.append({**row, 'support_role': role})

  _append_rows(top_rows, 'Surfaced leader')
  _append_rows(transition_rows, 'Transition')
  _append_rows(near_miss_rows, 'Near miss')

  if not combined_rows:
    return _candidate_visual_empty_packet(view_id='comparative_ranking_snapshot', detail_mode=detail_mode, now=now)

  ordered_rows = sorted(
    combined_rows,
    key=lambda row: (
      _candidate_row_rank(row, 9999),
      str(row.get('ticker') or ''),
    ),
  )
  points = [
    {
      'x': 'Rank {rank}'.format(rank=_candidate_row_rank(row, index)),
      'y': _decimal_number(row.get('edge_net_per_contract')),
      'rank': _candidate_row_rank(row, index),
      'ticker': row.get('ticker'),
      'qualifier_tier': row.get('qualifier_tier'),
      'role': row.get('support_role'),
    }
    for index, row in enumerate(ordered_rows, start=1)
  ]
  packet = _visual_packet(
    view_id='comparative_ranking_snapshot',
    scope_id='candidate_landscape',
    window_id='current',
    render_mode=mode,
    detail_mode=detail_mode,
    now=now,
    headline='Rankings now read as ordinal supporting evidence for the current surfaced set, while preserved carry-forward history remains a separate History view.',
    next_action='Use Rankings to compare the current ordinal edge spread; use History when you need preserved saved-set carry-forward evidence.',
    series=[
      {
        'id': 'comparative_ranking_snapshot',
        'label': 'Edge net by ordinal rank',
        'chip_label': 'EDGE',
        'tooltip': 'Current surfaced candidates ranked by ordinal edge evidence; exact tickers remain in the table.',
        'kind': 'horizontal_bar',
        'unit': 'edge',
        'points': points,
      }
    ],
    table={
      'columns': ['Rank', 'Role', 'Ticker', 'Tier', 'Density', 'Liquidity', 'Edge'],
      'rows': [
        [
          _candidate_row_rank(row, index),
          row.get('support_role'),
          row.get('ticker'),
          row.get('qualifier_tier'),
          row.get('density_weight'),
          row.get('liquidity_score'),
          row.get('edge_net_per_contract'),
        ]
        for index, row in enumerate(ordered_rows, start=1)
      ],
    },
    report=None,
    source_contracts=VISUAL_VIEW_CATALOG['comparative_ranking_snapshot']['source_contracts'],
    captured_at=captured_at or now,
  )
  packet['ranking_snapshot'] = {
    'evidence_role': 'ordinal_supporting_evidence',
    'history_view_id': 'saved_set_carry_forward',
    'top_row_count': len(top_rows),
    'transition_row_count': len(transition_rows),
    'near_miss_row_count': len(near_miss_rows),
    'x_axis_meaning': 'ordinal_rank',
  }
  packet['advisory_only'] = True
  packet['no_workflow_authority'] = True
  return packet

def _analysis_threshold_progress_packet(
  *,
  analytical_outputs: dict[str, Any] | None,
  mode: str,
  detail_mode: str,
  now: datetime,
  captured_at: datetime | None,
) -> dict[str, Any]:
  activation = _analysis_activation_state(analytical_outputs)
  status = str(activation['status'])
  current_count = activation['current_count']
  threshold = activation['threshold']
  remaining = activation['remaining_count']
  guidance = 'Run find candidates.'
  if status == 'threshold_undetermined':
    empty_copy = _visual_empty_copy('analysis_threshold_progress')
    packet = _visual_packet(
      view_id='analysis_threshold_progress',
      scope_id='analysis',
      window_id='current',
      render_mode='plot',
      detail_mode=detail_mode,
      now=now,
      headline=empty_copy['headline'],
      next_action=empty_copy['next_action'],
      series=[],
      table=None,
      report=None,
      source_contracts=VISUAL_VIEW_CATALOG['analysis_threshold_progress']['source_contracts'],
      status=status,
      empty_reason=empty_copy['empty_reason'],
      captured_at=captured_at or now,
    )
    packet['available_modes'] = ['plot']
    packet['view']['available_modes'] = ['plot']
    packet['view']['render_mode'] = 'plot'
    packet['threshold_state'] = {
      'status': status,
      'current_count': 0,
      'threshold': None,
      'remaining_count': None,
      'message': empty_copy['empty_reason'],
      'action_label': empty_copy['next_action'],
    }
    return packet

  packet = _visual_packet(
    view_id='analysis_threshold_progress',
    scope_id='analysis',
    window_id='current',
    render_mode=mode,
    detail_mode=detail_mode,
    now=now,
    headline='Analysis threshold progress is active.',
    next_action=guidance,
    series=[],
    table={
      'columns': ['Field', 'Value'],
      'rows': [
        ['Surfaced rows', current_count],
        ['Activation threshold', threshold],
        ['Rows remaining', remaining],
        ['Guidance', guidance],
      ],
    },
    report=None,
    source_contracts=VISUAL_VIEW_CATALOG['analysis_threshold_progress']['source_contracts'],
    status=status,
    captured_at=captured_at or now,
  )
  packet['threshold_state'] = {
    'status': status,
    'current_count': current_count,
    'threshold': threshold,
    'remaining_count': remaining,
    'message': _visual_empty_copy('analysis_threshold_progress')['empty_reason'] if status == 'threshold_undetermined' else 'Threshold progress is active.',
    'action_label': guidance,
  }
  return packet


def _build_factor_contribution_visual(
  analytical_outputs: dict[str, Any] | None,
  *,
  mode: str,
  detail_mode: str,
  now: datetime,
  captured_at: datetime | None,
) -> dict[str, Any]:
  if not _analysis_activation_state(analytical_outputs)['ready']:
    return _analysis_threshold_progress_packet(
      analytical_outputs=analytical_outputs,
      mode=mode,
      detail_mode=detail_mode,
      now=now,
      captured_at=captured_at,
    )
  source_packet = analytical_outputs.get('factor_contribution') if analytical_outputs else None
  if not isinstance(source_packet, dict):
    return _analysis_visual_empty_packet(view_id='factor_contribution', detail_mode=detail_mode, now=now, captured_at=captured_at)
  candidate_rows = list(source_packet.get('candidate_rows', []))
  report = _build_visual_report(
    'Factors report',
    [
      {
        'heading': 'Current factor posture',
        'lines': [
          'Candidates included: {count}'.format(count=len(candidate_rows)),
          'Binding limiter: {limiter}'.format(
            limiter=str(((source_packet.get('sizing_context') or {}).get('binding_limiter') if isinstance(source_packet.get('sizing_context'), dict) else '--') or '--')
          ),
          'This factor surface is explanation-only and does not alter workflow authority.',
        ],
      },
      {
        'heading': 'Candidate notes',
        'lines': [
          '{ticker} :: density {density} :: edge weight {edge_weight} :: liquidity weight {liquidity_weight}'.format(
            ticker=str(row.get('ticker') or '--'),
            density=str(row.get('density_weight') or '--'),
            edge_weight=str(((row.get('density_components') or {}).get('edge_weight') if isinstance(row.get('density_components'), dict) else '--') or '--'),
            liquidity_weight=str(((row.get('density_components') or {}).get('liquidity_weight') if isinstance(row.get('density_components'), dict) else '--') or '--'),
          )
          for row in candidate_rows[:5]
        ] or ['No factor rows are currently available.'],
      },
    ],
  )
  return _visual_packet(
    view_id='factor_contribution',
    scope_id='analysis',
    window_id='current',
    render_mode=mode,
    detail_mode=detail_mode,
    now=now,
    headline='',
    next_action='',
    series=[
      {
        'id': 'factor_edge_weight',
        'label': 'Edge weight',
        'chip_label': 'EDGE',
        'tooltip': 'Pool-mean edge weight across all candidates in this run',
        'kind': 'bar',
        'unit': 'weight',
        'points': [
          {
            'x': str(row.get('ticker') or '--'),
            'y': _metric_number(((row.get('density_components') or {}).get('edge_weight') if isinstance(row.get('density_components'), dict) else '0')),
          }
          for row in candidate_rows
        ],
      },
      {
        'id': 'factor_liquidity_weight',
        'label': 'Liquidity weight',
        'chip_label': 'LIQUIDITY',
        'tooltip': 'Pool-mean liquidity weight across all candidates in this run',
        'kind': 'bar',
        'unit': 'weight',
        'points': [
          {
            'x': str(row.get('ticker') or '--'),
            'y': _metric_number(((row.get('density_components') or {}).get('liquidity_weight') if isinstance(row.get('density_components'), dict) else '0')),
          }
          for row in candidate_rows
        ],
      },
    ],
    table={
      'columns': ['Ticker', 'Rank', 'Tier', 'Density', 'Edge weight', 'Liquidity weight'],
      'rows': [
        [
          row.get('ticker'),
          row.get('rank'),
          row.get('qualifier_tier'),
          row.get('density_weight'),
          ((row.get('density_components') or {}).get('edge_weight') if isinstance(row.get('density_components'), dict) else '--'),
          ((row.get('density_components') or {}).get('liquidity_weight') if isinstance(row.get('density_components'), dict) else '--'),
        ]
        for row in candidate_rows
      ],
    },
    report=report,
    source_contracts=VISUAL_VIEW_CATALOG['factor_contribution']['source_contracts'],
    captured_at=captured_at or now,
  )


def _build_parameter_sensitivity_delta_visual(
  analytical_outputs: dict[str, Any] | None,
  *,
  mode: str,
  detail_mode: str,
  now: datetime,
  captured_at: datetime | None,
) -> dict[str, Any]:
  if not _analysis_activation_state(analytical_outputs)['ready']:
    return _analysis_threshold_progress_packet(
      analytical_outputs=analytical_outputs,
      mode=mode,
      detail_mode=detail_mode,
      now=now,
      captured_at=captured_at,
    )
  source_packet = analytical_outputs.get('parameter_sensitivity_delta') if analytical_outputs else None
  advisory_packet = analytical_outputs.get('advisory_parameter_adjustment') if analytical_outputs else None
  if not isinstance(source_packet, dict):
    return _analysis_visual_empty_packet(view_id='parameter_sensitivity_delta', detail_mode=detail_mode, now=now, captured_at=captured_at)
  scenarios = list(source_packet.get('scenarios', []))
  scenario_points = sorted(
    [
      {
        'x': str(row.get('display_label') or str(row.get('scenario_id') or '--').replace('_', ' ')),
        'y': _metric_number(((row.get('derived_delta') or {}).get('dynamic_max_contracts_delta') if isinstance(row.get('derived_delta'), dict) else '0')),
      }
      for row in scenarios
    ],
    key=lambda point: abs(float(point['y'])),
    reverse=True,
  )
  report = _build_visual_report(
    'Sensitivity report',
    [
      {
        'heading': 'Current advisory posture',
        'lines': [
          'Recommendation: {status}'.format(status=str((advisory_packet or {}).get('recommendation_status') or 'no_change_recommended')),
          str((advisory_packet or {}).get('reason_summary') or 'Sensitivity remains advisory-only in this slice.'),
        ],
      },
      {
        'heading': 'Scenario deltas',
        'lines': [
          '{scenario} :: target {value} :: contracts delta {delta}'.format(
            scenario=str(row.get('scenario_id') or '--'),
            value=str(row.get('scenario_value') or '--'),
            delta=str(((row.get('derived_delta') or {}).get('dynamic_max_contracts_delta') if isinstance(row.get('derived_delta'), dict) else '--') or '--'),
          )
          for row in scenarios
        ] or ['No bounded sensitivity scenarios are currently available.'],
      },
    ],
  )
  packet = _visual_packet(
    view_id='parameter_sensitivity_delta',
    scope_id='analysis',
    window_id='current',
    render_mode=mode,
    detail_mode=detail_mode,
    now=now,
    headline='',
    next_action='',
    series=[
      {
        'id': 'parameter_sensitivity_delta',
        'label': 'Contracts delta',
        'chip_label': 'CONTRACTS',
        'tooltip': 'Change in maximum allowed contracts under each sensitivity scenario',
        'kind': 'horizontal_bar',
        'unit': 'contracts',
        'points': scenario_points,
      }
    ],
    table={
      'columns': ['Scenario', 'Baseline', 'Scenario value', 'Pct delta', 'Contracts delta', 'Limiter'],
      'rows': [
        [
          row.get('scenario_id'),
          row.get('baseline_value'),
          row.get('scenario_value'),
          ((row.get('derived_delta') or {}).get('dynamic_pair_notional_pct_delta') if isinstance(row.get('derived_delta'), dict) else '--'),
          ((row.get('derived_delta') or {}).get('dynamic_max_contracts_delta') if isinstance(row.get('derived_delta'), dict) else '--'),
          ((row.get('derived_delta') or {}).get('binding_limiter') if isinstance(row.get('derived_delta'), dict) else '--'),
        ]
        for row in scenarios
      ],
    },
    report=report,
    source_contracts=VISUAL_VIEW_CATALOG['parameter_sensitivity_delta']['source_contracts'],
    captured_at=captured_at or now,
  )
  packet['advisory_only'] = bool(source_packet.get('advisory_only', True))
  packet['no_auto_apply'] = bool(source_packet.get('no_auto_apply', True))
  baseline_value = _float_number(scenarios[0].get('baseline_value') if scenarios else None)
  packet['baseline_value'] = baseline_value
  return packet


def _diagnostic_feature_ids() -> list[str]:
  return ['edge_strength', 'liquidity_depth', 'density_weight', 'timing_pressure', 'sizing_capacity']


def _float_number(value: Any) -> float:
  try:
    return float(Decimal(str(value)))
  except Exception:
    return 0.0


def _analysis_component_vector(row: dict[str, Any]) -> list[float]:
  components = row.get('score_components') if isinstance(row.get('score_components'), dict) else {}
  return [_float_number(components.get(feature_id)) for feature_id in _diagnostic_feature_ids()]


def _mean_vector(vectors: list[list[float]]) -> list[float]:
  if not vectors:
    return [0.0 for _ in _diagnostic_feature_ids()]
  size = len(vectors[0])
  return [sum(vector[index] for vector in vectors) / len(vectors) for index in range(size)]


def _center_vectors(vectors: list[list[float]], means: list[float]) -> list[list[float]]:
  return [[value - means[index] for index, value in enumerate(vector)] for vector in vectors]


def _covariance_matrix(centered_vectors: list[list[float]]) -> list[list[float]]:
  size = len(_diagnostic_feature_ids())
  if len(centered_vectors) <= 1:
    return [[0.0 for _ in range(size)] for _ in range(size)]
  denominator = float(len(centered_vectors) - 1)
  return [
    [
      sum(vector[row_index] * vector[column_index] for vector in centered_vectors) / denominator
      for column_index in range(size)
    ]
    for row_index in range(size)
  ]


def _matrix_vector_product(matrix: list[list[float]], vector: list[float]) -> list[float]:
  return [sum(value * vector[index] for index, value in enumerate(row)) for row in matrix]


def _vector_norm(vector: list[float]) -> float:
  return sum(value * value for value in vector) ** 0.5


def _principal_axis(matrix: list[list[float]], *, seed_index: int) -> tuple[list[float], float]:
  size = len(matrix)
  vector = [0.0 for _ in range(size)]
  vector[min(seed_index, size - 1)] = 1.0
  for _ in range(18):
    next_vector = _matrix_vector_product(matrix, vector)
    norm = _vector_norm(next_vector)
    if norm <= 0:
      break
    vector = [value / norm for value in next_vector]
  product = _matrix_vector_product(matrix, vector)
  eigenvalue = sum(vector[index] * product[index] for index in range(size))
  return vector, max(eigenvalue, 0.0)


def _deflate_matrix(matrix: list[list[float]], axis: list[float], eigenvalue: float) -> list[list[float]]:
  return [
    [value - (eigenvalue * axis[row_index] * axis[column_index]) for column_index, value in enumerate(row)]
    for row_index, row in enumerate(matrix)
  ]


def _project_vector(vector: list[float], axis: list[float]) -> float:
  return sum(value * axis[index] for index, value in enumerate(vector))


def _cosine_similarity(left: list[float], right: list[float]) -> float:
  denominator = _vector_norm(left) * _vector_norm(right)
  if denominator <= 0:
    return 0.0
  return sum(left[index] * right[index] for index in range(min(len(left), len(right)))) / denominator


def _diagnostic_regime_label(vector: list[float], status: str) -> str:
  edge, liquidity, density, _timing, sizing = vector
  if status == 'near_miss':
    return 'near_miss_heavy'
  if density >= 1.1:
    return 'crowded_opportunity'
  if liquidity >= 1.1 and edge < 0.9:
    return 'high_liquidity_low_edge'
  if edge >= 1.1 and liquidity < 0.9:
    return 'low_liquidity_high_edge'
  if sizing < 0.75:
    return 'sizing_constrained'
  return 'balanced_candidate_shape'


def _diagnostic_seeded_regime_vectors() -> dict[str, list[float]]:
  return {
    'sparse': [0.55, 0.65, 0.35, 0.45, 0.75],
    'crowded': [1.05, 1.10, 1.45, 0.62, 0.88],
    'near_miss_heavy': [0.88, 0.82, 0.92, 0.50, 0.70],
    'high_liquidity_low_edge': [0.62, 1.35, 0.95, 0.55, 0.82],
    'low_liquidity_high_edge': [1.35, 0.62, 0.82, 0.58, 0.78],
  }


def _diagnostic_confidence_label(row_count: int) -> str:
  if row_count <= 1:
    return 'single_row_explanatory_only'
  if row_count < 5:
    return 'low_sample_directional_only'
  return 'sample_supported_explanatory'


def _diagnostic_education_payload(
  *,
  row_count: int,
  top_similarity: dict[str, Any] | None,
  dominant_regime: str,
) -> dict[str, Any]:
  confidence = _diagnostic_confidence_label(row_count)
  return {
    'layer': 'analysis_diagnostics_education.v1',
    'confidence': {
      'label': confidence,
      'row_count': row_count,
      'message': {
        'single_row_explanatory_only': 'One retained row can document method shape, but it cannot support stable clusters.',
        'low_sample_directional_only': 'Low sample size supports directional explanation only; treat clusters and similarity as provisional.',
        'sample_supported_explanatory': 'Sample size is sufficient for an explanatory diagnostic reading, but still not workflow authority.',
      }[confidence],
    },
    'method_cards': [
      {
        'id': 'weight_vector',
        'title': 'Weight vector',
        'input': 'OV-U5A score component values and component weights.',
        'output': 'Mean weighted contribution by component.',
        'interpretation': 'Shows which mathematical factors dominate the retained candidate surface.',
        'limitation': 'Weights explain ranking pressure; they do not approve any trade.',
      },
      {
        'id': 'projection',
        'title': 'Projection',
        'input': 'Centered score-component vectors.',
        'output': 'PC1/PC2 diagnostic coordinates for each retained candidate.',
        'interpretation': 'Shows whether candidates separate into visible shape groups.',
        'limitation': 'Projection axes are descriptive and may be unstable with low samples.',
      },
      {
        'id': 'covariance_correlation',
        'title': 'Covariance and correlation',
        'input': 'Centered score-component vectors across retained candidates.',
        'output': 'Pairwise movement matrices for decision factors.',
        'interpretation': 'Highlights factors that move together or independently.',
        'limitation': 'Small samples can make relationships look cleaner than they are.',
      },
      {
        'id': 'regime_similarity',
        'title': 'Regime similarity',
        'input': 'Current mean vector and deterministic seeded regime references.',
        'output': 'Cosine similarity scores and bounded regime labels.',
        'interpretation': 'Compares the current opportunity shape to known proof regimes.',
        'limitation': 'Seeded regimes are proof fixtures, not production classifications.',
      },
      {
        'id': 'sensitivity_gradient',
        'title': 'Sensitivity gradient',
        'input': 'Bounded parameter-sensitivity scenario deltas.',
        'output': 'Finite-difference contract-impact gradients.',
        'interpretation': 'Shows which bounded parameter moves have the largest local effect.',
        'limitation': 'This is not an optimizer and does not auto-apply settings.',
      },
    ],
    'reviewer_summary': [
      'Inputs: retained candidate score components from the OV-U5A math contract.',
      'Outputs: projection points, matrix diagnostics, similarity scores, and regime labels.',
      'Interpretation: explains model posture and opportunity shape for review.',
      'Boundary: educational and advisory only; no workflow authority is added.',
      'Current dominant regime: {regime}.'.format(regime=dominant_regime or '--'),
      'Top seeded similarity: {regime} ({score}).'.format(
        regime=str((top_similarity or {}).get('regime') or '--'),
        score=str((top_similarity or {}).get('cosine_similarity') or '--'),
      ),
    ],
  }


def _build_analysis_linear_diagnostics_visual(
  analytical_outputs: dict[str, Any] | None,
  *,
  mode: str,
  detail_mode: str,
  now: datetime,
  captured_at: datetime | None,
) -> dict[str, Any]:
  if not _analysis_activation_state(analytical_outputs)['ready']:
    return _analysis_threshold_progress_packet(
      analytical_outputs=analytical_outputs,
      mode=mode,
      detail_mode=detail_mode,
      now=now,
      captured_at=captured_at,
    )
  rows = _candidate_math_rows(analytical_outputs)
  if not rows:
    packet = _analysis_visual_empty_packet(view_id='analysis_linear_diagnostics', detail_mode=detail_mode, now=now, captured_at=captured_at)
    packet['linear_diagnostics'] = {'status': 'empty', 'authority_boundary': 'explanation_only_not_workflow_authority'}
    packet['advisory_only'] = True
    packet['no_workflow_authority'] = True
    return packet

  ordered_rows = sorted(rows, key=lambda row: _candidate_row_rank(row, 9999))
  vectors = [_analysis_component_vector(row) for row in ordered_rows]
  means = _mean_vector(vectors)
  centered = _center_vectors(vectors, means)
  covariance = _covariance_matrix(centered)
  pc1_axis, pc1_variance = _principal_axis(covariance, seed_index=0)
  pc2_axis, pc2_variance = _principal_axis(_deflate_matrix(covariance, pc1_axis, pc1_variance), seed_index=1)
  total_variance = sum(covariance[index][index] for index in range(len(covariance)))
  points_by_status: dict[str, list[dict[str, Any]]] = {'selected': [], 'near_miss': [], 'rejected': []}
  table_rows: list[list[Any]] = []
  regime_counts: dict[str, int] = {}
  for index, row in enumerate(ordered_rows, start=1):
    feature_vector = row.get('feature_vector') if isinstance(row.get('feature_vector'), dict) else {}
    composite_score = row.get('composite_score') if isinstance(row.get('composite_score'), dict) else {}
    status = str(feature_vector.get('selection_status') or 'rejected')
    if status not in points_by_status:
      status = 'rejected'
    vector = vectors[index - 1]
    centered_vector = centered[index - 1]
    pc1 = _project_vector(centered_vector, pc1_axis)
    pc2 = _project_vector(centered_vector, pc2_axis)
    regime = _diagnostic_regime_label(vector, status)
    regime_counts[regime] = regime_counts.get(regime, 0) + 1
    point = {
      'x': round(pc1, 6),
      'y': round(pc2, 6),
      'ticker': row.get('ticker'),
      'rank': _candidate_row_rank(row, index),
      'status': status,
      'regime': regime,
      'weighted_score': _float_number(composite_score.get('weighted_score')),
    }
    points_by_status[status].append(point)
    table_rows.append([
      point['rank'],
      row.get('ticker'),
      status,
      point['x'],
      point['y'],
      composite_score.get('weighted_score'),
      regime,
    ])

  sensitivity_packet = analytical_outputs.get('parameter_sensitivity_delta') if isinstance(analytical_outputs, dict) else None
  sensitivity_rows = list(sensitivity_packet.get('scenarios', [])) if isinstance(sensitivity_packet, dict) else []
  sensitivity_gradients = []
  for row in sensitivity_rows:
    delta_value = _float_number(row.get('delta_value'))
    contract_delta = _float_number(((row.get('derived_delta') or {}).get('dynamic_max_contracts_delta') if isinstance(row.get('derived_delta'), dict) else 0))
    sensitivity_gradients.append({
      'scenario_id': row.get('scenario_id'),
      'parameter': row.get('parameter'),
      'delta_value': delta_value,
      'dynamic_max_contracts_gradient': (contract_delta / delta_value) if delta_value else 0.0,
      'limitation': 'bounded local finite difference; not an optimizer',
    })

  seeded_vectors = _diagnostic_seeded_regime_vectors()
  vector_similarity = [
    {
      'regime': regime,
      'cosine_similarity': round(_cosine_similarity(means, vector), 6),
      'reference': 'deterministic_seeded_regime_vector',
    }
    for regime, vector in seeded_vectors.items()
  ]
  vector_similarity.sort(key=lambda row: row['cosine_similarity'], reverse=True)
  weight_vector = ((_candidate_math_contract(analytical_outputs) or {}).get('model_reference') or {}).get('component_weights') or {}
  weight_vector_rows = [
    {
      'component': component_id,
      'weight': str(weight_vector.get(component_id) or OV_U5A_COMPONENT_WEIGHTS.get(component_id) or '0'),
      'mean_component_value': round(means[index], 6),
      'mean_weighted_contribution': round(means[index] * _float_number(weight_vector.get(component_id) or OV_U5A_COMPONENT_WEIGHTS.get(component_id) or 0), 6),
    }
    for index, component_id in enumerate(_diagnostic_feature_ids())
  ]
  dominant_regime = max(regime_counts.items(), key=lambda item: item[1])[0] if regime_counts else '--'
  education = _diagnostic_education_payload(
    row_count=len(ordered_rows),
    top_similarity=vector_similarity[0] if vector_similarity else None,
    dominant_regime=dominant_regime,
  )
  report = _build_visual_report(
    'Analysis diagnostics report',
    [
      {
        'heading': 'Inputs',
        'lines': [
          'Source: retained OV-U5A candidate score components.',
          'Vector fields: {fields}.'.format(fields=', '.join(_diagnostic_feature_ids())),
          'Normalization: OV-U5A bounded score_components on shared 0..2 scale.',
        ],
      },
      {
        'heading': 'Method and outputs',
        'lines': [
          'Projection: deterministic covariance power iteration into PC1/PC2 diagnostic coordinates.',
          'Matrices: covariance and correlation across score components.',
          'Regimes: cosine similarity to deterministic seeded references plus bounded per-row labels.',
        ],
      },
      {
        'heading': 'Interpretation',
        'lines': [
          'Candidate rows: {count}'.format(count=len(ordered_rows)),
          'Top seeded-regime similarity: {regime} ({score})'.format(
            regime=str(vector_similarity[0]['regime']) if vector_similarity else '--',
            score=str(vector_similarity[0]['cosine_similarity']) if vector_similarity else '--',
          ),
          'Dominant observed regime: {regime}'.format(regime=dominant_regime),
        ],
      },
      {
        'heading': 'Confidence and boundary',
        'lines': [
          'Confidence: {label}.'.format(label=education['confidence']['label']),
          education['confidence']['message'],
          'This view explains model posture; it does not submit, save, select, or auto-apply anything.',
        ],
      },
    ],
  )
  packet = _visual_packet(
    view_id='analysis_linear_diagnostics',
    scope_id='analysis',
    window_id='current',
    render_mode=mode,
    detail_mode=detail_mode,
    now=now,
    headline='Analysis diagnostics project candidate feature vectors into explanatory axes and compare them with seeded regimes.',
    next_action='Use diagnostics to explain model posture; do not treat projection clusters as autonomous workflow authority.',
    series=[
      {'id': 'diagnostic_selected_projection', 'label': 'Selected', 'kind': 'scatter', 'unit': 'diagnostic_projection', 'points': points_by_status['selected']},
      {'id': 'diagnostic_near_miss_projection', 'label': 'Near miss', 'kind': 'scatter', 'unit': 'diagnostic_projection', 'points': points_by_status['near_miss']},
      {'id': 'diagnostic_rejected_projection', 'label': 'Rejected', 'kind': 'scatter', 'unit': 'diagnostic_projection', 'points': points_by_status['rejected']},
    ],
    table={
      'columns': ['Rank', 'Ticker', 'Status', 'PC1', 'PC2', 'Weighted score', 'Regime'],
      'rows': table_rows,
    },
    report=report,
    source_contracts=VISUAL_VIEW_CATALOG['analysis_linear_diagnostics']['source_contracts'],
    captured_at=captured_at or now,
  )
  packet['linear_diagnostics'] = {
    'method_family': 'deterministic_linear_algebra_explanation',
    'input_vector_fields': _diagnostic_feature_ids(),
    'normalization': 'OV-U5A bounded score_components on shared 0..2 scale',
    'projection': {
      'method': 'covariance_power_iteration_two_axis_projection',
      'pc1_axis': [round(value, 6) for value in pc1_axis],
      'pc2_axis': [round(value, 6) for value in pc2_axis],
      'pc1_explained_variance_ratio': round((pc1_variance / total_variance) if total_variance > 0 else 0.0, 6),
      'pc2_explained_variance_ratio': round((pc2_variance / total_variance) if total_variance > 0 else 0.0, 6),
    },
    'weight_vector_diagnostics': weight_vector_rows,
    'sensitivity_gradients': sensitivity_gradients,
    'covariance_matrix': {
      'feature_ids': _diagnostic_feature_ids(),
      'values': [[round(value, 6) for value in row] for row in covariance],
    },
    'correlation_matrix': {
      'feature_ids': _diagnostic_feature_ids(),
      'values': [
        [
          round(
            covariance[row_index][column_index] / (((covariance[row_index][row_index] ** 0.5) * (covariance[column_index][column_index] ** 0.5)) or 1.0),
            6,
          )
          for column_index in range(len(covariance))
        ]
        for row_index in range(len(covariance))
      ],
    },
    'vector_similarity': vector_similarity,
    'regime_clusters': {
      'assignment_method': 'bounded_component_thresholds_against_seeded_regime_references',
      'counts': regime_counts,
      'seeded_regime_references': list(seeded_vectors.keys()),
    },
    'limitations': [
      'Low sample counts are explanatory only and may not form stable clusters.',
      'Projection axes are deterministic diagnostics, not autonomous trading instructions.',
      'Seeded regime references are proof fixtures for interpretation and validation.',
    ],
    'authority_boundary': 'explanation_only_not_workflow_authority',
  }
  packet['diagnostic_education'] = education
  packet['advisory_only'] = True
  packet['no_workflow_authority'] = True
  return packet


def _build_diagnostics_scatter_visual(
  analytical_outputs: dict[str, Any] | None,
  *,
  mode: str,
  detail_mode: str,
  now: datetime,
  captured_at: datetime | None,
) -> dict[str, Any]:
  """PCA scatter with series IDs selected/near_miss/rejected for Tier-3 toggle wiring."""
  if not _analysis_activation_state(analytical_outputs)['ready']:
    return _analysis_threshold_progress_packet(
      analytical_outputs=analytical_outputs,
      mode=mode,
      detail_mode=detail_mode,
      now=now,
      captured_at=captured_at,
    )
  rows = _candidate_math_rows(analytical_outputs)
  if not rows:
    packet = _analysis_visual_empty_packet(view_id='analysis_linear_diagnostics', detail_mode=detail_mode, now=now, captured_at=captured_at)
    packet['shared_graph_mode'] = True
    packet['shared_series_contract'] = {
      'scope_id': 'analysis',
      'toggle_semantics': 'independent_series_visibility',
      'metric_ids': ['selected', 'near_miss', 'rejected'],
      'default_visible_metric_ids': ['selected', 'near_miss', 'rejected'],
    }
    packet['linear_diagnostics'] = {'status': 'empty', 'authority_boundary': 'explanation_only_not_workflow_authority'}
    packet['advisory_only'] = True
    packet['no_workflow_authority'] = True
    return packet

  ordered_rows = sorted(rows, key=lambda row: _candidate_row_rank(row, 9999))
  vectors = [_analysis_component_vector(row) for row in ordered_rows]
  means = _mean_vector(vectors)
  centered = _center_vectors(vectors, means)
  covariance = _covariance_matrix(centered)
  pc1_axis, pc1_variance = _principal_axis(covariance, seed_index=0)
  pc2_axis, pc2_variance = _principal_axis(_deflate_matrix(covariance, pc1_axis, pc1_variance), seed_index=1)
  total_variance = sum(covariance[index][index] for index in range(len(covariance)))
  points_by_status: dict[str, list[dict[str, Any]]] = {'selected': [], 'near_miss': [], 'rejected': []}
  table_rows: list[list[Any]] = []
  regime_counts: dict[str, int] = {}
  for index, row in enumerate(ordered_rows, start=1):
    feature_vector = row.get('feature_vector') if isinstance(row.get('feature_vector'), dict) else {}
    composite_score = row.get('composite_score') if isinstance(row.get('composite_score'), dict) else {}
    status = str(feature_vector.get('selection_status') or 'rejected')
    if status not in points_by_status:
      status = 'rejected'
    vector = vectors[index - 1]
    centered_vector = centered[index - 1]
    pc1 = _project_vector(centered_vector, pc1_axis)
    pc2 = _project_vector(centered_vector, pc2_axis)
    regime = _diagnostic_regime_label(vector, status)
    regime_counts[regime] = regime_counts.get(regime, 0) + 1
    point = {
      'x': round(pc1, 6),
      'y': round(pc2, 6),
      'ticker': row.get('ticker'),
      'rank': _candidate_row_rank(row, index),
      'status': status,
      'regime': regime,
      'weighted_score': _float_number(composite_score.get('weighted_score')),
    }
    points_by_status[status].append(point)
    table_rows.append([
      point['rank'],
      row.get('ticker'),
      status,
      point['x'],
      point['y'],
      composite_score.get('weighted_score'),
      regime,
    ])

  sensitivity_packet = analytical_outputs.get('parameter_sensitivity_delta') if isinstance(analytical_outputs, dict) else None
  sensitivity_rows = list(sensitivity_packet.get('scenarios', [])) if isinstance(sensitivity_packet, dict) else []
  sensitivity_gradients = []
  for row in sensitivity_rows:
    delta_value = _float_number(row.get('delta_value'))
    contract_delta = _float_number(((row.get('derived_delta') or {}).get('dynamic_max_contracts_delta') if isinstance(row.get('derived_delta'), dict) else 0))
    sensitivity_gradients.append({
      'scenario_id': row.get('scenario_id'),
      'parameter': row.get('parameter'),
      'delta_value': delta_value,
      'dynamic_max_contracts_gradient': (contract_delta / delta_value) if delta_value else 0.0,
      'limitation': 'bounded local finite difference; not an optimizer',
    })

  seeded_vectors = _diagnostic_seeded_regime_vectors()
  vector_similarity = [
    {
      'regime': regime,
      'cosine_similarity': round(_cosine_similarity(means, vector), 6),
      'reference': 'deterministic_seeded_regime_vector',
    }
    for regime, vector in seeded_vectors.items()
  ]
  vector_similarity.sort(key=lambda row: row['cosine_similarity'], reverse=True)
  weight_vector = ((_candidate_math_contract(analytical_outputs) or {}).get('model_reference') or {}).get('component_weights') or {}
  weight_vector_rows = [
    {
      'component': component_id,
      'weight': str(weight_vector.get(component_id) or OV_U5A_COMPONENT_WEIGHTS.get(component_id) or '0'),
      'mean_component_value': round(means[index], 6),
      'mean_weighted_contribution': round(means[index] * _float_number(weight_vector.get(component_id) or OV_U5A_COMPONENT_WEIGHTS.get(component_id) or 0), 6),
    }
    for index, component_id in enumerate(_diagnostic_feature_ids())
  ]
  dominant_regime = max(regime_counts.items(), key=lambda item: item[1])[0] if regime_counts else '--'
  education = _diagnostic_education_payload(
    row_count=len(ordered_rows),
    top_similarity=vector_similarity[0] if vector_similarity else None,
    dominant_regime=dominant_regime,
  )
  report = _build_visual_report(
    'Analysis diagnostics report',
    [
      {
        'heading': 'Inputs',
        'lines': [
          'Source: retained OV-U5A candidate score components.',
          'Vector fields: {fields}.'.format(fields=', '.join(_diagnostic_feature_ids())),
          'Normalization: OV-U5A bounded score_components on shared 0..2 scale.',
        ],
      },
      {
        'heading': 'Method and outputs',
        'lines': [
          'Projection: deterministic covariance power iteration into PC1/PC2 diagnostic coordinates.',
          'Matrices: covariance and correlation across score components.',
          'Regimes: cosine similarity to deterministic seeded references plus bounded per-row labels.',
        ],
      },
      {
        'heading': 'Interpretation',
        'lines': [
          'Candidate rows: {count}'.format(count=len(ordered_rows)),
          'Top seeded-regime similarity: {regime} ({score})'.format(
            regime=str(vector_similarity[0]['regime']) if vector_similarity else '--',
            score=str(vector_similarity[0]['cosine_similarity']) if vector_similarity else '--',
          ),
          'Dominant observed regime: {regime}'.format(regime=dominant_regime),
        ],
      },
      {
        'heading': 'Confidence and boundary',
        'lines': [
          'Confidence: {label}.'.format(label=education['confidence']['label']),
          education['confidence']['message'],
          'This view explains model posture; it does not submit, save, select, or auto-apply anything.',
        ],
      },
    ],
  )
  packet = _visual_packet(
    view_id='analysis_linear_diagnostics',
    scope_id='analysis',
    window_id='current',
    render_mode=mode,
    detail_mode=detail_mode,
    now=now,
    headline='Analysis diagnostics project candidate feature vectors into explanatory axes and compare them with seeded regimes.',
    next_action='Use diagnostics to explain model posture; do not treat projection clusters as autonomous workflow authority.',
    series=[
      {'id': 'selected', 'label': 'Selected', 'chip_label': 'SELECTED', 'tooltip': 'Selected candidates projected into the covariance space', 'kind': 'scatter', 'marker_shape': 'triangle', 'unit': 'diagnostic_projection', 'points': points_by_status['selected']},
      {'id': 'near_miss', 'label': 'Near miss', 'chip_label': 'NEAR MISS', 'tooltip': 'Near-miss candidates projected into the covariance space', 'kind': 'scatter', 'marker_shape': 'triangle', 'unit': 'diagnostic_projection', 'points': points_by_status['near_miss']},
      {'id': 'rejected', 'label': 'Rejected', 'chip_label': 'REJECTED', 'tooltip': 'Rejected candidates projected into the covariance space', 'kind': 'scatter', 'marker_shape': 'triangle', 'unit': 'diagnostic_projection', 'points': points_by_status['rejected']},
    ],
    table={
      'columns': ['Rank', 'Ticker', 'Status', 'PC1', 'PC2', 'Weighted score', 'Regime'],
      'rows': table_rows,
    },
    report=report,
    source_contracts=VISUAL_VIEW_CATALOG['analysis_linear_diagnostics']['source_contracts'],
    captured_at=captured_at or now,
  )
  packet['shared_graph_mode'] = True
  packet['shared_series_contract'] = {
    'scope_id': 'analysis',
    'toggle_semantics': 'independent_series_visibility',
    'metric_ids': ['selected', 'near_miss', 'rejected'],
    'default_visible_metric_ids': ['selected', 'near_miss', 'rejected'],
  }
  packet['linear_diagnostics'] = {
    'method_family': 'deterministic_linear_algebra_explanation',
    'input_vector_fields': _diagnostic_feature_ids(),
    'normalization': 'OV-U5A bounded score_components on shared 0..2 scale',
    'x_label': 'PC1 (covariance projection)',
    'y_label': 'PC2 (covariance projection)',
    'projection': {
      'method': 'covariance_power_iteration_two_axis_projection',
      'pc1_axis': [round(value, 6) for value in pc1_axis],
      'pc2_axis': [round(value, 6) for value in pc2_axis],
      'pc1_explained_variance_ratio': round((pc1_variance / total_variance) if total_variance > 0 else 0.0, 6),
      'pc2_explained_variance_ratio': round((pc2_variance / total_variance) if total_variance > 0 else 0.0, 6),
    },
    'weight_vector_diagnostics': weight_vector_rows,
    'sensitivity_gradients': sensitivity_gradients,
    'covariance_matrix': {
      'feature_ids': _diagnostic_feature_ids(),
      'values': [[round(value, 6) for value in row] for row in covariance],
    },
    'correlation_matrix': {
      'feature_ids': _diagnostic_feature_ids(),
      'values': [
        [
          round(
            covariance[row_index][column_index] / (((covariance[row_index][row_index] ** 0.5) * (covariance[column_index][column_index] ** 0.5)) or 1.0),
            6,
          )
          for column_index in range(len(covariance))
        ]
        for row_index in range(len(covariance))
      ],
    },
    'vector_similarity': vector_similarity,
    'regime_clusters': {
      'assignment_method': 'bounded_component_thresholds_against_seeded_regime_references',
      'counts': regime_counts,
      'seeded_regime_references': list(seeded_vectors.keys()),
    },
    'limitations': [
      'Low sample counts are explanatory only and may not form stable clusters.',
      'Projection axes are deterministic diagnostics, not autonomous trading instructions.',
      'Seeded regime references are proof fixtures for interpretation and validation.',
    ],
    'authority_boundary': 'explanation_only_not_workflow_authority',
  }
  packet['diagnostic_education'] = education
  packet['advisory_only'] = True
  packet['no_workflow_authority'] = True
  return packet


def _build_factors_timeseries_visual(
  connection: Any,
  analytical_outputs: dict[str, Any] | None,
  *,
  operation_lane: str,
  mode: str,
  detail_mode: str,
  now: datetime,
  captured_at: datetime | None,
) -> dict[str, Any]:
  """Pool-mean edge/liquidity weights per historical run as a time series."""
  rows = connection.execute(
    '''
    SELECT r.recorded_at_utc, c.detail_json
    FROM candidate_review_runs r
    INNER JOIN candidate_review_candidates c ON c.run_id = r.run_id
    WHERE r.operation_lane = ?
    ORDER BY r.recorded_at_utc ASC, c.id ASC
    ''',
    (operation_lane,),
  ).fetchall()

  buckets: dict[str, dict[str, Any]] = {}
  for row in rows:
    recorded_at = str(row['recorded_at_utc'] or '')
    bucket = buckets.setdefault(recorded_at, {'recorded_at_utc': recorded_at, 'edge_sum': 0.0, 'liquidity_sum': 0.0, 'count': 0})
    try:
      detail = json.loads(row['detail_json']) if row['detail_json'] else {}
    except (TypeError, json.JSONDecodeError):
      detail = {}
    dc = detail.get('density_components') if isinstance(detail.get('density_components'), dict) else {}
    edge = dc.get('edge_weight')
    liquidity = dc.get('liquidity_weight')
    if edge is not None or liquidity is not None:
      bucket['edge_sum'] += float(edge or 0)
      bucket['liquidity_sum'] += float(liquidity or 0)
      bucket['count'] += 1

  run_buckets = [b for b in buckets.values() if b['count'] > 0]
  run_buckets.sort(key=lambda b: b['recorded_at_utc'])

  # Fall back to latest analytical outputs when no historical run data exists
  if not run_buckets and isinstance(analytical_outputs, dict):
    source_packet = analytical_outputs.get('factor_contribution')
    if isinstance(source_packet, dict):
      candidate_rows = list(source_packet.get('candidate_rows', []))
      for index, candidate_row in enumerate(candidate_rows, start=1):
        dc = candidate_row.get('density_components') if isinstance(candidate_row.get('density_components'), dict) else {}
        key = str(index)
        run_buckets.append({
          'recorded_at_utc': key,
          'edge_sum': float(dc.get('edge_weight') or 0),
          'liquidity_sum': float(dc.get('liquidity_weight') or 0),
          'count': 1,
        })

  if not run_buckets:
    packet = _analysis_visual_empty_packet(view_id='factors_timeseries', detail_mode=detail_mode, now=now, captured_at=captured_at)
    packet['shared_graph_mode'] = True
    packet['shared_series_contract'] = {
      'scope_id': 'analysis',
      'toggle_semantics': 'independent_series_visibility',
      'metric_ids': ['edge', 'liquidity'],
      'default_visible_metric_ids': ['edge', 'liquidity'],
    }
    packet['advisory_only'] = True
    packet['no_workflow_authority'] = True
    return packet

  edge_points = [{'x': b['recorded_at_utc'], 'y': round(b['edge_sum'] / b['count'], 6)} for b in run_buckets]
  liquidity_points = [{'x': b['recorded_at_utc'], 'y': round(b['liquidity_sum'] / b['count'], 6)} for b in run_buckets]
  table_rows = [
    [b['recorded_at_utc'], round(b['edge_sum'] / b['count'], 6), round(b['liquidity_sum'] / b['count'], 6), b['count']]
    for b in run_buckets
  ]
  packet = _visual_packet(
    view_id='factors_timeseries',
    scope_id='analysis',
    window_id='current',
    render_mode='table' if mode == 'table' else 'plot',
    detail_mode=detail_mode,
    now=now,
    headline='Factor weights show pool-mean edge and liquidity per candidate run.',
    next_action='Compare edge and liquidity trends across runs to understand factor balance drift.',
    series=[
      {'id': 'edge', 'label': 'Edge', 'chip_label': 'EDGE', 'tooltip': 'Pool-mean edge weight across all candidates in this run', 'kind': 'line', 'unit': 'weight', 'points': edge_points},
      {'id': 'liquidity', 'label': 'Liquidity', 'chip_label': 'LIQUIDITY', 'tooltip': 'Pool-mean liquidity weight across all candidates in this run', 'kind': 'line', 'unit': 'weight', 'points': liquidity_points},
    ],
    table={'columns': ['Run at', 'Edge mean', 'Liquidity mean', 'Candidates'], 'rows': table_rows},
    report=None,
    source_contracts=VISUAL_VIEW_CATALOG['factors_timeseries']['source_contracts'],
    captured_at=captured_at or now,
  )
  packet['shared_graph_mode'] = True
  packet['shared_series_contract'] = {
    'scope_id': 'analysis',
    'toggle_semantics': 'independent_series_visibility',
    'metric_ids': ['edge', 'liquidity'],
    'default_visible_metric_ids': ['edge', 'liquidity'],
  }
  packet['advisory_only'] = True
  packet['no_workflow_authority'] = True
  return packet


def _build_saved_set_carry_forward_visual(
  connection: Any,
  *,
  operation_lane: str,
  window_id: str,
  mode: str,
  detail_mode: str,
  now: datetime,
) -> dict[str, Any]:
  del operation_lane
  sandbox_sets = fetch_saved_set_history(connection, operation_lane='sandbox')
  live_sets = fetch_saved_set_history(connection, operation_lane='live')

  if not sandbox_sets and not live_sets:
    empty_copy = _visual_empty_copy('saved_set_carry_forward')
    return _visual_packet(
      view_id='saved_set_carry_forward',
      scope_id='candidate_landscape',
      window_id=window_id,
      render_mode='plot',
      detail_mode=detail_mode,
      now=now,
      headline=empty_copy['headline'],
      next_action=empty_copy['next_action'],
      series=[],
      table=None,
      source_contracts=VISUAL_VIEW_CATALOG['saved_set_carry_forward']['source_contracts'],
      status='empty',
      empty_reason=empty_copy['empty_reason'],
    )

  def _size_points(sets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{'x': s['recorded_at_utc'], 'y': s['saved_key_count']} for s in sets]

  def _delta_points(sets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    prev_keys: set[str] = set()
    for s in sets:
      current_keys = set(s['member_keys'])
      net = (len(current_keys - prev_keys) - len(prev_keys - current_keys)) if prev_keys else 0
      points.append({'x': s['recorded_at_utc'], 'y': net})
      prev_keys = current_keys
    return points

  chart_series: list[dict[str, Any]] = [
    {
      'id': 'sandbox_size',
      'label': 'Sandbox size',
      'chip_label': 'SIZE',
      'lane': 'sandbox',
      'tooltip': 'Candidate count in the saved sandbox set at each checkpoint',
      'kind': 'line',
      'unit': 'count',
      'points': _size_points(sandbox_sets) if sandbox_sets else [],
    },
    {
      'id': 'sandbox_delta',
      'label': 'Sandbox delta',
      'chip_label': 'DELTA',
      'lane': 'sandbox',
      'tooltip': 'Net membership change in the sandbox set vs. the prior save',
      'kind': 'line',
      'unit': 'count',
      'points': _delta_points(sandbox_sets) if sandbox_sets else [],
    },
    {
      'id': 'live_size',
      'label': 'Live size',
      'chip_label': 'SIZE',
      'lane': 'live',
      'tooltip': 'Candidate count in the saved live set at each checkpoint',
      'kind': 'line',
      'unit': 'count',
      'points': _size_points(live_sets) if live_sets else [],
    },
    {
      'id': 'live_delta',
      'label': 'Live delta',
      'chip_label': 'DELTA',
      'lane': 'live',
      'tooltip': 'Net membership change in the live set vs. the prior save',
      'kind': 'line',
      'unit': 'count',
      'points': _delta_points(live_sets) if live_sets else [],
    },
  ]

  all_sets = sorted(sandbox_sets + live_sets, key=lambda s: s['recorded_at_utc'])
  table: dict[str, Any] = {
    'columns': ['Saved at', 'Lane', 'Set size'],
    'rows': [
      [s['recorded_at_utc'], s['operation_lane'], s['saved_key_count']]
      for s in all_sets
    ],
  }
  captured_at = max(
    (_parse_recorded_at(s['recorded_at_utc']) for s in all_sets if s.get('recorded_at_utc')),
    default=now,
  )
  total_sets = len(all_sets)
  result_packet = _visual_packet(
    view_id='saved_set_carry_forward',
    scope_id='candidate_landscape',
    window_id=window_id,
    render_mode='table' if mode == 'table' else 'plot',
    detail_mode=detail_mode,
    now=now,
    headline='Saved-set carry-forward shows {count} retained selection snapshot{plural} across sandbox and live lanes.'.format(
      count=total_sets,
      plural='s' if total_sets != 1 else '',
    ),
    next_action='Review carry-forward stability: converging set size and low net-delta indicates disciplined candidate selection.',
    series=chart_series,
    table=table,
    source_contracts=VISUAL_VIEW_CATALOG['saved_set_carry_forward']['source_contracts'],
    captured_at=captured_at,
  )
  result_packet['shared_graph_mode'] = True
  result_packet['shared_series_contract'] = {
    'scope_id': 'candidate_landscape',
    'toggle_semantics': 'independent_series_visibility',
    'metric_ids': ['sandbox_size', 'sandbox_delta', 'live_size', 'live_delta'],
    'default_visible_metric_ids': ['sandbox_size', 'sandbox_delta', 'live_size', 'live_delta'],
  }
  return result_packet


def _build_actionability_status_distribution_visual(
  connection: Any,
  *,
  operation_lane: str,
  window_id: str,
  mode: str,
  detail_mode: str,
  now: datetime,
) -> dict[str, Any]:
  del operation_lane
  all_rows = fetch_saved_set_evaluation_history(connection)

  if not all_rows:
    empty_copy = _visual_empty_copy('actionability_status_distribution')
    return _visual_packet(
      view_id='actionability_status_distribution',
      scope_id='analysis',
      window_id=window_id,
      render_mode='plot',
      detail_mode=detail_mode,
      now=now,
      headline=empty_copy['headline'],
      next_action=empty_copy['next_action'],
      series=[],
      table=None,
      source_contracts=VISUAL_VIEW_CATALOG['actionability_status_distribution']['source_contracts'],
      status='empty',
      empty_reason=empty_copy['empty_reason'],
    )

  known_statuses = ('revalidation_required', 'expired_actionability', 'offline_limited')
  sandbox_counts: dict[str, int] = {s: 0 for s in known_statuses}
  live_counts: dict[str, int] = {s: 0 for s in known_statuses}
  captured_at: datetime = now

  for row in all_rows:
    status = str(row.get('actionability_status') or 'revalidation_required')
    if row.get('operation_lane') == 'live':
      live_counts[status] = live_counts.get(status, 0) + 1
    else:
      sandbox_counts[status] = sandbox_counts.get(status, 0) + 1
    if row.get('recorded_at_utc'):
      row_dt = _parse_recorded_at(row['recorded_at_utc'])
      if row_dt > captured_at:
        captured_at = row_dt

  status_labels = {
    'revalidation_required': 'Needs revalidation',
    'expired_actionability': 'Expired',
    'offline_limited': 'Offline limited',
  }
  all_statuses = sorted(
    {row.get('actionability_status') or 'revalidation_required' for row in all_rows}
    | set(known_statuses)
  )

  chart_series: list[dict[str, Any]] = [
    {
      'id': 'sandbox_actionability',
      'label': 'Sandbox',
      'chip_label': 'SANDBOX',
      'tooltip': 'Actionability status distribution for the sandbox lane',
      'kind': 'bar',
      'unit': 'count',
      'points': [
        {'x': status_labels.get(s, s), 'y': sandbox_counts.get(s, 0)}
        for s in all_statuses
      ],
    },
    {
      'id': 'live_actionability',
      'label': 'Live',
      'chip_label': 'LIVE',
      'tooltip': 'Actionability status distribution for the live lane',
      'kind': 'bar',
      'unit': 'count',
      'points': [
        {'x': status_labels.get(s, s), 'y': live_counts.get(s, 0)}
        for s in all_statuses
      ],
    },
  ]

  table_columns = ['Status', 'Sandbox', 'Live']
  table_rows = [
    [status_labels.get(s, s), sandbox_counts.get(s, 0), live_counts.get(s, 0)]
    for s in all_statuses
  ]
  total = len(all_rows)
  result_packet = _visual_packet(
    view_id='actionability_status_distribution',
    scope_id='analysis',
    window_id=window_id,
    render_mode='table' if mode == 'table' else 'plot',
    detail_mode=detail_mode,
    now=now,
    headline='Actionability history shows how {total} saved-set evaluation{plural} resolved across sandbox and live lanes.'.format(
      total=total,
      plural='s' if total != 1 else '',
    ),
    next_action='This view is informational only — it shows historical evaluation outcomes, not current actionability.',
    series=chart_series,
    table={'columns': table_columns, 'rows': table_rows},
    source_contracts=VISUAL_VIEW_CATALOG['actionability_status_distribution']['source_contracts'],
    captured_at=captured_at,
  )
  result_packet['shared_graph_mode'] = True
  result_packet['shared_series_contract'] = {
    'scope_id': 'analysis',
    'toggle_semantics': 'independent_series_visibility',
    'metric_ids': ['sandbox_actionability', 'live_actionability'],
    'default_visible_metric_ids': ['sandbox_actionability', 'live_actionability'],
  }
  return result_packet


def fetch_operational_visuals(
  settings: Settings | None = None,
  *,
  env_override: str | None = None,
  subaccount_override: int | None = None,
  scope: str | None = None,
  view: str | None = None,
  window: str | None = None,
  mode: str = 'plot',
  detail: str = 'med',
) -> dict[str, Any]:
  resolved_settings = _resolve_settings(
    settings,
    env_override=env_override,
    subaccount_override=subaccount_override,
  )
  connection = open_database(resolved_settings.state_db_path)
  now = datetime.now(UTC)
  analytical_outputs, analytical_captured_at = _load_latest_analytical_outputs(
    connection,
    operation_lane=resolved_settings.operation_lane,
  )

  inferred_scope_id = _resolve_visual_scope_id(view, scope)
  default_view = str(VISUAL_SCOPE_CATALOG[inferred_scope_id]['default_view'])
  default_window_id = str(VISUAL_SCOPE_CATALOG[inferred_scope_id].get('default_window') or 'current')
  candidate_view_id = view if view in VISUAL_VIEW_CATALOG else default_view
  if str(VISUAL_VIEW_CATALOG.get(candidate_view_id, {}).get('scope_id') or inferred_scope_id) != inferred_scope_id:
    candidate_view_id = default_view
  view_id = candidate_view_id
  allowed_window_ids = list(VISUAL_SCOPE_CATALOG[inferred_scope_id]['window_ids'])
  window_id = window if window in allowed_window_ids else default_window_id
  if window_id not in allowed_window_ids:
    window_id = allowed_window_ids[0]
  render_mode = mode if mode in {'plot', 'table', 'report'} else 'plot'
  detail_mode = _resolve_visual_detail_mode(detail)
  active_run_count = 0
  available_view_ids = _available_visual_view_ids(
    inferred_scope_id,
    active_run_count=active_run_count,
    analytical_outputs=analytical_outputs,
  )
  if view_id not in available_view_ids:
    view_id = available_view_ids[0] if available_view_ids else default_view
  if not _visual_window_enabled(view_id):
    window_id = 'current'
  visual_operation_lane = 'live' if inferred_scope_id == 'performance' else resolved_settings.operation_lane

  builders: dict[str, Callable[..., dict[str, Any]]] = {
    'pair_state_distribution': _build_pair_state_distribution_visual,
    'runtime_cadence': _build_runtime_cadence_visual,
    'cycle_outcomes': _build_cycle_outcomes_visual,
    'freshness_latency': _build_freshness_latency_visual,
    'performance_total': lambda connection, **kwargs: _build_performance_metric_visual(connection, operation_lane=kwargs['operation_lane'], window_id=kwargs['window_id'], mode=kwargs['mode'], detail_mode=kwargs['detail_mode'], now=kwargs['now'], settings=resolved_settings, view_id='performance_total'),
    'performance_delta': lambda connection, **kwargs: _build_performance_metric_visual(connection, operation_lane=kwargs['operation_lane'], window_id=kwargs['window_id'], mode=kwargs['mode'], detail_mode=kwargs['detail_mode'], now=kwargs['now'], settings=resolved_settings, view_id='performance_delta'),
    'performance_total_out': lambda connection, **kwargs: _build_performance_metric_visual(connection, operation_lane=kwargs['operation_lane'], window_id=kwargs['window_id'], mode=kwargs['mode'], detail_mode=kwargs['detail_mode'], now=kwargs['now'], settings=resolved_settings, view_id='performance_total_out'),
    'performance_total_in': lambda connection, **kwargs: _build_performance_metric_visual(connection, operation_lane=kwargs['operation_lane'], window_id=kwargs['window_id'], mode=kwargs['mode'], detail_mode=kwargs['detail_mode'], now=kwargs['now'], settings=resolved_settings, view_id='performance_total_in'),
    'performance_fees': lambda connection, **kwargs: _build_performance_metric_visual(connection, operation_lane=kwargs['operation_lane'], window_id=kwargs['window_id'], mode=kwargs['mode'], detail_mode=kwargs['detail_mode'], now=kwargs['now'], settings=resolved_settings, view_id='performance_fees'),
    'performance_waterfall': lambda connection, **kwargs: _build_performance_waterfall_visual(connection, operation_lane=kwargs['operation_lane'], window_id=kwargs['window_id'], mode=kwargs['mode'], detail_mode=kwargs['detail_mode'], now=kwargs['now'], settings=resolved_settings),
    'candidate_density_curve': lambda _connection, **kwargs: _build_candidate_density_curve_visual(analytical_outputs, mode=kwargs['mode'], detail_mode=kwargs['detail_mode'], now=kwargs['now'], captured_at=analytical_captured_at),
    'candidate_decision_boundary': lambda connection, **kwargs: _build_candidate_decision_boundary_visual(connection, analytical_outputs, operation_lane=kwargs['operation_lane'], mode=kwargs['mode'], detail_mode=kwargs['detail_mode'], now=kwargs['now'], captured_at=analytical_captured_at),
    'candidate_frontier_scatter': lambda _connection, **kwargs: _build_candidate_frontier_scatter_visual(analytical_outputs, settings=resolved_settings, mode=kwargs['mode'], detail_mode=kwargs['detail_mode'], now=kwargs['now'], captured_at=analytical_captured_at),
    'threshold_boundary_marker': lambda _connection, **kwargs: _build_threshold_boundary_marker_visual(analytical_outputs, mode=kwargs['mode'], detail_mode=kwargs['detail_mode'], now=kwargs['now'], captured_at=analytical_captured_at),
    'comparative_ranking_snapshot': lambda _connection, **kwargs: _build_comparative_ranking_snapshot_visual(analytical_outputs, mode=kwargs['mode'], detail_mode=kwargs['detail_mode'], now=kwargs['now'], captured_at=analytical_captured_at),
    'analysis_threshold_progress': lambda _connection, **kwargs: _analysis_threshold_progress_packet(analytical_outputs=analytical_outputs, mode=kwargs['mode'], detail_mode=kwargs['detail_mode'], now=kwargs['now'], captured_at=analytical_captured_at),
    'factor_contribution': lambda _connection, **kwargs: _build_factor_contribution_visual(analytical_outputs, mode=kwargs['mode'], detail_mode=kwargs['detail_mode'], now=kwargs['now'], captured_at=analytical_captured_at),
    'parameter_sensitivity_delta': lambda _connection, **kwargs: _build_parameter_sensitivity_delta_visual(analytical_outputs, mode=kwargs['mode'], detail_mode=kwargs['detail_mode'], now=kwargs['now'], captured_at=analytical_captured_at),
    'analysis_linear_diagnostics': lambda _connection, **kwargs: _build_diagnostics_scatter_visual(analytical_outputs, mode=kwargs['mode'], detail_mode=kwargs['detail_mode'], now=kwargs['now'], captured_at=analytical_captured_at),
    'factors_timeseries': lambda connection, **kwargs: _build_factors_timeseries_visual(connection, analytical_outputs, operation_lane=resolved_settings.operation_lane, mode=kwargs['mode'], detail_mode=kwargs['detail_mode'], now=kwargs['now'], captured_at=analytical_captured_at),
    'saved_set_carry_forward': _build_saved_set_carry_forward_visual,
    'actionability_status_distribution': _build_actionability_status_distribution_visual,
  }
  packet = builders[view_id](
    connection,
    operation_lane=visual_operation_lane,
    window_id=window_id,
    mode=render_mode,
    detail_mode=detail_mode,
    now=now,
  )
  packet['available_scopes'] = [
    {
      'id': scope_key,
      'title': scope_meta['title'],
      'default_view': scope_meta['default_view'],
      'default_window': scope_meta.get('default_window', 'current'),
    }
    for scope_key, scope_meta in VISUAL_SCOPE_CATALOG.items()
  ]
  packet['available_views'] = [
    {
      'id': view_key,
      'title': VISUAL_VIEW_CATALOG[view_key]['title'],
      'table_supported': VISUAL_VIEW_CATALOG[view_key]['table_supported'],
      'report_supported': VISUAL_VIEW_CATALOG[view_key].get('report_supported', False),
    }
    for view_key in available_view_ids
  ]
  packet['available_windows'] = [
    {'id': key, 'label': config['label'], 'bucket': config['bucket']}
    for key, config in VISUAL_WINDOW_CONFIG.items()
    if key in allowed_window_ids
  ] if _visual_window_enabled(view_id) else [
    {'id': window_id, 'label': VISUAL_WINDOW_CONFIG[window_id]['label'], 'bucket': VISUAL_WINDOW_CONFIG[window_id]['bucket']}
  ]
  density_control = packet.get('controls', {}).get('density', {}) if isinstance(packet.get('controls'), dict) else {}
  packet['available_detail_modes'] = [
    {
      'id': key,
      'label': value['label'],
      'glyph': value['glyph'],
    }
    for key, value in VISUAL_DETAIL_CONFIG.items()
  ] if density_control.get('enabled') else [
    {
      'id': detail_mode,
      'label': VISUAL_DETAIL_CONFIG[detail_mode]['label'],
      'glyph': VISUAL_DETAIL_CONFIG[detail_mode]['glyph'],
    }
  ]
  packet['command_family'] = 'polyventure visuals'
  packet.update(_lane_runtime_posture(resolved_settings))
  packet['state_db_path_tail'] = _db_tail(resolved_settings.state_db_path)
  return packet


def _validate_env_alignment(settings: Settings) -> None:
  # Lane membership must be checked first: the offline lane carries no active
  # environment, so env/endpoint coherence checks below are meaningless for it.
  # The web shell's offline carve-out matches this exact message to treat the
  # offline lane as environment-ready.
  operation_lane = str(settings.operation_lane or '').lower()
  if operation_lane not in {'sandbox', 'live'}:
    raise ValueError('Configured operation lane must be sandbox or live.')

  api_url = settings.api_base_url.lower()
  if settings.kalshi_env == 'demo' and 'demo' not in api_url:
    raise ValueError('Configured demo environment does not match the API endpoint.')
  if settings.kalshi_env == 'prod' and 'demo' in api_url:
    raise ValueError('Configured prod environment is still pointing at a demo API endpoint.')

  active_websocket_url = str(settings.active_websocket_url or '').strip()
  sandbox_websocket_url = str(settings.sandbox_websocket_url or '').strip()
  live_websocket_url = str(settings.live_websocket_url or '').strip()
  expected_active_websocket_url = sandbox_websocket_url if operation_lane == 'sandbox' else live_websocket_url

  if not active_websocket_url:
    raise ValueError(f'Configured {operation_lane} operation lane does not have an active websocket endpoint.')
  if expected_active_websocket_url and active_websocket_url != expected_active_websocket_url:
    raise ValueError('Configured active websocket endpoint does not match the selected operation lane.')

  if not websocket_url_is_valid(active_websocket_url):
    raise ValueError('Configured active websocket endpoint is not a valid ws/wss URL.')
  if sandbox_websocket_url and not websocket_url_is_valid(sandbox_websocket_url):
    raise ValueError('Configured sandbox websocket endpoint is not a valid ws/wss URL.')
  if live_websocket_url and not websocket_url_is_valid(live_websocket_url):
    raise ValueError('Configured live websocket endpoint is not a valid ws/wss URL.')


def _load_current_pairs(
  connection: Any,
  *,
  operation_lane: str,
  lane_session_id: str | None = None,
) -> list[PairRuntimeState]:
  if lane_session_id:
    rows = connection.execute(
      '''
      SELECT ps.pair_id, ps.state, ps.detail_json, ps.recorded_at_utc
      FROM pair_states ps
      INNER JOIN (
        SELECT pair_id, MAX(id) AS max_id
        FROM pair_states
        WHERE operation_lane = ?
          AND lane_session_id = ?
        GROUP BY pair_id
      ) latest ON latest.max_id = ps.id
      WHERE ps.operation_lane = ?
      ORDER BY ps.id ASC
      ''',
      (operation_lane, lane_session_id, operation_lane),
    ).fetchall()
  else:
    rows = connection.execute(
      '''
      SELECT ps.pair_id, ps.state, ps.detail_json, ps.recorded_at_utc
      FROM pair_states ps
      INNER JOIN (
        SELECT pair_id, MAX(id) AS max_id
        FROM pair_states
        WHERE operation_lane = ?
        GROUP BY pair_id
      ) latest ON latest.max_id = ps.id
      WHERE ps.operation_lane = ?
      ORDER BY ps.id ASC
      ''',
      (operation_lane, operation_lane),
    ).fetchall()

  current_pairs: list[PairRuntimeState] = []
  for row in rows:
    state = row['state']
    # Fix C (LIVE_AUTOMATION_LOOP_STABILIZATION_DIAGNOSIS_2026-06-25): exclude the FULL canonical
    # terminal set, not a narrower {LOCKED,CANCELED,ERROR}. FILLED/SETTLED/SETTLED_EXPOSURE are
    # terminal in the canonical reconcile set (see reconcile_pairs) but were leaking into the
    # "current/active" pair surface here due to FILLED-vs-LOCKED vocabulary drift. EXPOSURE_CAPPED
    # and other open one-sided states are intentionally NOT terminal here -- they require the Kalshi
    # settlement/exposure reconcile (Fix B) to resolve, not a display-layer exclusion.
    if state in {'LOCKED', 'CANCELED', 'ERROR', 'FILLED', 'SETTLED', 'SETTLED_EXPOSURE'}:
      continue
    detail = json.loads(row['detail_json']) if row['detail_json'] else {}
    current_pairs.append(
      PairRuntimeState(
        pair_id=row['pair_id'],
        state=state,
        yes_filled_contracts=Decimal(str(detail.get('yes_filled_contracts', '0'))),
        no_filled_contracts=Decimal(str(detail.get('no_filled_contracts', '0'))),
        average_yes_price=Decimal(str(detail.get('average_yes_price', '0'))),
        average_no_price=Decimal(str(detail.get('average_no_price', '0'))),
        realized_fees_dollars=Decimal(str(detail.get('realized_fees_dollars', '0'))),
        last_update_at=_parse_recorded_at(row['recorded_at_utc']),
        websocket_connected=bool(detail.get('websocket_connected', False)),
      )
    )
  return current_pairs


def _replace_market_with_orderbook(market: Any, orderbook: Any) -> Any:
  yes_bid = orderbook.best_yes_bid if orderbook.best_yes_bid is not None else market.yes_bid_dollars
  no_bid = orderbook.best_no_bid if orderbook.best_no_bid is not None else market.no_bid_dollars
  yes_ask = (
    orderbook.best_yes_ask_implied
    if orderbook.best_yes_ask_implied is not None
    else market.yes_ask_dollars
  )
  no_ask = (
    orderbook.best_no_ask_implied
    if orderbook.best_no_ask_implied is not None
    else market.no_ask_dollars
  )
  return replace(
    market,
    yes_bid_dollars=yes_bid,
    no_bid_dollars=no_bid,
    yes_ask_dollars=yes_ask,
    no_ask_dollars=no_ask,
  )


def _websocket_hydrate_orderbooks(
  *,
  settings: Settings,
  private_key: object,
  target_tickers: list[str],
  timeout_sec: float = 2.0,
  max_events: int = 200,
) -> tuple[dict[str, OrderbookSnapshot], dict[str, Any]]:
  posture = {
    'websocket_connected': False,
    'websocket_status': 'not_connected_on_current_dry_run_surface',
    'websocket_subscription_count': 0,
    'last_websocket_event_at': None,
    'websocket_event_count': 0,
  }
  websocket_url = str(settings.active_websocket_url or '').strip()
  if not websocket_url:
    posture['websocket_status'] = 'websocket_url_unconfigured'
    return {}, posture
  if not target_tickers:
    posture['websocket_status'] = 'no_target_tickers'
    return {}, posture

  orderbook_cache: dict[str, OrderbookSnapshot] = {}

  def _on_message(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
      return
    message_type = str(payload.get('type') or '').lower()
    msg = payload.get('msg') or {}
    if not isinstance(msg, dict):
      return
    ticker = str(msg.get('market_ticker') or msg.get('ticker') or '')
    if not ticker:
      return
    posture['last_websocket_event_at'] = datetime.now(UTC).isoformat()
    posture['websocket_event_count'] = int(posture['websocket_event_count']) + 1
    if message_type in {'ticker', 'orderbook_snapshot'}:
      snapshot_payload = dict(msg)
      snapshot_payload['ticker'] = ticker
      orderbook_cache[ticker] = normalize_orderbook_snapshot(snapshot_payload)
      return
    if message_type == 'orderbook_delta' and ticker in orderbook_cache:
      try:
        orderbook_cache[ticker] = apply_orderbook_delta(orderbook_cache[ticker], msg)
      except ValueError:
        snapshot_payload = dict(msg)
        snapshot_payload['ticker'] = ticker
        orderbook_cache[ticker] = normalize_orderbook_snapshot(snapshot_payload)

  websocket_client = KalshiWebSocketClient(
    ws_url=websocket_url,
    api_key_id=settings.api_key_id,
    private_key=private_key,
    logger=LOGGER,
    on_message=_on_message,
  )

  async def _hydrate() -> int:
    return await websocket_client.hydrate_orderbooks(
      channels=['ticker', 'orderbook_snapshot', 'orderbook_delta'],
      market_tickers=target_tickers,
      timeout_sec=timeout_sec,
      max_events=max_events,
    )

  try:
    event_count = asyncio.run(_hydrate())
    posture['websocket_connected'] = True
    posture['websocket_status'] = 'connected'
    posture['websocket_subscription_count'] = 3
    posture['websocket_event_count'] = max(int(posture['websocket_event_count']), event_count)
  except WebSocketAuthError:
    posture['websocket_status'] = 'auth_failed'
  except WebSocketError as exc:
    posture['websocket_status'] = exc.__class__.__name__
  except RuntimeError as exc:
    posture['websocket_status'] = 'runtime_loop_unavailable'
    LOGGER.warning('websocket_hydrate_failed code=%s', exc.__class__.__name__)
  return orderbook_cache, posture


def _emit_scan_progress(
  progress_callback: ScanProgressCallback | None,
  stage: str,
  message: str,
  *,
  detail: dict[str, Any] | None = None,
  progress_percent: float | None = None,
) -> None:
  if progress_callback is None:
    return
  progress_callback(stage, message, detail, progress_percent)


def _scan_cancel_requested(cancel_event: object | None) -> bool:
  return bool(cancel_event is not None and hasattr(cancel_event, 'is_set') and cancel_event.is_set())


def _raise_if_scan_canceled(
  cancel_event: object | None,
  *,
  progress_callback: ScanProgressCallback | None = None,
  stage: str = 'cancel_requested',
  message: str = 'Cancel requested; waiting for the current scan checkpoint.',
) -> None:
  if not _scan_cancel_requested(cancel_event):
    return
  _emit_scan_progress(
    progress_callback,
    stage,
    message,
    detail={'canceled_by_user': True},
    progress_percent=None,
  )
  raise ScanCanceledError('Scan canceled by user.')


def _market_requires_orderbook_review(
  market: Any,
  *,
  recorded_at: datetime,
  settings: Settings,
) -> bool:
  status = str(getattr(market, 'status', '') or '').lower()
  if status not in {'open', 'active'}:
    return False
  close_time = getattr(market, 'close_time', None)
  if close_time is None:
    return False
  seconds_to_close = int((close_time - recorded_at.astimezone(UTC)).total_seconds())
  if seconds_to_close < settings.entry_window_end_sec:
    return False
  if seconds_to_close > settings.entry_window_start_sec:
    return False
  return True


SCAN_HEARTBEAT_INTERVAL_SEC = 1.0


def enrich_with_orderbook(client: Any, ticker: str) -> dict[str, Any]:
  return client.get_orderbook(ticker, depth=0)


def _binary_shape_signature(market: Any, suitability: Any) -> str:
  series = str(getattr(suitability, 'series_ticker', '') or getattr(market, 'series_ticker', '') or '').strip()
  reason = str(getattr(suitability, 'reason', '') or 'binary_suitability_unknown').strip()
  market_count = int(getattr(suitability, 'market_count', 0) or 0)
  return '{series}|{reason}|siblings:{count}'.format(
    series=series or 'series:unknown',
    reason=reason,
    count=market_count,
  )


def _market_with_binary_suitability(market: Any, suitability: Any) -> Any:
  try:
    return replace(
      market,
      binary_suitability_status=str(getattr(suitability, 'status', '') or ''),
      binary_suitability_reason=str(getattr(suitability, 'reason', '') or ''),
      binary_suitability_event_ticker=str(getattr(suitability, 'event_ticker', '') or ''),
      binary_suitability_series_ticker=str(getattr(suitability, 'series_ticker', '') or ''),
      binary_suitability_category=str(getattr(suitability, 'category', '') or ''),
      binary_suitability_market_count=int(getattr(suitability, 'market_count', 0) or 0),
      binary_suitability_sibling_tickers=tuple(str(item) for item in (getattr(suitability, 'sibling_tickers', ()) or ())),
    )
  except TypeError:
    return market


def _binary_suitability_filter(
  client: Any,
  markets: list[Any],
  *,
  connection: Any | None = None,
  operation_lane: str | None = None,
  lane_session_id: str | None = None,
  recorded_at: datetime | None = None,
  cancel_event: object | None = None,
) -> tuple[list[Any], dict[str, Any]]:
  _raise_if_scan_canceled(cancel_event)
  stats: dict[str, Any] = {
    'binary_suitability_gate': 'skipped_client_no_event_readback',
    'event_family_readback_count': 0,
    'event_family_readback_failure_count': 0,
    'binary_suitability_eligible_count': len(markets),
    'binary_suitability_rejected_count': 0,
    'binary_suitability_unknown_count': 0,
    'binary_suitability_rejection_reasons': {},
    'known_non_binary_ledger_update_count': 0,
  }
  get_event = getattr(client, 'get_event', None)
  if not callable(get_event):
    return markets, stats

  by_event: dict[str, Any | None] = {}
  for market in markets:
    _raise_if_scan_canceled(cancel_event)
    event_ticker = str(getattr(market, 'event_ticker', '') or '').strip()
    if not event_ticker or event_ticker in by_event:
      continue
    try:
      by_event[event_ticker] = get_event(event_ticker)
      stats['event_family_readback_count'] += 1
    except Exception as exc:
      by_event[event_ticker] = None
      stats['event_family_readback_failure_count'] += 1
      if connection is not None:
        persist_runtime_event(
          connection,
          level='WARN',
          event_type='event_family_readback_failed',
          recorded_at_utc=(recorded_at or datetime.now(UTC)).isoformat(),
          operation_lane=operation_lane or 'sandbox',
          lane_session_id=lane_session_id,
          detail={'event_ticker': event_ticker, 'error_family': type(exc).__name__},
        )

  eligible: list[Any] = []
  reasons: dict[str, int] = {}
  for market in markets:
    _raise_if_scan_canceled(cancel_event)
    event_ticker = str(getattr(market, 'event_ticker', '') or '').strip()
    suitability = classify_binary_suitability(market, by_event.get(event_ticker))
    if suitability.status == 'eligible':
      eligible.append(_market_with_binary_suitability(market, suitability))
      continue
    reason = suitability.reason or 'binary_suitability_unknown'
    reasons[reason] = reasons.get(reason, 0) + 1
    if suitability.status == 'unknown':
      stats['binary_suitability_unknown_count'] += 1
    else:
      stats['binary_suitability_rejected_count'] += 1
    runtime_event_id = None
    if connection is not None:
      persist_runtime_event(
        connection,
        level='INFO',
        event_type='candidate_binary_suitability_rejected',
        recorded_at_utc=(recorded_at or datetime.now(UTC)).isoformat(),
        operation_lane=operation_lane or 'sandbox',
        lane_session_id=lane_session_id,
        detail={
          'ticker': str(getattr(market, 'ticker', '') or ''),
          'event_ticker': event_ticker,
          'reason': reason,
          'market_count': suitability.market_count,
          'series_ticker': suitability.series_ticker,
          'category': suitability.category,
        },
      )
      try:
        row = connection.execute('SELECT last_insert_rowid()').fetchone()
        runtime_event_id = str(row[0]) if row is not None else None
      except Exception:
        runtime_event_id = None
      event = by_event.get(event_ticker)
      actionability = {
        'multi_lane_range_event': 'deferred_threshold_range',
        'rules_indicate_ladder': 'deferred_threshold_range',
        'multivariate_event': 'deferred_multivariate',
        'non_binary_event_family': 'deferred_non_binary',
      }.get(reason, 'unknown_fail_closed' if suitability.status == 'unknown' else 'deferred_non_binary')
      persist_known_non_binary_market(
        connection,
        recorded_at_utc=(recorded_at or datetime.now(UTC)).isoformat(),
        operation_lane=operation_lane or 'sandbox',
        lane_session_id=lane_session_id,
        classification_reason=reason,
        actionability=actionability,
        market_ticker=str(getattr(market, 'ticker', '') or ''),
        event_ticker=event_ticker,
        series_ticker=str(getattr(suitability, 'series_ticker', '') or getattr(market, 'series_ticker', '') or ''),
        shape_signature=_binary_shape_signature(market, suitability),
        market_count=int(getattr(suitability, 'market_count', 0) or 0),
        mutually_exclusive=getattr(event, 'mutually_exclusive', None) if event is not None else None,
        sample_sibling_tickers=tuple(str(item) for item in (getattr(suitability, 'sibling_tickers', ()) or ()))[:12],
        source_run_id=lane_session_id,
        source_runtime_event_id=runtime_event_id,
        detail={
          'source': 'binary_suitability_filter',
          'status': str(getattr(suitability, 'status', '') or ''),
          'category': str(getattr(suitability, 'category', '') or ''),
        },
      )
      stats['known_non_binary_ledger_update_count'] += 1

  stats.update(
    {
      'binary_suitability_gate': 'applied',
      'binary_suitability_eligible_count': len(eligible),
      'binary_suitability_rejection_reasons': reasons,
    }
  )
  return eligible, stats


def _load_candidate_market_set(
  client: Any,
  *,
  recorded_at: datetime,
  connection: Any | None = None,
  operation_lane: str | None = None,
  lane_session_id: str | None = None,
  websocket_orderbooks: dict[str, OrderbookSnapshot] | None = None,
  settings: Settings | None = None,
  private_key: object | None = None,
  progress_callback: ScanProgressCallback | None = None,
  cancel_event: object | None = None,
) -> tuple[list[Any], list[Any], dict[str, Any], int, dict[str, Any]]:
  _raise_if_scan_canceled(cancel_event, progress_callback=progress_callback)
  _emit_scan_progress(
    progress_callback,
    'loading_markets',
    'Loading open markets for candidate review.',
    detail={'market_limit': 1000},
    progress_percent=0.18,
  )
  _min_close_ts: int | None = None
  _max_close_ts: int | None = None
  if settings is not None:
    _min_close_ts = int(recorded_at.timestamp()) + settings.entry_window_end_sec + settings.entry_window_fetch_padding_sec
    _max_close_ts = int(recorded_at.timestamp()) + settings.entry_window_start_sec
    markets = fetch_open_markets(client, limit=1000, min_close_ts=_min_close_ts, max_close_ts=_max_close_ts)
  else:
    markets = fetch_open_markets(client, limit=1000)
  _raise_if_scan_canceled(cancel_event, progress_callback=progress_callback)
  enriched_markets: list[Any] = []
  market_by_ticker: dict[str, Any] = {}
  enriched_count = 0
  websocket_posture = {
    'websocket_connected': False,
    'websocket_status': 'not_connected_on_current_dry_run_surface',
    'websocket_subscription_count': 0,
    'last_websocket_event_at': None,
    'websocket_event_count': 0,
  }
  websocket_orderbooks = websocket_orderbooks or {}
  supports_live_websocket = str(getattr(client.__class__, '__module__', '')) == 'polyventure.http_client'
  if settings is not None and private_key is not None and supports_live_websocket:
    if not markets:
      websocket_posture['websocket_status'] = 'skipped_no_entry_window_markets'
    else:
      _raise_if_scan_canceled(cancel_event, progress_callback=progress_callback)
      _emit_scan_progress(
        progress_callback,
        'hydrating_orderbooks',
        'Hydrating orderbooks from the websocket feed.',
        detail={'market_count': len(markets)},
        progress_percent=0.38,
      )
      _ws_target_count = len(markets)
      _ws_max_events  = max(200, _ws_target_count * 2)
      _ws_timeout_sec = min(15.0, 2.0 + _ws_target_count / 100)
      websocket_orderbooks, websocket_posture = _websocket_hydrate_orderbooks(
        settings=settings,
        private_key=private_key,
        target_tickers=[market.ticker for market in markets],
        timeout_sec=_ws_timeout_sec,
        max_events=_ws_max_events,
      )
      _raise_if_scan_canceled(cancel_event, progress_callback=progress_callback)
  elif settings is not None and private_key is not None:
    websocket_posture['websocket_status'] = 'websocket_hydration_skipped_for_non_http_client'
  reviewable_markets = (
    [
      market
      for market in markets
      if settings is None or _market_requires_orderbook_review(
        market,
        recorded_at=recorded_at,
        settings=settings,
      )
    ]
  )
  binary_eligible_markets, binary_suitability_stats = _binary_suitability_filter(
    client,
    reviewable_markets,
    connection=connection,
    operation_lane=operation_lane,
    lane_session_id=lane_session_id,
    recorded_at=recorded_at,
    cancel_event=cancel_event,
  )
  _raise_if_scan_canceled(cancel_event, progress_callback=progress_callback)
  reviewable_markets = binary_eligible_markets
  reviewable_market_tickers = {market.ticker for market in reviewable_markets}
  open_or_active_market_count = sum(
    1
    for market in markets
    if str(getattr(market, 'status', '') or '').lower() in {'open', 'active'}
  )
  close_time_known_market_count = sum(
    1
    for market in markets
    if str(getattr(market, 'status', '') or '').lower() in {'open', 'active'}
    and getattr(market, 'close_time', None) is not None
  )
  _emit_scan_progress(
    progress_callback,
    'screening_candidate_universe',
    'Screening the candidate universe before orderbook enrichment.',
    detail={
      'loaded_market_count': len(markets),
      'orderbook_enrichment_target_count': len(reviewable_markets),
      'scan_shape_summary': {
        'summary_phase': 'pre_enrichment',
        'loaded_market_count': len(markets),
        'open_or_active_market_count': open_or_active_market_count,
        'close_time_known_market_count': close_time_known_market_count,
        'entry_window_eligible_market_count': len(reviewable_markets),
        'orderbook_enrichment_target_count': len(reviewable_markets),
        'binary_suitability': binary_suitability_stats,
        # Lane B1 (FIND_CANDIDATES_RETRY_PROJECTION_COHERENCE_BMAP_2026-06-19): names-only
        # diagnostic for empty-fetch investigation. Records the computed entry-window close-time
        # bounds (epoch seconds) and config so a future live run can show whether a zero
        # loaded_market_count is a window-bound computation defect or a genuine/upstream zero.
        # Additive only -- no behavior change, no extra market fetch.
        'entry_window_fetch': {
          'applied': settings is not None,
          'recorded_at_epoch': int(recorded_at.timestamp()),
          'entry_window_end_sec': settings.entry_window_end_sec if settings is not None else None,
          'entry_window_start_sec': settings.entry_window_start_sec if settings is not None else None,
          'entry_window_fetch_padding_sec': settings.entry_window_fetch_padding_sec if settings is not None else None,
          'min_close_ts': _min_close_ts,
          'max_close_ts': _max_close_ts,
          'fetched_market_count': len(markets),
        },
      },
    },
    progress_percent=0.44,
  )
  _emit_scan_progress(
    progress_callback,
    'enriching_remaining_orderbooks',
    'Enriching remaining orderbooks for candidate scoring.',
    detail={
      'loaded_market_count': len(markets),
      'market_count': len(reviewable_markets),
      'orderbook_review_market_count': len(reviewable_markets),
      'orderbook_enrichment_target_count': len(reviewable_markets),
      'websocket_orderbook_count': len(websocket_orderbooks),
    },
    progress_percent=0.56,
  )
  total_markets = len(reviewable_markets)
  progress_emit_interval = 250 if total_markets >= 250 else max(1, total_markets // 5) if total_markets else 1
  websocket_hit_count = 0
  rest_fallback_count = 0
  enrichment_failure_count = 0
  review_index = 0
  last_heartbeat_at = time.monotonic()
  for market in reviewable_markets:
    _raise_if_scan_canceled(cancel_event, progress_callback=progress_callback)
    enriched_market = market
    if market.ticker in reviewable_market_tickers:
      review_index += 1
      if market.ticker in websocket_orderbooks:
        enriched_market = _replace_market_with_orderbook(market, websocket_orderbooks[market.ticker])
        enriched_count += 1
        websocket_hit_count += 1
      elif hasattr(client, 'get_orderbook'):
        rest_fallback_count += 1
        try:
          orderbook = enrich_with_orderbook(client, market.ticker)
          enriched_market = _replace_market_with_orderbook(market, orderbook)
          enriched_count += 1
        except Exception as exc:
          enrichment_failure_count += 1
          if connection is not None:
            persist_runtime_event(
              connection,
              level='WARN',
              event_type='orderbook_enrichment_failed',
              recorded_at_utc=recorded_at.isoformat(),
              operation_lane=operation_lane or 'sandbox',
              lane_session_id=lane_session_id,
              detail={'ticker': market.ticker, 'message': str(exc)},
            )
      if review_index < total_markets and (time.monotonic() - last_heartbeat_at) >= SCAN_HEARTBEAT_INTERVAL_SEC:
        _emit_scan_progress(
          progress_callback,
          'enriching_remaining_orderbooks',
          'Enriching remaining orderbooks for candidate scoring.',
          detail={
            'loaded_market_count': len(markets),
            'market_count': total_markets,
            'orderbook_review_market_count': total_markets,
            'orderbook_enrichment_target_count': total_markets,
            'processed_market_count': review_index,
            'remaining_market_count': max(total_markets - review_index, 0),
            'websocket_orderbook_count': len(websocket_orderbooks),
            'websocket_hit_count': websocket_hit_count,
            'rest_fallback_count': rest_fallback_count,
            'orderbook_enrichment_count': enriched_count,
            'orderbook_enrichment_failure_count': enrichment_failure_count,
          },
          progress_percent=None,
        )
        last_heartbeat_at = time.monotonic()
    enriched_markets.append(enriched_market)
    market_by_ticker[enriched_market.ticker] = enriched_market
    if total_markets and market.ticker in reviewable_market_tickers and (review_index == total_markets or review_index % progress_emit_interval == 0):
      progress_ratio = review_index / total_markets
      progress_percent = min(0.74, 0.56 + (0.18 * progress_ratio))
      _emit_scan_progress(
        progress_callback,
        'enriching_remaining_orderbooks',
        'Enriching remaining orderbooks for candidate scoring.',
        detail={
          'loaded_market_count': len(markets),
          'market_count': total_markets,
          'orderbook_review_market_count': total_markets,
          'orderbook_enrichment_target_count': total_markets,
          'processed_market_count': review_index,
          'remaining_market_count': max(total_markets - review_index, 0),
          'websocket_orderbook_count': len(websocket_orderbooks),
          'websocket_hit_count': websocket_hit_count,
          'rest_fallback_count': rest_fallback_count,
          'orderbook_enrichment_count': enriched_count,
          'orderbook_enrichment_failure_count': enrichment_failure_count,
        },
        progress_percent=progress_percent,
      )
  websocket_posture.update(
    {
      'websocket_orderbook_count': len(websocket_orderbooks),
      'websocket_hit_count': websocket_hit_count,
      'orderbook_review_market_count': total_markets,
      'rest_fallback_count': rest_fallback_count,
      'orderbook_enrichment_failure_count': enrichment_failure_count,
      **binary_suitability_stats,
    }
  )
  return markets, enriched_markets, market_by_ticker, enriched_count, websocket_posture


def _scan_shape_summary(
  markets: list[Any],
  *,
  candidate_markets: list[Any],
  orderbook_enrichment_count: int,
  candidate_count: int,
  websocket_orderbook_count: int,
  orderbook_review_market_count: int | None = None,
  rest_fallback_count: int = 0,
  orderbook_enrichment_failure_count: int = 0,
  websocket_hit_count: int | None = None,
  binary_suitability: dict[str, Any] | None = None,
) -> dict[str, Any]:
  loaded_market_count = len(markets)
  open_or_active_market_count = sum(
    1
    for market in markets
    if str(getattr(market, 'status', '') or '').lower() in {'open', 'active'}
  )
  close_time_known_market_count = sum(
    1
    for market in markets
    if str(getattr(market, 'status', '') or '').lower() in {'open', 'active'}
    and getattr(market, 'close_time', None) is not None
  )
  entry_window_eligible_market_count = len(candidate_markets)
  reviewed_market_count = int(orderbook_review_market_count if orderbook_review_market_count is not None else entry_window_eligible_market_count)
  quote_ready_market_count = int(orderbook_enrichment_count)
  profitability_pass_market_count = int(candidate_count)
  api_orderbook_enrichment_count = max(int(orderbook_enrichment_count) - int(websocket_orderbook_count), 0)
  candidate_conversion_from_loaded_markets = round(
    (candidate_count / loaded_market_count),
    4,
  ) if loaded_market_count else 0.0
  candidate_conversion_from_enriched_markets = round(
    (candidate_count / orderbook_enrichment_count),
    4,
  ) if orderbook_enrichment_count else 0.0
  summary = {
    'loaded_market_count': loaded_market_count,
    'open_or_active_market_count': open_or_active_market_count,
    'close_time_known_market_count': close_time_known_market_count,
    'entry_window_eligible_market_count': entry_window_eligible_market_count,
    'orderbook_review_market_count': reviewed_market_count,
    'quote_ready_market_count': quote_ready_market_count,
    'rest_fallback_count': int(rest_fallback_count),
    'orderbook_enrichment_failure_count': int(orderbook_enrichment_failure_count),
    'profitability_pass_market_count': profitability_pass_market_count,
    'qualifying_candidate_count': int(candidate_count),
    'websocket_orderbook_count': int(websocket_orderbook_count),
    'websocket_hit_count': int(websocket_hit_count if websocket_hit_count is not None else websocket_orderbook_count),
    'orderbook_enrichment_count': int(orderbook_enrichment_count),
    'api_orderbook_enrichment_count': api_orderbook_enrichment_count,
    'candidate_count': int(candidate_count),
    'candidate_conversion_from_loaded_markets': candidate_conversion_from_loaded_markets,
    'candidate_conversion_from_enriched_markets': candidate_conversion_from_enriched_markets,
    'binary_suitability': dict(binary_suitability or {}),
  }
  if candidate_count == 0:
    if loaded_market_count == 0:
      reason_family = 'entry_window_fetch_empty'
      blocking_stage = 'market_fetch'
    elif entry_window_eligible_market_count == 0:
      reason_family = 'entry_window_filter_empty'
      blocking_stage = 'entry_window'
    elif reviewed_market_count == 0:
      reason_family = 'binary_suitability_empty'
      blocking_stage = 'binary_suitability'
    elif quote_ready_market_count == 0:
      reason_family = 'orderbook_enrichment_empty'
      blocking_stage = 'orderbook_enrichment'
    else:
      reason_family = 'profitability_threshold_empty'
      blocking_stage = 'profitability_filter'
    summary['zero_candidate_reason_family'] = reason_family
    summary['zero_candidate_blocking_stage'] = blocking_stage
  return summary


def _unpack_candidate_market_set(
  payload: tuple[Any, ...],
) -> tuple[list[Any], list[Any], dict[str, Any], int, dict[str, Any]]:
  if len(payload) == 5:
    markets, candidate_markets, market_by_ticker, enriched_count, websocket_posture = payload
    return markets, candidate_markets, market_by_ticker, int(enriched_count), dict(websocket_posture)
  if len(payload) == 4:
    markets, candidate_markets, market_by_ticker, enriched_count = payload
    return (
      markets,
      candidate_markets,
      market_by_ticker,
      int(enriched_count),
      {
        'websocket_connected': False,
        'websocket_status': 'not_connected_on_current_dry_run_surface',
        'websocket_subscription_count': 0,
        'last_websocket_event_at': None,
        'websocket_event_count': 0,
        'websocket_orderbook_count': 0,
      },
    )
  raise ValueError('Unexpected candidate market payload shape.')


def _mark_auto_canceled_candidates_terminal(
  connection: Any,
  *,
  operation_lane: str,
  operator_lane_session_id: str,
  recorded_at: datetime,
) -> None:
  now_iso = recorded_at.isoformat()
  with connection:
    connection.execute(
      '''
      UPDATE candidate_review_candidates
      SET lifecycle_stage = 'terminal',
          terminal_cause = 'auto_cancel',
          terminal_at_utc = ?
      WHERE lifecycle_stage = 'in_flight'
        AND run_id IN (
            SELECT run_id FROM candidate_review_runs
            WHERE lane_session_id = ? AND operation_lane = ?
        )
        AND ticker IN (
            SELECT pp.ticker
            FROM pair_plans pp
            JOIN (
                SELECT pair_id, MAX(id) AS max_id
                FROM pair_states
                WHERE operation_lane = ?
                GROUP BY pair_id
            ) latest ON latest.pair_id = pp.pair_id
            JOIN pair_states ps ON ps.id = latest.max_id
            WHERE ps.state = 'CANCELED' AND pp.operation_lane = ?
        )
      ''',
      (now_iso, operator_lane_session_id, operation_lane, operation_lane, operation_lane),
    )


def _mark_expired_candidates_terminal(
  connection: Any,
  *,
  operation_lane: str,
  operator_lane_session_id: str,
  recorded_at: datetime,
) -> None:
  # Lane B: three-deadline candidate-expiry transition engine. Reads the discovery-time
  # deadline columns (Lane A) instead of markets_seen, so each transition fires at its
  # TRUE deadline, not at Kalshi close only. Operator-session + lane scoped (same scope
  # as the SSOT canonical query, so surfaces stay coherent). Set-based; once per cycle.
  #   #1 view-window lapse  : discovered/selected past view_expires_at_utc -> terminal
  #   #3 market-close backstop: any non-terminal past market_close_at_utc -> terminal
  #   #2 submit-window deadline is INERT pre-bridge-submit (no stage write here); the
  #      bridge-submit selector consumes submit_expires_at_utc once that lane is live.
  # Deadlines are stored second-precision Z-format (candidate_identity._iso_z); compare
  # against `now` in the SAME format so lexicographic order equals temporal order.
  now_z = (
    recorded_at.astimezone(UTC).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
  )
  now_iso = recorded_at.isoformat()
  with connection:
    # #1 view-window lapse — the candidate leaves the selection surface before close.
    connection.execute(
      '''
      UPDATE candidate_review_candidates
      SET lifecycle_stage = 'terminal',
          terminal_cause = 'expired_unfilled',
          terminal_subcause = 'view_window_lapsed',
          terminal_at_utc = ?
      WHERE lifecycle_stage IN ('discovered', 'selected')
        AND view_expires_at_utc IS NOT NULL
        AND view_expires_at_utc <= ?
        AND run_id IN (
            SELECT run_id FROM candidate_review_runs
            WHERE lane_session_id = ? AND operation_lane = ?
        )
      ''',
      (now_iso, now_z, operator_lane_session_id, operation_lane),
    )
    # #3 market-close backstop — anything still non-terminal at Kalshi close.
    connection.execute(
      '''
      UPDATE candidate_review_candidates
      SET lifecycle_stage = 'terminal',
          terminal_cause = 'expired_unfilled',
          terminal_subcause = 'market_closed',
          terminal_at_utc = ?
      WHERE lifecycle_stage NOT IN ('terminal')
        AND market_close_at_utc IS NOT NULL
        AND market_close_at_utc <= ?
        AND run_id IN (
            SELECT run_id FROM candidate_review_runs
            WHERE lane_session_id = ? AND operation_lane = ?
        )
      ''',
      (now_iso, now_z, operator_lane_session_id, operation_lane),
    )


def _reconcile_current_pairs(
  connection: Any,
  pairs: list[PairRuntimeState],
  *,
  settings: Settings,
  recorded_at: datetime,
  lane_session_id: str,
) -> tuple[list[dict[str, Any]], int]:
  reconciled: list[dict[str, Any]] = []
  timed_out_pairs = 0
  for pair in pairs:
    blocked_reason: str | None = None
    state = pair.state
    # Canonical posture: max_unhedged_sec is a shelter window keyed on seconds-to-close,
    # not an order-age timeout. The reconcile sweep does NOT force a one-sided pair to
    # ERROR on elapsed time; the deficient leg's repair order is left open to fill and
    # the shelter action / repair-close reconciler own the terminal outcome.
    public_state_id = _project_public_state_id(state, blocked_reason=blocked_reason)
    mobility_posture = _project_mobility_posture(
      public_state_id=public_state_id,
      recorded_at=pair.last_update_at,
      as_of=recorded_at,
    )
    failure_posture = _project_failure_posture(
      public_state_id=public_state_id,
      blocked_reason=blocked_reason,
    )
    action_contract = _project_action_contract(
      public_state_id=public_state_id,
      blocked_reason=blocked_reason,
      mobility_overlay_state=mobility_posture['mobility_overlay_state'],
    )
    reconciled.append(
      {
        'pair_id': pair.pair_id,
        'state': state,
        'public_state_id': public_state_id,
        'locked_contracts': str(pair.locked_contracts),
        'unmatched_contracts': str(pair.unmatched_contracts),
        'websocket_connected': pair.websocket_connected,
        'blocked_reason': blocked_reason,
        'mobility_overlay_state': mobility_posture['mobility_overlay_state'],
        'mobility_elapsed_ms': mobility_posture['mobility_elapsed_ms'],
        'failure_class': failure_posture['failure_class'],
        'failure_scope': failure_posture['failure_scope'],
        'allowed_actions': action_contract['allowed_actions'],
        'blocked_actions': action_contract['blocked_actions'],
      }
    )
  return reconciled, timed_out_pairs


def _pair_runtime_summary(snapshot: dict[str, Any], *, fee_reserve_dollars: Decimal) -> dict[str, Any]:
  detail = snapshot['detail']
  pair = PairRuntimeState(
    pair_id=snapshot['pair_id'],
    state=snapshot['state'],
    yes_filled_contracts=Decimal(str(detail.get('yes_filled_contracts', '0'))),
    no_filled_contracts=Decimal(str(detail.get('no_filled_contracts', '0'))),
    average_yes_price=Decimal(str(detail.get('average_yes_price', '0'))),
    average_no_price=Decimal(str(detail.get('average_no_price', '0'))),
    realized_fees_dollars=Decimal(str(detail.get('realized_fees_dollars', '0'))),
    last_update_at=_parse_recorded_at(snapshot['recorded_at_utc']),
    websocket_connected=bool(detail.get('websocket_connected', False)),
  )
  total_cost = pair.locked_contracts * (pair.average_yes_price + pair.average_no_price)
  total_in = pair.locked_contracts
  execution_intent_source = str(detail.get('execution_intent_source') or '').strip().lower()
  bridge_projection_active = bool(
    execution_intent_source == 'saved_set'
    or str(detail.get('saved_set_id') or '').strip()
    or str(detail.get('submit_response_id') or '').strip()
  )
  public_state_id = _project_public_state_id(snapshot['state'], detail=detail)
  blocked_reason = str(detail.get('blocked_reason') or detail.get('reason') or '').strip() or None
  mobility_posture = _project_mobility_posture(
    public_state_id=public_state_id,
    recorded_at=pair.last_update_at,
  )
  failure_posture = _project_failure_posture(
    public_state_id=public_state_id,
    blocked_reason=blocked_reason,
  )
  action_contract = _project_action_contract(
    public_state_id=public_state_id,
    blocked_reason=blocked_reason,
    mobility_overlay_state=mobility_posture['mobility_overlay_state'],
  )
  return {
    'pair_id': snapshot['pair_id'],
    'ticker': snapshot['ticker'],
    'state': snapshot['state'],
    'legacy_state': snapshot['state'],
    'contract_count': snapshot.get('contract_count'),
    'total_cost_dollars': str(total_cost),
    'total_in_dollars': str(total_in),
    'fees_dollars': str(pair.realized_fees_dollars),
    # Visuals-panel fee ledger reflects fees actually paid (settled), never
    # estimates: FILLED/SETTLED carry the real fee; in-flight, CANCELED, and
    # failed pairs contribute zero so the fee timeseries needs no follow-up
    # gating. The estimate stays on `fees_dollars` for the in-flight gross offset.
    'settled_fees_dollars': str(
      pair.realized_fees_dollars
      if public_state_id in {'FILLED', 'SETTLED', 'SETTLED_EXPOSURE'}
      else Decimal('0')
    ),
    'effective_density': detail.get('effective_density'),
    'dynamic_pair_notional_pct': detail.get('dynamic_pair_notional_pct'),
    'dynamic_pair_notional_cap_dollars': detail.get('dynamic_pair_notional_cap_dollars'),
    'dynamic_max_contracts': detail.get('dynamic_max_contracts'),
    'binding_limiter': detail.get('binding_limiter'),
    'execution_intent_source': execution_intent_source or None,
    'public_state_id': public_state_id,
    'terminal_state': (
      public_state_id
      if public_state_id in {'FILLED', 'SETTLED', 'SETTLED_EXPOSURE', 'SUBMIT_FAILED_TERMINAL', 'CANCELED'}
      else ''
    ),
    'pair_state_recorded_at_utc': str(snapshot.get('recorded_at_utc') or ''),
    'lane_session_id': str(snapshot.get('lane_session_id') or ''),
    'submit_response_id': (
      str(detail.get('submit_response_id') or '').strip() or None
      if bridge_projection_active
      else None
    ),
    'saved_set_id': (str(detail.get('saved_set_id') or '').strip() or None) if bridge_projection_active else None,
    'saved_set_recorded_at_utc': (
      str(detail.get('saved_set_recorded_at_utc') or '').strip() or None
    ) if bridge_projection_active else None,
    'saved_set_actionability_status': (
      str(detail.get('saved_set_actionability_status') or '').strip() or None
    ) if bridge_projection_active else None,
    'mobility_overlay_state': mobility_posture['mobility_overlay_state'],
    'mobility_elapsed_ms': mobility_posture['mobility_elapsed_ms'],
    'failure_class': failure_posture['failure_class'],
    'failure_scope': failure_posture['failure_scope'],
    'retry_allowed': action_contract['retry_allowed'],
    'allowed_actions': action_contract['allowed_actions'],
    'blocked_actions': action_contract['blocked_actions'],
    **compute_locked_pnl(pair, fee_reserve_dollars=fee_reserve_dollars),
  }


def _saved_set_bridge_guard_reason(saved_set: dict[str, Any] | None, *, operation_lane: str | None = None) -> str | None:
  if saved_set is None:
    return 'no_saved_set'
  if operation_lane is not None and str(saved_set.get('operation_lane') or '').strip() != str(operation_lane or '').strip():
    return 'saved_set_lane_mismatch'
  if not str(saved_set.get('run_id') or '').strip():
    return 'saved_set_run_missing'
  members = saved_set.get('members') if isinstance(saved_set.get('members'), list) else []
  if not members:
    return 'saved_set_empty'
  return None


def _submit_handoff_validation_error(reason: str) -> SubmitHandoffValidationError:
  exc = SubmitHandoffValidationError(reason)
  exc.polyventure_submit_bridge_phase = 'submit_handoff_validation'
  return exc


def _saved_set_member_keys(saved_set: dict[str, Any]) -> list[str]:
  members = saved_set.get('members') if isinstance(saved_set.get('members'), list) else []
  return [
    str(member.get('candidate_key') or member.get('candidate_uid') or '').strip()
    for member in members
    if isinstance(member, dict) and str(member.get('candidate_key') or member.get('candidate_uid') or '').strip()
  ]


def _resolve_submit_handoff_saved_set(
  connection: Any,
  *,
  submit_handoff: dict[str, Any],
  operation_lane: str,
) -> dict[str, Any]:
  handoff = submit_handoff if isinstance(submit_handoff, dict) else {}
  required_fields = (
    'handoff_id',
    'operation_lane',
    'operator_lane_session_id',
    'scan_session_id',
    'saved_set_id',
    'candidate_signature',
    'candidate_count',
    'candidate_keys',
  )
  for field in required_fields:
    value = handoff.get(field)
    if value in (None, '') or (field == 'candidate_keys' and not isinstance(value, list)):
      raise _submit_handoff_validation_error(f'submit_handoff_missing_{field}')
  if str(handoff.get('operation_lane') or '').strip() != str(operation_lane or '').strip():
    raise _submit_handoff_validation_error('submit_handoff_operation_lane_mismatch')

  candidate_keys = [str(key or '').strip() for key in handoff.get('candidate_keys') if str(key or '').strip()]
  if not candidate_keys:
    raise _submit_handoff_validation_error('submit_handoff_empty_candidate_keys')
  try:
    expected_count = int(handoff.get('candidate_count') or 0)
  except (TypeError, ValueError):
    expected_count = 0
  if expected_count != len(candidate_keys):
    raise _submit_handoff_validation_error('submit_handoff_candidate_count_mismatch')
  expected_signature = str(handoff.get('candidate_signature') or '').strip()
  if expected_signature != '|'.join(candidate_keys):
    raise _submit_handoff_validation_error('submit_handoff_candidate_signature_mismatch')

  saved_set = fetch_candidate_saved_set_for_handoff(
    connection,
    saved_set_id=str(handoff.get('saved_set_id') or '').strip(),
    operation_lane=operation_lane,
    lane_session_id=str(handoff.get('operator_lane_session_id') or '').strip(),
    run_id=str(handoff.get('scan_session_id') or '').strip(),
  )
  if saved_set is None:
    raise _submit_handoff_validation_error('submit_handoff_saved_set_not_found')
  member_keys = _saved_set_member_keys(saved_set)
  if member_keys != candidate_keys:
    raise _submit_handoff_validation_error('submit_handoff_candidate_keys_mismatch')
  if int(saved_set.get('saved_key_count') or 0) != expected_count:
    raise _submit_handoff_validation_error('submit_handoff_saved_key_count_mismatch')
  detail = saved_set.get('detail') if isinstance(saved_set.get('detail'), dict) else {}
  stored_signature = str(
    detail.get('candidate_signature')
    or detail.get('saved_signature')
    or ''
  ).strip()
  if stored_signature and stored_signature != expected_signature:
    raise _submit_handoff_validation_error('submit_handoff_saved_signature_mismatch')
  return saved_set


def _saved_set_member_ticker(member: dict[str, Any]) -> str:
  detail = member.get('detail') if isinstance(member.get('detail'), dict) else {}
  return str(
    member.get('ticker')
    or member.get('candidate_ticker')
    or member.get('market_ticker')
    or
    detail.get('ticker')
    or detail.get('candidate_ticker')
    or detail.get('market_ticker')
    or ''
  ).strip()


def _saved_set_member_candidate(member: dict[str, Any]) -> CandidatePair | None:
  detail = member.get('detail') if isinstance(member.get('detail'), dict) else {}
  source = {**detail, **{key: value for key, value in member.items() if key != 'detail'}}
  ticker = _saved_set_member_ticker(member)
  if not ticker:
    return None
  required_fields = (
    'seconds_to_close',
    'target_yes_bid',
    'target_no_bid',
    'edge_gross_per_contract',
    'fee_reserve_per_contract',
    'edge_net_per_contract',
    'asymmetry',
    'max_size_contracts',
  )
  if any(source.get(field) in (None, '') for field in required_fields):
    return None

  def _decimal_field(name: str, default: str = '0') -> Decimal:
    value = source.get(name)
    if value in (None, ''):
      value = default
    return Decimal(str(value))

  try:
    seconds_to_close = int(source.get('seconds_to_close') or 0)
    ranking_key_raw = source.get('ranking_key') if isinstance(source.get('ranking_key'), (list, tuple)) else []
    ranking_key = (
      Decimal(str(ranking_key_raw[0])) if len(ranking_key_raw) > 0 else _decimal_field('edge_net_per_contract'),
      Decimal(str(ranking_key_raw[1])) if len(ranking_key_raw) > 1 else -_decimal_field('asymmetry'),
      Decimal(str(ranking_key_raw[2])) if len(ranking_key_raw) > 2 else Decimal(str(source.get('volume_24h_fp') or 0)),
      Decimal(str(ranking_key_raw[3])) if len(ranking_key_raw) > 3 else Decimal(str(source.get('open_interest_fp') or 0)),
      int(ranking_key_raw[4]) if len(ranking_key_raw) > 4 else -seconds_to_close,
    )
    siblings = source.get('binary_suitability_sibling_tickers')
    if not isinstance(siblings, (list, tuple)):
      binary = source.get('binary_suitability') if isinstance(source.get('binary_suitability'), dict) else {}
      siblings = binary.get('sibling_tickers') if isinstance(binary.get('sibling_tickers'), (list, tuple)) else ()
      if not source.get('binary_suitability_status') and binary:
        source = {
          **source,
          'binary_suitability_status': binary.get('status'),
          'binary_suitability_reason': binary.get('reason'),
          'binary_suitability_event_ticker': binary.get('event_ticker'),
          'binary_suitability_series_ticker': binary.get('series_ticker'),
          'binary_suitability_category': binary.get('category'),
          'binary_suitability_market_count': binary.get('market_count'),
        }
    return CandidatePair(
      ticker=ticker,
      seconds_to_close=seconds_to_close,
      target_yes_bid=_decimal_field('target_yes_bid'),
      target_no_bid=_decimal_field('target_no_bid'),
      edge_gross_per_contract=_decimal_field('edge_gross_per_contract'),
      fee_reserve_per_contract=_decimal_field('fee_reserve_per_contract'),
      edge_net_per_contract=_decimal_field('edge_net_per_contract'),
      asymmetry=_decimal_field('asymmetry'),
      max_size_contracts=_decimal_field('max_size_contracts', '1'),
      ranking_key=ranking_key,
      binary_suitability_status=str(source.get('binary_suitability_status') or ''),
      binary_suitability_reason=str(source.get('binary_suitability_reason') or ''),
      binary_suitability_event_ticker=str(source.get('binary_suitability_event_ticker') or ''),
      binary_suitability_series_ticker=str(source.get('binary_suitability_series_ticker') or ''),
      binary_suitability_category=str(source.get('binary_suitability_category') or ''),
      binary_suitability_market_count=int(source.get('binary_suitability_market_count') or 0),
      binary_suitability_sibling_tickers=tuple(str(item) for item in siblings),
    )
  except (InvalidOperation, TypeError, ValueError):
    return None


def _submit_binary_proof_block(
  client: Any,
  candidate: Any,
  candidate_market: Any,
) -> tuple[str | None, dict[str, Any]]:
  ticker = str(getattr(candidate, 'ticker', '') or '').strip()
  event_ticker = str(
    getattr(candidate, 'binary_suitability_event_ticker', '')
    or getattr(candidate_market, 'event_ticker', '')
    or ''
  ).strip()
  candidate_status = str(getattr(candidate, 'binary_suitability_status', '') or '').strip().lower()
  if not event_ticker:
    return 'binary_proof_missing_event', {
      'ticker': ticker,
      'candidate_binary_status': candidate_status,
    }
  get_event = getattr(client, 'get_event', None)
  if not callable(get_event):
    return 'binary_proof_readback_unavailable', {
      'ticker': ticker,
      'event_ticker': event_ticker,
      'candidate_binary_status': candidate_status,
    }
  try:
    event = get_event(event_ticker)
  except Exception as exc:
    return 'binary_proof_readback_failed', {
      'ticker': ticker,
      'event_ticker': event_ticker,
      'error_family': type(exc).__name__,
      'candidate_binary_status': candidate_status,
    }
  suitability = classify_binary_suitability(candidate_market, event)
  detail = {
    'ticker': ticker,
    'event_ticker': event_ticker,
    'candidate_binary_status': candidate_status,
    'fresh_binary_status': suitability.status,
    'fresh_binary_reason': suitability.reason,
    'fresh_binary_market_count': suitability.market_count,
    'fresh_binary_series_ticker': suitability.series_ticker,
    'fresh_binary_category': suitability.category,
    'fresh_binary_sibling_sample': list(suitability.sibling_tickers[:12]),
  }
  if suitability.status != 'eligible':
    return 'binary_proof_rejected' if suitability.status == 'rejected' else 'binary_proof_unknown', detail
  if not candidate_status:
    return 'candidate_binary_proof_missing', detail
  if candidate_status != 'eligible':
    detail['candidate_binary_reason'] = str(getattr(candidate, 'binary_suitability_reason', '') or '')
    return 'candidate_binary_proof_not_eligible', detail
  return None, detail


def _resolve_saved_set_execution_candidate(
  saved_set: dict[str, Any] | None,
) -> tuple[Any | None, dict[str, Any] | None]:
  if saved_set is None:
    return None, None
  members = saved_set.get('members') if isinstance(saved_set.get('members'), list) else []
  for member in members:
    if not isinstance(member, dict):
      continue
    candidate = _saved_set_member_candidate(member)
    if candidate is not None:
      return candidate, member
  return None, None


def _resolve_saved_set_execution_candidates(
  saved_set: dict[str, Any] | None,
) -> tuple[list[CandidatePair], list[dict[str, Any]]]:
  if saved_set is None:
    return [], []
  members = saved_set.get('members') if isinstance(saved_set.get('members'), list) else []
  resolved_candidates: list[CandidatePair] = []
  resolved_members: list[dict[str, Any]] = []
  for member in members:
    if not isinstance(member, dict):
      return [], []
    candidate = _saved_set_member_candidate(member)
    if candidate is None:
      return [], []
    resolved_candidates.append(candidate)
    resolved_members.append(member)
  return resolved_candidates, resolved_members


def _project_saved_set_snapshot(
  saved_set: dict[str, Any] | None,
  *,
  guard_reason: str | None = None,
  matched_candidate_ticker: str | None = None,
) -> dict[str, Any]:
  if saved_set is None:
    return {
      'present': False,
      'eligible_for_submission': False,
      'guard_reason': guard_reason or 'no_saved_set',
      'saved_set_id': None,
      'recorded_at_utc': None,
      'operation_lane': None,
      'state_id': None,
      'actionability_status': None,
      'saved_key_count': 0,
      'member_tickers': [],
      'matched_candidate_ticker': None,
    }
  members = saved_set.get('members') if isinstance(saved_set.get('members'), list) else []
  latest_evaluation = saved_set.get('latest_evaluation') if isinstance(saved_set.get('latest_evaluation'), dict) else {}
  member_tickers = [ticker for ticker in (_saved_set_member_ticker(member) for member in members if isinstance(member, dict)) if ticker]
  normalized_guard_reason = str(guard_reason or '').strip().lower()
  actionability_status = str(latest_evaluation.get('actionability_status') or '').strip() or None
  if normalized_guard_reason:
    actionability_status = {
      'saved_set_not_eligible': 'candidate_not_currently_eligible',
      'saved_set_member_detail_unavailable': 'saved_set_member_detail_unavailable',
      'saved_set_run_missing': 'saved_set_run_missing',
      'saved_set_lane_mismatch': 'saved_set_lane_mismatch',
      'saved_set_empty': 'saved_set_empty',
      'no_saved_set': 'no_saved_set',
    }.get(normalized_guard_reason, f'blocked_{normalized_guard_reason}')
  return {
    'present': True,
    'eligible_for_submission': guard_reason is None,
    'guard_reason': guard_reason,
    'saved_set_id': str(saved_set.get('saved_set_id') or '').strip() or None,
    'recorded_at_utc': str(saved_set.get('recorded_at_utc') or '').strip() or None,
    'operation_lane': str(saved_set.get('operation_lane') or '').strip() or None,
    'state_id': str(saved_set.get('state_id') or '').strip() or None,
    'actionability_status': actionability_status,
    'saved_key_count': int(saved_set.get('saved_key_count') or 0),
    'member_tickers': member_tickers,
    'matched_candidate_ticker': str(matched_candidate_ticker or '').strip() or None,
  }


def _submit_bridge_response_id(
  *,
  blocked_reason: str | None,
  legacy_state: str | None,
  has_active_pair: bool = False,
) -> str | None:
  normalized_reason = str(blocked_reason or '').strip().lower()
  if normalized_reason in {'no_saved_set', 'saved_set_empty', 'saved_set_run_missing', 'saved_set_lane_mismatch', 'saved_set_member_detail_unavailable'}:
    return 'SUBMIT_BLOCKED_NO_SAVED_SET'
  if normalized_reason == 'saved_set_not_eligible':
    return 'SUBMIT_BLOCKED_NOT_ELIGIBLE'
  if normalized_reason == 'dynamic_notional_cap_below_one_contract':
    return 'SUBMIT_BLOCKED_INSUFFICIENT_FUNDS'
  if normalized_reason in {'stale_funds_requires_reconcile', 'live_price_unavailable'}:
    return 'SUBMIT_REJECTED_RETRYABLE'
  if normalized_reason == 'pair_plan_validation' or normalized_reason.startswith('coverability_'):
    return 'SUBMIT_REJECTED_TERMINAL'
  if normalized_reason in {'already_active_pair', 'unmatched_exposure_timeout'}:
    return 'SUBMIT_BLOCKED_ALREADY_ACTIVE'
  if normalized_reason == 'risk_gate_blocked_new_pair':
    return 'SUBMIT_BLOCKED_ALREADY_ACTIVE' if has_active_pair else 'SUBMIT_BLOCKED_CAPABILITY_GATE'
  if normalized_reason.startswith('binary_proof_') or normalized_reason.startswith('candidate_binary_proof_'):
    return 'SUBMIT_BLOCKED_CAPABILITY_GATE'
  normalized_state = str(legacy_state or '').strip().upper()
  if normalized_state in {'PARTIAL_ONE_SIDE', 'RESTING_ONE_SIDE', 'ASYMMETRIC_EXPOSURE', 'REPAIR_LIVE', 'EXPOSURE_CAPPED', 'RECONCILE_REQUIRED'}:
    return 'SUBMIT_ACCEPTED_ASYMMETRIC'
  if normalized_state in {'SUBMIT_FAILED_RETRYABLE'}:
    return 'SUBMIT_REJECTED_RETRYABLE'
  if normalized_state in {'SUBMIT_FAILED_TERMINAL'}:
    return 'SUBMIT_REJECTED_TERMINAL'
  if normalized_state in {'ERROR'}:
    return 'SUBMIT_UNKNOWN_RECONCILE_REQUIRED'
  if normalized_state:
    return 'SUBMIT_ACCEPTED_DISPATCHING'
  return None


def _zero_fill_rejected_order_error(detail: dict[str, Any] | None) -> bool:
  # GUARD_RECOVERY_CORRECTION_AND_WATCH_STALE_ERROR_PROJECTION_BMAP_2026-07-02 (W1 / D17): a raw
  # ERROR pair may project terminal no-exposure ONLY when the persisted detail proves a definitive
  # Kalshi 4xx rejection with zero fills on both legs and no remote order id anywhere in the
  # detail. Timeouts, transport errors, and 5xx responses do not qualify — the order may exist
  # remotely — and any missing or unparsable field fails closed to reconcile attention.
  payload = detail or {}
  if str(payload.get('reason') or '').strip().lower() != 'live_order_api_error':
    return False
  if str(payload.get('error_family') or '').strip() != 'KalshiHttpError':
    return False
  try:
    status_code = int(payload.get('kalshi_status_code'))
  except (TypeError, ValueError):
    return False
  if not 400 <= status_code <= 499:
    return False
  try:
    if Decimal(str(payload.get('yes_filled_contracts'))) != 0:
      return False
    if Decimal(str(payload.get('no_filled_contracts'))) != 0:
      return False
  except (InvalidOperation, TypeError, ValueError):
    return False
  for key, value in payload.items():
    if 'order_id' in str(key).lower() and str(value or '').strip():
      return False
  return True


def _project_public_state_id(
  legacy_state: str | None,
  *,
  detail: dict[str, Any] | None = None,
  blocked_reason: str | None = None,
) -> str:
  normalized_detail = detail or {}
  projected = str(normalized_detail.get('public_state_id') or '').strip().upper()
  if projected:
    return projected
  normalized_reason = str(blocked_reason or '').strip().lower()
  if normalized_reason == 'live_price_unavailable':
    return 'SUBMIT_FAILED_RETRYABLE'
  if normalized_reason in {
    'no_saved_set',
    'saved_set_empty',
    'saved_set_not_eligible',
    'saved_set_run_missing',
    'saved_set_lane_mismatch',
    'saved_set_member_detail_unavailable',
    'dynamic_notional_cap_below_one_contract',
    'stale_funds_requires_reconcile',
    'risk_gate_blocked_new_pair',
    'binary_proof_rejected',
    'binary_proof_unknown',
    'binary_proof_missing_event',
    'binary_proof_readback_unavailable',
    'binary_proof_readback_failed',
    'candidate_binary_proof_missing',
    'candidate_binary_proof_not_eligible',
    'pair_plan_validation',
  }:
    return 'UPSTREAM_REVIEW_HOLD'
  if normalized_reason.startswith('coverability_'):
    return 'UPSTREAM_REVIEW_HOLD'
  if normalized_reason in {'already_active_pair', 'unmatched_exposure_timeout'}:
    return 'RECONCILE_REQUIRED'
  normalized_state = str(legacy_state or '').strip().upper()
  # W2: qualified zero-fill rejected-order errors rest as auditable terminal history instead of a
  # phantom reconcile hold; every unqualified ERROR keeps the reconcile-attention projection.
  if normalized_state == 'ERROR' and _zero_fill_rejected_order_error(detail):
    return 'ERROR_NO_EXPOSURE'
  return {
    'PLANNED': 'UPSTREAM_REVIEW_HOLD',
    'SUBMITTING': 'SUBMITTING',
    'RESTING_BOTH': 'RESTING_BOTH',
    'PARTIAL_ONE_SIDE': 'PARTIAL_ONE_SIDE',
    'ASYMMETRIC_EXPOSURE': 'ASYMMETRIC_EXPOSURE',
    'REPAIR_LIVE': 'REPAIR_LIVE',
    'EXPOSURE_CAPPED': 'EXPOSURE_CAPPED',
    'SETTLED': 'SETTLED',
    'SETTLED_EXPOSURE': 'SETTLED_EXPOSURE',
    'PARTIAL_BOTH': 'PARTIAL_BOTH',
    'LOCKED': 'LOCKED',
    'FILLED': 'FILLED',
    'CANCELING': 'CANCEL_PENDING',
    'CANCELED': 'CANCELED',
    'ERROR': 'RECONCILE_REQUIRED',
  }.get(normalized_state, 'UPSTREAM_REVIEW_HOLD')


def _submit_bridge_detail_fields(
  *,
  legacy_state: str,
  saved_set_snapshot: dict[str, Any] | None,
  submit_response_id: str | None,
) -> dict[str, Any]:
  detail: dict[str, Any] = {
    'execution_intent_source': 'saved_set',
    'public_state_id': _project_public_state_id(legacy_state),
  }
  if submit_response_id:
    detail['submit_response_id'] = submit_response_id
  if isinstance(saved_set_snapshot, dict):
    if saved_set_snapshot.get('saved_set_id'):
      detail['saved_set_id'] = saved_set_snapshot['saved_set_id']
    if saved_set_snapshot.get('recorded_at_utc'):
      detail['saved_set_recorded_at_utc'] = saved_set_snapshot['recorded_at_utc']
    if saved_set_snapshot.get('actionability_status'):
      detail['saved_set_actionability_status'] = saved_set_snapshot['actionability_status']
  return detail


def _persist_submit_bridge_phase_failed(
  connection: sqlite3.Connection,
  *,
  recorded_at_utc: str,
  operation_lane: str,
  lane_session_id: str,
  phase: str,
  exc: Exception,
  saved_set_id: str | None = None,
  ticker: str | None = None,
) -> None:
  setattr(exc, 'polyventure_submit_bridge_phase', phase)
  persist_runtime_event(
    connection,
    level='ERROR',
    event_type='submit_bridge_phase_failed',
    recorded_at_utc=recorded_at_utc,
    operation_lane=operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'failure_phase': phase,
      'error_family': type(exc).__name__,
      'error_message': str(exc),
      **kalshi_error_safe_detail(exc),
      **({'saved_set_id': saved_set_id} if saved_set_id else {}),
      **({'ticker': ticker} if ticker else {}),
    },
  )


def _is_candidate_local_pair_plan_rejection(exc: Exception) -> bool:
  return str(exc).strip() in {
    'Candidate gross edge is below the configured minimum.',
    'Candidate net edge is below the configured profit floor.',
  }


def _persist_submit_bridge_candidate_rejected_before_order(
  connection: sqlite3.Connection,
  *,
  recorded_at_utc: str,
  operation_lane: str,
  lane_session_id: str,
  phase: str,
  exc: Exception,
  saved_set_id: str | None = None,
  ticker: str | None = None,
  detail: dict[str, Any] | None = None,
) -> None:
  extra_detail = dict(detail or {})
  persist_runtime_event(
    connection,
    level='WARN',
    event_type='submit_bridge_blocked',
    recorded_at_utc=recorded_at_utc,
    operation_lane=operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'blocked_reason': phase,
      'failure_phase': phase,
      'error_family': type(exc).__name__,
      'error_message': str(exc),
      'money_path_crossed': False,
      'pair_plan_created': False,
      'orders_created': False,
      **kalshi_error_safe_detail(exc),
      **extra_detail,
      **({'saved_set_id': saved_set_id} if saved_set_id else {}),
      **({'ticker': ticker} if ticker else {}),
    },
  )


def _candidate_evidence_uid(candidate: Any) -> str | None:
  return str(getattr(candidate, 'candidate_uid', '') or getattr(candidate, 'candidate_key', '') or getattr(candidate, 'ticker', '') or '').strip() or None


def _decimal_evidence(value: Any) -> str | None:
  if value is None:
    return None
  try:
    return str(Decimal(str(value)))
  except (InvalidOperation, TypeError, ValueError):
    return str(value)


def _persist_submit_bridge_final_coverability_checked(
  connection: sqlite3.Connection,
  *,
  recorded_at_utc: str,
  operation_lane: str,
  lane_session_id: str,
  candidate: Any,
  saved_set_id: str | None,
  ok: bool,
  guard_reason: str,
  message: str = '',
  final_checked_at_utc: str | None = None,
  best_yes_bid: Any = None,
  best_no_bid: Any = None,
  repriced_yes_price: Any = None,
  repriced_no_price: Any = None,
  max_divergence: Any = None,
  settings: Settings | None = None,
  yes_flow_window_fp: Any = None,
  no_flow_window_fp: Any = None,
  flow_window_sec: Any = None,
  flow_participation_k: Any = None,
  intended_contract_count_for_floor: Any = None,
  required_flow_window_fp: Any = None,
  flow_threshold_pass: bool | None = None,
  yes_depth_within_band: Any = None,
  no_depth_within_band: Any = None,
) -> None:
  divergence: str | None = None
  profitability_available = False
  profitability_basis = 'unavailable_before_final_reprice'
  edge_gross_per_contract: str | None = None
  fee_reserve_per_contract: str | None = None
  edge_net_per_contract: str | None = None
  min_edge_dollars: str | None = None
  min_profit_dollars: str | None = None
  gross_edge_margin_to_min_edge: str | None = None
  net_profit_margin_to_min_profit: str | None = None
  edge_threshold_pass: bool | None = None
  profit_threshold_pass: bool | None = None
  threshold_outcome = 'unavailable_before_final_reprice'
  if repriced_yes_price is not None and repriced_no_price is not None:
    try:
      yes_price = Decimal(str(repriced_yes_price))
      no_price = Decimal(str(repriced_no_price))
      divergence = str(abs(yes_price - no_price))
      if settings is not None:
        edge_gross = Decimal('1') - yes_price - no_price
        fee_reserve = Decimal(str(settings.fee_reserve_dollars))
        edge_net = edge_gross - fee_reserve
        min_edge = Decimal(str(settings.min_edge_dollars))
        min_profit = Decimal(str(settings.min_profit_dollars))
        edge_threshold_pass = edge_gross >= min_edge
        profit_threshold_pass = edge_net >= min_profit
        profitability_available = True
        profitability_basis = 'post_reprice_final_prices'
        edge_gross_per_contract = str(edge_gross)
        fee_reserve_per_contract = str(fee_reserve)
        edge_net_per_contract = str(edge_net)
        min_edge_dollars = str(min_edge)
        min_profit_dollars = str(min_profit)
        gross_edge_margin_to_min_edge = str(edge_gross - min_edge)
        net_profit_margin_to_min_profit = str(edge_net - min_profit)
        threshold_outcome = 'pass' if edge_threshold_pass and profit_threshold_pass else 'below_threshold'
    except (InvalidOperation, TypeError, ValueError):
      divergence = None
  final_check_elapsed_sec: str | None = None
  if recorded_at_utc and final_checked_at_utc:
    try:
      event_recorded_at = datetime.fromisoformat(str(recorded_at_utc).replace('Z', '+00:00'))
      final_checked_at = datetime.fromisoformat(str(final_checked_at_utc).replace('Z', '+00:00'))
      if event_recorded_at.tzinfo is None:
        event_recorded_at = event_recorded_at.replace(tzinfo=UTC)
      if final_checked_at.tzinfo is None:
        final_checked_at = final_checked_at.replace(tzinfo=UTC)
      final_check_elapsed_sec = str((final_checked_at.astimezone(UTC) - event_recorded_at.astimezone(UTC)).total_seconds())
    except (TypeError, ValueError):
      final_check_elapsed_sec = None
  persist_runtime_event(
    connection,
    level='INFO' if ok else 'WARN',
    event_type='submit_bridge_final_coverability_checked',
    recorded_at_utc=recorded_at_utc,
    operation_lane=operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'final_coverability_validation': True,
      'source': 'final_set_coverability_gate',
      'sizing_phase': 'pre_sizing',
      'ok': ok,
      'reason': '' if ok else 'final_coverability_blocked',
      'guard_reason': '' if ok else guard_reason,
      'message': '' if ok else str(message or guard_reason),
      'ticker': str(getattr(candidate, 'ticker', '') or '').strip(),
      'candidate_uid': _candidate_evidence_uid(candidate),
      'saved_set_id': saved_set_id,
      'submit_mode': 'unresolved',
      'final_checked_at_utc': final_checked_at_utc,
      'event_recorded_at_basis': 'submit_bridge_recorded_at',
      'submit_bridge_recorded_at_utc': recorded_at_utc,
      'final_checked_at_basis': 'actual_final_orderbook_check_time' if final_checked_at_utc else 'unavailable_before_final_orderbook',
      'final_check_elapsed_sec': final_check_elapsed_sec,
      'best_yes_bid': _decimal_evidence(best_yes_bid),
      'best_no_bid': _decimal_evidence(best_no_bid),
      'repriced_yes_price': _decimal_evidence(repriced_yes_price),
      'repriced_no_price': _decimal_evidence(repriced_no_price),
      'divergence': divergence,
      'max_divergence': _decimal_evidence(max_divergence),
      'profitability_evidence_available': profitability_available,
      'profitability_basis': profitability_basis,
      'edge_gross_per_contract': edge_gross_per_contract,
      'fee_reserve_per_contract': fee_reserve_per_contract,
      'edge_net_per_contract': edge_net_per_contract,
      'min_edge_dollars': min_edge_dollars,
      'min_profit_dollars': min_profit_dollars,
      'gross_edge_margin_to_min_edge': gross_edge_margin_to_min_edge,
      'net_profit_margin_to_min_profit': net_profit_margin_to_min_profit,
      'edge_threshold_pass': edge_threshold_pass,
      'profit_threshold_pass': profit_threshold_pass,
      'threshold_outcome': threshold_outcome,
      'yes_flow_window_fp': _decimal_evidence(yes_flow_window_fp),
      'no_flow_window_fp': _decimal_evidence(no_flow_window_fp),
      'flow_window_sec': _decimal_evidence(flow_window_sec),
      'flow_participation_k': _decimal_evidence(flow_participation_k),
      'intended_contract_count_for_floor': _decimal_evidence(intended_contract_count_for_floor),
      'required_flow_window_fp': _decimal_evidence(required_flow_window_fp),
      'flow_threshold_pass': flow_threshold_pass,
      'yes_depth_within_band': _decimal_evidence(yes_depth_within_band),
      'no_depth_within_band': _decimal_evidence(no_depth_within_band),
    },
  )


def _persist_submit_bridge_final_sizing_resolved(
  connection: sqlite3.Connection,
  *,
  recorded_at_utc: str,
  operation_lane: str,
  lane_session_id: str,
  candidates: list[Any],
  sizing_summary: dict[str, Any],
  blocked_count: int,
  block_reasons: list[str],
  saved_set_id: str | None,
) -> None:
  persist_runtime_event(
    connection,
    level='INFO',
    event_type='submit_bridge_final_sizing_resolved',
    recorded_at_utc=recorded_at_utc,
    operation_lane=operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'source': 'final_set_sizing_after_coverability',
      'sizing_phase': 'post_final_coverability_pre_pair_plan',
      'qualifying_candidate_count': len(candidates),
      'final_submit_tickers': [str(getattr(candidate, 'ticker', '') or '').strip() for candidate in candidates],
      'sizing_summary': dict(sizing_summary),
      'blocked_count': blocked_count,
      'block_reasons': list(block_reasons),
      'saved_set_id': saved_set_id,
    },
  )


def _project_mobility_posture(
  *,
  public_state_id: str | None,
  recorded_at: datetime | None,
  as_of: datetime | None = None,
) -> dict[str, Any]:
  normalized_state = str(public_state_id or '').strip().upper()
  effective_as_of = (as_of or datetime.now(UTC)).astimezone(UTC)
  effective_recorded_at = recorded_at.astimezone(UTC) if recorded_at is not None else effective_as_of
  elapsed_ms = max(int((effective_as_of - effective_recorded_at).total_seconds() * 1000), 0)
  if normalized_state == 'RECONCILE_REQUIRED':
    return {'mobility_overlay_state': 'AUTO_CANCEL_RECONCILE_REQUIRED', 'mobility_elapsed_ms': elapsed_ms}
  if normalized_state == 'CANCEL_PENDING':
    return {'mobility_overlay_state': 'AUTO_CANCEL_DISPATCHING', 'mobility_elapsed_ms': elapsed_ms}
  if normalized_state == 'CANCELED':
    return {'mobility_overlay_state': 'AUTO_CANCEL_COMPLETE', 'mobility_elapsed_ms': elapsed_ms}
  if normalized_state not in {'RESTING_ONE_SIDE', 'PARTIAL_ONE_SIDE'}:
    return {'mobility_overlay_state': 'AUTO_CANCEL_IDLE', 'mobility_elapsed_ms': elapsed_ms}
  elapsed_sec = elapsed_ms / 1000
  if elapsed_sec >= AUTO_CANCEL_DISPATCH_SEC:
    state = 'AUTO_CANCEL_DISPATCHING'
  elif elapsed_sec >= AUTO_CANCEL_ARMED_SEC:
    state = 'AUTO_CANCEL_ARMED'
  elif elapsed_sec >= AUTO_CANCEL_RECOMMENDED_SEC:
    state = 'AUTO_CANCEL_RECOMMENDED'
  else:
    state = 'AUTO_CANCEL_IDLE'
  return {'mobility_overlay_state': state, 'mobility_elapsed_ms': elapsed_ms}


def _project_failure_posture(
  *,
  public_state_id: str | None,
  blocked_reason: str | None = None,
) -> dict[str, str]:
  normalized_reason = str(blocked_reason or '').strip().lower()
  normalized_state = str(public_state_id or '').strip().upper()
  if normalized_reason in {'unmatched_exposure_timeout', 'already_active_pair'}:
    return {'failure_class': 'HARD_STOP', 'failure_scope': 'pair_local'}
  if normalized_reason in {'stale_funds_requires_reconcile', 'live_price_unavailable'}:
    return {'failure_class': 'SILENT_CONTINUE', 'failure_scope': 'interaction_local'}
  if normalized_reason in {
    'dynamic_notional_cap_below_one_contract',
    'no_saved_set',
    'saved_set_empty',
    'saved_set_not_eligible',
    'saved_set_run_missing',
    'saved_set_lane_mismatch',
    'saved_set_member_detail_unavailable',
    'pair_plan_validation',
  }:
    return {'failure_class': 'SILENT_CONTINUE', 'failure_scope': 'interaction_local'}
  if normalized_reason.startswith('coverability_'):
    return {'failure_class': 'SILENT_CONTINUE', 'failure_scope': 'interaction_local'}
  if normalized_reason.startswith('binary_proof_') or normalized_reason.startswith('candidate_binary_proof_'):
    return {'failure_class': 'SILENT_CONTINUE', 'failure_scope': 'interaction_local'}
  # One-sided (unmatched) live exposure is a serious money condition -> hard-stop for
  # operator attention. Matched/transitional states stay silent-continue.
  if normalized_state in {'RESTING_ONE_SIDE', 'PARTIAL_ONE_SIDE', 'ASYMMETRIC_EXPOSURE', 'REPAIR_LIVE', 'EXPOSURE_CAPPED'}:
    return {'failure_class': 'HARD_STOP', 'failure_scope': 'pair_local'}
  if normalized_state in {'PARTIAL_BOTH', 'RECONCILE_REQUIRED', 'CANCEL_PENDING'}:
    return {'failure_class': 'SILENT_CONTINUE', 'failure_scope': 'pair_local'}
  if normalized_state == 'ERROR_NO_EXPOSURE':
    # W2: terminal no-exposure failure — silent continue, pair-local, no operator attention.
    return {'failure_class': 'SILENT_CONTINUE', 'failure_scope': 'pair_local'}
  if normalized_state == 'SUBMIT_FAILED_RETRYABLE':
    return {'failure_class': 'SILENT_CONTINUE', 'failure_scope': 'interaction_local'}
  if normalized_state == 'SUBMIT_FAILED_TERMINAL':
    return {'failure_class': 'HARD_STOP', 'failure_scope': 'interaction_local'}
  return {'failure_class': 'SILENT_CONTINUE', 'failure_scope': 'interaction_local'}


def _project_action_contract(
  *,
  public_state_id: str | None,
  blocked_reason: str | None = None,
  mobility_overlay_state: str | None = None,
) -> dict[str, Any]:
  normalized_reason = str(blocked_reason or '').strip().lower()
  normalized_state = str(public_state_id or '').strip().upper()
  overlay_state = str(mobility_overlay_state or '').strip().upper()
  allowed_actions: list[str]
  if normalized_reason in {
    'no_saved_set',
    'saved_set_empty',
    'saved_set_not_eligible',
    'saved_set_run_missing',
    'saved_set_lane_mismatch',
    'saved_set_member_detail_unavailable',
  }:
    allowed_actions = ['RESAVE_AND_RETRY']
  elif normalized_reason == 'dynamic_notional_cap_below_one_contract':
    allowed_actions = ['WAIT']
  elif normalized_reason == 'stale_funds_requires_reconcile':
    allowed_actions = ['RECONCILE', 'WAIT']
  elif normalized_reason == 'live_price_unavailable':
    allowed_actions = ['RETRY_SUBMIT', 'WAIT']
  elif normalized_reason == 'pair_plan_validation' or normalized_reason.startswith('coverability_'):
    allowed_actions = ['WAIT']
  elif normalized_reason.startswith('binary_proof_') or normalized_reason.startswith('candidate_binary_proof_'):
    allowed_actions = ['WAIT']
  elif normalized_reason in {'already_active_pair', 'unmatched_exposure_timeout'}:
    allowed_actions = ['RECONCILE', 'CANCEL_PAIR', 'ESCALATE_OPERATOR']
  elif normalized_state in {'SUBMITTING', 'RESTING_BOTH'}:
    allowed_actions = ['WAIT', 'RECONCILE', 'CANCEL_PAIR']
  elif normalized_state in {'RESTING_ONE_SIDE', 'PARTIAL_ONE_SIDE', 'PARTIAL_BOTH', 'ASYMMETRIC_EXPOSURE', 'REPAIR_LIVE', 'EXPOSURE_CAPPED'}:
    allowed_actions = ['RECONCILE', 'CANCEL_PAIR', 'ESCALATE_OPERATOR']
    if overlay_state in {'AUTO_CANCEL_RECOMMENDED', 'AUTO_CANCEL_ARMED', 'AUTO_CANCEL_DISPATCHING'}:
      allowed_actions.append('AUTO_CANCEL')
  elif normalized_state == 'LOCKED':
    allowed_actions = ['WAIT']
  elif normalized_state == 'CANCEL_PENDING':
    allowed_actions = ['WAIT', 'RECONCILE']
  elif normalized_state == 'CANCELED':
    allowed_actions = ['WAIT']
  elif normalized_state == 'RECONCILE_REQUIRED':
    allowed_actions = ['RECONCILE', 'CANCEL_PAIR', 'ESCALATE_OPERATOR']
  elif normalized_state == 'SUBMIT_FAILED_RETRYABLE':
    allowed_actions = ['RETRY_SUBMIT', 'WAIT']
  elif normalized_state == 'SUBMIT_FAILED_TERMINAL':
    allowed_actions = ['ESCALATE_OPERATOR']
  else:
    allowed_actions = ['WAIT']
  allowed = sorted({action for action in allowed_actions if action in ACTION_VOCABULARY})
  blocked = [action for action in ACTION_VOCABULARY if action not in allowed]
  return {
    'allowed_actions': allowed,
    'blocked_actions': blocked,
    'retry_allowed': 'RETRY_SUBMIT' in allowed,
  }


def _project_funds_posture(
  *,
  balance: Decimal | None = None,
  as_of: datetime | None = None,
  latest_heartbeat_payload: dict[str, Any] | None = None,
  now: datetime | None = None,
) -> dict[str, Any]:
  effective_now = (now or datetime.now(UTC)).astimezone(UTC)
  snapshot = str(balance) if balance is not None else None
  funds_as_of = _iso_utc(as_of) if as_of is not None else None
  funds_refresh_status = 'fresh' if balance is not None and as_of is not None else 'unavailable'
  funds_refresh_reason: str | None = None
  if latest_heartbeat_payload is not None:
    detail = latest_heartbeat_payload.get('detail') if isinstance(latest_heartbeat_payload.get('detail'), dict) else {}
    snapshot = str(detail.get('available_funds_snapshot') or snapshot or '').strip() or None
    funds_as_of = str(detail.get('available_funds_as_of') or funds_as_of or '').strip() or None
    funds_refresh_status = str(detail.get('funds_refresh_status') or funds_refresh_status).strip() or 'unavailable'
    funds_refresh_reason = str(detail.get('funds_refresh_reason') or '').strip() or None
  stale = False
  if funds_as_of:
    try:
      funds_dt = _parse_recorded_at(funds_as_of)
      stale = (effective_now - funds_dt.astimezone(UTC)).total_seconds() * 1000 > BALANCE_STALENESS_GRACE_MS
    except Exception:
      stale = True
      funds_refresh_status = 'stale'
      funds_refresh_reason = funds_refresh_reason or 'funds_timestamp_invalid'
  if stale:
    funds_refresh_status = 'stale'
    funds_refresh_reason = funds_refresh_reason or 'balance_staleness_grace_exceeded'
  return {
    'available_funds_snapshot': snapshot,
    'available_funds_as_of': funds_as_of,
    'funds_refresh_status': funds_refresh_status,
    'funds_refresh_reason': funds_refresh_reason,
    'balance_staleness_grace_ms': BALANCE_STALENESS_GRACE_MS,
    'stale': stale,
    'stale_blocks_submit': stale,
  }


def _refresh_reporting_funds_posture(
  settings: Settings,
  *,
  latest_heartbeat_payload: dict[str, Any] | None = None,
  latest_funds_heartbeat_payload: dict[str, Any] | None = None,
  client_factory: ClientFactory | None = None,
  suppress_live_refresh: bool = False,
) -> dict[str, Any]:
  fallback_posture = _project_funds_posture(
    latest_heartbeat_payload=latest_funds_heartbeat_payload or latest_heartbeat_payload
  )
  # S2 diagnostic: funds_refresh_ms isolates the live funds API call (the bounded
  # interactive call) so the rebuild's API cost can be separated from DB cost. None
  # outside the live lane, where no API call is made.
  fallback_posture['funds_refresh_ms'] = None
  fallback_posture['funds_source'] = 'heartbeat_snapshot'
  # The deck rebuild must not make its own live balance call when it does not need
  # to: that call competes with the scan loop AND the funds-on-heartbeat (FH) beat
  # for the shared per-account rate limit and the GIL, stalling the deck for tens of
  # seconds (prereq #1) and starving the FH beat so the strict gate goes unavailable
  # (prereq #2). The FH beat publishes authoritative funds each ~2s, so when that
  # snapshot is FRESH (within the staleness grace) we serve it and skip the call.
  # The original available_funds_as_of is preserved, so staleness (and the
  # stale-blocks-submit gate) is computed truthfully — this is the same lane's own
  # fresh value, not a silent substitution. suppress_live_refresh remains an explicit
  # override (e.g. while a scan is active); F1 generalizes it to "skip whenever the
  # heartbeat is fresh", so the synchronous refresh fires only when the heartbeat is
  # stale/absent (e.g. the first beat after a mode change, or the WS is down) — at
  # most ~once per staleness-grace window instead of once per rebuild.
  if suppress_live_refresh:
    return fallback_posture
  if str(settings.operation_lane or '').strip().lower() != 'live':
    return fallback_posture
  _heartbeat_fresh = (
    str(fallback_posture.get('funds_refresh_status') or '').strip() == 'fresh'
    and not bool(fallback_posture.get('stale'))
    and fallback_posture.get('available_funds_snapshot') is not None
  )
  if _heartbeat_fresh:
    fallback_posture['funds_source'] = 'heartbeat_fresh'
    return fallback_posture

  _funds_t0 = time.monotonic()
  try:
    private_key_path = resolve_private_key_path(settings)
    private_key = load_private_key(private_key_path)
    client = (
      client_factory(settings, private_key)
      if client_factory is not None
      else KalshiHttpClient(settings, private_key, request_timeout=3, max_attempts=1)
    )
    recorded_at = datetime.now(UTC)
    balance = client.get_balance()
    posture = _project_funds_posture(balance=balance, as_of=recorded_at, now=recorded_at)
    posture['funds_refresh_ms'] = round((time.monotonic() - _funds_t0) * 1000.0, 1)
    posture['funds_source'] = 'live_refresh'
    return posture
  except KalshiHttpError as exc:
    fallback_posture['funds_refresh_ms'] = round((time.monotonic() - _funds_t0) * 1000.0, 1)
    if fallback_posture['available_funds_snapshot'] is None:
      fallback_posture['funds_refresh_status'] = 'unavailable'
      fallback_posture['funds_refresh_reason'] = str(getattr(exc, 'reason_code', '') or 'account_scope_refresh_failed')
    return fallback_posture
  except Exception:
    fallback_posture['funds_refresh_ms'] = round((time.monotonic() - _funds_t0) * 1000.0, 1)
    if fallback_posture['available_funds_snapshot'] is None:
      fallback_posture['funds_refresh_status'] = 'unavailable'
      fallback_posture['funds_refresh_reason'] = 'account_scope_refresh_failed'
    return fallback_posture


def _latest_heartbeat_payload(connection: Any, *, operation_lane: str) -> dict[str, Any] | None:
  latest_heartbeat = connection.execute(
    '''
    SELECT component, status, operation_lane, lane_session_id, recorded_at_utc, detail_json
    FROM service_heartbeats
    WHERE operation_lane = ?
    ORDER BY id DESC
    LIMIT 1
    ''',
    (operation_lane,),
  ).fetchone()
  if latest_heartbeat is None:
    return None
  return {
    'component': latest_heartbeat['component'],
    'status': latest_heartbeat['status'],
    'operation_lane': latest_heartbeat['operation_lane'],
    'lane_session_id': latest_heartbeat['lane_session_id'],
    'recorded_at_utc': latest_heartbeat['recorded_at_utc'],
    'detail': json.loads(latest_heartbeat['detail_json']) if latest_heartbeat['detail_json'] else {},
  }


def _latest_funds_heartbeat_payload(connection: Any, *, operation_lane: str) -> dict[str, Any] | None:
  # FB-1: fetches the most-recent heartbeat that carried a fresh funds snapshot,
  # so the banner fallback reads real balance data instead of a funds-less
  # websocket-session beat. Staleness is still enforced by _project_funds_posture.
  latest_heartbeat = connection.execute(
    '''
    SELECT component, status, operation_lane, lane_session_id, recorded_at_utc, detail_json
    FROM service_heartbeats
    WHERE operation_lane = ?
    AND detail_json LIKE '%"funds_refresh_status": "fresh"%'
    ORDER BY id DESC
    LIMIT 1
    ''',
    (operation_lane,),
  ).fetchone()
  if latest_heartbeat is None:
    return None
  return {
    'component': latest_heartbeat['component'],
    'status': latest_heartbeat['status'],
    'operation_lane': latest_heartbeat['operation_lane'],
    'lane_session_id': latest_heartbeat['lane_session_id'],
    'recorded_at_utc': latest_heartbeat['recorded_at_utc'],
    'detail': json.loads(latest_heartbeat['detail_json']) if latest_heartbeat['detail_json'] else {},
  }


def _heartbeat_balance_at(connection: Any, *, operation_lane: str, at_utc: datetime) -> Decimal:
  # Account-cash component of the UI gross aggregate, as-of a point in time. This
  # is the convenience/visualization path only -- the authoritative Kalshi balance
  # for backend pricing is resolved separately and must never be routed through
  # this helper. Returns the most recent fresh funds snapshot at or before `at_utc`;
  # Decimal('0') when none exists (fail closed to no-cash, never a guess).
  #
  # Three-tier fallthrough (DATABASE_ROTATION_AND_TRIM BMAP §3.3):
  #   Step 1 — raw table (age < 24h after rotation)
  #   Step 2 — hourly consolidated tier (age 24h–7d)
  #   Step 3 — daily consolidated tier (age > 7d)
  bound = at_utc.astimezone(UTC).isoformat()
  # Raw tier window: after rotation all rows older than 24h are consolidated into
  # service_heartbeats_consolidated. Scanning the raw table for old bucket endpoints
  # (e.g. the 'all' window spanning months) forces O(N) LIKE scans on accumulated
  # session rows just to prove absence — skip Step 1 for timestamps outside the window.
  _raw_cutoff = (datetime.now(UTC) - timedelta(hours=24)).isoformat()

  # Step 1: raw heartbeats — only for timestamps within the 24h raw retention window
  if bound >= _raw_cutoff:
    row = connection.execute(
      '''
      SELECT detail_json
      FROM service_heartbeats
      WHERE operation_lane = ?
      AND recorded_at_utc <= ?
      AND detail_json LIKE '%"funds_refresh_status": "fresh"%'
      ORDER BY id DESC
      LIMIT 1
      ''',
      (operation_lane, bound),
    ).fetchone()
    if row is not None and row['detail_json']:
      snapshot = (json.loads(row['detail_json']) or {}).get('available_funds_snapshot')
      if snapshot is not None:
        try:
          return Decimal(str(snapshot))
        except (InvalidOperation, ValueError, TypeError):
          pass

  # Step 2: hourly consolidated tier
  row = connection.execute(
    '''
    SELECT latest_balance_snapshot
    FROM service_heartbeats_consolidated
    WHERE tier = 'hourly'
    AND operation_lane = ?
    AND bucket_start_utc <= ?
    ORDER BY bucket_start_utc DESC
    LIMIT 1
    ''',
    (operation_lane, bound),
  ).fetchone()
  if row is not None and row['latest_balance_snapshot']:
    try:
      return Decimal(str(row['latest_balance_snapshot']))
    except (InvalidOperation, ValueError, TypeError):
      pass

  # Step 3: daily consolidated tier
  row = connection.execute(
    '''
    SELECT latest_balance_snapshot
    FROM service_heartbeats_consolidated
    WHERE tier = 'daily'
    AND operation_lane = ?
    AND bucket_start_utc <= ?
    ORDER BY bucket_start_utc DESC
    LIMIT 1
    ''',
    (operation_lane, bound),
  ).fetchone()
  if row is not None and row['latest_balance_snapshot']:
    try:
      return Decimal(str(row['latest_balance_snapshot']))
    except (InvalidOperation, ValueError, TypeError):
      pass

  return Decimal('0')


def _pair_total_per_contract(candidate: Any, settings: Settings) -> Decimal:
  return candidate.target_yes_bid + candidate.target_no_bid + Decimal(str(settings.fee_reserve_dollars))


def _cash_limited_contracts(candidate: Any, balance: Decimal, settings: Settings) -> Decimal:
  total_per_contract = _pair_total_per_contract(candidate, settings)
  if total_per_contract <= 0:
    raise ValueError('Per-contract spend must be positive.')
  return (balance / total_per_contract).to_integral_value(rounding=ROUND_FLOOR)


def _sizing_binding_limiter(
  candidate: Any,
  balance: Decimal,
  settings: Settings,
  *,
  dynamic_max_contracts: Decimal,
) -> str:
  limits = {
    'cash_limit': _cash_limited_contracts(candidate, balance, settings),
    'configured_contract_cap': Decimal(str(settings.max_pair_contracts)),
    'candidate_size_cap': candidate.max_size_contracts,
    'dynamic_notional_cap': dynamic_max_contracts,
  }
  minimum = min(limits.values())
  for label in (
    'dynamic_notional_cap',
    'configured_contract_cap',
    'candidate_size_cap',
    'cash_limit',
  ):
    if limits[label] == minimum:
      return label
  return 'cash_limit'


def _build_dynamic_sizing_summary(
  candidates: list[Any],
  *,
  balance: Decimal,
  settings: Settings,
  previous_density: Decimal | None = None,
) -> dict[str, Any]:
  summary: dict[str, Any] = {
    'account_equity_dollars_used_for_sizing': str(balance),
    'qualifying_candidate_count': len(candidates),
    'instantaneous_density': '0',
    'effective_density': '0',
    'dynamic_pair_notional_pct': None,
    'dynamic_pair_notional_cap_dollars': None,
    'dynamic_max_contracts': None,
    'binding_limiter': None,
  }
  if not candidates:
    return summary

  candidate = candidates[0]
  instantaneous_density = compute_instantaneous_qualifying_density(candidates, settings)
  effective_density = compute_effective_qualifying_density(
    instantaneous_density,
    settings,
    previous_density=previous_density,
  )
  dynamic_pair_notional_pct = compute_dynamic_pair_notional_pct(effective_density, settings)
  dynamic_pair_notional_cap_dollars = compute_dynamic_pair_notional_cap_dollars(
    balance,
    effective_density,
    settings,
  )
  dynamic_max_contracts = compute_dynamic_max_contracts(
    candidate,
    balance,
    effective_density,
    settings,
  )
  summary.update(
    {
      'instantaneous_density': str(instantaneous_density),
      'effective_density': str(effective_density),
      'dynamic_pair_notional_pct': str(dynamic_pair_notional_pct),
      'dynamic_pair_notional_cap_dollars': str(dynamic_pair_notional_cap_dollars),
      'dynamic_max_contracts': str(dynamic_max_contracts),
      'binding_limiter': _sizing_binding_limiter(
        candidate,
        balance,
        settings,
        dynamic_max_contracts=dynamic_max_contracts,
      ),
    }
  )
  return summary


def _prepare_bridge_submit_survivors(
  *,
  client: Any,
  connection: sqlite3.Connection,
  candidates: list[CandidatePair],
  balance: Decimal,
  settings: Settings,
  recorded_at: datetime,
  lane_session_id: str,
  saved_set_snapshot: dict[str, Any],
  candidate_market_by_ticker: dict[str, Any],
  mode: str,
  confirm_targeted: bool,
) -> dict[str, Any]:
  prepared_candidates: list[CandidatePair] = []
  final_coverability_context_by_ticker: dict[str, dict[str, Any]] = {}
  flow_evidence_by_ticker: dict[str, dict[str, Any]] = {}
  recent_trades_by_ticker: dict[str, dict[str, Any]] = {}
  blocked_count = 0
  block_reasons: list[str] = []
  blocked_reason: str | None = None
  saved_set_id = saved_set_snapshot.get('saved_set_id')
  max_divergence = getattr(settings, 'max_divergence', None)

  # Serial-prep work bound (selection-alignment BMAP 2026-07-02, C5/D-B): only the
  # top-K ranked members enter the per-candidate readbacks so the final checks stay
  # fresh. Efficiency bound only — never a risk gate; 0/invalid disables the cap.
  try:
    submit_prep_top_k = int(getattr(settings, 'submit_prep_top_k', 0) or 0)
  except (TypeError, ValueError):
    submit_prep_top_k = 0
  if submit_prep_top_k > 0 and len(candidates) > submit_prep_top_k:
    ordered_candidates = sorted(candidates, key=lambda item: item.ranking_key, reverse=True)
    deferred_candidates = ordered_candidates[submit_prep_top_k:]
    candidates = ordered_candidates[:submit_prep_top_k]
    for deferred_candidate in deferred_candidates:
      blocked_reason = 'survivor_prep_top_k_deferred'
      blocked_count += 1
      block_reasons.append(blocked_reason)
      persist_runtime_event(
        connection,
        level='INFO',
        event_type='submit_bridge_blocked',
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        detail={
          'blocked_reason': blocked_reason,
          'ticker': deferred_candidate.ticker,
          'submit_prep_top_k': submit_prep_top_k,
          'saved_set_id': saved_set_id,
        },
      )

  for candidate in candidates:
    # Cheapest-rejection-first (C4): one fresh orderbook read answers the static
    # coverability question that historically blocks most candidates, before the
    # expensive market/event-family readbacks run. Every gate below still runs
    # for every candidate that can reach an order — only the order changed.
    try:
      orderbook = client.get_orderbook(candidate.ticker)
      yes_price_live = orderbook.best_yes_bid
      no_price_live = orderbook.best_no_bid
      final_checked_at_utc = datetime.now(UTC).isoformat()
    except Exception as exc:
      blocked_reason = 'final_orderbook_read_failed'
      blocked_count += 1
      block_reasons.append(blocked_reason)
      _persist_submit_bridge_candidate_rejected_before_order(
        connection,
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        phase=blocked_reason,
        exc=exc,
        saved_set_id=saved_set_snapshot.get('saved_set_id'),
        ticker=candidate.ticker,
        detail={'error_family': type(exc).__name__},
      )
      _persist_submit_bridge_final_coverability_checked(
        connection,
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        candidate=candidate,
        saved_set_id=saved_set_id,
        ok=False,
        guard_reason=blocked_reason,
        message=str(exc),
        final_checked_at_utc=datetime.now(UTC).isoformat(),
        max_divergence=max_divergence,
        settings=settings,
      )
      continue
    if yes_price_live is None or no_price_live is None or yes_price_live <= 0 or no_price_live <= 0:
      blocked_reason = 'live_price_unavailable'
      blocked_count += 1
      block_reasons.append(blocked_reason)
      persist_runtime_event(
        connection,
        level='WARN',
        event_type='live_order_price_fetch_blocked',
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        detail={
          'blocked_reason': blocked_reason,
          'ticker': candidate.ticker,
          'best_yes_bid': str(yes_price_live) if yes_price_live is not None else None,
          'best_no_bid': str(no_price_live) if no_price_live is not None else None,
        },
      )
      _persist_submit_bridge_final_coverability_checked(
        connection,
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        candidate=candidate,
        saved_set_id=saved_set_id,
        ok=False,
        guard_reason=blocked_reason,
        message=blocked_reason,
        final_checked_at_utc=final_checked_at_utc,
        best_yes_bid=yes_price_live,
        best_no_bid=no_price_live,
        max_divergence=max_divergence,
        settings=settings,
      )
      continue
    repriced_candidate = reprice_candidate(
      candidate,
      yes_price_live,
      no_price_live,
      settings,
    )
    static_guard = evaluate_pre_submit_coverability_static_prices(
      yes_price=repriced_candidate.target_yes_bid,
      no_price=repriced_candidate.target_no_bid,
      settings=settings,
      best_yes_bid=yes_price_live,
      best_no_bid=no_price_live,
    )
    if not static_guard.ok:
      blocked_reason = str(static_guard.reason or 'coverability_static_blocked')
      blocked_count += 1
      block_reasons.append(blocked_reason)
      _persist_submit_bridge_candidate_rejected_before_order(
        connection,
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        phase=blocked_reason,
        exc=ValueError(static_guard.message or blocked_reason),
        saved_set_id=saved_set_snapshot.get('saved_set_id'),
        ticker=candidate.ticker,
        detail=static_guard.detail,
      )
      _persist_submit_bridge_final_coverability_checked(
        connection,
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        candidate=candidate,
        saved_set_id=saved_set_id,
        ok=False,
        guard_reason=blocked_reason,
        message=static_guard.message or blocked_reason,
        final_checked_at_utc=final_checked_at_utc,
        best_yes_bid=yes_price_live,
        best_no_bid=no_price_live,
        repriced_yes_price=repriced_candidate.target_yes_bid,
        repriced_no_price=repriced_candidate.target_no_bid,
        max_divergence=max_divergence,
        settings=settings,
      )
      continue
    try:
      fresh_market = client.get_market(candidate.ticker)
    except Exception as exc:
      blocked_reason = 'fresh_market_readback_failed'
      blocked_count += 1
      block_reasons.append(blocked_reason)
      persist_runtime_event(
        connection,
        level='WARN',
        event_type='submit_bridge_blocked',
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        detail={'blocked_reason': blocked_reason, 'ticker': candidate.ticker, 'error_family': type(exc).__name__},
      )
      _persist_submit_bridge_final_coverability_checked(
        connection,
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        candidate=candidate,
        saved_set_id=saved_set_id,
        ok=False,
        guard_reason=blocked_reason,
        message=str(exc),
        final_checked_at_utc=final_checked_at_utc,
        best_yes_bid=yes_price_live,
        best_no_bid=no_price_live,
        repriced_yes_price=repriced_candidate.target_yes_bid,
        repriced_no_price=repriced_candidate.target_no_bid,
        max_divergence=max_divergence,
        settings=settings,
      )
      continue
    close_time = getattr(fresh_market, 'close_time', None)
    market_status = str(getattr(fresh_market, 'status', '') or '').strip().lower()
    if close_time is None or market_status not in {'open', 'active'}:
      blocked_reason = 'fresh_market_close_truth_unavailable'
      blocked_count += 1
      block_reasons.append(blocked_reason)
      persist_runtime_event(
        connection,
        level='WARN',
        event_type='submit_bridge_blocked',
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        detail={
          'blocked_reason': blocked_reason,
          'ticker': candidate.ticker,
          'market_status': market_status,
          'close_time_present': close_time is not None,
        },
      )
      _persist_submit_bridge_final_coverability_checked(
        connection,
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        candidate=candidate,
        saved_set_id=saved_set_id,
        ok=False,
        guard_reason=blocked_reason,
        message=blocked_reason,
        final_checked_at_utc=final_checked_at_utc,
        best_yes_bid=yes_price_live,
        best_no_bid=no_price_live,
        repriced_yes_price=repriced_candidate.target_yes_bid,
        repriced_no_price=repriced_candidate.target_no_bid,
        max_divergence=max_divergence,
        settings=settings,
      )
      continue
    try:
      proof_block_reason, proof_detail = _submit_binary_proof_block(client, candidate, fresh_market)
    except Exception as exc:
      _persist_submit_bridge_phase_failed(
        connection,
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        phase='binary_proof',
        exc=exc,
        saved_set_id=saved_set_snapshot.get('saved_set_id'),
        ticker=candidate.ticker,
      )
      raise
    if proof_block_reason is not None:
      blocked_reason = proof_block_reason
      blocked_count += 1
      block_reasons.append(blocked_reason)
      persist_runtime_event(
        connection,
        level='WARN',
        event_type='submit_binary_proof_blocked',
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        detail={'blocked_reason': blocked_reason, **proof_detail},
      )
      persist_known_non_binary_market(
        connection,
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        classification_reason=str(proof_detail.get('fresh_binary_reason') or blocked_reason),
        actionability='unknown_fail_closed' if 'unknown' in blocked_reason else 'deferred_non_binary',
        market_ticker=str(proof_detail.get('ticker') or candidate.ticker),
        event_ticker=str(proof_detail.get('event_ticker') or ''),
        series_ticker=str(proof_detail.get('fresh_binary_series_ticker') or ''),
        shape_signature='{series}|{reason}|siblings:{count}'.format(
          series=str(proof_detail.get('fresh_binary_series_ticker') or 'series:unknown'),
          reason=str(proof_detail.get('fresh_binary_reason') or blocked_reason),
          count=int(proof_detail.get('fresh_binary_market_count') or 0),
        ),
        market_count=int(proof_detail.get('fresh_binary_market_count') or 0),
        sample_sibling_tickers=proof_detail.get('fresh_binary_sibling_sample') if isinstance(proof_detail.get('fresh_binary_sibling_sample'), list) else (),
        source_run_id=lane_session_id,
        source_runtime_event_id=None,
        detail={'source': 'submit_binary_proof_gate', **proof_detail},
      )
      _persist_submit_bridge_final_coverability_checked(
        connection,
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        candidate=candidate,
        saved_set_id=saved_set_id,
        ok=False,
        guard_reason=blocked_reason,
        message=blocked_reason,
        final_checked_at_utc=final_checked_at_utc,
        best_yes_bid=yes_price_live,
        best_no_bid=no_price_live,
        repriced_yes_price=repriced_candidate.target_yes_bid,
        repriced_no_price=repriced_candidate.target_no_bid,
        max_divergence=max_divergence,
        settings=settings,
      )
      continue
    fresh_seconds_to_close = int((close_time.astimezone(UTC) - recorded_at.astimezone(UTC)).total_seconds())
    if fresh_seconds_to_close < settings.entry_window_end_sec:
      blocked_reason = 'fresh_market_too_close_to_close'
      blocked_count += 1
      block_reasons.append(blocked_reason)
      persist_runtime_event(
        connection,
        level='WARN',
        event_type='submit_bridge_blocked',
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        detail={
          'blocked_reason': blocked_reason,
          'ticker': candidate.ticker,
          'seconds_to_close': fresh_seconds_to_close,
          'entry_window_end_sec': settings.entry_window_end_sec,
        },
      )
      _persist_submit_bridge_final_coverability_checked(
        connection,
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        candidate=candidate,
        saved_set_id=saved_set_id,
        ok=False,
        guard_reason=blocked_reason,
        message=blocked_reason,
        final_checked_at_utc=final_checked_at_utc,
        best_yes_bid=yes_price_live,
        best_no_bid=no_price_live,
        repriced_yes_price=repriced_candidate.target_yes_bid,
        repriced_no_price=repriced_candidate.target_no_bid,
        max_divergence=max_divergence,
        settings=settings,
      )
      continue
    repriced_candidate = replace(repriced_candidate, seconds_to_close=fresh_seconds_to_close)
    prepared_candidates.append(repriced_candidate)
    # Flow/depth observation capture (BMAP 2026-07-02): best-effort resting-depth
    # summary from the finalist orderbook already read above -- no new reads, and
    # any failure degrades to null evidence without touching the survivor path.
    yes_depth_within_band: Any = None
    no_depth_within_band: Any = None
    try:
      depth_band = summarize_depth_within_band(
        orderbook,
        Decimal(str(repriced_candidate.target_yes_bid)),
        Decimal(str(repriced_candidate.target_no_bid)),
      )
      yes_depth_within_band = depth_band['yes_depth_within_band']
      no_depth_within_band = depth_band['no_depth_within_band']
    except Exception:
      yes_depth_within_band = None
      no_depth_within_band = None
    final_coverability_context_by_ticker[candidate.ticker] = {
      'final_checked_at_utc': final_checked_at_utc,
      'best_yes_bid': yes_price_live,
      'best_no_bid': no_price_live,
      'repriced_yes_price': repriced_candidate.target_yes_bid,
      'repriced_no_price': repriced_candidate.target_no_bid,
      'yes_depth_within_band': yes_depth_within_band,
      'no_depth_within_band': no_depth_within_band,
    }
    candidate_market_by_ticker[candidate.ticker] = fresh_market

  survivors = list(prepared_candidates)
  final_sizing_summary = _build_dynamic_sizing_summary(survivors, balance=balance, settings=settings)
  while survivors:
    sizing_summary = _build_dynamic_sizing_summary(survivors, balance=balance, settings=settings)
    final_sizing_summary = sizing_summary
    dynamic_raw = sizing_summary.get('dynamic_max_contracts')
    dynamic_max_contracts = Decimal(str(dynamic_raw if dynamic_raw is not None else '0'))
    if dynamic_max_contracts < 1:
      blocked_reason = 'dynamic_notional_cap_below_one_contract'
      blocked_count += len(survivors)
      block_reasons.extend([blocked_reason for _candidate in survivors])
      for candidate in survivors:
        _persist_submit_bridge_candidate_rejected_before_order(
          connection,
          recorded_at_utc=recorded_at.isoformat(),
          operation_lane=settings.operation_lane,
          lane_session_id=lane_session_id,
          phase=blocked_reason,
          exc=ValueError(blocked_reason),
          saved_set_id=saved_set_snapshot.get('saved_set_id'),
          ticker=candidate.ticker,
          detail={
            'dynamic_pair_notional_cap_dollars': sizing_summary.get('dynamic_pair_notional_cap_dollars'),
            'dynamic_max_contracts': sizing_summary.get('dynamic_max_contracts'),
          },
        )
        context = final_coverability_context_by_ticker.get(candidate.ticker, {})
        _persist_submit_bridge_final_coverability_checked(
          connection,
          recorded_at_utc=recorded_at.isoformat(),
          operation_lane=settings.operation_lane,
          lane_session_id=lane_session_id,
          candidate=candidate,
          saved_set_id=saved_set_id,
          ok=False,
          guard_reason=blocked_reason,
          message=blocked_reason,
          max_divergence=max_divergence,
          settings=settings,
          **context,
        )
      survivors = []
      final_sizing_summary = _build_dynamic_sizing_summary(survivors, balance=balance, settings=settings)
      break
    sizing_settings = replace(
      settings,
      max_pair_contracts=float(min(Decimal(str(settings.max_pair_contracts)), dynamic_max_contracts)),
    )
    next_survivors: list[CandidatePair] = []
    removed_any = False
    for candidate in survivors:
      try:
        plan = build_pair_order_plan(candidate, balance, sizing_settings)
        candidate_market = candidate_market_by_ticker.get(candidate.ticker)
        validate_pair_plan(
          plan,
          candidate,
          settings,
          market_status=str(getattr(candidate_market, 'status', '') or 'open'),
          account_limits_loaded=True,
          mode=mode,
          confirm_targeted=confirm_targeted,
          as_of=recorded_at,
        )
      except Exception as exc:
        if _is_candidate_local_pair_plan_rejection(exc):
          blocked_reason = 'pair_plan_validation'
          blocked_count += 1
          block_reasons.append(blocked_reason)
          removed_any = True
          _persist_submit_bridge_candidate_rejected_before_order(
            connection,
            recorded_at_utc=recorded_at.isoformat(),
            operation_lane=settings.operation_lane,
            lane_session_id=lane_session_id,
            phase=blocked_reason,
            exc=exc,
            saved_set_id=saved_set_snapshot.get('saved_set_id'),
            ticker=candidate.ticker,
          )
          context = final_coverability_context_by_ticker.get(candidate.ticker, {})
          _persist_submit_bridge_final_coverability_checked(
            connection,
            recorded_at_utc=recorded_at.isoformat(),
            operation_lane=settings.operation_lane,
            lane_session_id=lane_session_id,
            candidate=candidate,
            saved_set_id=saved_set_id,
            ok=False,
            guard_reason=blocked_reason,
            message=str(exc),
            max_divergence=max_divergence,
            settings=settings,
            **context,
          )
          continue
        raise
      try:
        recent_trades = recent_trades_by_ticker.get(candidate.ticker)
        if recent_trades is None:
          recent_trades = client.get_recent_trades(candidate.ticker, window_sec=settings.flow_window_sec)
          recent_trades_by_ticker[candidate.ticker] = recent_trades
        flow_guard = evaluate_flow_coverability(
          recent_trades.get('yes_flow_fp'),
          recent_trades.get('no_flow_fp'),
          plan.contract_count,
          settings,
        )
      except Exception:
        flow_guard = CoverabilityGuardResult(
          ok=False,
          reason='coverability_flow_unavailable',
          message='Recent per-side flow is unavailable for coverability validation.',
        )
      # Flow/depth observation capture (BMAP 2026-07-02): stash the flow numbers the
      # gate just used so the coverability evidence event retains them for both
      # passed and flow-blocked candidates. Best-effort; never disturbs the verdict.
      flow_evidence: dict[str, Any] = {'flow_threshold_pass': bool(flow_guard.ok)}
      try:
        if isinstance(recent_trades, dict):
          flow_evidence['yes_flow_window_fp'] = recent_trades.get('yes_flow_fp')
          flow_evidence['no_flow_window_fp'] = recent_trades.get('no_flow_fp')
        flow_evidence['flow_window_sec'] = settings.flow_window_sec
        flow_evidence['flow_participation_k'] = settings.flow_participation_k
        flow_evidence['intended_contract_count_for_floor'] = plan.contract_count
        if settings.flow_participation_k is not None:
          flow_evidence['required_flow_window_fp'] = (
            Decimal(str(settings.flow_participation_k)) * Decimal(str(plan.contract_count))
          )
      except Exception:
        pass
      flow_evidence_by_ticker[candidate.ticker] = flow_evidence
      if not flow_guard.ok:
        blocked_reason = str(flow_guard.reason or 'coverability_flow_blocked')
        blocked_count += 1
        block_reasons.append(blocked_reason)
        removed_any = True
        _persist_submit_bridge_candidate_rejected_before_order(
          connection,
          recorded_at_utc=recorded_at.isoformat(),
          operation_lane=settings.operation_lane,
          lane_session_id=lane_session_id,
          phase=blocked_reason,
          exc=ValueError(flow_guard.message or blocked_reason),
          saved_set_id=saved_set_snapshot.get('saved_set_id'),
          ticker=candidate.ticker,
          detail=flow_guard.detail,
        )
        context = final_coverability_context_by_ticker.get(candidate.ticker, {})
        _persist_submit_bridge_final_coverability_checked(
          connection,
          recorded_at_utc=recorded_at.isoformat(),
          operation_lane=settings.operation_lane,
          lane_session_id=lane_session_id,
          candidate=candidate,
          saved_set_id=saved_set_id,
          ok=False,
          guard_reason=blocked_reason,
          message=flow_guard.message or blocked_reason,
          max_divergence=max_divergence,
          settings=settings,
          **context,
          **flow_evidence,
        )
        continue
      next_survivors.append(candidate)
    survivors = next_survivors
    if not removed_any:
      break
    final_sizing_summary = _build_dynamic_sizing_summary(survivors, balance=balance, settings=settings)
  for candidate in survivors:
    context = final_coverability_context_by_ticker.get(candidate.ticker, {})
    _persist_submit_bridge_final_coverability_checked(
      connection,
      recorded_at_utc=recorded_at.isoformat(),
      operation_lane=settings.operation_lane,
      lane_session_id=lane_session_id,
      candidate=candidate,
      saved_set_id=saved_set_id,
      ok=True,
      guard_reason='',
      max_divergence=max_divergence,
      settings=settings,
      **context,
      **flow_evidence_by_ticker.get(candidate.ticker, {}),
    )
  return {
    'candidates': survivors,
    'sizing_summary': final_sizing_summary,
    'recent_trades_by_ticker': recent_trades_by_ticker,
    'blocked_count': blocked_count,
    'block_reasons': block_reasons,
    'blocked_reason': blocked_reason,
  }


def _unavailable_account_limits() -> AccountLimits:
  unavailable_bucket = AccountBucketLimit(refill_rate=0, bucket_capacity=0)
  return AccountLimits(
    usage_tier='unavailable_account_scope',
    read=unavailable_bucket,
    write=unavailable_bucket,
  )


def _load_scan_account_posture(
  client: Any,
  *,
  progress_callback: ScanProgressCallback | None,
) -> tuple[Decimal, AccountLimits, dict[str, Any]]:
  try:
    balance = client.get_balance()
    limits = client.get_account_api_limits()
    return balance, limits, {
      'status': 'loaded',
      'account_scope_available': True,
      'balance_available': True,
      'limits_available': True,
      'sizing_mode': 'account_balance',
    }
  except KalshiHttpError as exc:
    if str(getattr(exc, 'reason_code', '') or '') != 'auth_failed':
      raise
    degraded_posture = {
      'status': 'degraded',
      'account_scope_available': False,
      'balance_available': False,
      'limits_available': False,
      'sizing_mode': 'zero_balance_read_only_discovery',
      'reason_code': 'account_scope_auth_failed',
      'upstream_reason_code': getattr(exc, 'reason_code', 'auth_failed'),
      'message': 'Account-scoped Kalshi endpoints rejected authentication; continuing read-only market discovery without balance-based sizing.',
      'next_action': getattr(exc, 'next_action', 'Verify account-scope API access before order-capable execution.'),
    }
    _emit_scan_progress(
      progress_callback,
      'account_posture_degraded',
      'Account endpoints rejected authentication; continuing read-only market discovery without balance-based sizing.',
      detail=degraded_posture,
      progress_percent=0.1,
    )
    return Decimal('0'), _unavailable_account_limits(), degraded_posture


def _latest_pair_snapshots(connection: Any, *, operation_lane: str) -> list[dict[str, Any]]:
  rows = connection.execute(
    '''
    SELECT ps.pair_id, pp.ticker, pp.contract_count, ps.state, ps.detail_json, ps.recorded_at_utc, ps.lane_session_id
    FROM pair_states ps
    INNER JOIN (
      SELECT pair_id, MAX(id) AS max_id
      FROM pair_states
      WHERE operation_lane = ?
      GROUP BY pair_id
    ) latest ON latest.max_id = ps.id
    INNER JOIN pair_plans pp ON pp.pair_id = ps.pair_id
    WHERE ps.operation_lane = ? AND pp.operation_lane = ?
    ORDER BY ps.id ASC
    '''
    ,
    (operation_lane, operation_lane, operation_lane),
  ).fetchall()
  snapshots = []
  for row in rows:
    detail = json.loads(row['detail_json']) if row['detail_json'] else {}
    snapshots.append({
      'pair_id': row['pair_id'],
      'ticker': row['ticker'],
      'contract_count': row['contract_count'],
      'state': row['state'],
      'public_state_id': _project_public_state_id(row['state'], detail=detail),
      'detail': detail,
      'recorded_at_utc': row['recorded_at_utc'],
      'lane_session_id': row['lane_session_id'],
    })
  return snapshots


def _decimal_from_detail(detail: dict[str, Any], key: str) -> Decimal:
  try:
    return Decimal(str(detail.get(key, '0') or '0'))
  except (InvalidOperation, ValueError):
    return Decimal('0')


def _local_fill_truth(connection: Any, pair_id: str) -> dict[str, Decimal]:
  """Authoritative per-side fill truth from the local fills/orders SSOT.

  Filled counts and fees come from the recorded Kalshi fills; the domain
  per-contract price comes from the executed order row (unambiguously domain,
  which sidesteps the NO-leg fill-complement storage). Fail-soft to zeros: this
  read protects money truth and must never raise into the reconcile path.
  """
  truth: dict[str, Decimal] = {
    'yes_filled_contracts': Decimal('0'),
    'no_filled_contracts': Decimal('0'),
    'yes_domain_price': Decimal('0'),
    'no_domain_price': Decimal('0'),
    'yes_fees': Decimal('0'),
    'no_fees': Decimal('0'),
  }
  if not pair_id:
    return truth
  try:
    for row in connection.execute(
      'SELECT side, contract_count, fee_dollars FROM fills WHERE pair_id = ?',
      (pair_id,),
    ).fetchall():
      side = str(row['side'] or '').strip().lower()
      if side not in ('yes', 'no'):
        continue
      try:
        truth[f'{side}_filled_contracts'] += Decimal(str(row['contract_count'] or '0'))
        truth[f'{side}_fees'] += Decimal(str(row['fee_dollars'] or '0'))
      except (InvalidOperation, ValueError):
        continue
    for row in connection.execute(
      'SELECT side, price_dollars FROM orders WHERE pair_id = ?',
      (pair_id,),
    ).fetchall():
      side = str(row['side'] or '').strip().lower()
      if side not in ('yes', 'no'):
        continue
      try:
        truth[f'{side}_domain_price'] = Decimal(str(row['price_dollars'] or '0'))
      except (InvalidOperation, ValueError):
        continue
  except Exception:  # pragma: no cover - money-truth read must never break reconcile
    return truth
  return truth


def _fill_truth_has_exposure(fill_truth: dict[str, Decimal]) -> bool:
  return fill_truth['yes_filled_contracts'] > 0 or fill_truth['no_filled_contracts'] > 0


def _pair_has_fill_bearing_exposure(pair: dict[str, Any], *, connection: Any = None) -> bool:
  # Authoritative source is the local fills SSOT; the transient state detail is a
  # fallback only (a reconcile cycle can zero the detail counts -- see the
  # settlement fill-truth BMAP 2026-07-03 -- but the fills record never lies).
  if connection is not None and _fill_truth_has_exposure(_local_fill_truth(connection, str(pair.get('pair_id') or ''))):
    return True
  detail = pair.get('detail') if isinstance(pair.get('detail'), dict) else {}
  return (
    _decimal_from_detail(detail, 'yes_filled_contracts') > 0
    or _decimal_from_detail(detail, 'no_filled_contracts') > 0
  )


def _alignment_candidate_pairs(pairs: list[dict[str, Any]]) -> list[dict[str, Any]]:
  terminal_states = {'LOCKED', 'CANCELED', 'ERROR', 'FILLED', 'SETTLED', 'SETTLED_EXPOSURE'}
  return [
    pair for pair in pairs
    if str(pair.get('state') or '').strip().upper() not in terminal_states
  ]


def _pair_order_ids_from_history(connection: Any, pair_id: str, *, operation_lane: str) -> dict[str, str]:
  rows = connection.execute(
    '''
    SELECT detail_json
    FROM pair_states
    WHERE pair_id = ? AND operation_lane = ?
    ORDER BY id DESC
    ''',
    (pair_id, operation_lane),
  ).fetchall()
  order_ids = {'yes_order_id': '', 'no_order_id': ''}
  for row in rows:
    try:
      detail = json.loads(row['detail_json']) if row['detail_json'] else {}
    except json.JSONDecodeError:
      continue
    for key in ('yes_order_id', 'no_order_id'):
      if not order_ids[key]:
        order_ids[key] = str(detail.get(key) or '').strip()
    if order_ids['yes_order_id'] and order_ids['no_order_id']:
      break
  return order_ids


def _order_is_closed(order: Any) -> bool:
  status = str(getattr(order, 'status', '') or '').strip().lower()
  remaining = _submitted_order_remaining_count(order)
  return remaining <= 0 and status in {'canceled', 'cancelled', 'executed', 'filled'}


def _market_readback_result(raw_market: dict[str, Any]) -> str:
  for key in ('result', 'settlement_result', 'winning_side', 'outcome'):
    value = str(raw_market.get(key) or '').strip().lower()
    if value in {'yes', 'no'}:
      return value
  return ''


def _market_readback_close_time(raw_market: dict[str, Any]) -> str:
  for key in ('close_time', 'close_ts', 'expiration_time', 'latest_expiration_time'):
    value = raw_market.get(key)
    if value is None:
      continue
    return str(value)
  return ''


def _market_readback_finalized(raw_market: dict[str, Any]) -> bool:
  status = str(raw_market.get('status') or '').strip().lower()
  if status in {'determined', 'finalized', 'settled'}:
    return True
  if str(raw_market.get('settlement_ts') or '').strip():
    return True
  return bool(_market_readback_result(raw_market))


def _safe_read_fills(client: Any, ticker: str, yes_order_id: str, no_order_id: str) -> dict[str, Any]:
  get_fills = getattr(client, 'get_fills', None)
  if not callable(get_fills):
    return {'fills_readback_status': 'unavailable_client_no_get_fills'}
  try:
    fills = get_fills(ticker=ticker)
  except Exception as exc:
    return {'fills_readback_status': 'failed', 'fills_error_family': type(exc).__name__}
  relevant = [
    fill for fill in fills
    if str(fill.get('order_id') or fill.get('order_id_to_use') or '').strip() in {yes_order_id, no_order_id}
    or str(fill.get('ticker') or fill.get('market_ticker') or '').strip() == ticker
  ]
  return {
    'fills_readback_status': 'ok',
    'fills_readback_count': len(relevant),
  }


def _truth_market_from_snapshot(market: Any, ticker: str) -> dict[str, Any]:
  close_time = getattr(market, 'close_time', None)
  return {
    'ticker': ticker,
    'status': str(getattr(market, 'status', '') or ''),
    'close_time': close_time.isoformat() if hasattr(close_time, 'isoformat') else str(close_time or ''),
  }


def _read_alignment_market(client: Any, ticker: str) -> dict[str, Any]:
  get_market_readback = getattr(client, 'get_market_readback', None)
  if callable(get_market_readback):
    raw_market = get_market_readback(ticker)
    if isinstance(raw_market, dict) and raw_market:
      return raw_market
  return _truth_market_from_snapshot(client.get_market(ticker), ticker)


def _call_positions_for_ticker(client: Any, ticker: str) -> list[Any]:
  try:
    return list(client.get_positions(ticker=ticker))
  except TypeError:
    return [
      position for position in list(client.get_positions())
      if str(getattr(position, 'ticker', '') or '').strip() == ticker
    ]


def _alignment_position_packet(position: Any) -> dict[str, Any]:
  position_fp = Decimal(str(getattr(position, 'position_fp', '0') or '0'))
  contract_count = Decimal(str(getattr(position, 'contract_count', '0') or '0'))
  return {
    'ticker': str(getattr(position, 'ticker', '') or ''),
    'side': str(getattr(position, 'side', '') or ''),
    'contract_count': str(contract_count),
    'position_fp': str(position_fp),
    'average_price_dollars': str(getattr(position, 'average_price_dollars', '0') or '0'),
    'market_exposure_dollars': str(getattr(position, 'market_exposure_dollars', '0') or '0'),
    'realized_pnl_dollars': str(getattr(position, 'realized_pnl_dollars', '0') or '0'),
    'fees_dollars': str(getattr(position, 'fees_dollars', '0') or '0'),
  }


def _alignment_order_packet(order: Any) -> dict[str, Any]:
  return {
    'order_id': str(getattr(order, 'order_id', '') or ''),
    'ticker': str(getattr(order, 'ticker', '') or ''),
    'side': str(getattr(order, 'side', '') or ''),
    'status': str(getattr(order, 'status', '') or ''),
    'remaining_count': str(getattr(order, 'remaining_count', '0') or '0'),
    'fill_count': str(getattr(order, 'fill_count', '0') or '0'),
  }


def _alignment_open_positions(position_packets: list[dict[str, Any]], ticker: str) -> list[dict[str, Any]]:
  open_positions: list[dict[str, Any]] = []
  for packet in position_packets:
    if str(packet.get('ticker') or '').strip() != ticker:
      continue
    contract_count = Decimal(str(packet.get('contract_count') or '0'))
    position_fp = Decimal(str(packet.get('position_fp') or '0'))
    if contract_count != 0 or position_fp != 0:
      open_positions.append(packet)
  return open_positions


def _alignment_resting_orders(order_packets: list[dict[str, Any]], ticker: str) -> list[dict[str, Any]]:
  resting: list[dict[str, Any]] = []
  for packet in order_packets:
    if str(packet.get('ticker') or '').strip() != ticker:
      continue
    status = str(packet.get('status') or '').strip().lower()
    remaining = Decimal(str(packet.get('remaining_count') or '0'))
    if status == 'resting' and remaining > 0:
      resting.append(packet)
  return resting


def _alignment_realized_pnl(position_packets: list[dict[str, Any]]) -> tuple[str, str]:
  if not position_packets:
    return '0', 'kalshi_positions_absent'
  total = sum(
    (Decimal(str(packet.get('realized_pnl_dollars') or '0')) for packet in position_packets),
    Decimal('0'),
  )
  return str(total), 'kalshi_positions'


def _resolve_settlement_realized_pnl(
  position_packets: list[dict[str, Any]],
  *,
  finalized: bool,
  market_result: str,
  settlement_value: str,
  fill_truth: dict[str, Decimal],
) -> tuple[str | None, str]:
  """Realized-P&L authority for a reconcile cycle.

  Prefer the exchange realized P&L while the position is still open. Once the
  market finalizes the settled position drops off the positions read-back, so for
  a locally-filled pair derive the deterministic binary settlement P&L from the
  authoritative fill cost basis + the authoritative market result. Fail closed: a
  real fill with no authoritative result is never booked at $0 (settlement
  fill-truth BMAP 2026-07-03)."""
  if position_packets:
    return _alignment_realized_pnl(position_packets)
  if finalized and _fill_truth_has_exposure(fill_truth):
    result = str(market_result or '').strip().lower()
    try:
      settle = Decimal(str(settlement_value)) if str(settlement_value or '').strip() else Decimal('0')
    except (InvalidOperation, ValueError):
      settle = Decimal('0')
    if result in ('yes', 'no') and settle > 0:
      pnl = Decimal('0')
      for side in ('yes', 'no'):
        contracts = fill_truth[f'{side}_filled_contracts']
        if contracts <= 0:
          continue
        payout = contracts * settle if result == side else Decimal('0')
        pnl += payout - (contracts * fill_truth[f'{side}_domain_price']) - fill_truth[f'{side}_fees']
      return str(pnl), 'local_fill_settlement_reconciliation'
    return None, 'result_unavailable_pending_readback'
  return _alignment_realized_pnl(position_packets)


def _alignment_exchange_position_detail(open_positions: list[dict[str, Any]]) -> dict[str, Any]:
  if not open_positions:
    return {
      'exchange_position_contracts': '0',
      'exchange_position_side': '',
      'exchange_market_exposure_dollars': '0',
    }
  contracts = sum((Decimal(str(packet.get('contract_count') or '0')) for packet in open_positions), Decimal('0'))
  exposure = sum((Decimal(str(packet.get('market_exposure_dollars') or '0')) for packet in open_positions), Decimal('0'))
  sides = sorted({str(packet.get('side') or '').strip() for packet in open_positions if str(packet.get('side') or '').strip()})
  return {
    'exchange_position_contracts': str(contracts),
    'exchange_position_side': ','.join(sides),
    'exchange_market_exposure_dollars': str(exposure),
  }


def _persist_alignment_event(
  connection: Any,
  *,
  event_type: str,
  level: str,
  pair: dict[str, Any],
  recorded_at_utc: str,
  operation_lane: str,
  lane_session_id: str,
  detail: dict[str, Any],
) -> None:
  persist_runtime_event(
    connection,
    level=level,
    event_type=event_type,
    pair_id=str(pair.get('pair_id') or ''),
    recorded_at_utc=recorded_at_utc,
    operation_lane=operation_lane,
    lane_session_id=lane_session_id,
    detail={'ticker': str(pair.get('ticker') or ''), **detail},
  )


def align_pairs_with_kalshi(
  connection: Any,
  *,
  settings: Settings,
  client: Any,
  pairs: list[dict[str, Any]],
  recorded_at_utc: str,
  operation_lane: str,
  lane_session_id: str,
  reason: str,
) -> AlignmentResult:
  del settings
  if str(operation_lane or '').strip().lower() != 'live' or not pairs:
    return AlignmentResult(
      aligned_pairs=pairs,
      truth_by_ticker={},
      terminalized=[],
      preserved=[],
      readback_status={},
      degraded=False,
    )

  truth_by_ticker: dict[str, KalshiAlignmentTruth] = {}
  terminalized: list[KalshiAlignmentChange] = []
  preserved: list[KalshiAlignmentChange] = []
  readback_status: dict[str, str] = {}
  degraded = False
  wrote_transition = False

  for pair in pairs:
    pair_id = str(pair.get('pair_id') or '')
    ticker = str(pair.get('ticker') or '').strip()
    state_before = str(pair.get('state') or '').strip()
    detail = pair.get('detail') if isinstance(pair.get('detail'), dict) else {}
    if not pair_id or not ticker:
      degraded = True
      continue
    try:
      raw_market = _read_alignment_market(client, ticker)
      positions = _call_positions_for_ticker(client, ticker)
      position_packets = [_alignment_position_packet(position) for position in positions]
      market_result = _market_readback_result(raw_market)
      market_status = str(raw_market.get('status') or '').strip().lower()
      settlement_ts = str(raw_market.get('settlement_ts') or '').strip()
      settlement_value = str(raw_market.get('settlement_value_dollars') or '').strip()
      finalized = _market_readback_finalized(raw_market)
      if finalized:
        order_packets: list[dict[str, Any]] = []
      else:
        list_orders = getattr(client, 'list_orders', None)
        if not callable(list_orders):
          raise AttributeError('list_orders')
        order_packets = [
          _alignment_order_packet(order)
          for order in list_orders(ticker=ticker, status='resting')
        ]
      open_positions = _alignment_open_positions(position_packets, ticker)
      resting_orders = _alignment_resting_orders(order_packets, ticker)
      fill_truth = _local_fill_truth(connection, pair_id)
      realized_pnl, realized_pnl_source = _resolve_settlement_realized_pnl(
        position_packets,
        finalized=finalized,
        market_result=market_result,
        settlement_value=settlement_value,
        fill_truth=fill_truth,
      )
      truth_by_ticker[ticker] = KalshiAlignmentTruth(
        ticker=ticker,
        market=raw_market,
        positions=position_packets,
        resting_orders=order_packets,
        readback_status='ok',
      )
      readback_status[ticker] = 'ok'
    except Exception as exc:
      degraded = True
      readback_status[ticker] = 'failed'
      truth_by_ticker[ticker] = KalshiAlignmentTruth(
        ticker=ticker,
        market={},
        positions=[],
        resting_orders=[],
        readback_status='failed',
        error_family=type(exc).__name__,
      )
      _persist_alignment_event(
        connection,
        event_type='pair_alignment_readback_failed',
        level='WARN',
        pair=pair,
        recorded_at_utc=recorded_at_utc,
        operation_lane=operation_lane,
        lane_session_id=lane_session_id,
        detail={'reason': reason, 'cause': 'readback_failed', 'error_family': type(exc).__name__},
      )
      preserved.append(KalshiAlignmentChange(pair_id, ticker, state_before, state_before, 'readback_failed'))
      continue

    base_detail = {
      **detail,
      'ticker': ticker,
      'alignment_reason': reason,
      'alignment_source': 'kalshi_readback',
      'market_status': market_status,
      'market_result': market_result,
      'settlement_ts': settlement_ts,
      'settlement_value_dollars': settlement_value,
      'positions_readback_status': 'ok',
      'orders_readback_status': 'ok',
      'open_position_count': len(open_positions),
      'resting_order_count': len(resting_orders),
      'realized_pnl_dollars': realized_pnl,
      'realized_pnl_source': realized_pnl_source,
      # Carry the authoritative local fill counts on EVERY reconcile transition so
      # a later empty position read-back can never erase a recorded fill (settlement
      # fill-truth BMAP 2026-07-03). Overrides any stale value spread from **detail.
      'yes_filled_contracts': str(fill_truth['yes_filled_contracts']),
      'no_filled_contracts': str(fill_truth['no_filled_contracts']),
      'kalshi_alignment_recorded_at_utc': recorded_at_utc,
      **_alignment_exchange_position_detail(open_positions),
    }

    if finalized:
      has_local_fill = _pair_has_fill_bearing_exposure(pair, connection=connection)
      terminal_state = 'SETTLED_EXPOSURE' if has_local_fill else 'SETTLED'
      terminal_reason = 'market_finalized_one_sided_exposure' if has_local_fill else 'market_finalized_no_open_exposure'
      persist_pair_state_transition(
        connection,
        pair_id=pair_id,
        state=terminal_state,
        recorded_at_utc=recorded_at_utc,
        operation_lane=operation_lane,
        lane_session_id=lane_session_id,
        detail={
          **base_detail,
          'reason': terminal_reason,
          'terminal_reason': terminal_reason,
          **_submit_bridge_detail_fields(
            legacy_state=terminal_state,
            saved_set_snapshot=None,
            submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state=terminal_state),
          ),
        },
      )
      _persist_alignment_event(
        connection,
        event_type='pair_alignment_settled',
        level='INFO',
        pair=pair,
        recorded_at_utc=recorded_at_utc,
        operation_lane=operation_lane,
        lane_session_id=lane_session_id,
        detail={
          'reason': reason,
          'terminal_state': terminal_state,
          'terminal_reason': terminal_reason,
          'market_result': market_result,
          'open_position_count': len(open_positions),
          'resting_order_count': len(resting_orders),
        },
      )
      terminalized.append(KalshiAlignmentChange(pair_id, ticker, state_before, terminal_state, terminal_reason))
      wrote_transition = True
      continue

    if not open_positions and not resting_orders:
      if _pair_has_fill_bearing_exposure(pair, connection=connection):
        degraded = True
        _persist_alignment_event(
          connection,
          event_type='pair_alignment_readback_failed',
          level='WARN',
          pair=pair,
          recorded_at_utc=recorded_at_utc,
          operation_lane=operation_lane,
          lane_session_id=lane_session_id,
          detail={'reason': reason, 'cause': 'local_fill_without_exchange_exposure'},
        )
        preserved.append(KalshiAlignmentChange(pair_id, ticker, state_before, state_before, 'local_fill_without_exchange_exposure'))
        continue
      terminal_reason = 'kalshi_alignment_no_exposure'
      persist_pair_state_transition(
        connection,
        pair_id=pair_id,
        state='SETTLED',
        recorded_at_utc=recorded_at_utc,
        operation_lane=operation_lane,
        lane_session_id=lane_session_id,
        detail={
          **base_detail,
          'reason': terminal_reason,
          'terminal_reason': terminal_reason,
          **_submit_bridge_detail_fields(
            legacy_state='SETTLED',
            saved_set_snapshot=None,
            submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state='SETTLED'),
          ),
        },
      )
      _persist_alignment_event(
        connection,
        event_type='pair_alignment_no_exposure',
        level='INFO',
        pair=pair,
        recorded_at_utc=recorded_at_utc,
        operation_lane=operation_lane,
        lane_session_id=lane_session_id,
        detail={'reason': reason, 'terminal_state': 'SETTLED', 'terminal_reason': terminal_reason},
      )
      terminalized.append(KalshiAlignmentChange(pair_id, ticker, state_before, 'SETTLED', terminal_reason))
      wrote_transition = True
      continue

    preserve_reason = 'kalshi_alignment_preserved'
    persist_pair_state_transition(
      connection,
      pair_id=pair_id,
      state=state_before,
      recorded_at_utc=recorded_at_utc,
      operation_lane=operation_lane,
      lane_session_id=lane_session_id,
      detail={**base_detail, 'reason': preserve_reason},
    )
    _persist_alignment_event(
      connection,
      event_type='pair_alignment_preserved',
      level='INFO',
      pair=pair,
      recorded_at_utc=recorded_at_utc,
      operation_lane=operation_lane,
      lane_session_id=lane_session_id,
      detail={
        'reason': reason,
        'preserve_reason': preserve_reason,
        'open_position_count': len(open_positions),
        'resting_order_count': len(resting_orders),
      },
    )
    preserved.append(KalshiAlignmentChange(pair_id, ticker, state_before, state_before, preserve_reason))
    wrote_transition = True

  aligned_pairs = (
    _latest_pair_snapshots(connection, operation_lane=operation_lane)
    if wrote_transition
    else pairs
  )
  return AlignmentResult(
    aligned_pairs=aligned_pairs,
    truth_by_ticker=truth_by_ticker,
    terminalized=terminalized,
    preserved=preserved,
    readback_status=readback_status,
    degraded=degraded,
  )


def _reconcile_repair_close_exposures(
  connection: Any,
  *,
  settings: Settings,
  client_factory: ClientFactory | None,
  pairs: list[dict[str, Any]],
  recorded_at_utc: str,
  lane_session_id: str,
) -> int:
  candidates = [
    pair for pair in pairs
    # ERROR is the SSOT-conformant frozen-residual state; REPAIR_LIVE retained for legacy in-flight rows.
    if str(pair.get('state') or '').strip().upper() in {'ERROR', 'REPAIR_LIVE'}
    and _pair_has_fill_bearing_exposure(pair, connection=connection)
  ]
  if not candidates:
    return 0
  if str(settings.operation_lane or '').strip().lower() != 'live':
    return 0

  private_key_path = resolve_private_key_path(settings)
  private_key = load_private_key(private_key_path)
  client = (
    client_factory(settings, private_key)
    if client_factory is not None
    else KalshiHttpClient(settings, private_key, request_timeout=3, max_attempts=1)
  )

  reconciled = 0
  for pair in candidates:
    detail = pair.get('detail') if isinstance(pair.get('detail'), dict) else {}
    order_ids = _pair_order_ids_from_history(connection, str(pair['pair_id']), operation_lane=settings.operation_lane)
    yes_order_id = order_ids['yes_order_id']
    no_order_id = order_ids['no_order_id']
    if not yes_order_id or not no_order_id:
      persist_pair_state_transition(
        connection,
        pair_id=pair['pair_id'],
        state='RECONCILE_REQUIRED',
        recorded_at_utc=recorded_at_utc,
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        detail={**detail, 'reason': 'repair_close_missing_order_ids', 'public_state_id': 'RECONCILE_REQUIRED'},
      )
      reconciled += 1
      continue
    try:
      yes_order = client.get_order(yes_order_id)
      no_order = client.get_order(no_order_id)
      get_market_readback = getattr(client, 'get_market_readback', None)
      raw_market = get_market_readback(pair['ticker']) if callable(get_market_readback) else {}
      if not isinstance(raw_market, dict) or not raw_market:
        market = client.get_market(pair['ticker'])
        raw_market = {
          'status': getattr(market, 'status', ''),
          'close_time': getattr(market, 'close_time', ''),
        }
    except Exception as exc:
      persist_pair_state_transition(
        connection,
        pair_id=pair['pair_id'],
        state='RECONCILE_REQUIRED',
        recorded_at_utc=recorded_at_utc,
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        detail={
          **detail,
          'reason': 'repair_close_readback_failed',
          'readback_error_family': type(exc).__name__,
          'public_state_id': 'RECONCILE_REQUIRED',
        },
      )
      reconciled += 1
      continue

    yes_filled = _submitted_order_fill_count(yes_order)
    no_filled = _submitted_order_fill_count(no_order)
    both_filled = yes_filled > 0 and no_filled > 0 and yes_filled == no_filled
    market_result = _market_readback_result(raw_market)
    market_status = str(raw_market.get('status') or '').strip().lower()
    market_close_time = _market_readback_close_time(raw_market)
    positions_detail = _position_readback_detail(client)
    fills_detail = _safe_read_fills(client, str(pair['ticker']), yes_order_id, no_order_id)
    if both_filled:
      terminal_state = 'LOCKED'
      terminal_reason = 'repair_close_both_legs_locked'
      settlement_outcome = 'flat'
    elif not _market_readback_finalized(raw_market) or market_result not in {'yes', 'no'}:
      terminal_state = 'RECONCILE_REQUIRED'
      terminal_reason = 'repair_close_market_not_finalized'
      settlement_outcome = ''
    elif (yes_filled > 0 or no_filled > 0) and _order_is_closed(yes_order) and _order_is_closed(no_order):
      terminal_state = 'SETTLED_EXPOSURE'
      terminal_reason = 'market_finalized_one_sided_exposure'
      yes_cost = yes_filled * Decimal(str(getattr(yes_order, 'price_dollars', detail.get('average_yes_price', '0')) or '0'))
      no_cost = no_filled * Decimal(str(getattr(no_order, 'price_dollars', detail.get('average_no_price', '0')) or '0'))
      payout = (yes_filled if market_result == 'yes' else Decimal('0')) + (no_filled if market_result == 'no' else Decimal('0'))
      net = payout - yes_cost - no_cost
      settlement_outcome = 'gain' if net > 0 else 'loss' if net < 0 else 'flat'
    else:
      terminal_state = 'RECONCILE_REQUIRED'
      terminal_reason = 'repair_close_orders_not_closed'
      settlement_outcome = ''

    persist_pair_state_transition(
      connection,
      pair_id=pair['pair_id'],
      state=terminal_state,
      recorded_at_utc=recorded_at_utc,
      operation_lane=settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={
        **detail,
        'ticker': pair['ticker'],
        'reason': terminal_reason,
        'terminal_reason': terminal_reason,
        'settlement_outcome': settlement_outcome,
        'yes_order_id': yes_order_id,
        'no_order_id': no_order_id,
        'yes_order_status': getattr(yes_order, 'status', ''),
        'no_order_status': getattr(no_order, 'status', ''),
        'yes_filled_contracts': str(yes_filled),
        'no_filled_contracts': str(no_filled),
        'market_result': market_result,
        'market_status': market_status,
        'market_close_time': market_close_time,
        'orders_readback_status': 'ok',
        **positions_detail,
        **fills_detail,
        **_submit_bridge_detail_fields(
          legacy_state=terminal_state,
          saved_set_snapshot=None,
          submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state=terminal_state),
        ),
      },
    )
    persist_runtime_event(
      connection,
      level='INFO' if terminal_state in {'FILLED', 'SETTLED_EXPOSURE'} else 'WARN',
      event_type='repair_close_reconciled',
      pair_id=pair['pair_id'],
      recorded_at_utc=recorded_at_utc,
      operation_lane=settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={
        'ticker': pair['ticker'],
        'terminal_state': terminal_state,
        'terminal_reason': terminal_reason,
        'settlement_outcome': settlement_outcome,
      },
    )
    reconciled += 1
  return reconciled


def _persist_submit_fill_cancel_reconcile_chronology(
  connection: Any,
  *,
  plan: Any,
  settings: Settings,
  lane_session_id: str,
  recorded_at: datetime,
  sizing_summary: dict[str, Any],
  saved_set_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
  sequence = 0
  event_packet: list[dict[str, Any]] = []
  submit_response_id = _submit_bridge_response_id(blocked_reason=None, legacy_state='SUBMITTING')

  def _append_event_packet(
    *,
    event_type: str,
    execution_status: str,
    seq: str,
    ts_ms: int,
    as_of_time: str,
    order_id: str | None = None,
    client_order_id: str | None = None,
    trade_id: str | None = None,
    outcome_side: str | None = None,
    book_side: str | None = None,
    use_yes_price: bool | None = None,
    user_data_timestamp: int | None = None,
  ) -> None:
    packet: dict[str, Any] = {
      'event_type': event_type,
      'execution_status': execution_status,
      'profile': 'submit_order_bridge',
      'operation_lane': settings.operation_lane,
      'lane_session_id': lane_session_id,
      'market_ticker': plan.ticker,
      'market_id': None,
      'order_id': order_id,
      'client_order_id': client_order_id,
      'trade_id': trade_id,
      'seq': seq,
      'ts_ms': ts_ms,
      'as_of_time': as_of_time,
      'user_data_timestamp': user_data_timestamp,
      'outcome_side': outcome_side,
      'book_side': book_side,
      'use_yes_price': use_yes_price,
    }
    event_packet.append(packet)

  def _next_event(offset_ms: int) -> tuple[str, int, str]:
    nonlocal sequence
    sequence += 1
    event_at = recorded_at + timedelta(milliseconds=offset_ms)
    return event_at.isoformat(), int(event_at.timestamp() * 1000), f'f2-seq-{sequence:03d}'

  submit_ts, submit_ts_ms, submit_seq = _next_event(10)
  submitting_pair = PairRuntimeState(
    pair_id=plan.pair_id,
    state='SUBMITTING',
    yes_filled_contracts=Decimal('0'),
    no_filled_contracts=Decimal('0'),
    average_yes_price=plan.yes_price,
    average_no_price=plan.no_price,
    realized_fees_dollars=Decimal('0'),
    last_update_at=datetime.fromisoformat(submit_ts),
    websocket_connected=False,
  )
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state='SUBMITTING',
    recorded_at_utc=submit_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'ticker': plan.ticker,
      'reason': 'submit_order_bridge_intent',
      'seq': submit_seq,
      'ts_ms': submit_ts_ms,
      'as_of_time': submit_ts,
      'yes_filled_contracts': '0',
      'no_filled_contracts': '0',
      'average_yes_price': str(plan.yes_price),
      'average_no_price': str(plan.no_price),
      'realized_fees_dollars': '0',
      'websocket_connected': False,
      'effective_density': sizing_summary.get('effective_density'),
      'dynamic_pair_notional_pct': sizing_summary.get('dynamic_pair_notional_pct'),
      'dynamic_pair_notional_cap_dollars': sizing_summary.get('dynamic_pair_notional_cap_dollars'),
      'dynamic_max_contracts': sizing_summary.get('dynamic_max_contracts'),
      'binding_limiter': sizing_summary.get('binding_limiter'),
      **_submit_bridge_detail_fields(
        legacy_state='SUBMITTING',
        saved_set_snapshot=saved_set_snapshot,
        submit_response_id=submit_response_id,
      ),
    },
  )
  persist_runtime_event(
    connection,
    level='INFO',
    event_type='submit_order_intent',
    pair_id=plan.pair_id,
    recorded_at_utc=submit_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'seq': submit_seq,
      'ts_ms': submit_ts_ms,
      'as_of_time': submit_ts,
      'state': 'SUBMITTING',
      'profile': 'submit_order_bridge',
      **_submit_bridge_detail_fields(
        legacy_state='SUBMITTING',
        saved_set_snapshot=saved_set_snapshot,
        submit_response_id=submit_response_id,
      ),
    },
  )

  submitted_orders = simulate_submit_pair(
    plan,
    submitted_at=datetime.fromisoformat(submit_ts),
  )
  persist_order_statuses(
    connection,
    operation_lane=settings.operation_lane,
    statuses=[
      {'order_id': submitted_orders[0].order_id, 'status': submitted_orders[0].status},
      {'order_id': submitted_orders[1].order_id, 'status': submitted_orders[1].status},
    ],
  )

  resting_ts, resting_ts_ms, resting_seq = _next_event(20)
  _append_event_packet(
    event_type='submit_order_intent',
    execution_status='submitted',
    seq=submit_seq,
    ts_ms=submit_ts_ms,
    as_of_time=submit_ts,
    order_id=submitted_orders[0].order_id,
    client_order_id=submitted_orders[0].client_order_id,
    outcome_side='yes',
    book_side='buy',
    use_yes_price=True,
  )
  _append_event_packet(
    event_type='submit_order_intent',
    execution_status='submitted',
    seq=submit_seq,
    ts_ms=submit_ts_ms,
    as_of_time=submit_ts,
    order_id=submitted_orders[1].order_id,
    client_order_id=submitted_orders[1].client_order_id,
    outcome_side='no',
    book_side='buy',
    use_yes_price=False,
  )
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state='RESTING_BOTH',
    recorded_at_utc=resting_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'ticker': plan.ticker,
      'seq': resting_seq,
      'ts_ms': resting_ts_ms,
      'as_of_time': resting_ts,
      'yes_filled_contracts': '0',
      'no_filled_contracts': '0',
      'average_yes_price': str(plan.yes_price),
      'average_no_price': str(plan.no_price),
      'realized_fees_dollars': '0',
      'websocket_connected': False,
      **_submit_bridge_detail_fields(
        legacy_state='RESTING_BOTH',
        saved_set_snapshot=saved_set_snapshot,
        submit_response_id=submit_response_id,
      ),
    },
  )
  persist_runtime_event(
    connection,
    level='INFO',
    event_type='user_orders',
    pair_id=plan.pair_id,
    recorded_at_utc=resting_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'seq': resting_seq,
      'ts_ms': resting_ts_ms,
      'as_of_time': resting_ts,
      'order_states': [submitted_orders[0].status, submitted_orders[1].status],
      **_submit_bridge_detail_fields(
        legacy_state='RESTING_BOTH',
        saved_set_snapshot=saved_set_snapshot,
        submit_response_id=submit_response_id,
      ),
    },
  )
  _append_event_packet(
    event_type='user_orders',
    execution_status='resting',
    seq=resting_seq,
    ts_ms=resting_ts_ms,
    as_of_time=resting_ts,
    order_id=submitted_orders[0].order_id,
    client_order_id=submitted_orders[0].client_order_id,
    outcome_side='yes',
    book_side='buy',
    use_yes_price=True,
  )
  _append_event_packet(
    event_type='user_orders',
    execution_status='resting',
    seq=resting_seq,
    ts_ms=resting_ts_ms,
    as_of_time=resting_ts,
    order_id=submitted_orders[1].order_id,
    client_order_id=submitted_orders[1].client_order_id,
    outcome_side='no',
    book_side='buy',
    use_yes_price=False,
  )

  partial_yes = max(plan.contract_count, Decimal('1'))
  partial_no = max(plan.contract_count - Decimal('1'), Decimal('0'))
  partial_fees = Decimal(str(settings.fee_reserve_dollars))
  partial_ts, partial_ts_ms, partial_seq = _next_event(30)
  partial_pair = simulate_partial_fill(
    plan,
    yes_filled=partial_yes,
    no_filled=partial_no,
    as_of=datetime.fromisoformat(partial_ts),
    realized_fees_dollars=partial_fees,
  )
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state=partial_pair.state,
    recorded_at_utc=partial_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'ticker': plan.ticker,
      'seq': partial_seq,
      'ts_ms': partial_ts_ms,
      'as_of_time': partial_ts,
      'yes_filled_contracts': str(partial_pair.yes_filled_contracts),
      'no_filled_contracts': str(partial_pair.no_filled_contracts),
      'average_yes_price': str(partial_pair.average_yes_price),
      'average_no_price': str(partial_pair.average_no_price),
      'realized_fees_dollars': str(partial_pair.realized_fees_dollars),
      'websocket_connected': partial_pair.websocket_connected,
      'synthetic_fill': True,
      **_submit_bridge_detail_fields(
        legacy_state=partial_pair.state,
        saved_set_snapshot=saved_set_snapshot,
        submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state=partial_pair.state),
      ),
    },
  )
  if partial_pair.yes_filled_contracts > 0:
    persist_fill(
      connection,
      FillEvent(
        fill_id=f'{plan.pair_id}-fill-yes-{partial_seq}',
        pair_id=plan.pair_id,
        order_id=submitted_orders[0].order_id,
        client_order_id=submitted_orders[0].client_order_id,
        side='yes',
        price_dollars=plan.yes_price,
        contract_count=partial_pair.yes_filled_contracts,
        fee_dollars=partial_fees / Decimal('2'),
        created_at=datetime.fromisoformat(partial_ts),
      ),
      operation_lane=settings.operation_lane,
    )
  if partial_pair.no_filled_contracts > 0:
    persist_fill(
      connection,
      FillEvent(
        fill_id=f'{plan.pair_id}-fill-no-{partial_seq}',
        pair_id=plan.pair_id,
        order_id=submitted_orders[1].order_id,
        client_order_id=submitted_orders[1].client_order_id,
        side='no',
        price_dollars=plan.no_price,
        contract_count=partial_pair.no_filled_contracts,
        fee_dollars=partial_fees / Decimal('2'),
        created_at=datetime.fromisoformat(partial_ts),
      ),
      operation_lane=settings.operation_lane,
    )
  persist_runtime_event(
    connection,
    level='INFO',
    event_type='fill',
    pair_id=plan.pair_id,
    recorded_at_utc=partial_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'seq': partial_seq,
      'ts_ms': partial_ts_ms,
      'as_of_time': partial_ts,
      'state': partial_pair.state,
      'yes_filled_contracts': str(partial_pair.yes_filled_contracts),
      'no_filled_contracts': str(partial_pair.no_filled_contracts),
      **_submit_bridge_detail_fields(
        legacy_state=partial_pair.state,
        saved_set_snapshot=saved_set_snapshot,
        submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state=partial_pair.state),
      ),
    },
  )
  if partial_pair.yes_filled_contracts > 0:
    _append_event_packet(
      event_type='fill',
      execution_status='partial_fill',
      seq=partial_seq,
      ts_ms=partial_ts_ms,
      as_of_time=partial_ts,
      order_id=submitted_orders[0].order_id,
      client_order_id=submitted_orders[0].client_order_id,
      trade_id=f'{plan.pair_id}-fill-yes-{partial_seq}',
      outcome_side='yes',
      book_side='buy',
      use_yes_price=True,
    )
  if partial_pair.no_filled_contracts > 0:
    _append_event_packet(
      event_type='fill',
      execution_status='partial_fill',
      seq=partial_seq,
      ts_ms=partial_ts_ms,
      as_of_time=partial_ts,
      order_id=submitted_orders[1].order_id,
      client_order_id=submitted_orders[1].client_order_id,
      trade_id=f'{plan.pair_id}-fill-no-{partial_seq}',
      outcome_side='no',
      book_side='buy',
      use_yes_price=False,
    )

  persist_runtime_event(
    connection,
    level='INFO',
    event_type='market_positions',
    pair_id=plan.pair_id,
    recorded_at_utc=partial_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'seq': partial_seq,
      'ts_ms': partial_ts_ms,
      'as_of_time': partial_ts,
      'position_contracts_yes': str(partial_pair.yes_filled_contracts),
      'position_contracts_no': str(partial_pair.no_filled_contracts),
      'position_state': partial_pair.state,
      'profile': 'submit_order_bridge',
      **_submit_bridge_detail_fields(
        legacy_state=partial_pair.state,
        saved_set_snapshot=saved_set_snapshot,
        submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state=partial_pair.state),
      ),
    },
  )
  _append_event_packet(
    event_type='market_positions',
    execution_status='position_updated',
    seq=partial_seq,
    ts_ms=partial_ts_ms,
    as_of_time=partial_ts,
    outcome_side='mixed',
    book_side='position',
    use_yes_price=None,
  )

  reconcile_ts, reconcile_ts_ms, reconcile_seq = _next_event(40)
  reconciled_pair = reconcile_pair(
    partial_pair,
    as_of=datetime.fromisoformat(reconcile_ts),
  )
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state=reconciled_pair.state,
    recorded_at_utc=reconcile_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'ticker': plan.ticker,
      'seq': reconcile_seq,
      'ts_ms': reconcile_ts_ms,
      'as_of_time': reconcile_ts,
      'yes_filled_contracts': str(reconciled_pair.yes_filled_contracts),
      'no_filled_contracts': str(reconciled_pair.no_filled_contracts),
      'average_yes_price': str(reconciled_pair.average_yes_price),
      'average_no_price': str(reconciled_pair.average_no_price),
      'realized_fees_dollars': str(reconciled_pair.realized_fees_dollars),
      'websocket_connected': reconciled_pair.websocket_connected,
      'user_data_timestamp': reconcile_ts_ms,
      **_submit_bridge_detail_fields(
        legacy_state=reconciled_pair.state,
        saved_set_snapshot=saved_set_snapshot,
        submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state=reconciled_pair.state),
      ),
    },
  )
  persist_runtime_event(
    connection,
    level='INFO',
    event_type='reconcile_snapshot',
    pair_id=plan.pair_id,
    recorded_at_utc=reconcile_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'seq': reconcile_seq,
      'ts_ms': reconcile_ts_ms,
      'as_of_time': reconcile_ts,
      'user_data_timestamp': reconcile_ts_ms,
      'state': reconciled_pair.state,
      **_submit_bridge_detail_fields(
        legacy_state=reconciled_pair.state,
        saved_set_snapshot=saved_set_snapshot,
        submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state=reconciled_pair.state),
      ),
    },
  )
  _append_event_packet(
    event_type='reconcile_snapshot',
    execution_status='reconciled',
    seq=reconcile_seq,
    ts_ms=reconcile_ts_ms,
    as_of_time=reconcile_ts,
    user_data_timestamp=reconcile_ts_ms,
    outcome_side='mixed',
    book_side='reconcile',
  )

  cancel_ts, cancel_ts_ms, cancel_seq = _next_event(50)
  canceled_orders = simulate_cancel_pair(
    submitted_orders,
    canceled_at=datetime.fromisoformat(cancel_ts),
  )
  persist_order_statuses(
    connection,
    operation_lane=settings.operation_lane,
    statuses=[
      {'order_id': canceled_orders[0].order_id, 'status': canceled_orders[0].status},
      {'order_id': canceled_orders[1].order_id, 'status': canceled_orders[1].status},
    ],
  )
  canceled_pair = cancel_pair(
    reconciled_pair,
    canceled_at=datetime.fromisoformat(cancel_ts),
  )
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state='CANCELED',
    recorded_at_utc=cancel_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'ticker': plan.ticker,
      'reason': 'sandbox_simulated_cancel_after_partial_fill',
      'seq': cancel_seq,
      'ts_ms': cancel_ts_ms,
      'as_of_time': cancel_ts,
      'yes_filled_contracts': str(canceled_pair.yes_filled_contracts),
      'no_filled_contracts': str(canceled_pair.no_filled_contracts),
      'average_yes_price': str(canceled_pair.average_yes_price),
      'average_no_price': str(canceled_pair.average_no_price),
      'realized_fees_dollars': str(canceled_pair.realized_fees_dollars),
      'websocket_connected': canceled_pair.websocket_connected,
      **_submit_bridge_detail_fields(
        legacy_state='CANCELED',
        saved_set_snapshot=saved_set_snapshot,
        submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state='CANCELED'),
      ),
    },
  )
  persist_runtime_event(
    connection,
    level='INFO',
    event_type='cancel_applied',
    pair_id=plan.pair_id,
    recorded_at_utc=cancel_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'seq': cancel_seq,
      'ts_ms': cancel_ts_ms,
      'as_of_time': cancel_ts,
      'state': 'CANCELED',
      **_submit_bridge_detail_fields(
        legacy_state='CANCELED',
        saved_set_snapshot=saved_set_snapshot,
        submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state='CANCELED'),
      ),
    },
  )
  _append_event_packet(
    event_type='cancel_applied',
    execution_status='canceled',
    seq=cancel_seq,
    ts_ms=cancel_ts_ms,
    as_of_time=cancel_ts,
    order_id=canceled_orders[0].order_id,
    client_order_id=canceled_orders[0].client_order_id,
    outcome_side='yes',
    book_side='cancel',
    use_yes_price=True,
  )
  _append_event_packet(
    event_type='cancel_applied',
    execution_status='canceled',
    seq=cancel_seq,
    ts_ms=cancel_ts_ms,
    as_of_time=cancel_ts,
    order_id=canceled_orders[1].order_id,
    client_order_id=canceled_orders[1].client_order_id,
    outcome_side='no',
    book_side='cancel',
    use_yes_price=False,
  )
  locked_pnl = compute_locked_pnl(
    canceled_pair,
    fee_reserve_dollars=Decimal(str(settings.fee_reserve_dollars)),
  )
  persist_pnl_snapshot(
    connection,
    PairPnlSnapshot(
      pair_id=plan.pair_id,
      locked_contracts=Decimal(locked_pnl['locked_contracts']),
      gross_dollars=Decimal(locked_pnl['gross_dollars']),
      net_projected_dollars=Decimal(locked_pnl['net_projected_dollars']),
      net_realized_dollars=Decimal(locked_pnl['net_realized_dollars']),
      recorded_at=datetime.fromisoformat(cancel_ts),
    ),
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
  )

  return {
    'enabled': True,
    'profile': 'submit_order_bridge',
    'terminal_state': 'CANCELED',
    'sequence_count': sequence,
    'contract_version': TRANCHE_F_EXECUTION_EVENT_CONTRACT_VERSION,
    'event_packet': event_packet,
    'required_event_fields': [
      'event_type',
      'execution_status',
      'operation_lane',
      'lane_session_id',
      'market_ticker',
      'order_id',
      'client_order_id',
      'trade_id',
      'seq',
      'ts_ms',
      'as_of_time',
      'user_data_timestamp',
      'outcome_side',
      'book_side',
      'use_yes_price',
    ],
    'states': ['SUBMITTING', 'RESTING_BOTH', partial_pair.state, reconciled_pair.state, 'CANCELED'],
    'chronology': {
      'submit': {'as_of_time': submit_ts, 'seq': submit_seq, 'ts_ms': submit_ts_ms},
      'fill': {'as_of_time': partial_ts, 'seq': partial_seq, 'ts_ms': partial_ts_ms},
      'reconcile': {'as_of_time': reconcile_ts, 'seq': reconcile_seq, 'ts_ms': reconcile_ts_ms},
      'cancel': {'as_of_time': cancel_ts, 'seq': cancel_seq, 'ts_ms': cancel_ts_ms},
    },
  }


def _check_live_order_units(plan: Any) -> tuple[str, str, str, str] | None:
  """Pre-validate every outbound order money value against the Kalshi-units boundary
  BEFORE any order is placed, so an invalid value fails the whole pair atomically (no
  partial submission). Returns ``(side, value_dollars, blocked_reason, unit_reason)`` on
  the first invalid value, or ``None`` when all values convert cleanly."""
  for leg, price_value in (('yes', plan.yes_price), ('no', plan.no_price)):
    try:
      price_dollars_to_fp4(outbound_leg_price_dollars(leg, price_value))
    except KalshiUnitError as exc:
      return (leg, str(price_value), 'price_precision_invalid', exc.reason)
  try:
    count_contracts_to_int(plan.contract_count)
    group_limit_to_wire(plan.contract_count * 2)
  except KalshiUnitError as exc:
    blocked_reason = 'count_invalid' if exc.reason.startswith('count') else 'group_limit_invalid'
    return ('pair', str(plan.contract_count), blocked_reason, exc.reason)
  return None


def _emit_live_order_units_blocked(
  connection: Any,
  *,
  plan: Any,
  settings: Settings,
  lane_session_id: str,
  now_ts: str,
  side: str,
  value_dollars: str,
  blocked_reason: str,
  unit_reason: str,
  saved_set_snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
  """Fail-closed terminal for an invalid outbound order unit: CANCEL the pair, persist a
  names-only blocked event, and return the terminal chronology packet. Mirrors the
  zero-price guard shape; no order is placed."""
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state='CANCELED',
    recorded_at_utc=now_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'ticker': plan.ticker,
      'reason': blocked_reason,
      'yes_filled_contracts': '0',
      'no_filled_contracts': '0',
      'average_yes_price': str(plan.yes_price),
      'average_no_price': str(plan.no_price),
      'realized_fees_dollars': '0',
      'websocket_connected': False,
      **_submit_bridge_detail_fields(
        legacy_state='CANCELED',
        saved_set_snapshot=saved_set_snapshot,
        submit_response_id=_submit_bridge_response_id(blocked_reason=blocked_reason, legacy_state='CANCELED'),
      ),
    },
  )
  persist_runtime_event(
    connection,
    level='WARN',
    event_type='live_order_units_blocked',
    pair_id=plan.pair_id,
    recorded_at_utc=now_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'ticker': plan.ticker,
      'side': side,
      'value_dollars': value_dollars,
      'blocked_reason': blocked_reason,
      'unit_reason': unit_reason,
    },
  )
  return {
    'enabled': True,
    'profile': 'submit_order_bridge',
    'terminal_state': 'CANCELED',
    'blocked_reason': blocked_reason,
    'sequence_count': 0,
    'contract_version': TRANCHE_F_EXECUTION_EVENT_CONTRACT_VERSION,
    'event_packet': [],
  }


def _submitted_order_fill_count(order: Any) -> Decimal:
  fill_count = getattr(order, 'fill_count', None)
  if fill_count is None:
    fill_count = getattr(order, 'contract_count', Decimal('0')) - getattr(order, 'remaining_count', Decimal('0'))
  return max(Decimal('0'), Decimal(str(fill_count)))


def _submitted_order_cancelable(order: Any) -> bool:
  status = str(getattr(order, 'status', '') or '').strip().lower()
  remaining_count = max(Decimal('0'), Decimal(str(getattr(order, 'remaining_count', Decimal('0')))))
  if status in {'executed', 'filled', 'canceled', 'cancelled'}:
    return False
  return remaining_count > 0


def _submitted_order_remaining_count(order: Any) -> Decimal:
  return max(Decimal('0'), Decimal(str(getattr(order, 'remaining_count', Decimal('0')))))


def _blend_fill_price(
  existing_count: Decimal,
  existing_price: Decimal,
  add_count: Decimal,
  add_price: Decimal,
) -> Decimal:
  """Volume-weighted average fill price after a catch-up fill is added to a leg."""
  total = existing_count + add_count
  if total <= 0:
    return existing_price
  return ((existing_count * existing_price) + (add_count * add_price)) / total


def capture_pair_liquidity_observation(
  client: Any,
  connection: Any,
  *,
  pair_id: str,
  ticker: str,
  phase: str,
  orderbook: Any,
  intended_yes_price: Any,
  intended_no_price: Any,
  intended_contract_count: Any,
  market: Any,
  settings: Settings,
  recorded_at_utc: str,
  lane_session_id: str | None,
  recent_trades: dict[str, Any] | None = None,
  recent_trades_read_failed: bool = False,
) -> None:
  """Lane A: record one authoritative liquidity/flow observation (fail-soft).

  Reuses an orderbook already read on the decision path and makes one bounded trades
  read for per-side final-window flow. Never raises into the order path -- on any
  read/compute failure it writes a row with empty ladders and a ``readback_failed``
  marker, so the gap itself becomes evidence. Additive instrumentation only."""
  observation: dict[str, Any]
  try:
    band = summarize_depth_within_band(
      orderbook,
      Decimal(str(intended_yes_price)),
      Decimal(str(intended_no_price)),
    )
    if recent_trades_read_failed:
      raise RuntimeError('recent trades read failed')
    flow = recent_trades if recent_trades is not None else client.get_recent_trades(ticker, window_sec=settings.flow_window_sec)
    divergence = abs(Decimal(str(intended_yes_price)) - Decimal(str(intended_no_price)))
    observation = {
      'readback_status': 'ok',
      'yes_bid_depth_json': json.dumps(band['yes_bid_depth_json']),
      'no_bid_depth_json': json.dumps(band['no_bid_depth_json']),
      'best_yes_bid': band['best_yes_bid'],
      'best_no_bid': band['best_no_bid'],
      'yes_depth_within_band': band['yes_depth_within_band'],
      'no_depth_within_band': band['no_depth_within_band'],
      'yes_flow_window_fp': flow.get('yes_flow_fp'),
      'no_flow_window_fp': flow.get('no_flow_fp'),
      'flow_window_sec': settings.flow_window_sec,
      'divergence': divergence,
      'volume_24h_fp': getattr(market, 'volume_24h_fp', None),
      'volume_fp': getattr(market, 'volume_fp', None),
      'open_interest_fp': getattr(market, 'open_interest_fp', None),
      'intended_yes_price': intended_yes_price,
      'intended_no_price': intended_no_price,
      'intended_contract_count': intended_contract_count,
    }
  except Exception:  # pragma: no cover - capture must never break the order path
    observation = {
      'readback_status': 'readback_failed',
      'yes_bid_depth_json': '[]',
      'no_bid_depth_json': '[]',
      'flow_window_sec': getattr(settings, 'flow_window_sec', None),
      'intended_yes_price': intended_yes_price,
      'intended_no_price': intended_no_price,
      'intended_contract_count': intended_contract_count,
      'divergence': None,
    }
  try:
    persist_pair_liquidity_observation(
      connection,
      pair_id=pair_id,
      ticker=ticker,
      phase=phase,
      operation_lane=settings.operation_lane,
      recorded_at_utc=recorded_at_utc,
      observation=observation,
      lane_session_id=lane_session_id,
    )
  except Exception:  # pragma: no cover - never break the order path on a persist failure
    pass


def _emit_unmatched_exposure_alert(
  connection: Any,
  *,
  settings: Settings,
  plan: Any,
  created_at_utc: str,
  detail: dict[str, Any],
) -> None:
  """Names-only operator alert when a one-sided fill is frozen to ERROR (SSOT step 6).

  Never raises into the order path; alert failure is logged but not fatal."""
  try:
    profile_token = resolve_active_profile_token(
      connection,
      settings.operation_lane,
      key_path=settings.private_key_file,
    )
    persist_operator_notification(
      connection,
      created_at_utc=created_at_utc,
      operation_lane=settings.operation_lane,
      profile_token=profile_token,
      level='error',
      title='Unmatched exposure frozen for review',
      body=(
        'A paired entry left one-sided exposure that could not be hedged within the edge '
        'and was frozen for operator review. Ticker {ticker}; unmatched {unmatched} contracts.'.format(
          ticker=str(plan.ticker),
          unmatched=str(detail.get('unmatched_contracts', '')),
        )
      ),
      source='unmatched_exposure_resolution',
    )
  except Exception as exc:  # pragma: no cover - alert must never break the order path
    persist_runtime_event(
      connection,
      level='WARN',
      event_type='unmatched_exposure_alert_failed',
      pair_id=plan.pair_id,
      recorded_at_utc=created_at_utc,
      operation_lane=settings.operation_lane,
      lane_session_id='',
      detail={'ticker': str(plan.ticker), 'error_family': type(exc).__name__},
    )


def _fresh_market_close_posture(client: Any, ticker: str, *, as_of: datetime) -> dict[str, Any]:
  get_market = getattr(client, 'get_market', None)
  if not callable(get_market):
    return {
      'fresh_close_readback_status': 'unavailable_client_no_get_market',
      'fresh_seconds_to_close': None,
      'fresh_market_status': '',
    }
  try:
    market = get_market(ticker)
  except Exception as exc:
    return {
      'fresh_close_readback_status': 'failed',
      'fresh_close_readback_error': type(exc).__name__,
      'fresh_seconds_to_close': None,
      'fresh_market_status': '',
    }
  close_time = getattr(market, 'close_time', None)
  market_status = str(getattr(market, 'status', '') or '').strip().lower()
  if close_time is None:
    return {
      'fresh_close_readback_status': 'missing_close_time',
      'fresh_seconds_to_close': None,
      'fresh_market_status': market_status,
    }
  if close_time.tzinfo is None:
    close_time = close_time.replace(tzinfo=UTC)
  seconds_to_close = int((close_time - as_of).total_seconds())
  return {
    'fresh_close_readback_status': 'ok',
    'fresh_seconds_to_close': max(0, seconds_to_close),
    'fresh_market_status': market_status,
  }


def _position_readback_detail(client: Any) -> dict[str, Any]:
  try:
    positions = client.get_positions()
  except Exception as exc:
    return {
      'positions_readback_status': 'failed',
      'positions_error_family': type(exc).__name__,
    }
  return {
    'positions_readback_status': 'ok',
    'positions': [
      {
        'ticker': str(position.ticker),
        'side': str(position.side),
        'contract_count': str(position.contract_count),
        'average_price_dollars': str(position.average_price_dollars),
        'realized_pnl_dollars': str(position.realized_pnl_dollars),
        'fees_dollars': str(position.fees_dollars),
      }
      for position in positions
    ],
  }


def _persist_live_leg_fill(
  connection: Any,
  *,
  plan: Any,
  order: Any,
  side: str,
  price_dollars: Decimal,
  contract_count: Decimal,
  created_at_iso: str,
  operation_lane: str,
  replace_existing: bool = True,
) -> None:
  if contract_count <= 0:
    return
  fill_id = f'{plan.pair_id}-fill-{side}'
  if not replace_existing:
    existing = connection.execute('SELECT 1 FROM fills WHERE fill_id = ? LIMIT 1', (fill_id,)).fetchone()
    if existing is not None:
      return
  client_order_id = str(getattr(order, 'client_order_id', '') or '')
  if not client_order_id:
    client_order_id = plan.yes_client_order_id if side == 'yes' else plan.no_client_order_id
  persist_fill(
    connection,
    FillEvent(
      fill_id=fill_id,
      pair_id=plan.pair_id,
      order_id=str(getattr(order, 'order_id', '') or ''),
      client_order_id=client_order_id,
      side=side,
      price_dollars=price_dollars,
      contract_count=contract_count,
      fee_dollars=Decimal('0'),
      created_at=datetime.fromisoformat(created_at_iso),
    ),
    operation_lane=operation_lane,
  )


def _live_order_payloads_for_batch_plan(plan: Any, *, order_group_id: str) -> list[dict[str, object]]:
  return [
    {
      'ticker': plan.ticker,
      'side': 'yes',
      'yes_price': plan.yes_price,
      'count': plan.contract_count,
      'client_order_id': plan.yes_client_order_id,
      'time_in_force': plan.time_in_force,
      'post_only': plan.post_only,
      'cancel_order_on_pause': plan.cancel_order_on_pause,
      'subaccount': plan.subaccount,
      'order_group_id': order_group_id,
    },
    {
      'ticker': plan.ticker,
      'side': 'no',
      'no_price': plan.no_price,
      'count': plan.contract_count,
      'client_order_id': plan.no_client_order_id,
      'time_in_force': plan.time_in_force,
      'post_only': plan.post_only,
      'cancel_order_on_pause': plan.cancel_order_on_pause,
      'subaccount': plan.subaccount,
      'order_group_id': order_group_id,
    },
  ]


def _live_batch_chronology(terminal_state: str, *, blocked_reason: str | None = None) -> dict[str, Any]:
  chronology: dict[str, Any] = {
    'enabled': True,
    'profile': 'submit_order_bridge',
    'terminal_state': terminal_state,
    'sequence_count': 0,
    'contract_version': TRANCHE_F_EXECUTION_EVENT_CONTRACT_VERSION,
    'event_packet': [],
  }
  if blocked_reason:
    chronology['blocked_reason'] = blocked_reason
  return chronology


def _persist_live_batch_terminal(
  connection: Any,
  *,
  plan: Any,
  settings: Settings,
  lane_session_id: str,
  saved_set_snapshot: dict[str, Any] | None,
  state: str,
  reason: str,
  level: str = 'WARN',
  detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
  event_ts = datetime.now(UTC).isoformat()
  state_detail = {
    'ticker': plan.ticker,
    'reason': reason,
    'yes_filled_contracts': '0',
    'no_filled_contracts': '0',
    'average_yes_price': str(plan.yes_price),
    'average_no_price': str(plan.no_price),
    'realized_fees_dollars': '0',
    'websocket_connected': False,
    **(detail or {}),
    **_submit_bridge_detail_fields(
      legacy_state=state,
      saved_set_snapshot=saved_set_snapshot,
      submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state=state),
    ),
  }
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state=state,
    recorded_at_utc=event_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail=state_detail,
  )
  persist_runtime_event(
    connection,
    level=level,
    event_type='live_order_batch_submit_result',
    pair_id=plan.pair_id,
    recorded_at_utc=event_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={'ticker': plan.ticker, 'terminal_state': state, 'reason': reason, **(detail or {})},
  )
  return _live_batch_chronology(state, blocked_reason=reason if state == 'CANCELED' else None)


def _orders_by_client_order_id(orders: list[SubmittedOrder]) -> tuple[dict[str, SubmittedOrder], set[str], list[SubmittedOrder], set[str]]:
  by_client_order_id: dict[str, SubmittedOrder] = {}
  duplicate_ids: set[str] = set()
  duplicate_order_ids: set[str] = set()
  seen_order_ids: set[str] = set()
  malformed: list[SubmittedOrder] = []
  for order in orders:
    client_order_id = str(getattr(order, 'client_order_id', '') or '')
    order_id = str(getattr(order, 'order_id', '') or '')
    if not client_order_id or not order_id:
      malformed.append(order)
      continue
    if order_id in seen_order_ids:
      duplicate_order_ids.add(order_id)
    else:
      seen_order_ids.add(order_id)
    if client_order_id in by_client_order_id:
      duplicate_ids.add(client_order_id)
      continue
    by_client_order_id[client_order_id] = order
  return by_client_order_id, duplicate_ids, malformed, duplicate_order_ids


def _batch_pair_by_client_id(plans: list[Any]) -> dict[str, Any]:
  pair_by_client_id: dict[str, Any] = {}
  for plan in plans:
    pair_by_client_id[str(plan.yes_client_order_id)] = plan
    pair_by_client_id[str(plan.no_client_order_id)] = plan
  return pair_by_client_id


def _attribute_batch_order_to_pair(order: SubmittedOrder, plans: list[Any], pair_by_client_id: dict[str, Any]) -> str:
  client_order_id = str(getattr(order, 'client_order_id', '') or '').strip()
  if client_order_id in pair_by_client_id:
    return str(pair_by_client_id[client_order_id].pair_id)
  ticker = str(getattr(order, 'ticker', '') or '').strip()
  if ticker:
    matching_plans = [plan for plan in plans if str(plan.ticker) == ticker]
    if len(matching_plans) == 1:
      return str(matching_plans[0].pair_id)
  return ''


def _classify_batch_pair_acceptance(
  *,
  plans: list[Any],
  orders_by_client_id: dict[str, SubmittedOrder],
  duplicate_ids: set[str],
  malformed_orders: list[SubmittedOrder],
  duplicate_order_ids: set[str],
) -> dict[str, BatchPairAcceptanceClassification]:
  pair_by_client_id = _batch_pair_by_client_id(plans)
  expected_ids_by_pair_id = {
    str(plan.pair_id): {str(plan.yes_client_order_id), str(plan.no_client_order_id)}
    for plan in plans
  }
  duplicate_ids_by_pair_id: dict[str, set[str]] = {str(plan.pair_id): set() for plan in plans}
  unknown_ids_by_pair_id: dict[str, set[str]] = {str(plan.pair_id): set() for plan in plans}
  malformed_count_by_pair_id: dict[str, int] = {str(plan.pair_id): 0 for plan in plans}
  duplicate_remote_ids_by_pair_id: dict[str, set[str]] = {str(plan.pair_id): set() for plan in plans}
  global_ambiguous_pair_ids: set[str] = set()

  for duplicate_id in duplicate_ids:
    plan = pair_by_client_id.get(str(duplicate_id))
    if plan is not None:
      duplicate_ids_by_pair_id[str(plan.pair_id)].add(str(duplicate_id))
    else:
      global_ambiguous_pair_ids.update(str(plan.pair_id) for plan in plans)

  for client_order_id, order in orders_by_client_id.items():
    if client_order_id not in pair_by_client_id:
      pair_id = _attribute_batch_order_to_pair(order, plans, pair_by_client_id)
      if pair_id:
        unknown_ids_by_pair_id[pair_id].add(client_order_id)
      else:
        global_ambiguous_pair_ids.update(str(plan.pair_id) for plan in plans)

  order_id_to_pair_ids: dict[str, set[str]] = {}
  for order in orders_by_client_id.values():
    order_id = str(getattr(order, 'order_id', '') or '').strip()
    if not order_id:
      continue
    pair_id = _attribute_batch_order_to_pair(order, plans, pair_by_client_id)
    if pair_id:
      order_id_to_pair_ids.setdefault(order_id, set()).add(pair_id)
  for order_id in duplicate_order_ids:
    touched_pair_ids = order_id_to_pair_ids.get(str(order_id), set())
    if not touched_pair_ids:
      global_ambiguous_pair_ids.update(str(plan.pair_id) for plan in plans)
      continue
    for pair_id in touched_pair_ids:
      duplicate_remote_ids_by_pair_id[pair_id].add(str(order_id))
    if len(touched_pair_ids) > 1:
      global_ambiguous_pair_ids.update(touched_pair_ids)

  for malformed_order in malformed_orders:
    pair_id = _attribute_batch_order_to_pair(malformed_order, plans, pair_by_client_id)
    if pair_id:
      malformed_count_by_pair_id[pair_id] += 1
      continue
    status = str(getattr(malformed_order, 'status', '') or '').strip().lower()
    order_id = str(getattr(malformed_order, 'order_id', '') or '').strip()
    remaining_count = getattr(malformed_order, 'remaining_count', Decimal('0'))
    fill_count = getattr(malformed_order, 'fill_count', Decimal('0'))
    if order_id or status in {'resting', 'open', 'pending', 'executed', 'filled'} or remaining_count > 0 or fill_count > 0:
      global_ambiguous_pair_ids.update(str(plan.pair_id) for plan in plans)

  classifications: dict[str, BatchPairAcceptanceClassification] = {}
  for plan in plans:
    pair_id = str(plan.pair_id)
    yes_order = orders_by_client_id.get(str(plan.yes_client_order_id))
    no_order = orders_by_client_id.get(str(plan.no_client_order_id))
    accepted_count = int(yes_order is not None) + int(no_order is not None)
    missing = tuple(sorted(expected_ids_by_pair_id[pair_id] - set(orders_by_client_id)))
    reasons: list[str] = []
    if missing:
      reasons.append('missing_client_order_ids')
    if malformed_count_by_pair_id[pair_id]:
      reasons.append('malformed_rows')
    if duplicate_ids_by_pair_id[pair_id]:
      reasons.append('duplicate_client_order_ids')
    if duplicate_remote_ids_by_pair_id[pair_id]:
      reasons.append('duplicate_remote_order_ids')
    if unknown_ids_by_pair_id[pair_id]:
      reasons.append('unknown_client_order_ids')
    if pair_id in global_ambiguous_pair_ids:
      reasons.append('global_ambiguity')

    if pair_id in global_ambiguous_pair_ids:
      classification: Literal['both_accepted', 'none_accepted', 'partial_or_ambiguous', 'global_ambiguous'] = 'global_ambiguous'
    elif accepted_count == 2 and not reasons:
      classification = 'both_accepted'
    elif accepted_count == 0:
      classification = 'none_accepted'
      if not reasons:
        reasons.append('no_accepted_rows')
    else:
      classification = 'partial_or_ambiguous'

    classifications[pair_id] = BatchPairAcceptanceClassification(
      pair_id=pair_id,
      ticker=str(plan.ticker),
      yes_order=yes_order,
      no_order=no_order,
      classification=classification,
      missing_client_order_ids=missing,
      malformed_order_count=malformed_count_by_pair_id[pair_id],
      duplicate_client_order_ids=tuple(sorted(duplicate_ids_by_pair_id[pair_id])),
      duplicate_remote_order_ids=tuple(sorted(duplicate_remote_ids_by_pair_id[pair_id])),
      unknown_client_order_ids=tuple(sorted(unknown_ids_by_pair_id[pair_id])),
      classification_reasons=tuple(dict.fromkeys(reasons)),
    )
  return classifications


def _readback_batch_orders_by_client_id(
  client: Any,
  *,
  plans: list[Any],
  recorded_at: datetime,
) -> dict[str, SubmittedOrder]:
  if not hasattr(client, 'list_orders_for_batch_readback'):
    return {}
  wanted = {
    str(plan.yes_client_order_id): plan
    for plan in plans
  } | {
    str(plan.no_client_order_id): plan
    for plan in plans
  }
  found: dict[str, SubmittedOrder] = {}
  min_ts = int(recorded_at.timestamp() * 1000) - 30000
  max_ts = int(datetime.now(UTC).timestamp() * 1000) + 30000
  for ticker in sorted({str(plan.ticker) for plan in plans}):
    try:
      raw_orders = client.list_orders_for_batch_readback(
        ticker=ticker,
        status=None,
        min_ts=min_ts,
        max_ts=max_ts,
        limit=100,
        max_pages=3,
      )
    except Exception:
      continue
    for raw_order in raw_orders:
      client_order_id = str(raw_order.get('client_order_id') or '')
      if client_order_id in wanted and client_order_id not in found:
        found[client_order_id] = submitted_order_from_payload(raw_order)
  return found


def _partial_ambiguous_cleanup_for_order(client: Any, order: SubmittedOrder) -> tuple[dict[str, Any], list[dict[str, Any]]]:
  result = {
    'side': str(order.side),
    'client_order_id': str(order.client_order_id),
    'order_id': str(order.order_id),
    'initial_status': str(order.status),
  }
  statuses: list[dict[str, Any]] = []
  try:
    latest = client.get_order(order.order_id)
  except Exception as exc:
    result.update({
      'readback_status': 'failed',
      'cleanup_action': 'readback_failed_no_cancel',
      'error_family': type(exc).__name__,
    })
    return result, statuses

  status = str(getattr(latest, 'status', '') or '').strip().lower()
  statuses.append({'order_id': latest.order_id, 'status': latest.status})
  result.update({
    'readback_status': 'ok',
    'latest_status': status,
  })
  try:
    fill_count = _submitted_order_fill_count(latest)
    remaining_count = _submitted_order_remaining_count(latest)
  except Exception as exc:
    result.update({
      'cleanup_action': 'malformed_status_no_cancel',
      'error_family': type(exc).__name__,
    })
    return result, statuses

  result.update({
    'fill_count': str(fill_count),
    'remaining_count': str(remaining_count),
  })
  if status in {'canceled', 'cancelled'}:
    result['cleanup_action'] = 'already_canceled'
    return result, statuses
  if status in {'executed', 'filled'} or remaining_count == 0:
    result['cleanup_action'] = 'already_filled_no_cancel'
    return result, statuses
  if fill_count > 0 and remaining_count > 0:
    result['cleanup_action'] = 'partial_fill_cancel_attempted'
  elif fill_count == 0 and remaining_count > 0:
    result['cleanup_action'] = 'zero_fill_cancel_attempted'
  else:
    result['cleanup_action'] = 'malformed_status_no_cancel'
    return result, statuses

  try:
    cancel_result = client.cancel_order_v2(latest.order_id)
    result['cancel_status'] = str(cancel_result.get('status') or 'requested') if isinstance(cancel_result, dict) else 'requested'
  except Exception as exc:
    result.update({
      'cancel_status': 'failed',
      'cancel_error_family': type(exc).__name__,
    })

  try:
    post_cancel = client.get_order(latest.order_id)
    statuses.append({'order_id': post_cancel.order_id, 'status': post_cancel.status})
    result.update({
      'post_cancel_readback_status': 'ok',
      'post_cancel_status': str(getattr(post_cancel, 'status', '') or '').strip().lower(),
      'post_cancel_fill_count': str(_submitted_order_fill_count(post_cancel)),
      'post_cancel_remaining_count': str(_submitted_order_remaining_count(post_cancel)),
    })
  except Exception as exc:
    result.update({
      'post_cancel_readback_status': 'failed',
      'post_cancel_error_family': type(exc).__name__,
    })
  return result, statuses


def _validate_accepted_pair_settlement_input(settlement_input: AcceptedPairSettlementInput) -> None:
  plan = settlement_input.plan
  if settlement_input.dispatch_index < 0:
    raise ValueError('Accepted settlement dispatch_index must be non-negative')
  if not str(plan.pair_id or '').strip():
    raise ValueError('Accepted settlement plan pair_id is required')
  if not str(plan.ticker or '').strip():
    raise ValueError('Accepted settlement plan ticker is required')
  if not str(settlement_input.order_group_id or '').strip():
    raise ValueError('Accepted settlement order_group_id is required')
  if settlement_input.submit_mode not in {'single_create_v2', 'batch_create_v2'}:
    raise ValueError('Accepted settlement submit_mode is invalid')
  if settlement_input.yes_order.client_order_id != plan.yes_client_order_id:
    raise ValueError('Accepted settlement YES client_order_id mismatch')
  if settlement_input.no_order.client_order_id != plan.no_client_order_id:
    raise ValueError('Accepted settlement NO client_order_id mismatch')
  if settlement_input.yes_order.ticker != plan.ticker or settlement_input.no_order.ticker != plan.ticker:
    raise ValueError('Accepted settlement ticker mismatch')
  if settlement_input.yes_order.side != 'yes' or settlement_input.no_order.side != 'no':
    raise ValueError('Accepted settlement side mismatch')
  if not settlement_input.yes_order.order_id or not settlement_input.no_order.order_id:
    raise ValueError('Accepted settlement remote order IDs are required')


def _register_accepted_pair_orders(
  connection: Any,
  *,
  settlement_input: AcceptedPairSettlementInput,
  settings: Settings,
  lane_session_id: str,
  reason: str = 'batch_submit_accepted',
) -> None:
  _validate_accepted_pair_settlement_input(settlement_input)
  plan = settlement_input.plan
  yes_order = settlement_input.yes_order
  no_order = settlement_input.no_order
  resting_ts = datetime.now(UTC).isoformat()
  promote_order_id(
    connection,
    operation_lane=settings.operation_lane,
    pair_id=plan.pair_id,
    client_order_id=plan.yes_client_order_id,
    side='yes',
    remote_order_id=yes_order.order_id,
    status=yes_order.status,
  )
  promote_order_id(
    connection,
    operation_lane=settings.operation_lane,
    pair_id=plan.pair_id,
    client_order_id=plan.no_client_order_id,
    side='no',
    remote_order_id=no_order.order_id,
    status=no_order.status,
  )
  persist_order_statuses(
    connection,
    operation_lane=settings.operation_lane,
    statuses=[
      {'order_id': yes_order.order_id, 'status': yes_order.status},
      {'order_id': no_order.order_id, 'status': no_order.status},
    ],
  )
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state='RESTING_BOTH',
    recorded_at_utc=resting_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'ticker': plan.ticker,
      'reason': reason,
      'submit_mode': settlement_input.submit_mode,
      'order_group_id': settlement_input.order_group_id,
      'yes_order_id': yes_order.order_id,
      'no_order_id': no_order.order_id,
      'yes_filled_contracts': str(_submitted_order_fill_count(yes_order)),
      'no_filled_contracts': str(_submitted_order_fill_count(no_order)),
      'average_yes_price': str(yes_order.price_dollars or plan.yes_price),
      'average_no_price': str(no_order.price_dollars or plan.no_price),
      'realized_fees_dollars': '0',
      'websocket_connected': False,
      **_submit_bridge_detail_fields(
        legacy_state='RESTING_BOTH',
        saved_set_snapshot=settlement_input.saved_set_snapshot,
        submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state='RESTING_BOTH'),
      ),
    },
  )


def _settle_live_pair_orders_after_acceptance(
  client: Any,
  connection: Any,
  *,
  settlement_input: AcceptedPairSettlementInput,
  settings: Settings,
  lane_session_id: str,
  recorded_at: datetime,
  registration_done: bool = False,
  skip_observe: bool = False,
  shelter_trigger_detail: dict[str, Any] | None = None,
  observed_elapsed_sec: int = 0,
) -> dict[str, Any]:
  _validate_accepted_pair_settlement_input(settlement_input)
  if not registration_done:
    _register_accepted_pair_orders(
      connection,
      settlement_input=settlement_input,
      settings=settings,
      lane_session_id=lane_session_id,
      reason='batch_submit_accepted' if settlement_input.submit_mode == 'batch_create_v2' else 'live_order_accepted',
    )

  plan = settlement_input.plan
  yes_order = settlement_input.yes_order
  no_order = settlement_input.no_order
  saved_set_snapshot = settlement_input.saved_set_snapshot
  shelter_window_sec = max(0, int(settings.max_unhedged_sec))
  poll_interval_sec = 5
  yes_filled = Decimal('0')
  no_filled = Decimal('0')
  yes_fill_price = plan.yes_price
  no_fill_price = plan.no_price
  partial_emitted = False

  if shelter_trigger_detail is None:
    close_posture = _fresh_market_close_posture(client, plan.ticker, as_of=datetime.now(UTC))
    initial_seconds_to_close = close_posture.get('fresh_seconds_to_close')
    if isinstance(initial_seconds_to_close, int):
      max_observe_sec = max(0, initial_seconds_to_close - shelter_window_sec)
    else:
      max_observe_sec = 0
    shelter_trigger_detail = {
      'shelter_window_sec': shelter_window_sec,
      **close_posture,
    }
  else:
    initial_seconds_to_close = shelter_trigger_detail.get('fresh_seconds_to_close')
    max_observe_sec = 0

  while not skip_observe:
    fresh_seconds_to_close = shelter_trigger_detail.get('fresh_seconds_to_close')
    if isinstance(fresh_seconds_to_close, int) and fresh_seconds_to_close <= shelter_window_sec:
      shelter_trigger_detail['shelter_trigger_source'] = 'fresh_close_readback'
      break
    if observed_elapsed_sec >= max_observe_sec:
      shelter_trigger_detail['shelter_trigger_source'] = (
        'fresh_close_readback_unavailable'
        if not isinstance(fresh_seconds_to_close, int)
        else 'observed_elapsed_close_projection'
      )
      break

    time.sleep(poll_interval_sec)
    observed_elapsed_sec += poll_interval_sec

    try:
      yes_state = client.get_order(yes_order.order_id)
      no_state = client.get_order(no_order.order_id)
    except Exception:
      break

    yes_filled = _submitted_order_fill_count(yes_state)
    no_filled = _submitted_order_fill_count(no_state)
    if yes_filled > 0:
      yes_fill_price = yes_state.price_dollars
    if no_filled > 0:
      no_fill_price = no_state.price_dollars

    if (yes_filled > 0 or no_filled > 0) and not partial_emitted:
      partial_ts = datetime.now(UTC).isoformat()
      partial_emitted = True
      partial_state = 'PARTIAL_ONE_SIDE'
      persist_pair_state_transition(
        connection,
        pair_id=plan.pair_id,
        state=partial_state,
        recorded_at_utc=partial_ts,
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        detail={
          'ticker': plan.ticker,
          'yes_filled_contracts': str(yes_filled),
          'no_filled_contracts': str(no_filled),
          'average_yes_price': str(yes_fill_price),
          'average_no_price': str(no_fill_price),
          'realized_fees_dollars': '0',
          'websocket_connected': False,
          **_submit_bridge_detail_fields(
            legacy_state=partial_state,
            saved_set_snapshot=saved_set_snapshot,
            submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state=partial_state),
          ),
        },
      )
      if yes_filled > 0:
        _persist_live_leg_fill(
          connection,
          plan=plan,
          order=yes_state,
          side='yes',
          price_dollars=yes_fill_price,
          contract_count=yes_filled,
          created_at_iso=partial_ts,
          operation_lane=settings.operation_lane,
        )
      if no_filled > 0:
        _persist_live_leg_fill(
          connection,
          plan=plan,
          order=no_state,
          side='no',
          price_dollars=no_fill_price,
          contract_count=no_filled,
          created_at_iso=partial_ts,
          operation_lane=settings.operation_lane,
        )

    if yes_filled >= plan.contract_count and no_filled >= plan.contract_count:
      filled_ts = datetime.now(UTC).isoformat()
      persist_order_statuses(
        connection,
        operation_lane=settings.operation_lane,
        statuses=[
          {'order_id': yes_order.order_id, 'status': yes_state.status},
          {'order_id': no_order.order_id, 'status': no_state.status},
        ],
      )
      _persist_live_leg_fill(
        connection,
        plan=plan,
        order=yes_state,
        side='yes',
        price_dollars=yes_fill_price,
        contract_count=yes_filled,
        created_at_iso=filled_ts,
        operation_lane=settings.operation_lane,
        replace_existing=False,
      )
      _persist_live_leg_fill(
        connection,
        plan=plan,
        order=no_state,
        side='no',
        price_dollars=no_fill_price,
        contract_count=no_filled,
        created_at_iso=filled_ts,
        operation_lane=settings.operation_lane,
        replace_existing=False,
      )
      persist_pair_state_transition(
        connection,
        pair_id=plan.pair_id,
        state='FILLED',
        recorded_at_utc=filled_ts,
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        detail={
          'ticker': plan.ticker,
          'reason': 'both_legs_filled',
          'yes_filled_contracts': str(yes_filled),
          'no_filled_contracts': str(no_filled),
          'average_yes_price': str(yes_fill_price),
          'average_no_price': str(no_fill_price),
          'realized_fees_dollars': '0',
          'websocket_connected': False,
        },
      )
      persist_runtime_event(
        connection,
        level='INFO',
        event_type='live_orders_both_filled',
        pair_id=plan.pair_id,
        recorded_at_utc=filled_ts,
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        detail={
          'yes_filled_contracts': str(yes_filled),
          'no_filled_contracts': str(no_filled),
          'average_yes_price': str(yes_fill_price),
          'average_no_price': str(no_fill_price),
          'submit_mode': settlement_input.submit_mode,
        },
      )
      return _live_batch_chronology('FILLED')

    close_posture = _fresh_market_close_posture(client, plan.ticker, as_of=datetime.now(UTC))
    fresh_seconds_to_close = close_posture.get('fresh_seconds_to_close')
    projected_seconds_to_close = (
      max(0, initial_seconds_to_close - observed_elapsed_sec)
      if isinstance(initial_seconds_to_close, int)
      else None
    )
    if isinstance(projected_seconds_to_close, int) and (
      not isinstance(fresh_seconds_to_close, int) or projected_seconds_to_close < fresh_seconds_to_close
    ):
      close_posture = {**close_posture, 'fresh_seconds_to_close': projected_seconds_to_close}
    shelter_trigger_detail = {
      'shelter_window_sec': shelter_window_sec,
      'observed_elapsed_sec': observed_elapsed_sec,
      **close_posture,
    }

  cancel_ts = datetime.now(UTC).isoformat()
  shelter_trigger_detail = {
    'shelter_window_sec': shelter_window_sec,
    'observed_elapsed_sec': observed_elapsed_sec,
    **(shelter_trigger_detail or {}),
  }
  try:
    yes_state = client.get_order(yes_order.order_id)
    no_state = client.get_order(no_order.order_id)
  except Exception as exc:
    position_detail = _position_readback_detail(client)
    persist_pair_state_transition(
      connection,
      pair_id=plan.pair_id,
      state='RECONCILE_REQUIRED',
      recorded_at_utc=cancel_ts,
      operation_lane=settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={
        'ticker': plan.ticker,
        'reason': 'live_order_shelter_readback_failed',
        'error_family': type(exc).__name__,
        'yes_filled_contracts': str(yes_filled),
        'no_filled_contracts': str(no_filled),
        'average_yes_price': str(yes_fill_price),
        'average_no_price': str(no_fill_price),
        'realized_fees_dollars': '0',
        'websocket_connected': False,
        **shelter_trigger_detail,
        **position_detail,
        **_submit_bridge_detail_fields(
          legacy_state='RECONCILE_REQUIRED',
          saved_set_snapshot=saved_set_snapshot,
          submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state='RECONCILE_REQUIRED'),
        ),
      },
    )
    return _live_batch_chronology('RECONCILE_REQUIRED')

  yes_filled = _submitted_order_fill_count(yes_state)
  no_filled = _submitted_order_fill_count(no_state)
  if yes_filled > 0:
    yes_fill_price = yes_state.price_dollars
  if no_filled > 0:
    no_fill_price = no_state.price_dollars

  if yes_filled == 0 and no_filled == 0:
    cancel_targets = {'yes', 'no'}
    repair_leg = ''
    ahead_leg = ''
  elif yes_filled > no_filled:
    cancel_targets = {'yes'}
    repair_leg = 'no'
    ahead_leg = 'yes'
  elif no_filled > yes_filled:
    cancel_targets = {'no'}
    repair_leg = 'yes'
    ahead_leg = 'no'
  else:
    cancel_targets = {'yes', 'no'}
    repair_leg = ''
    ahead_leg = ''

  cancel_results: list[dict[str, str]] = []
  for leg, state in (('yes', yes_state), ('no', no_state)):
    if leg not in cancel_targets:
      cancel_results.append({'leg': leg, 'order_id': state.order_id, 'status': 'preserved_repair_order'})
      continue
    if not _submitted_order_cancelable(state):
      cancel_results.append({'leg': leg, 'order_id': state.order_id, 'status': 'not_cancelable'})
      continue
    try:
      client.cancel_order_v2(state.order_id)
      cancel_results.append({'leg': leg, 'order_id': state.order_id, 'status': 'cancel_requested'})
    except KalshiHttpError as exc:
      cancel_results.append({
        'leg': leg,
        'order_id': state.order_id,
        'status': 'cancel_failed',
        **kalshi_error_safe_detail(exc),
      })

  try:
    yes_final = client.get_order(yes_order.order_id)
    no_final = client.get_order(no_order.order_id)
  except Exception as exc:
    position_detail = _position_readback_detail(client)
    persist_pair_state_transition(
      connection,
      pair_id=plan.pair_id,
      state='RECONCILE_REQUIRED',
      recorded_at_utc=cancel_ts,
      operation_lane=settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={
        'ticker': plan.ticker,
        'reason': 'live_order_post_shelter_readback_failed',
        'error_family': type(exc).__name__,
        'cancel_results': cancel_results,
        'yes_filled_contracts': str(yes_filled),
        'no_filled_contracts': str(no_filled),
        'average_yes_price': str(yes_fill_price),
        'average_no_price': str(no_fill_price),
        'realized_fees_dollars': '0',
        'websocket_connected': False,
        **shelter_trigger_detail,
        **position_detail,
        **_submit_bridge_detail_fields(
          legacy_state='RECONCILE_REQUIRED',
          saved_set_snapshot=saved_set_snapshot,
          submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state='RECONCILE_REQUIRED'),
        ),
      },
    )
    return _live_batch_chronology('RECONCILE_REQUIRED')

  yes_filled = _submitted_order_fill_count(yes_final)
  no_filled = _submitted_order_fill_count(no_final)
  if yes_filled > 0:
    yes_fill_price = yes_final.price_dollars
  if no_filled > 0:
    no_fill_price = no_final.price_dollars
  yes_status = str(yes_final.status or '').strip().lower()
  no_status = str(no_final.status or '').strip().lower()
  both_zero_canceled = (
    yes_filled == 0
    and no_filled == 0
    and yes_status in {'canceled', 'cancelled'}
    and no_status in {'canceled', 'cancelled'}
  )
  both_filled = yes_filled >= plan.contract_count and no_filled >= plan.contract_count
  any_fill = yes_filled > 0 or no_filled > 0
  cancel_failed = any(item.get('status') == 'cancel_failed' for item in cancel_results)
  unmatched_contracts = abs(yes_filled - no_filled)
  repair_state = no_final if repair_leg == 'no' else yes_final if repair_leg == 'yes' else None
  repair_remaining = _submitted_order_remaining_count(repair_state) if repair_state is not None else Decimal('0')
  repair_status = str(getattr(repair_state, 'status', '') or '').strip().lower() if repair_state is not None else ''
  repair_live = repair_remaining > 0 and repair_status not in {'canceled', 'cancelled', 'executed', 'filled'}
  position_detail = _position_readback_detail(client) if any_fill or cancel_failed else {}
  if both_filled:
    terminal_state = 'FILLED'
    terminal_reason = 'both_legs_filled_after_shelter_readback'
  elif both_zero_canceled and not cancel_failed:
    terminal_state = 'CANCELED'
    terminal_reason = 'shelter_window_no_fill_canceled'
  elif unmatched_contracts > 0 and cancel_failed:
    terminal_state = 'RECONCILE_REQUIRED'
    terminal_reason = 'asymmetric_exposure_cancel_failed'
  elif unmatched_contracts > 0 and repair_live:
    terminal_state = 'REPAIR_LIVE'
    terminal_reason = 'asymmetric_exposure_repair_order_preserved'
  elif unmatched_contracts > 0:
    terminal_state = 'EXPOSURE_CAPPED'
    terminal_reason = 'asymmetric_exposure_capped_repair_unavailable'
  elif yes_filled > 0 and no_filled > 0:
    terminal_state = 'PARTIAL_BOTH'
    terminal_reason = 'matched_partial_remaining_sheltered'
  elif any_fill:
    terminal_state = 'RECONCILE_REQUIRED'
    terminal_reason = 'one_sided_live_fill_requires_reconciliation'
  else:
    terminal_state = 'RECONCILE_REQUIRED'
    terminal_reason = 'live_order_shelter_reconciliation_ambiguous'

  persist_order_statuses(
    connection,
    operation_lane=settings.operation_lane,
    statuses=[
      {'order_id': yes_order.order_id, 'status': yes_final.status},
      {'order_id': no_order.order_id, 'status': no_final.status},
    ],
  )
  _persist_live_leg_fill(
    connection,
    plan=plan,
    order=yes_final,
    side='yes',
    price_dollars=yes_fill_price,
    contract_count=yes_filled,
    created_at_iso=cancel_ts,
    operation_lane=settings.operation_lane,
  )
  _persist_live_leg_fill(
    connection,
    plan=plan,
    order=no_final,
    side='no',
    price_dollars=no_fill_price,
    contract_count=no_filled,
    created_at_iso=cancel_ts,
    operation_lane=settings.operation_lane,
  )
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state=terminal_state,
    recorded_at_utc=cancel_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'ticker': plan.ticker,
      'reason': terminal_reason,
      'submit_mode': settlement_input.submit_mode,
      'yes_order_id': yes_order.order_id,
      'no_order_id': no_order.order_id,
      'yes_filled_contracts': str(yes_filled),
      'no_filled_contracts': str(no_filled),
      'average_yes_price': str(yes_fill_price),
      'average_no_price': str(no_fill_price),
      'realized_fees_dollars': '0',
      'yes_order_status': yes_final.status,
      'no_order_status': no_final.status,
      'cancel_results': cancel_results,
      'repair_leg': repair_leg,
      'ahead_leg': ahead_leg,
      'repair_remaining_contracts': str(repair_remaining),
      'unmatched_contracts': str(unmatched_contracts),
      'websocket_connected': False,
      **shelter_trigger_detail,
      **position_detail,
      **_submit_bridge_detail_fields(
        legacy_state=terminal_state,
        saved_set_snapshot=saved_set_snapshot,
        submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state=terminal_state),
      ),
    },
  )
  persist_runtime_event(
    connection,
    level='INFO' if terminal_state in {'CANCELED', 'FILLED'} else 'WARN',
    event_type='live_order_shelter_action',
    pair_id=plan.pair_id,
    recorded_at_utc=cancel_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'ticker': plan.ticker,
      'reason': 'shelter_window_reached',
      'submit_mode': settlement_input.submit_mode,
      'shelter_window_sec': shelter_window_sec,
      'observed_elapsed_sec': observed_elapsed_sec,
      'yes_filled_contracts': str(yes_filled),
      'no_filled_contracts': str(no_filled),
      'terminal_state': terminal_state,
      'terminal_reason': terminal_reason,
      'repair_leg': repair_leg,
      'ahead_leg': ahead_leg,
      'cancel_results': cancel_results,
      **shelter_trigger_detail,
    },
  )
  return _live_batch_chronology(terminal_state)


def _settle_live_batch_pairs_after_acceptance(
  client: Any,
  connection: Any,
  *,
  accepted_pairs: list[AcceptedPairSettlementInput],
  settings: Settings,
  lane_session_id: str,
  recorded_at: datetime,
) -> dict[str, dict[str, Any]]:
  sorted_pairs = sorted(accepted_pairs, key=lambda item: item.dispatch_index)
  seen_pair_ids: set[str] = set()
  seen_order_ids: set[str] = set()
  seen_client_ids: set[str] = set()
  for settlement_input in sorted_pairs:
    _validate_accepted_pair_settlement_input(settlement_input)
    ids = {
      settlement_input.yes_order.order_id,
      settlement_input.no_order.order_id,
    }
    client_ids = {
      settlement_input.plan.yes_client_order_id,
      settlement_input.plan.no_client_order_id,
    }
    if settlement_input.plan.pair_id in seen_pair_ids or seen_order_ids.intersection(ids) or seen_client_ids.intersection(client_ids):
      raise ValueError('Accepted settlement batch contains duplicate pair, order, or client IDs')
    seen_pair_ids.add(settlement_input.plan.pair_id)
    seen_order_ids.update(ids)
    seen_client_ids.update(client_ids)

  for settlement_input in sorted_pairs:
    _register_accepted_pair_orders(
      connection,
      settlement_input=settlement_input,
      settings=settings,
      lane_session_id=lane_session_id,
      reason='batch_submit_accepted',
    )

  chronologies: dict[str, dict[str, Any]] = {}
  active: dict[str, dict[str, Any]] = {}
  shelter_window_sec = max(0, int(settings.max_unhedged_sec))
  poll_interval_sec = 5
  for settlement_input in sorted_pairs:
    close_posture = _fresh_market_close_posture(client, settlement_input.plan.ticker, as_of=datetime.now(UTC))
    initial_seconds_to_close = close_posture.get('fresh_seconds_to_close')
    max_observe_sec = max(0, initial_seconds_to_close - shelter_window_sec) if isinstance(initial_seconds_to_close, int) else 0
    active[settlement_input.plan.pair_id] = {
      'input': settlement_input,
      'initial_seconds_to_close': initial_seconds_to_close,
      'max_observe_sec': max_observe_sec,
      'observed_elapsed_sec': 0,
      'partial_emitted': False,
      'shelter_trigger_detail': {'shelter_window_sec': shelter_window_sec, **close_posture},
    }

  while active:
    due_pair_ids: list[str] = []
    for pair_id, record in list(active.items()):
      settlement_input = record['input']
      plan = settlement_input.plan
      shelter_trigger_detail = dict(record['shelter_trigger_detail'])
      observed_elapsed_sec = int(record['observed_elapsed_sec'])
      fresh_seconds_to_close = shelter_trigger_detail.get('fresh_seconds_to_close')
      if isinstance(fresh_seconds_to_close, int) and fresh_seconds_to_close <= shelter_window_sec:
        shelter_trigger_detail['shelter_trigger_source'] = 'fresh_close_readback'
        record['shelter_trigger_detail'] = shelter_trigger_detail
        due_pair_ids.append(pair_id)
        continue
      if observed_elapsed_sec >= int(record['max_observe_sec']):
        shelter_trigger_detail['shelter_trigger_source'] = (
          'fresh_close_readback_unavailable'
          if not isinstance(fresh_seconds_to_close, int)
          else 'observed_elapsed_close_projection'
        )
        record['shelter_trigger_detail'] = shelter_trigger_detail
        due_pair_ids.append(pair_id)
        continue

      try:
        yes_state = client.get_order(settlement_input.yes_order.order_id)
        no_state = client.get_order(settlement_input.no_order.order_id)
      except Exception as exc:
        chronologies[pair_id] = _persist_live_batch_terminal(
          connection,
          plan=plan,
          settings=settings,
          lane_session_id=lane_session_id,
          saved_set_snapshot=settlement_input.saved_set_snapshot,
          state='RECONCILE_REQUIRED',
          reason='live_order_shelter_readback_failed',
          level='ERROR',
          detail={'error_family': type(exc).__name__, **shelter_trigger_detail},
        )
        active.pop(pair_id, None)
        continue

      persist_order_statuses(
        connection,
        operation_lane=settings.operation_lane,
        statuses=[
          {'order_id': settlement_input.yes_order.order_id, 'status': yes_state.status},
          {'order_id': settlement_input.no_order.order_id, 'status': no_state.status},
        ],
      )
      yes_filled = _submitted_order_fill_count(yes_state)
      no_filled = _submitted_order_fill_count(no_state)
      if (yes_filled > 0 or no_filled > 0) and not bool(record['partial_emitted']):
        partial_ts = datetime.now(UTC).isoformat()
        record['partial_emitted'] = True
        persist_pair_state_transition(
          connection,
          pair_id=plan.pair_id,
          state='PARTIAL_ONE_SIDE',
          recorded_at_utc=partial_ts,
          operation_lane=settings.operation_lane,
          lane_session_id=lane_session_id,
          detail={
            'ticker': plan.ticker,
            'yes_filled_contracts': str(yes_filled),
            'no_filled_contracts': str(no_filled),
            'average_yes_price': str(yes_state.price_dollars if yes_filled > 0 else plan.yes_price),
            'average_no_price': str(no_state.price_dollars if no_filled > 0 else plan.no_price),
            'realized_fees_dollars': '0',
            'websocket_connected': False,
            **_submit_bridge_detail_fields(
              legacy_state='PARTIAL_ONE_SIDE',
              saved_set_snapshot=settlement_input.saved_set_snapshot,
              submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state='PARTIAL_ONE_SIDE'),
            ),
          },
        )
        _persist_live_leg_fill(
          connection,
          plan=plan,
          order=yes_state,
          side='yes',
          price_dollars=yes_state.price_dollars if yes_filled > 0 else plan.yes_price,
          contract_count=yes_filled,
          created_at_iso=partial_ts,
          operation_lane=settings.operation_lane,
        )
        _persist_live_leg_fill(
          connection,
          plan=plan,
          order=no_state,
          side='no',
          price_dollars=no_state.price_dollars if no_filled > 0 else plan.no_price,
          contract_count=no_filled,
          created_at_iso=partial_ts,
          operation_lane=settings.operation_lane,
        )

      if yes_filled >= plan.contract_count and no_filled >= plan.contract_count:
        shelter_trigger_detail['shelter_trigger_source'] = 'both_legs_filled_during_batch_observe'
        record['shelter_trigger_detail'] = shelter_trigger_detail
        due_pair_ids.append(pair_id)
        continue

      close_posture = _fresh_market_close_posture(client, plan.ticker, as_of=datetime.now(UTC))
      fresh_seconds_to_close = close_posture.get('fresh_seconds_to_close')
      initial_seconds_to_close = record['initial_seconds_to_close']
      projected_seconds_to_close = (
        max(0, int(initial_seconds_to_close) - observed_elapsed_sec)
        if isinstance(initial_seconds_to_close, int)
        else None
      )
      if isinstance(projected_seconds_to_close, int) and (
        not isinstance(fresh_seconds_to_close, int) or projected_seconds_to_close < fresh_seconds_to_close
      ):
        close_posture = {**close_posture, 'fresh_seconds_to_close': projected_seconds_to_close}
      record['shelter_trigger_detail'] = {
        'shelter_window_sec': shelter_window_sec,
        'observed_elapsed_sec': observed_elapsed_sec,
        **close_posture,
      }

    for pair_id in due_pair_ids:
      record = active.pop(pair_id, None)
      if record is None:
        continue
      settlement_input = record['input']
      if settlement_input.plan.pair_id in chronologies:
        continue
      try:
        chronologies[settlement_input.plan.pair_id] = _settle_live_pair_orders_after_acceptance(
          client,
          connection,
          settlement_input=settlement_input,
          settings=settings,
          lane_session_id=lane_session_id,
          recorded_at=recorded_at,
          registration_done=True,
          skip_observe=True,
          shelter_trigger_detail=dict(record['shelter_trigger_detail']),
          observed_elapsed_sec=int(record['observed_elapsed_sec']),
        )
      except Exception as exc:
        event_ts = datetime.now(UTC).isoformat()
        persist_pair_state_transition(
          connection,
          pair_id=settlement_input.plan.pair_id,
          state='RECONCILE_REQUIRED',
          recorded_at_utc=event_ts,
          operation_lane=settings.operation_lane,
          lane_session_id=lane_session_id,
          detail={
            'ticker': settlement_input.plan.ticker,
            'reason': 'batch_settlement_exception_reconcile_required',
            'error_family': type(exc).__name__,
            'submit_mode': settlement_input.submit_mode,
            'yes_order_id': settlement_input.yes_order.order_id,
            'no_order_id': settlement_input.no_order.order_id,
            'yes_filled_contracts': '0',
            'no_filled_contracts': '0',
            'average_yes_price': str(settlement_input.plan.yes_price),
            'average_no_price': str(settlement_input.plan.no_price),
            'realized_fees_dollars': '0',
            'websocket_connected': False,
            **_submit_bridge_detail_fields(
              legacy_state='RECONCILE_REQUIRED',
              saved_set_snapshot=settlement_input.saved_set_snapshot,
              submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state='RECONCILE_REQUIRED'),
            ),
          },
        )
        chronologies[settlement_input.plan.pair_id] = _live_batch_chronology('RECONCILE_REQUIRED')

    if active:
      time.sleep(poll_interval_sec)
      for record in active.values():
        record['observed_elapsed_sec'] = int(record['observed_elapsed_sec']) + poll_interval_sec

  for settlement_input in sorted_pairs:
    if settlement_input.plan.pair_id not in chronologies:
      chronologies[settlement_input.plan.pair_id] = _persist_live_batch_terminal(
        connection,
        plan=settlement_input.plan,
        settings=settings,
        lane_session_id=lane_session_id,
        saved_set_snapshot=settlement_input.saved_set_snapshot,
        state='RECONCILE_REQUIRED',
        reason='batch_settlement_missing_chronology_reconcile_required',
        level='ERROR',
        detail={'submit_mode': settlement_input.submit_mode},
      )
  return chronologies


def _place_live_pair_orders_batch(
  client: Any,
  connection: Any,
  *,
  plans: list[Any],
  settings: Settings,
  lane_session_id: str,
  recorded_at: datetime,
  sizing_summary: dict[str, Any],
  saved_set_snapshot: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
  if len(plans) <= 1:
    raise ValueError('batch live submit requires at least two plans')

  now_ts = recorded_at.isoformat()
  chronologies: dict[str, dict[str, Any]] = {}
  payloads: list[dict[str, object]] = []
  expected_plan_by_client_id: dict[str, Any] = {}
  order_group_id_by_pair_id: dict[str, str] = {}

  for plan in plans:
    if plan.yes_price <= 0 or plan.no_price <= 0:
      chronologies[plan.pair_id] = _persist_live_batch_terminal(
        connection,
        plan=plan,
        settings=settings,
        lane_session_id=lane_session_id,
        saved_set_snapshot=saved_set_snapshot,
        state='CANCELED',
        reason='zero_price_guard',
      )
      continue
    units_block = _check_live_order_units(plan)
    if units_block is not None:
      side, value_dollars, blocked_reason, unit_reason = units_block
      chronologies[plan.pair_id] = _emit_live_order_units_blocked(
        connection,
        plan=plan,
        settings=settings,
        lane_session_id=lane_session_id,
        now_ts=now_ts,
        side=side,
        value_dollars=value_dollars,
        blocked_reason=blocked_reason,
        unit_reason=unit_reason,
        saved_set_snapshot=saved_set_snapshot,
      )
      continue
    persist_pair_state_transition(
      connection,
      pair_id=plan.pair_id,
      state='SUBMITTING',
      recorded_at_utc=now_ts,
      operation_lane=settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={
        'ticker': plan.ticker,
        'reason': 'live_batch_order_placement_intent',
        'submit_mode': 'batch_create_v2',
        'yes_filled_contracts': '0',
        'no_filled_contracts': '0',
        'average_yes_price': str(plan.yes_price),
        'average_no_price': str(plan.no_price),
        'realized_fees_dollars': '0',
        'websocket_connected': False,
        **_submit_bridge_detail_fields(
          legacy_state='SUBMITTING',
          saved_set_snapshot=saved_set_snapshot,
          submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state='SUBMITTING'),
        ),
      },
    )
    persist_runtime_event(
      connection,
      level='INFO',
      event_type='live_order_submitting',
      pair_id=plan.pair_id,
      recorded_at_utc=now_ts,
      operation_lane=settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={'ticker': plan.ticker, 'post_only': plan.post_only, 'submit_mode': 'batch_create_v2'},
    )
    try:
      order_group_id = client.create_order_group(
        contracts_limit_fp=plan.contract_count * 2,
        subaccount=plan.subaccount,
      )
    except KalshiHttpError as exc:
      chronologies[plan.pair_id] = _persist_live_batch_terminal(
        connection,
        plan=plan,
        settings=settings,
        lane_session_id=lane_session_id,
        saved_set_snapshot=saved_set_snapshot,
        state='ERROR',
        reason='batch_order_group_create_failed',
        level='ERROR',
        detail={'error_family': type(exc).__name__, **kalshi_error_safe_detail(exc)},
      )
      continue
    persist_runtime_event(
      connection,
      level='INFO',
      event_type='live_order_group_created',
      pair_id=plan.pair_id,
      recorded_at_utc=datetime.now(UTC).isoformat(),
      operation_lane=settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={
        'ticker': plan.ticker,
        'order_group_id': order_group_id,
        'submit_mode': 'batch_create_v2',
        'yes_client_order_id': plan.yes_client_order_id,
        'no_client_order_id': plan.no_client_order_id,
      },
    )
    order_group_id_by_pair_id[plan.pair_id] = order_group_id
    payloads.extend(_live_order_payloads_for_batch_plan(plan, order_group_id=order_group_id))
    expected_plan_by_client_id[str(plan.yes_client_order_id)] = plan
    expected_plan_by_client_id[str(plan.no_client_order_id)] = plan

  dispatch_plans = [plan for plan in plans if plan.pair_id not in chronologies]
  if not dispatch_plans:
    return chronologies

  try:
    submitted_orders = client.create_orders_v2_batch(payloads)
  except KalshiHttpError as exc:
    readback_orders = _readback_batch_orders_by_client_id(client, plans=dispatch_plans, recorded_at=recorded_at)
    if not readback_orders:
      for plan in dispatch_plans:
        chronologies[plan.pair_id] = _persist_live_batch_terminal(
          connection,
          plan=plan,
          settings=settings,
          lane_session_id=lane_session_id,
          saved_set_snapshot=saved_set_snapshot,
          state='CANCELED',
          reason='batch_submit_failed_not_submitted',
          detail={'error_family': type(exc).__name__, **kalshi_error_safe_detail(exc)},
        )
      return chronologies
    submitted_orders = list(readback_orders.values())

  orders_by_client_id, duplicate_ids, malformed_orders, duplicate_order_ids = _orders_by_client_order_id(submitted_orders)
  pair_classifications = _classify_batch_pair_acceptance(
    plans=dispatch_plans,
    orders_by_client_id=orders_by_client_id,
    duplicate_ids=duplicate_ids,
    malformed_orders=malformed_orders,
    duplicate_order_ids=duplicate_order_ids,
  )
  accepted_settlement_inputs: list[AcceptedPairSettlementInput] = []
  cleanup_attempted_order_ids: set[str] = set()
  promoted_remote_order_ids: set[str] = set()

  for dispatch_index, plan in enumerate(dispatch_plans):
    pair_classification = pair_classifications[str(plan.pair_id)]
    yes_order = pair_classification.yes_order
    no_order = pair_classification.no_order
    if pair_classification.classification != 'both_accepted':
      accepted_order_by_order_id = {
        str(order.order_id): order
        for order in (yes_order, no_order)
        if order is not None and str(order.order_id or '').strip()
      }
      accepted_orders = list(accepted_order_by_order_id.values())
      if yes_order is not None:
        yes_remote_order_id = str(yes_order.order_id or '').strip()
        if yes_remote_order_id not in promoted_remote_order_ids:
          promote_order_id(
            connection,
            operation_lane=settings.operation_lane,
            pair_id=plan.pair_id,
            client_order_id=plan.yes_client_order_id,
            side='yes',
            remote_order_id=yes_order.order_id,
            status=yes_order.status,
          )
          promoted_remote_order_ids.add(yes_remote_order_id)
      if no_order is not None:
        no_remote_order_id = str(no_order.order_id or '').strip()
        if no_remote_order_id not in promoted_remote_order_ids:
          promote_order_id(
            connection,
            operation_lane=settings.operation_lane,
            pair_id=plan.pair_id,
            client_order_id=plan.no_client_order_id,
            side='no',
            remote_order_id=no_order.order_id,
            status=no_order.status,
          )
          promoted_remote_order_ids.add(no_remote_order_id)
      if accepted_orders:
        persist_order_statuses(
          connection,
          operation_lane=settings.operation_lane,
          statuses=[
            {'order_id': order.order_id, 'status': order.status}
            for order in accepted_orders
          ],
        )
      cleanup_results: list[dict[str, Any]] = []
      for accepted_order in accepted_orders:
        remote_order_id = str(accepted_order.order_id or '').strip()
        if remote_order_id in cleanup_attempted_order_ids:
          cleanup_results.append({
            'side': str(accepted_order.side),
            'client_order_id': str(accepted_order.client_order_id),
            'order_id': remote_order_id,
            'cleanup_action': 'duplicate_remote_order_cleanup_already_attempted',
          })
          continue
        cleanup_attempted_order_ids.add(remote_order_id)
        cleanup_result, cleanup_statuses = _partial_ambiguous_cleanup_for_order(client, accepted_order)
        cleanup_results.append(cleanup_result)
        persist_order_statuses(
          connection,
          operation_lane=settings.operation_lane,
          statuses=cleanup_statuses,
        )
      chronologies[plan.pair_id] = _persist_live_batch_terminal(
        connection,
        plan=plan,
        settings=settings,
        lane_session_id=lane_session_id,
        saved_set_snapshot=saved_set_snapshot,
        state='RECONCILE_REQUIRED',
        reason='batch_submit_reconcile_required',
        level='ERROR',
        detail={
          'batch_pair_acceptance_classification': pair_classification.classification,
          'classification_reasons': list(pair_classification.classification_reasons),
          'missing_client_order_ids': list(pair_classification.missing_client_order_ids),
          'unknown_client_order_ids': list(pair_classification.unknown_client_order_ids),
          'duplicate_client_order_ids': list(pair_classification.duplicate_client_order_ids),
          'duplicate_remote_order_ids': list(pair_classification.duplicate_remote_order_ids),
          'malformed_order_count': pair_classification.malformed_order_count,
          'accepted_yes_order_id': getattr(yes_order, 'order_id', '') if yes_order is not None else '',
          'accepted_no_order_id': getattr(no_order, 'order_id', '') if no_order is not None else '',
          'cleanup_results': cleanup_results,
        },
      )
      continue
    accepted_settlement_inputs.append(
      AcceptedPairSettlementInput(
        dispatch_index=dispatch_index,
        plan=plan,
        order_group_id=order_group_id_by_pair_id[plan.pair_id],
        yes_order=yes_order,
        no_order=no_order,
        sizing_summary=sizing_summary,
        saved_set_snapshot=saved_set_snapshot,
        submit_mode='batch_create_v2',
      )
    )

  if accepted_settlement_inputs:
    chronologies.update(
      _settle_live_batch_pairs_after_acceptance(
        client,
        connection,
        accepted_pairs=accepted_settlement_inputs,
        settings=settings,
        lane_session_id=lane_session_id,
        recorded_at=recorded_at,
      )
    )

  return chronologies


def _place_live_pair_orders(
  client: Any,
  connection: Any,
  *,
  plan: Any,
  settings: Settings,
  lane_session_id: str,
  recorded_at: datetime,
  sizing_summary: dict[str, Any],
  saved_set_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
  now_ts = recorded_at.isoformat()

  if plan.yes_price <= 0 or plan.no_price <= 0:
    persist_pair_state_transition(
      connection,
      pair_id=plan.pair_id,
      state='CANCELED',
      recorded_at_utc=now_ts,
      operation_lane=settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={
        'ticker': plan.ticker,
        'reason': 'zero_price_guard',
        'yes_filled_contracts': '0',
        'no_filled_contracts': '0',
        'average_yes_price': str(plan.yes_price),
        'average_no_price': str(plan.no_price),
        'realized_fees_dollars': '0',
        'websocket_connected': False,
        **_submit_bridge_detail_fields(
          legacy_state='CANCELED',
          saved_set_snapshot=saved_set_snapshot,
          submit_response_id=_submit_bridge_response_id(blocked_reason='zero_price_guard', legacy_state='CANCELED'),
        ),
      },
    )
    persist_runtime_event(
      connection,
      level='WARN',
      event_type='live_order_zero_price_guard',
      pair_id=plan.pair_id,
      recorded_at_utc=now_ts,
      operation_lane=settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={'ticker': plan.ticker, 'blocked_reason': 'zero_price_guard'},
    )
    return {
      'enabled': True,
      'profile': 'submit_order_bridge',
      'terminal_state': 'CANCELED',
      'blocked_reason': 'zero_price_guard',
      'sequence_count': 0,
      'contract_version': TRANCHE_F_EXECUTION_EVENT_CONTRACT_VERSION,
      'event_packet': [],
    }

  units_block = _check_live_order_units(plan)
  if units_block is not None:
    side, value_dollars, blocked_reason, unit_reason = units_block
    return _emit_live_order_units_blocked(
      connection,
      plan=plan,
      settings=settings,
      lane_session_id=lane_session_id,
      now_ts=now_ts,
      side=side,
      value_dollars=value_dollars,
      blocked_reason=blocked_reason,
      unit_reason=unit_reason,
      saved_set_snapshot=saved_set_snapshot,
    )

  submit_response_id = _submit_bridge_response_id(blocked_reason=None, legacy_state='SUBMITTING')
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state='SUBMITTING',
    recorded_at_utc=now_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'ticker': plan.ticker,
      'reason': 'live_order_placement_intent',
      'yes_filled_contracts': '0',
      'no_filled_contracts': '0',
      'average_yes_price': str(plan.yes_price),
      'average_no_price': str(plan.no_price),
      'realized_fees_dollars': '0',
      'websocket_connected': False,
      **_submit_bridge_detail_fields(
        legacy_state='SUBMITTING',
        saved_set_snapshot=saved_set_snapshot,
        submit_response_id=submit_response_id,
      ),
    },
  )
  persist_runtime_event(
    connection,
    level='INFO',
    event_type='live_order_submitting',
    pair_id=plan.pair_id,
    recorded_at_utc=now_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={'ticker': plan.ticker, 'post_only': plan.post_only},
  )

  yes_order = None
  no_order = None
  try:
    order_group_id = client.create_order_group(
      contracts_limit_fp=plan.contract_count * 2,
      subaccount=plan.subaccount,
    )
    persist_runtime_event(
      connection,
      level='INFO',
      event_type='live_order_group_created',
      pair_id=plan.pair_id,
      recorded_at_utc=datetime.now(UTC).isoformat(),
      operation_lane=settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={
        'ticker': plan.ticker,
        'order_group_id': order_group_id,
        'submit_mode': 'single_create_v2',
        'yes_client_order_id': plan.yes_client_order_id,
        'no_client_order_id': plan.no_client_order_id,
      },
    )
    yes_order = client.create_order_v2(
      ticker=plan.ticker,
      side='yes',
      yes_price=plan.yes_price,
      count=plan.contract_count,
      client_order_id=plan.yes_client_order_id,
      time_in_force=plan.time_in_force,
      post_only=plan.post_only,
      cancel_order_on_pause=plan.cancel_order_on_pause,
      subaccount=plan.subaccount,
      order_group_id=order_group_id,
    )
    no_order = client.create_order_v2(
      ticker=plan.ticker,
      side='no',
      no_price=plan.no_price,
      count=plan.contract_count,
      client_order_id=plan.no_client_order_id,
      time_in_force=plan.time_in_force,
      post_only=plan.post_only,
      cancel_order_on_pause=plan.cancel_order_on_pause,
      subaccount=plan.subaccount,
      order_group_id=order_group_id,
    )
  except KalshiHttpError as exc:
    if yes_order is not None and no_order is None:
      try:
        client.cancel_order_v2(yes_order.order_id)
      except KalshiHttpError:
        persist_runtime_event(
          connection,
          level='ERROR',
          event_type='live_order_unwind_failed',
          pair_id=plan.pair_id,
          recorded_at_utc=datetime.now(UTC).isoformat(),
          operation_lane=settings.operation_lane,
          lane_session_id=lane_session_id,
          detail={
            'ticker': plan.ticker,
            'leg': 'yes',
            'order_id': yes_order.order_id,
            'reason': 'unwind_cancel_failed_after_no_post_error',
          },
        )
    error_ts = datetime.now(UTC).isoformat()
    persist_pair_state_transition(
      connection,
      pair_id=plan.pair_id,
      state='ERROR',
      recorded_at_utc=error_ts,
      operation_lane=settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={
        'ticker': plan.ticker,
        'reason': 'live_order_api_error',
        'error_family': type(exc).__name__,
        **kalshi_error_safe_detail(exc),
        'yes_filled_contracts': '0',
        'no_filled_contracts': '0',
        'average_yes_price': str(plan.yes_price),
        'average_no_price': str(plan.no_price),
        'realized_fees_dollars': '0',
        'websocket_connected': False,
      },
    )
    persist_runtime_event(
      connection,
      level='ERROR',
      event_type='live_order_placement_error',
      pair_id=plan.pair_id,
      recorded_at_utc=error_ts,
      operation_lane=settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={
        'ticker': plan.ticker,
        'error_family': type(exc).__name__,
        **kalshi_error_safe_detail(exc),
      },
    )
    return {
      'enabled': True,
      'profile': 'submit_order_bridge',
      'terminal_state': 'ERROR',
      'sequence_count': 0,
      'contract_version': TRANCHE_F_EXECUTION_EVENT_CONTRACT_VERSION,
      'event_packet': [],
    }

  resting_ts = datetime.now(UTC).isoformat()
  promote_order_id(
    connection,
    operation_lane=settings.operation_lane,
    pair_id=plan.pair_id,
    client_order_id=plan.yes_client_order_id,
    side='yes',
    remote_order_id=yes_order.order_id,
    status=yes_order.status,
  )
  promote_order_id(
    connection,
    operation_lane=settings.operation_lane,
    pair_id=plan.pair_id,
    client_order_id=plan.no_client_order_id,
    side='no',
    remote_order_id=no_order.order_id,
    status=no_order.status,
  )
  persist_order_statuses(
    connection,
    operation_lane=settings.operation_lane,
    statuses=[
      {'order_id': yes_order.order_id, 'status': yes_order.status},
      {'order_id': no_order.order_id, 'status': no_order.status},
    ],
  )
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state='RESTING_BOTH',
    recorded_at_utc=resting_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'ticker': plan.ticker,
      'yes_order_id': yes_order.order_id,
      'no_order_id': no_order.order_id,
      'yes_filled_contracts': '0',
      'no_filled_contracts': '0',
      'average_yes_price': str(plan.yes_price),
      'average_no_price': str(plan.no_price),
      'realized_fees_dollars': '0',
      'websocket_connected': False,
      **_submit_bridge_detail_fields(
        legacy_state='RESTING_BOTH',
        saved_set_snapshot=saved_set_snapshot,
        submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state='RESTING_BOTH'),
      ),
    },
  )
  persist_runtime_event(
    connection,
    level='INFO',
    event_type='live_orders_resting',
    pair_id=plan.pair_id,
    recorded_at_utc=resting_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={'yes_order_id': yes_order.order_id, 'no_order_id': no_order.order_id},
  )

  shelter_window_sec = max(0, int(settings.max_unhedged_sec))
  poll_interval_sec = 5
  observed_elapsed_sec = 0
  yes_filled = Decimal('0')
  no_filled = Decimal('0')
  yes_fill_price = plan.yes_price
  no_fill_price = plan.no_price
  partial_emitted = False
  close_posture = _fresh_market_close_posture(client, plan.ticker, as_of=datetime.now(UTC))
  initial_seconds_to_close = close_posture.get('fresh_seconds_to_close')
  if isinstance(initial_seconds_to_close, int):
    max_observe_sec = max(0, initial_seconds_to_close - shelter_window_sec)
  else:
    max_observe_sec = 0
  shelter_trigger_detail = {
    'shelter_window_sec': shelter_window_sec,
    **close_posture,
  }

  while True:
    fresh_seconds_to_close = shelter_trigger_detail.get('fresh_seconds_to_close')
    if isinstance(fresh_seconds_to_close, int) and fresh_seconds_to_close <= shelter_window_sec:
      shelter_trigger_detail['shelter_trigger_source'] = 'fresh_close_readback'
      break
    if observed_elapsed_sec >= max_observe_sec:
      shelter_trigger_detail['shelter_trigger_source'] = (
        'fresh_close_readback_unavailable'
        if not isinstance(fresh_seconds_to_close, int)
        else 'observed_elapsed_close_projection'
      )
      break

    time.sleep(poll_interval_sec)
    observed_elapsed_sec += poll_interval_sec

    try:
      yes_state = client.get_order(yes_order.order_id)
      no_state = client.get_order(no_order.order_id)
    except Exception:
      break

    yes_filled = _submitted_order_fill_count(yes_state)
    no_filled = _submitted_order_fill_count(no_state)
    if yes_filled > 0:
      yes_fill_price = yes_state.price_dollars
    if no_filled > 0:
      no_fill_price = no_state.price_dollars

    if (yes_filled > 0 or no_filled > 0) and not partial_emitted:
      partial_ts = datetime.now(UTC).isoformat()
      partial_emitted = True
      partial_state = 'PARTIAL_ONE_SIDE'
      persist_pair_state_transition(
        connection,
        pair_id=plan.pair_id,
        state=partial_state,
        recorded_at_utc=partial_ts,
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        detail={
          'ticker': plan.ticker,
          'yes_filled_contracts': str(yes_filled),
          'no_filled_contracts': str(no_filled),
          'average_yes_price': str(yes_fill_price),
          'average_no_price': str(no_fill_price),
          'realized_fees_dollars': '0',
          'websocket_connected': False,
          **_submit_bridge_detail_fields(
            legacy_state=partial_state,
            saved_set_snapshot=saved_set_snapshot,
            submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state=partial_state),
          ),
        },
      )
      if yes_filled > 0:
        _persist_live_leg_fill(
          connection,
          plan=plan,
          order=yes_state,
          side='yes',
          price_dollars=yes_fill_price,
          contract_count=yes_filled,
          created_at_iso=partial_ts,
          operation_lane=settings.operation_lane,
        )
      if no_filled > 0:
        _persist_live_leg_fill(
          connection,
          plan=plan,
          order=no_state,
          side='no',
          price_dollars=no_fill_price,
          contract_count=no_filled,
          created_at_iso=partial_ts,
          operation_lane=settings.operation_lane,
        )

    if yes_filled >= plan.contract_count and no_filled >= plan.contract_count:
      filled_ts = datetime.now(UTC).isoformat()
      persist_order_statuses(
        connection,
        operation_lane=settings.operation_lane,
        statuses=[
          {'order_id': yes_order.order_id, 'status': yes_state.status},
          {'order_id': no_order.order_id, 'status': no_state.status},
        ],
      )
      _persist_live_leg_fill(
        connection,
        plan=plan,
        order=yes_state,
        side='yes',
        price_dollars=yes_fill_price,
        contract_count=yes_filled,
        created_at_iso=filled_ts,
        operation_lane=settings.operation_lane,
        replace_existing=False,
      )
      _persist_live_leg_fill(
        connection,
        plan=plan,
        order=no_state,
        side='no',
        price_dollars=no_fill_price,
        contract_count=no_filled,
        created_at_iso=filled_ts,
        operation_lane=settings.operation_lane,
        replace_existing=False,
      )
      persist_pair_state_transition(
        connection,
        pair_id=plan.pair_id,
        state='FILLED',
        recorded_at_utc=filled_ts,
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        detail={
          'ticker': plan.ticker,
          'reason': 'both_legs_filled',
          'yes_filled_contracts': str(yes_filled),
          'no_filled_contracts': str(no_filled),
          'average_yes_price': str(yes_fill_price),
          'average_no_price': str(no_fill_price),
          'realized_fees_dollars': '0',
          'websocket_connected': False,
        },
      )
      persist_runtime_event(
        connection,
        level='INFO',
        event_type='live_orders_both_filled',
        pair_id=plan.pair_id,
        recorded_at_utc=filled_ts,
        operation_lane=settings.operation_lane,
        lane_session_id=lane_session_id,
        detail={
          'yes_filled_contracts': str(yes_filled),
          'no_filled_contracts': str(no_filled),
          'average_yes_price': str(yes_fill_price),
          'average_no_price': str(no_fill_price),
        },
      )
      return {
        'enabled': True,
        'profile': 'submit_order_bridge',
        'terminal_state': 'FILLED',
        'sequence_count': 0,
        'contract_version': TRANCHE_F_EXECUTION_EVENT_CONTRACT_VERSION,
        'event_packet': [],
      }
    close_posture = _fresh_market_close_posture(client, plan.ticker, as_of=datetime.now(UTC))
    fresh_seconds_to_close = close_posture.get('fresh_seconds_to_close')
    projected_seconds_to_close = (
      max(0, initial_seconds_to_close - observed_elapsed_sec)
      if isinstance(initial_seconds_to_close, int)
      else None
    )
    if isinstance(projected_seconds_to_close, int) and (
      not isinstance(fresh_seconds_to_close, int) or projected_seconds_to_close < fresh_seconds_to_close
    ):
      close_posture = {**close_posture, 'fresh_seconds_to_close': projected_seconds_to_close}
    shelter_trigger_detail = {
      'shelter_window_sec': shelter_window_sec,
      'observed_elapsed_sec': observed_elapsed_sec,
      **close_posture,
    }

  cancel_ts = datetime.now(UTC).isoformat()
  try:
    yes_state = client.get_order(yes_order.order_id)
    no_state = client.get_order(no_order.order_id)
  except Exception as exc:
    position_detail = _position_readback_detail(client)
    persist_pair_state_transition(
      connection,
      pair_id=plan.pair_id,
      state='RECONCILE_REQUIRED',
      recorded_at_utc=cancel_ts,
      operation_lane=settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={
        'ticker': plan.ticker,
        'reason': 'live_order_shelter_readback_failed',
        'error_family': type(exc).__name__,
        'yes_filled_contracts': str(yes_filled),
        'no_filled_contracts': str(no_filled),
        'average_yes_price': str(yes_fill_price),
        'average_no_price': str(no_fill_price),
        'realized_fees_dollars': '0',
        'websocket_connected': False,
        **shelter_trigger_detail,
        **position_detail,
        **_submit_bridge_detail_fields(
          legacy_state='RECONCILE_REQUIRED',
          saved_set_snapshot=saved_set_snapshot,
          submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state='RECONCILE_REQUIRED'),
        ),
      },
    )
    return {
      'enabled': True,
      'profile': 'submit_order_bridge',
      'terminal_state': 'RECONCILE_REQUIRED',
      'sequence_count': 0,
      'contract_version': TRANCHE_F_EXECUTION_EVENT_CONTRACT_VERSION,
      'event_packet': [],
    }

  yes_filled = _submitted_order_fill_count(yes_state)
  no_filled = _submitted_order_fill_count(no_state)
  if yes_filled > 0:
    yes_fill_price = yes_state.price_dollars
  if no_filled > 0:
    no_fill_price = no_state.price_dollars

  # Canonical shelter action: cap only the ahead (over-filled) leg and preserve the
  # opposite repair order so the deficient side stays open to fill. This is a shelter
  # window keyed on seconds-to-close, NOT an order-age timeout, and it never crosses
  # the market to catch up or freezes the residual to ERROR.
  if yes_filled == 0 and no_filled == 0:
    cancel_targets = {'yes', 'no'}
    repair_leg = ''
    ahead_leg = ''
  elif yes_filled > no_filled:
    cancel_targets = {'yes'}
    repair_leg = 'no'
    ahead_leg = 'yes'
  elif no_filled > yes_filled:
    cancel_targets = {'no'}
    repair_leg = 'yes'
    ahead_leg = 'no'
  else:
    cancel_targets = {'yes', 'no'}
    repair_leg = ''
    ahead_leg = ''

  cancel_results: list[dict[str, str]] = []
  for leg, state in (('yes', yes_state), ('no', no_state)):
    if leg not in cancel_targets:
      cancel_results.append({'leg': leg, 'order_id': state.order_id, 'status': 'preserved_repair_order'})
      continue
    if not _submitted_order_cancelable(state):
      cancel_results.append({'leg': leg, 'order_id': state.order_id, 'status': 'not_cancelable'})
      continue
    try:
      client.cancel_order_v2(state.order_id)
      cancel_results.append({'leg': leg, 'order_id': state.order_id, 'status': 'cancel_requested'})
    except KalshiHttpError as exc:
      cancel_results.append({
        'leg': leg,
        'order_id': state.order_id,
        'status': 'cancel_failed',
        **kalshi_error_safe_detail(exc),
      })

  try:
    yes_final = client.get_order(yes_order.order_id)
    no_final = client.get_order(no_order.order_id)
  except Exception as exc:
    position_detail = _position_readback_detail(client)
    persist_pair_state_transition(
      connection,
      pair_id=plan.pair_id,
      state='RECONCILE_REQUIRED',
      recorded_at_utc=cancel_ts,
      operation_lane=settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={
        'ticker': plan.ticker,
        'reason': 'live_order_post_shelter_readback_failed',
        'error_family': type(exc).__name__,
        'cancel_results': cancel_results,
        'yes_filled_contracts': str(yes_filled),
        'no_filled_contracts': str(no_filled),
        'average_yes_price': str(yes_fill_price),
        'average_no_price': str(no_fill_price),
        'realized_fees_dollars': '0',
        'websocket_connected': False,
        **shelter_trigger_detail,
        **position_detail,
        **_submit_bridge_detail_fields(
          legacy_state='RECONCILE_REQUIRED',
          saved_set_snapshot=saved_set_snapshot,
          submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state='RECONCILE_REQUIRED'),
        ),
      },
    )
    return {
      'enabled': True,
      'profile': 'submit_order_bridge',
      'terminal_state': 'RECONCILE_REQUIRED',
      'sequence_count': 0,
      'contract_version': TRANCHE_F_EXECUTION_EVENT_CONTRACT_VERSION,
      'event_packet': [],
    }

  yes_filled = _submitted_order_fill_count(yes_final)
  no_filled = _submitted_order_fill_count(no_final)
  if yes_filled > 0:
    yes_fill_price = yes_final.price_dollars
  if no_filled > 0:
    no_fill_price = no_final.price_dollars
  yes_status = str(yes_final.status or '').strip().lower()
  no_status = str(no_final.status or '').strip().lower()
  both_zero_canceled = (
    yes_filled == 0
    and no_filled == 0
    and yes_status in {'canceled', 'cancelled'}
    and no_status in {'canceled', 'cancelled'}
  )
  both_filled = yes_filled >= plan.contract_count and no_filled >= plan.contract_count
  any_fill = yes_filled > 0 or no_filled > 0
  cancel_failed = any(item.get('status') == 'cancel_failed' for item in cancel_results)
  unmatched_contracts = abs(yes_filled - no_filled)
  repair_state = no_final if repair_leg == 'no' else yes_final if repair_leg == 'yes' else None
  repair_remaining = _submitted_order_remaining_count(repair_state) if repair_state is not None else Decimal('0')
  repair_status = str(getattr(repair_state, 'status', '') or '').strip().lower() if repair_state is not None else ''
  repair_live = repair_remaining > 0 and repair_status not in {'canceled', 'cancelled', 'executed', 'filled'}
  position_detail = _position_readback_detail(client) if any_fill or cancel_failed else {}
  if both_filled:
    terminal_state = 'FILLED'
    terminal_reason = 'both_legs_filled_after_shelter_readback'
  elif both_zero_canceled and not cancel_failed:
    terminal_state = 'CANCELED'
    terminal_reason = 'shelter_window_no_fill_canceled'
  elif unmatched_contracts > 0 and cancel_failed:
    terminal_state = 'RECONCILE_REQUIRED'
    terminal_reason = 'asymmetric_exposure_cancel_failed'
  elif unmatched_contracts > 0 and repair_live:
    terminal_state = 'REPAIR_LIVE'
    terminal_reason = 'asymmetric_exposure_repair_order_preserved'
  elif unmatched_contracts > 0:
    terminal_state = 'EXPOSURE_CAPPED'
    terminal_reason = 'asymmetric_exposure_capped_repair_unavailable'
  elif yes_filled > 0 and no_filled > 0:
    terminal_state = 'PARTIAL_BOTH'
    terminal_reason = 'matched_partial_remaining_sheltered'
  elif any_fill:
    terminal_state = 'RECONCILE_REQUIRED'
    terminal_reason = 'one_sided_live_fill_requires_reconciliation'
  else:
    terminal_state = 'RECONCILE_REQUIRED'
    terminal_reason = 'live_order_shelter_reconciliation_ambiguous'

  persist_order_statuses(
    connection,
    operation_lane=settings.operation_lane,
    statuses=[
      {'order_id': yes_order.order_id, 'status': yes_final.status},
      {'order_id': no_order.order_id, 'status': no_final.status},
    ],
  )
  _persist_live_leg_fill(
    connection,
    plan=plan,
    order=yes_final,
    side='yes',
    price_dollars=yes_fill_price,
    contract_count=yes_filled,
    created_at_iso=cancel_ts,
    operation_lane=settings.operation_lane,
  )
  _persist_live_leg_fill(
    connection,
    plan=plan,
    order=no_final,
    side='no',
    price_dollars=no_fill_price,
    contract_count=no_filled,
    created_at_iso=cancel_ts,
    operation_lane=settings.operation_lane,
  )
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state=terminal_state,
    recorded_at_utc=cancel_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'ticker': plan.ticker,
      'reason': terminal_reason,
      'yes_order_id': yes_order.order_id,
      'no_order_id': no_order.order_id,
      'yes_filled_contracts': str(yes_filled),
      'no_filled_contracts': str(no_filled),
      'average_yes_price': str(yes_fill_price),
      'average_no_price': str(no_fill_price),
      'realized_fees_dollars': '0',
      'yes_order_status': yes_final.status,
      'no_order_status': no_final.status,
      'cancel_results': cancel_results,
      'repair_leg': repair_leg,
      'ahead_leg': ahead_leg,
      'repair_remaining_contracts': str(repair_remaining),
      'unmatched_contracts': str(unmatched_contracts),
      'websocket_connected': False,
      **shelter_trigger_detail,
      **position_detail,
      **_submit_bridge_detail_fields(
        legacy_state=terminal_state,
        saved_set_snapshot=saved_set_snapshot,
        submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state=terminal_state),
      ),
    },
  )
  persist_runtime_event(
    connection,
    level='INFO' if terminal_state in {'CANCELED', 'FILLED'} else 'WARN',
    event_type='live_order_shelter_action',
    pair_id=plan.pair_id,
    recorded_at_utc=cancel_ts,
    operation_lane=settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'ticker': plan.ticker,
      'reason': 'shelter_window_reached',
      'shelter_window_sec': shelter_window_sec,
      'observed_elapsed_sec': observed_elapsed_sec,
      'yes_filled_contracts': str(yes_filled),
      'no_filled_contracts': str(no_filled),
      'terminal_state': terminal_state,
      'terminal_reason': terminal_reason,
      'repair_leg': repair_leg,
      'ahead_leg': ahead_leg,
      'cancel_results': cancel_results,
      **shelter_trigger_detail,
    },
  )
  return {
    'enabled': True,
    'profile': 'submit_order_bridge',
    'terminal_state': terminal_state,
    'sequence_count': 0,
    'contract_version': TRANCHE_F_EXECUTION_EVENT_CONTRACT_VERSION,
    'event_packet': [],
  }


SIGNED_MONEY_EVIDENCE_SCHEMA_VERSION = 'signed-money-evidence.v1'


def _money_evidence_signature_payload(
  execution_chronology: dict[str, Any],
  *,
  operation_lane: str,
  lane_session_id: str,
  pair_id: str,
) -> dict[str, Any]:
  """Canonical money-evidence payload bound by the signature (Lane L5b).

  Binds the operating lane, session, and pair to the full submit/fill/cancel/reconcile outcome so a
  retained money record cannot be re-attributed to a different lane/session/pair or silently edited.
  """
  return {
    'schema_version': SIGNED_MONEY_EVIDENCE_SCHEMA_VERSION,
    'operation_lane': operation_lane,
    'lane_session_id': lane_session_id,
    'pair_id': pair_id,
    'profile': execution_chronology.get('profile'),
    'terminal_state': execution_chronology.get('terminal_state'),
    'blocked_reason': execution_chronology.get('blocked_reason'),
    'contract_version': execution_chronology.get('contract_version'),
    'sequence_count': execution_chronology.get('sequence_count'),
    'states': execution_chronology.get('states'),
    'chronology': execution_chronology.get('chronology'),
    'event_packet': execution_chronology.get('event_packet'),
  }


def _attach_money_evidence_signature(
  execution_chronology: dict[str, Any],
  *,
  operation_lane: str,
  lane_session_id: str,
  pair_id: str,
) -> dict[str, Any]:
  """Sign a money-evidence chronology record in place and return it (Lane L5b)."""
  execution_chronology['signed_evidence'] = signed_evidence.sign_evidence_record(
    _money_evidence_signature_payload(
      execution_chronology,
      operation_lane=operation_lane,
      lane_session_id=lane_session_id,
      pair_id=pair_id,
    )
  )
  return execution_chronology


def _attach_money_evidence_signature_for_result_slot(
  execution_chronology: dict[str, Any],
  *,
  operation_lane: str,
  lane_session_id: str,
  pair_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
  try:
    signed_chronology = _attach_money_evidence_signature(
      execution_chronology,
      operation_lane=operation_lane,
      lane_session_id=lane_session_id,
      pair_id=pair_id,
    )
  except Exception as exc:
    failed_chronology = dict(execution_chronology)
    failed_chronology['signed_evidence'] = {'signature_status': 'failed'}
    return failed_chronology, {
      'signed_money_evidence_status': 'failed',
      'signed_money_evidence_error_family': type(exc).__name__,
      'signed_money_evidence_error_message': str(exc),
    }
  signed_evidence_block = signed_chronology.get('signed_evidence') if isinstance(signed_chronology, dict) else None
  signature_status = (
    str(signed_evidence_block.get('signature_status') or '').strip()
    if isinstance(signed_evidence_block, dict)
    else ''
  )
  return signed_chronology, {
    'signed_money_evidence_status': signature_status or 'signed',
  }


def _signing_key_blocked_execution_chronology(
  *,
  operation_lane: str,
  lane_session_id: str,
  pair_id: str,
) -> dict[str, Any]:
  """Fail-closed money record when no signing key is provisioned (Lane L5b).

  The signing key is a hard precondition for any money submit: with no key the orders are NOT placed
  (uniform across sandbox and live), and an explicit blocked + unsigned record is emitted.
  """
  return _attach_money_evidence_signature(
    {
      'enabled': True,
      'profile': 'submit_order_bridge',
      'terminal_state': 'BLOCKED',
      'blocked_reason': 'signing_key_unavailable',
      'sequence_count': 0,
      'contract_version': TRANCHE_F_EXECUTION_EVENT_CONTRACT_VERSION,
      'event_packet': [],
    },
    operation_lane=operation_lane,
    lane_session_id=lane_session_id,
    pair_id=pair_id,
  )


def run_scan_once(
  settings: Settings | None = None,
  *,
  env_override: str | None = None,
  subaccount_override: int | None = None,
  client_factory: ClientFactory | None = None,
  progress_callback: ScanProgressCallback | None = None,
  cancel_event: object | None = None,
  operator_lane_session_id: str | None = None,
) -> dict[str, Any]:
  resolved_settings = _resolve_settings(
    settings,
    env_override=env_override,
    subaccount_override=subaccount_override,
  )
  _validate_env_alignment(resolved_settings)
  private_key_path = resolve_private_key_path(resolved_settings)
  private_key = load_private_key(private_key_path)
  client = (client_factory or KalshiHttpClient)(resolved_settings, private_key)

  balance, limits, account_posture = _load_scan_account_posture(
    client,
    progress_callback=progress_callback,
  )
  _raise_if_scan_canceled(cancel_event, progress_callback=progress_callback)
  recorded_at = datetime.now(UTC)
  operator_lane_session_id_normalized = str(operator_lane_session_id or '').strip()
  lane_session_id = (
    operator_lane_session_id_normalized
    if operator_lane_session_id_normalized
    else _lane_session_id(resolved_settings.operation_lane)
  )
  _emit_scan_progress(
    progress_callback,
    'loading_markets',
    'Loading markets and account posture for candidate review.',
    detail={'operation_lane': resolved_settings.operation_lane},
    progress_percent=0.12,
  )
  markets, candidate_markets, candidate_market_by_ticker, orderbook_enrichment_count, websocket_posture = _unpack_candidate_market_set(
    _load_candidate_market_set(
      client,
      recorded_at=recorded_at,
      operation_lane=resolved_settings.operation_lane,
      lane_session_id=lane_session_id,
      settings=resolved_settings,
      private_key=private_key,
      progress_callback=progress_callback,
      cancel_event=cancel_event,
    )
  )
  _raise_if_scan_canceled(cancel_event, progress_callback=progress_callback)
  _emit_scan_progress(
    progress_callback,
    'ranking_candidates',
    'Ranking candidate markets against the current filters.',
    detail={
      'market_count': len(markets),
      'orderbook_enrichment_count': orderbook_enrichment_count,
    },
    progress_percent=0.76,
  )
  divergence_screen_stats: dict[str, object] = {}
  live_candidates = find_candidates(
    candidate_markets,
    recorded_at,
    resolved_settings,
    screen_stats=divergence_screen_stats,
  )
  _raise_if_scan_canceled(cancel_event, progress_callback=progress_callback)
  sizing_summary = _build_dynamic_sizing_summary(
    live_candidates,
    balance=balance,
    settings=resolved_settings,
  )
  live_candidate_records = [
    _candidate_projection_record(
      candidate,
      rank=rank,
      qualifier_tier='live_qualifying',
      market_by_ticker=candidate_market_by_ticker,
      settings=resolved_settings,
    )
    for rank, candidate in enumerate(live_candidates, start=1)
  ]
  sandbox_candidates_extended: list[dict[str, Any]] | None = None
  sandbox_extended_count: int | None = None
  sandbox_transition_rank: int | None = None
  sandbox_relaxation_factor: float | None = None
  if resolved_settings.operation_lane == 'sandbox':
    relaxed_settings, relaxation_factor = _sandbox_relaxed_settings(resolved_settings)
    if str(os.getenv('KALSHI_SIMULATION_MODE') or '').strip().lower() == 'inject':
      inject_settings = replace(resolved_settings, min_edge_dollars=-1.0, min_profit_dollars=-1.0)
      sandbox_candidates = find_candidates(candidate_markets, recorded_at, inject_settings)
    else:
      sandbox_candidates = find_candidates(candidate_markets, recorded_at, relaxed_settings)
    _raise_if_scan_canceled(cancel_event, progress_callback=progress_callback)
    sandbox_candidates_extended, sandbox_transition_rank = _sandbox_candidate_projection(
      sandbox_candidates,
      {candidate.ticker for candidate in live_candidates},
      market_by_ticker=candidate_market_by_ticker,
      settings=resolved_settings,
    )
    sandbox_extended_count = len(sandbox_candidates)
    sandbox_relaxation_factor = float(relaxation_factor)
  _raise_if_scan_canceled(cancel_event, progress_callback=progress_callback)
  # Z4: Collect near-miss evidence using a fixed widening factor, independent of sandbox policy scope.
  already_surfaced_tickers: set[str] = {candidate.ticker for candidate in live_candidates}
  if sandbox_candidates_extended is not None:
    already_surfaced_tickers.update(
      str(c.get('ticker') or '') for c in sandbox_candidates_extended
    )
  near_miss_widened = _near_miss_widened_settings(resolved_settings)
  near_miss_raw = find_candidates(candidate_markets, recorded_at, near_miss_widened)
  near_miss_candidate_records = _near_miss_candidate_projection(
    near_miss_raw,
    already_surfaced_tickers,
    market_by_ticker=candidate_market_by_ticker,
    settings=resolved_settings,
  )
  _raise_if_scan_canceled(cancel_event, progress_callback=progress_callback)
  _emit_scan_progress(
    progress_callback,
    'finalizing_result',
    'Finalizing candidate results for shell display.',
    detail={
      'candidate_count': len(live_candidates),
      **({'sandbox_extended_count': sandbox_extended_count} if sandbox_extended_count is not None else {}),
    },
    progress_percent=0.92,
  )
  _raise_if_scan_canceled(cancel_event, progress_callback=progress_callback)
  completed_at = datetime.now(UTC)
  connection = open_database(resolved_settings.state_db_path)
  returned_candidates = (
    sandbox_candidates_extended if sandbox_candidates_extended is not None else live_candidate_records[:10]
  )
  analytical_outputs = _build_analytical_outputs(
    returned_candidates,
    sandbox_candidates if sandbox_candidates_extended is not None else live_candidates,
    recorded_at=recorded_at,
    operation_lane=resolved_settings.operation_lane,
    lane_session_id=lane_session_id,
    transition_rank=sandbox_transition_rank,
    sizing_summary=sizing_summary,
    balance=balance,
    settings=resolved_settings,
    near_miss_candidates=near_miss_candidate_records or None,
  )
  # C1: persist the last ready dynamic-sizing computation so the panel does not cold-start
  # at "needs more data" next session. Ready == this scan produced live candidates. Derived
  # values go to analytical_snapshots (keep-latest-5), never the operator working-default tier.
  if live_candidates:
    persist_dynamic_sizing_snapshot(
      connection,
      resolved_settings.operation_lane,
      {
        'effective_density': sizing_summary.get('effective_density'),
        'dynamic_pair_notional_pct': sizing_summary.get('dynamic_pair_notional_pct'),
        'dynamic_pair_notional_cap_dollars': sizing_summary.get('dynamic_pair_notional_cap_dollars'),
        'dynamic_max_contracts': sizing_summary.get('dynamic_max_contracts'),
        'binding_limiter': sizing_summary.get('binding_limiter'),
      },
      lane_session_id=lane_session_id,
    )
  # Z6: Persist near-miss frontier evidence to analytical_snapshots (Variant E research corpus).
  if near_miss_candidate_records:
    persist_analytical_snapshot(
      connection,
      operation_lane=resolved_settings.operation_lane,
      lane_session_id=lane_session_id,
      snapshot_type='near_miss_frontier',
      evidence_class='near_miss',
      recorded_at_utc=recorded_at.isoformat(),
      detail={
        'near_miss_count': len(near_miss_candidate_records),
        'evidence_factor': str(_NEAR_MISS_EVIDENCE_FACTOR),
        'candidates': _candidate_evidence_preview(near_miss_candidate_records),
      },
    )
  _persist_candidate_math_contract(
    connection,
    operation_lane=resolved_settings.operation_lane,
    lane_session_id=lane_session_id,
    operator_lane_session_id=operator_lane_session_id or None,
    recorded_at=recorded_at,
    source_action='scan-once',
    analytical_outputs=analytical_outputs,
  )
  persist_operator_action(
    connection,
    action='scan-once',
    recorded_at_utc=recorded_at.isoformat(),
    operation_lane=resolved_settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'candidate_count': len(live_candidates),
      'market_count': len(markets),
      'effective_density': sizing_summary['effective_density'],
      'dynamic_pair_notional_pct': sizing_summary['dynamic_pair_notional_pct'],
      'active_websocket_url_tail': _websocket_label(resolved_settings.active_websocket_url),
      'websocket_status': websocket_posture['websocket_status'],
      'account_posture': account_posture,
      'candidates': _candidate_evidence_preview(
        sandbox_candidates_extended if sandbox_candidates_extended is not None else live_candidate_records,
      ),
    },
  )
  persist_runtime_event(
    connection,
    level='INFO',
    event_type='scan_complete',
    recorded_at_utc=completed_at.isoformat(),
    operation_lane=resolved_settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'scan_started_at_utc': recorded_at.isoformat(),
      'scan_completed_at_utc': completed_at.isoformat(),
      'candidate_count': len(live_candidates),
      'divergence_screen': divergence_screen_stats,
      'orderbook_enrichment_count': orderbook_enrichment_count,
      'effective_density': sizing_summary['effective_density'],
      'dynamic_pair_notional_pct': sizing_summary['dynamic_pair_notional_pct'],
      'active_websocket_url_tail': _websocket_label(resolved_settings.active_websocket_url),
      'websocket_status': websocket_posture['websocket_status'],
      'account_posture': account_posture,
      'websocket_subscription_count': websocket_posture['websocket_subscription_count'],
      'last_websocket_event_at': websocket_posture['last_websocket_event_at'],
      'binary_suitability': {
        key: websocket_posture.get(key)
        for key in (
          'binary_suitability_gate',
          'event_family_readback_count',
          'event_family_readback_failure_count',
          'binary_suitability_eligible_count',
          'binary_suitability_rejected_count',
          'binary_suitability_unknown_count',
          'binary_suitability_rejection_reasons',
        )
        if key in websocket_posture
      },
      'candidates': _candidate_evidence_preview(
        sandbox_candidates_extended if sandbox_candidates_extended is not None else live_candidate_records,
      ),
      **({'sandbox_extended_count': sandbox_extended_count} if sandbox_extended_count is not None else {}),
      'analytical_outputs': analytical_outputs,
    },
  )

  return {
    'decision': 'planned',
    'command_family': 'polyventure scan-once',
    'mode': 'ab_guarded',
    'dry_run': True,
    'dry_run_explanation': 'No order was submitted.',
    'balance_dollars': str(balance),
    'account_posture': account_posture,
    'market_count': len(markets),
    'candidate_count': len(live_candidates),
    'scan_shape_summary': _scan_shape_summary(
      markets,
      candidate_markets=candidate_markets,
      orderbook_enrichment_count=orderbook_enrichment_count,
      candidate_count=len(live_candidates),
      websocket_orderbook_count=int(websocket_posture.get('websocket_orderbook_count') or 0),
      orderbook_review_market_count=int(websocket_posture.get('orderbook_review_market_count') or len(candidate_markets)),
      rest_fallback_count=int(websocket_posture.get('rest_fallback_count') or 0),
      orderbook_enrichment_failure_count=int(websocket_posture.get('orderbook_enrichment_failure_count') or 0),
      websocket_hit_count=int(websocket_posture.get('websocket_hit_count') or websocket_posture.get('websocket_orderbook_count') or 0),
      binary_suitability={
        key: websocket_posture.get(key)
        for key in (
          'binary_suitability_gate',
          'event_family_readback_count',
          'event_family_readback_failure_count',
          'binary_suitability_eligible_count',
          'binary_suitability_rejected_count',
          'binary_suitability_unknown_count',
          'binary_suitability_rejection_reasons',
        )
        if key in websocket_posture
      },
    ),
    **sizing_summary,
    'orderbook_enrichment_count': orderbook_enrichment_count,
    'candidates': returned_candidates,
    **(
      {
        'sandbox_edge_relaxation_factor_applied': sandbox_relaxation_factor,
        'sandbox_extended_count': sandbox_extended_count,
        'sandbox_candidates_extended': sandbox_candidates_extended,
        'transition_rank': sandbox_transition_rank,
      }
      if sandbox_candidates_extended is not None
      else {}
    ),
    'analytical_outputs': analytical_outputs,
    'account_limits': {
      'usage_tier': limits.usage_tier,
      'read': asdict(limits.read),
      'write': asdict(limits.write),
    },
    'settings': safe_settings_summary(resolved_settings),
    'private_key_path_tail': str(Path(private_key_path).name),
    **_lane_runtime_posture(
      resolved_settings,
      lane_session_id=lane_session_id,
      connection_state=(
        'connected' if websocket_posture['websocket_connected']
        else 'skipped' if websocket_posture.get('websocket_status') == 'skipped_no_entry_window_markets'
        else 'waiting'
      ),
      websocket_connected=bool(websocket_posture['websocket_connected']),
    ),
    **(
      {
        'reason': 'scan_zero_found_retry',
        'message': '0 candidates found; retrying in 5 seconds.',
        'next_action': '0 candidates found; retrying in 5 seconds.',
        'scan_retry': {
          'active': True,
          'mode': 'zero_found_retry',
          'cycle_id': f'{lane_session_id}-{recorded_at.strftime("%Y%m%dT%H%M%SZ")}',
          'attempt_index': 1,
          'retry_after_sec': 5,
          'retry_countdown_remaining_sec': 5,
          'next_retry_at_utc': (recorded_at + timedelta(seconds=5)).isoformat(),
          'message': '0 candidates found; retrying in 5 seconds.',
        },
      }
      # SCHEDULER_ELIGIBILITY_THRESHOLD_REALIGNMENT_BMAP_2026-06-29 (supersedes the
      # 2026-06-25 empty-window exception): the reason a scan is zero-found is irrelevant.
      # Every completed scan with zero qualifying live candidates emits the retry metadata;
      # the scheduler is the sole authority that routes it to the retry threshold. The prior
      # `len(candidate_markets) > 0` guard suppressed retry on empty-window fetches and is removed.
      if len(live_candidates) == 0
      else {}
    ),
  }


def run_service_once(
  settings: Settings | None = None,
  *,
  mode: str = 'ab_guarded',
  allow_orders: bool = False,
  confirm_targeted: bool = False,
  env_override: str | None = None,
  subaccount_override: int | None = None,
  client_factory: ClientFactory | None = None,
  execution_profile: str | None = None,
  operator_lane_session_id: str | None = None,
  submit_handoff: dict[str, Any] | None = None,
) -> dict[str, Any]:
  if mode != 'ab_guarded' and not confirm_targeted:
    raise ValueError('Targeted mode requires explicit operator confirmation.')
  if allow_orders:
    raise ValueError(
      'Order-enabled runtime remains blocked until the sandbox-enable acceptance gates are satisfied.'
    )

  resolved_settings = _resolve_settings(
    settings,
    env_override=env_override,
    subaccount_override=subaccount_override,
  )
  _validate_env_alignment(resolved_settings)
  recorded_at = datetime.now(UTC)
  private_key_path = resolve_private_key_path(resolved_settings)
  private_key = load_private_key(private_key_path)
  client = (client_factory or KalshiHttpClient)(resolved_settings, private_key)

  balance = client.get_balance()
  limits = client.get_account_api_limits()
  funds_posture = _project_funds_posture(balance=balance, as_of=recorded_at)
  connection = open_database(resolved_settings.state_db_path)
  bridge_profile_active = str(execution_profile or '').strip().lower() == 'submit_order_bridge'
  operator_lane_session_id_normalized = str(operator_lane_session_id or '').strip()
  lane_session_id = (
    operator_lane_session_id_normalized
    if bridge_profile_active and operator_lane_session_id_normalized
    else _lane_session_id(resolved_settings.operation_lane)
  )
  persist_account_limits(
    connection,
    limits,
    recorded_at_utc=recorded_at.isoformat(),
    operation_lane=resolved_settings.operation_lane,
    lane_session_id=lane_session_id,
  )
  persist_service_heartbeat(
    connection,
    component='runtime-loop',
    status='startup-ok',
    recorded_at_utc=recorded_at.isoformat(),
    operation_lane=resolved_settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'mode': mode,
      'dry_run': True,
      'account_limits_loaded': True,
      'available_funds_snapshot': funds_posture['available_funds_snapshot'],
      'available_funds_as_of': funds_posture['available_funds_as_of'],
      'funds_refresh_status': funds_posture['funds_refresh_status'],
      'funds_refresh_reason': funds_posture['funds_refresh_reason'],
      'websocket_connected': False,
      'websocket_status': 'not_connected_on_current_dry_run_surface',
      'active_websocket_url_tail': _websocket_label(resolved_settings.active_websocket_url),
    },
  )

  markets, candidate_markets, candidate_market_by_ticker, orderbook_enrichment_count, websocket_posture = _unpack_candidate_market_set(
    _load_candidate_market_set(
      client,
      recorded_at=recorded_at,
      connection=connection,
      operation_lane=resolved_settings.operation_lane,
      lane_session_id=lane_session_id,
      settings=resolved_settings,
      private_key=private_key,
    )
  )
  for market in markets:
    record_market_seen(
      connection,
      ticker=market.ticker,
      status=market.status,
      close_time_utc=market.close_time.isoformat() if market.close_time is not None else None,
      last_seen_at_utc=recorded_at.isoformat(),
    )

  candidates = find_candidates(candidate_markets, recorded_at, resolved_settings)
  handoff_payload = submit_handoff if isinstance(submit_handoff, dict) else None
  saved_set = (
    _resolve_submit_handoff_saved_set(
      connection,
      submit_handoff=handoff_payload,
      operation_lane=resolved_settings.operation_lane,
    )
    if bridge_profile_active and handoff_payload is not None
    else fetch_latest_candidate_saved_set(connection, operation_lane=resolved_settings.operation_lane)
    if bridge_profile_active
    else None
  )
  saved_set_guard_reason = (
    _saved_set_bridge_guard_reason(saved_set, operation_lane=resolved_settings.operation_lane)
    if bridge_profile_active
    else None
  )
  saved_set_candidate = None
  saved_set_member = None
  saved_set_candidates: list[CandidatePair] = []
  saved_set_members: list[dict[str, Any]] = []
  saved_set_member_by_ticker: dict[str, dict[str, Any]] = {}
  if bridge_profile_active and saved_set_guard_reason is None:
    saved_set_candidates, saved_set_members = _resolve_saved_set_execution_candidates(saved_set)
    if not saved_set_candidates:
      saved_set_guard_reason = 'saved_set_member_detail_unavailable'
    else:
      saved_set_candidate = saved_set_candidates[0]
      saved_set_member = saved_set_members[0] if saved_set_members else None
      saved_set_member_by_ticker = {
        ticker: member
        for ticker, member in (
          (_saved_set_member_ticker(member), member)
          for member in saved_set_members
          if isinstance(member, dict)
        )
        if ticker
      }
      candidates = list(saved_set_candidates)
      _write_saved_set_candidates_in_flight(connection, saved_set=saved_set)
      persist_runtime_event(
        connection,
        level='INFO',
        event_type='candidate_queue_submitted',
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=resolved_settings.operation_lane,
        lane_session_id=lane_session_id,
        detail={
          'handoff_id': handoff_payload.get('handoff_id') if handoff_payload else None,
          'saved_set_id': saved_set.get('saved_set_id') if saved_set else None,
          'member_count': len(saved_set.get('members') or []) if saved_set else 0,
        },
      )
  try:
    sizing_summary = _build_dynamic_sizing_summary(
      candidates,
      balance=balance,
      settings=resolved_settings,
    )
  except Exception as exc:
    if bridge_profile_active:
      _persist_submit_bridge_phase_failed(
        connection,
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=resolved_settings.operation_lane,
        lane_session_id=lane_session_id,
        phase='candidate_math_projection',
        exc=exc,
        saved_set_id=saved_set.get('saved_set_id') if saved_set else None,
      )
    raise
  saved_set_snapshot = _project_saved_set_snapshot(
    saved_set,
    guard_reason=saved_set_guard_reason,
    matched_candidate_ticker=(str(saved_set_candidate.ticker) if saved_set_candidate is not None else None),
  )
  try:
    candidate_records = [
      _candidate_projection_record(
        candidate,
        rank=rank,
        qualifier_tier='live_qualifying',
        market_by_ticker=candidate_market_by_ticker,
        settings=resolved_settings,
      )
      for rank, candidate in enumerate(candidates, start=1)
    ]
    analytical_outputs = _build_analytical_outputs(
      candidate_records,
      candidates,
      recorded_at=recorded_at,
      operation_lane=resolved_settings.operation_lane,
      lane_session_id=lane_session_id,
      transition_rank=None,
      sizing_summary=sizing_summary,
      balance=balance,
      settings=resolved_settings,
      near_miss_candidates=None,
    )
    _persist_candidate_math_contract(
      connection,
      operation_lane=resolved_settings.operation_lane,
      lane_session_id=lane_session_id,
      operator_lane_session_id=operator_lane_session_id or None,
      recorded_at=recorded_at,
      source_action='runtime-cycle',
      analytical_outputs=analytical_outputs,
    )
  except Exception as exc:
    if bridge_profile_active:
      _persist_submit_bridge_phase_failed(
        connection,
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=resolved_settings.operation_lane,
        lane_session_id=lane_session_id,
        phase='candidate_math_projection',
        exc=exc,
        saved_set_id=saved_set_snapshot.get('saved_set_id'),
        ticker=str(saved_set_candidate.ticker) if saved_set_candidate is not None else None,
      )
    raise
  risk_gate_settings = resolved_settings
  if sizing_summary['dynamic_max_contracts'] is not None:
    effective_max_pair_contracts = min(
      Decimal(str(resolved_settings.max_pair_contracts)),
      Decimal(str(sizing_summary['dynamic_max_contracts'])),
    )
    risk_gate_settings = replace(
      resolved_settings,
      max_pair_contracts=float(effective_max_pair_contracts),
    )
  # Kalshi-truth alignment runs FIRST so settled/finalized pairs are terminalized from
  # exchange truth before any local timeout decision.
  try:
    kalshi_alignment_result = align_pairs_with_kalshi(
      connection,
      settings=resolved_settings,
      client=client,
      pairs=_alignment_candidate_pairs(
        _latest_pair_snapshots(connection, operation_lane=resolved_settings.operation_lane)
      ),
      recorded_at_utc=recorded_at.isoformat(),
      operation_lane=resolved_settings.operation_lane,
      lane_session_id=lane_session_id,
      reason='run_service_once_pre_submit_gate',
    )
  except Exception as exc:
    if bridge_profile_active:
      _persist_submit_bridge_phase_failed(
        connection,
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=resolved_settings.operation_lane,
        lane_session_id=lane_session_id,
        phase='risk_gate',
        exc=exc,
        saved_set_id=saved_set_snapshot.get('saved_set_id'),
      )
    raise
  # Cross-session safety: the unmatched-exposure timeout sweep then catches any remaining
  # stale one-sided exposure that did NOT settle, lane-wide (a frozen leg from ANY prior
  # session is still live risk), so it loads lane-scoped (no lane_session filter). The
  # session-scoped display/gate load is re-taken below (Lane E), keeping the execution
  # panel session-scoped.
  try:
    safety_sweep_pairs = _load_current_pairs(connection, operation_lane=resolved_settings.operation_lane)
    reconciled_pairs, timed_out_pair_count = _reconcile_current_pairs(
      connection,
      safety_sweep_pairs,
      settings=resolved_settings,
      recorded_at=recorded_at,
      lane_session_id=lane_session_id,
    )
    current_pairs = _load_current_pairs(connection, operation_lane=resolved_settings.operation_lane, lane_session_id=lane_session_id)
  except Exception as exc:
    if bridge_profile_active:
      _persist_submit_bridge_phase_failed(
        connection,
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=resolved_settings.operation_lane,
        lane_session_id=lane_session_id,
        phase='risk_gate',
        exc=exc,
        saved_set_id=saved_set_snapshot.get('saved_set_id'),
      )
    raise
  _mark_auto_canceled_candidates_terminal(
    connection,
    operation_lane=resolved_settings.operation_lane,
    operator_lane_session_id=operator_lane_session_id or lane_session_id,
    recorded_at=recorded_at,
  )
  _mark_expired_candidates_terminal(
    connection,
    operation_lane=resolved_settings.operation_lane,
    operator_lane_session_id=operator_lane_session_id or lane_session_id,
    recorded_at=recorded_at,
  )
  planned_pairs: list[dict[str, Any]] = []
  submit_guard_blocked_count = 0
  submit_guard_submitted_count = 0
  submit_guard_block_reasons: list[str] = []
  bridge_recent_trades_by_ticker: dict[str, dict[str, Any]] = {}
  final_prepared_live_bridge = False
  blocked_reason: str | None = None
  execution_chronology: dict[str, Any] = {'enabled': False, 'profile': ''}

  # At-point balance refresh: if funds_posture is stale and this is a live bridge-submit
  # cycle, make one live balance call to get a current snapshot before the gate check.
  # Fail-closed: a failed refresh leaves funds_posture unchanged (stale still blocks).
  if (
    bridge_profile_active
    and resolved_settings.operation_lane == 'live'
    and funds_posture['stale_blocks_submit']
  ):
    try:
      _fresh_balance = client.get_balance()
      funds_posture = _project_funds_posture(balance=_fresh_balance, as_of=datetime.now(UTC))
    except KalshiHttpError:
      pass

  if timed_out_pair_count:
    blocked_reason = 'unmatched_exposure_timeout'
    persist_runtime_event(
      connection,
      level='WARN',
      event_type='risk_gate_blocked',
      recorded_at_utc=recorded_at.isoformat(),
      operation_lane=resolved_settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={
        'open_pair_count': len(current_pairs),
        'timed_out_pair_count': timed_out_pair_count,
      },
    )
  elif funds_posture['stale_blocks_submit']:
    blocked_reason = 'stale_funds_requires_reconcile'
    persist_runtime_event(
      connection,
      level='WARN',
      event_type='submit_bridge_blocked' if bridge_profile_active else 'risk_gate_blocked',
      recorded_at_utc=recorded_at.isoformat(),
      operation_lane=resolved_settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={
        'blocked_reason': blocked_reason,
        'funds_refresh_status': funds_posture['funds_refresh_status'],
        'funds_refresh_reason': funds_posture['funds_refresh_reason'],
        'available_funds_as_of': funds_posture['available_funds_as_of'],
      },
    )
  elif bridge_profile_active and saved_set_guard_reason is not None:
    blocked_reason = saved_set_guard_reason
    persist_runtime_event(
      connection,
      level='WARN',
      event_type='submit_bridge_blocked',
      recorded_at_utc=recorded_at.isoformat(),
      operation_lane=resolved_settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={
        'blocked_reason': blocked_reason,
        'saved_set_id': saved_set_snapshot.get('saved_set_id'),
        'saved_set_state_id': saved_set_snapshot.get('state_id'),
        'saved_set_actionability_status': saved_set_snapshot.get('actionability_status'),
      },
    )
  elif candidates and can_open_new_pair(
    current_pairs,
    balance,
    risk_gate_settings,
    as_of=recorded_at,
    account_limits_loaded=True,
    mode=mode,
    confirm_targeted=confirm_targeted,
  ):
    if bridge_profile_active and resolved_settings.operation_lane == 'live':
      survivor_projection = _prepare_bridge_submit_survivors(
        client=client,
        connection=connection,
        candidates=list(candidates),
        balance=balance,
        settings=resolved_settings,
        recorded_at=recorded_at,
        lane_session_id=lane_session_id,
        saved_set_snapshot=saved_set_snapshot,
        candidate_market_by_ticker=candidate_market_by_ticker,
        mode=mode,
        confirm_targeted=confirm_targeted,
      )
      candidates = list(survivor_projection['candidates'])
      sizing_summary = dict(survivor_projection['sizing_summary'])
      bridge_recent_trades_by_ticker = dict(survivor_projection['recent_trades_by_ticker'])
      submit_guard_blocked_count += int(survivor_projection['blocked_count'])
      submit_guard_block_reasons.extend(list(survivor_projection['block_reasons']))
      final_prepared_live_bridge = True
      _persist_submit_bridge_final_sizing_resolved(
        connection,
        recorded_at_utc=recorded_at.isoformat(),
        operation_lane=resolved_settings.operation_lane,
        lane_session_id=lane_session_id,
        candidates=candidates,
        sizing_summary=sizing_summary,
        blocked_count=int(survivor_projection['blocked_count']),
        block_reasons=list(survivor_projection['block_reasons']),
        saved_set_id=saved_set_snapshot.get('saved_set_id'),
      )
      if survivor_projection.get('blocked_reason'):
        blocked_reason = str(survivor_projection['blocked_reason'])
      if sizing_summary['dynamic_max_contracts'] is not None:
        effective_max_pair_contracts = min(
          Decimal(str(resolved_settings.max_pair_contracts)),
          Decimal(str(sizing_summary['dynamic_max_contracts'])),
        )
        risk_gate_settings = replace(
          resolved_settings,
          max_pair_contracts=float(effective_max_pair_contracts),
        )
    _candidates_to_process = (
      list(candidates)
      if bridge_profile_active
      else list(candidates)
    )
    bridge_live_batch_submit_items: list[dict[str, Any]] = []
    bridge_live_batch_collection_enabled = (
      bridge_profile_active
      and resolved_settings.operation_lane == 'live'
      and str(execution_profile or '').strip().lower() == 'submit_order_bridge'
      and len(_candidates_to_process) > 1
    )
    for _batch_idx, candidate in enumerate(_candidates_to_process):
      if _batch_idx > 0:
        current_pairs = _load_current_pairs(connection, operation_lane=resolved_settings.operation_lane, lane_session_id=lane_session_id)
        if not can_open_new_pair(
          current_pairs, balance, risk_gate_settings,
          as_of=recorded_at, account_limits_loaded=True, mode=mode, confirm_targeted=confirm_targeted,
        ):
          break
      candidate_market = candidate_market_by_ticker.get(candidate.ticker)
      if candidate_market is None and not bridge_profile_active:
        continue
      dynamic_max_contracts = Decimal(str(sizing_summary['dynamic_max_contracts']))
      if dynamic_max_contracts < 1:
        if bridge_profile_active:
          blocked_reason = 'dynamic_notional_cap_below_one_contract'
          persist_runtime_event(
            connection,
            level='WARN',
            event_type='risk_gate_blocked',
            recorded_at_utc=recorded_at.isoformat(),
            operation_lane=resolved_settings.operation_lane,
            lane_session_id=lane_session_id,
            detail={
              'blocked_reason': blocked_reason,
              'dynamic_pair_notional_cap_dollars': sizing_summary['dynamic_pair_notional_cap_dollars'],
              'dynamic_max_contracts': sizing_summary['dynamic_max_contracts'],
            },
          )
        continue
      else:
        effective_max_pair_contracts = min(
          Decimal(str(resolved_settings.max_pair_contracts)),
          dynamic_max_contracts,
        )
        sizing_settings = replace(
          resolved_settings,
          max_pair_contracts=float(effective_max_pair_contracts),
        )
        finalist_orderbook = None
        if resolved_settings.operation_lane == 'live' and bridge_profile_active and not final_prepared_live_bridge:
          try:
            fresh_market = client.get_market(candidate.ticker)
          except Exception as exc:
            blocked_reason = 'fresh_market_readback_failed'
            persist_runtime_event(
              connection,
              level='WARN',
              event_type='submit_bridge_blocked',
              recorded_at_utc=recorded_at.isoformat(),
              operation_lane=resolved_settings.operation_lane,
              lane_session_id=lane_session_id,
              detail={
                'blocked_reason': blocked_reason,
                'ticker': candidate.ticker,
                'error_family': type(exc).__name__,
              },
            )
            continue
          close_time = getattr(fresh_market, 'close_time', None)
          market_status = str(getattr(fresh_market, 'status', '') or '').strip().lower()
          if close_time is None or market_status not in {'open', 'active'}:
            blocked_reason = 'fresh_market_close_truth_unavailable'
            persist_runtime_event(
              connection,
              level='WARN',
              event_type='submit_bridge_blocked',
              recorded_at_utc=recorded_at.isoformat(),
              operation_lane=resolved_settings.operation_lane,
              lane_session_id=lane_session_id,
              detail={
                'blocked_reason': blocked_reason,
                'ticker': candidate.ticker,
                'market_status': market_status,
                'close_time_present': close_time is not None,
              },
            )
            continue
          try:
            proof_block_reason, proof_detail = _submit_binary_proof_block(client, candidate, fresh_market)
          except Exception as exc:
            _persist_submit_bridge_phase_failed(
              connection,
              recorded_at_utc=recorded_at.isoformat(),
              operation_lane=resolved_settings.operation_lane,
              lane_session_id=lane_session_id,
              phase='binary_proof',
              exc=exc,
              saved_set_id=saved_set_snapshot.get('saved_set_id'),
              ticker=candidate.ticker,
            )
            raise
          if proof_block_reason is not None:
            blocked_reason = proof_block_reason
            persist_runtime_event(
              connection,
              level='WARN',
              event_type='submit_binary_proof_blocked',
              recorded_at_utc=recorded_at.isoformat(),
              operation_lane=resolved_settings.operation_lane,
              lane_session_id=lane_session_id,
              detail={
                'blocked_reason': blocked_reason,
                **proof_detail,
              },
            )
            persist_known_non_binary_market(
              connection,
              recorded_at_utc=recorded_at.isoformat(),
              operation_lane=resolved_settings.operation_lane,
              lane_session_id=lane_session_id,
              classification_reason=str(proof_detail.get('fresh_binary_reason') or blocked_reason),
              actionability='unknown_fail_closed' if 'unknown' in blocked_reason else 'deferred_non_binary',
              market_ticker=str(proof_detail.get('ticker') or candidate.ticker),
              event_ticker=str(proof_detail.get('event_ticker') or ''),
              series_ticker=str(proof_detail.get('fresh_binary_series_ticker') or ''),
              shape_signature='{series}|{reason}|siblings:{count}'.format(
                series=str(proof_detail.get('fresh_binary_series_ticker') or 'series:unknown'),
                reason=str(proof_detail.get('fresh_binary_reason') or blocked_reason),
                count=int(proof_detail.get('fresh_binary_market_count') or 0),
              ),
              market_count=int(proof_detail.get('fresh_binary_market_count') or 0),
              sample_sibling_tickers=proof_detail.get('fresh_binary_sibling_sample') if isinstance(proof_detail.get('fresh_binary_sibling_sample'), list) else (),
              source_run_id=lane_session_id,
              source_runtime_event_id=None,
              detail={'source': 'submit_binary_proof_gate', **proof_detail},
            )
            continue
          fresh_seconds_to_close = int((close_time.astimezone(UTC) - recorded_at.astimezone(UTC)).total_seconds())
          if fresh_seconds_to_close < resolved_settings.entry_window_end_sec:
            blocked_reason = 'fresh_market_too_close_to_close'
            persist_runtime_event(
              connection,
              level='WARN',
              event_type='submit_bridge_blocked',
              recorded_at_utc=recorded_at.isoformat(),
              operation_lane=resolved_settings.operation_lane,
              lane_session_id=lane_session_id,
              detail={
                'blocked_reason': blocked_reason,
                'ticker': candidate.ticker,
                'seconds_to_close': fresh_seconds_to_close,
                'entry_window_end_sec': resolved_settings.entry_window_end_sec,
              },
            )
            continue
          candidate_market = fresh_market
          candidate = replace(candidate, seconds_to_close=fresh_seconds_to_close)
          try:
            _ob = client.get_orderbook(candidate.ticker)
            finalist_orderbook = _ob
            yes_price_live = _ob.best_yes_bid
            no_price_live = _ob.best_no_bid
          except Exception as exc:
            _persist_submit_bridge_phase_failed(
              connection,
              recorded_at_utc=recorded_at.isoformat(),
              operation_lane=resolved_settings.operation_lane,
              lane_session_id=lane_session_id,
              phase='live_orderbook_readback',
              exc=exc,
              saved_set_id=saved_set_snapshot.get('saved_set_id'),
              ticker=candidate.ticker,
            )
            raise
          if yes_price_live is None or no_price_live is None or yes_price_live <= 0 or no_price_live <= 0:
            blocked_reason = 'live_price_unavailable'
            persist_runtime_event(
              connection,
              level='WARN',
              event_type='live_order_price_fetch_blocked',
              recorded_at_utc=recorded_at.isoformat(),
              operation_lane=resolved_settings.operation_lane,
              lane_session_id=lane_session_id,
              detail={
                'blocked_reason': blocked_reason,
                'ticker': candidate.ticker,
                'best_yes_bid': str(yes_price_live) if yes_price_live is not None else None,
                'best_no_bid': str(no_price_live) if no_price_live is not None else None,
              },
            )
            continue
          candidate = reprice_candidate(candidate, yes_price_live, no_price_live, resolved_settings)
        try:
          plan = build_pair_order_plan(candidate, balance, sizing_settings)
        except Exception as exc:
          if bridge_profile_active and _is_candidate_local_pair_plan_rejection(exc):
            blocked_reason = 'pair_plan_validation'
            _persist_submit_bridge_candidate_rejected_before_order(
              connection,
              recorded_at_utc=recorded_at.isoformat(),
              operation_lane=resolved_settings.operation_lane,
              lane_session_id=lane_session_id,
              phase=blocked_reason,
              exc=exc,
              saved_set_id=saved_set_snapshot.get('saved_set_id'),
              ticker=candidate.ticker,
            )
            continue
          if bridge_profile_active:
            _persist_submit_bridge_phase_failed(
              connection,
              recorded_at_utc=recorded_at.isoformat(),
              operation_lane=resolved_settings.operation_lane,
              lane_session_id=lane_session_id,
              phase='pair_plan_validation',
              exc=exc,
              saved_set_id=saved_set_snapshot.get('saved_set_id'),
              ticker=candidate.ticker,
            )
          raise
        validation_market_status = (
          str(getattr(candidate_market, 'status', '') or '')
          if candidate_market is not None
          else 'open'
        )
        try:
          validate_pair_plan(
            plan,
            candidate,
            resolved_settings,
            market_status=validation_market_status,
            account_limits_loaded=True,
            mode=mode,
            confirm_targeted=confirm_targeted,
            as_of=recorded_at,
          )
        except Exception as exc:
          if bridge_profile_active and _is_candidate_local_pair_plan_rejection(exc):
            blocked_reason = 'pair_plan_validation'
            _persist_submit_bridge_candidate_rejected_before_order(
              connection,
              recorded_at_utc=recorded_at.isoformat(),
              operation_lane=resolved_settings.operation_lane,
              lane_session_id=lane_session_id,
              phase=blocked_reason,
              exc=exc,
              saved_set_id=saved_set_snapshot.get('saved_set_id'),
              ticker=candidate.ticker,
            )
            continue
          if bridge_profile_active:
            _persist_submit_bridge_phase_failed(
              connection,
              recorded_at_utc=recorded_at.isoformat(),
              operation_lane=resolved_settings.operation_lane,
              lane_session_id=lane_session_id,
              phase='pair_plan_validation',
              exc=exc,
              saved_set_id=saved_set_snapshot.get('saved_set_id'),
              ticker=candidate.ticker,
            )
          raise
        coverability_guard_active = bridge_profile_active and resolved_settings.operation_lane == 'live' and not final_prepared_live_bridge
        coverability_recent_trades: dict[str, Any] | None = None
        coverability_recent_trades_failed = False
        static_guard = (
          evaluate_pre_submit_coverability_static(
            plan,
            resolved_settings,
            best_yes_bid=yes_price_live,
            best_no_bid=no_price_live,
          )
          if coverability_guard_active
          else CoverabilityGuardResult(ok=True)
        )
        if coverability_guard_active and not static_guard.ok:
          blocked_reason = str(static_guard.reason or 'coverability_static_blocked')
          submit_guard_blocked_count += 1
          submit_guard_block_reasons.append(blocked_reason)
          _persist_submit_bridge_candidate_rejected_before_order(
            connection,
            recorded_at_utc=recorded_at.isoformat(),
            operation_lane=resolved_settings.operation_lane,
            lane_session_id=lane_session_id,
            phase=blocked_reason,
            exc=ValueError(static_guard.message or blocked_reason),
            saved_set_id=saved_set_snapshot.get('saved_set_id'),
            ticker=candidate.ticker,
            detail=static_guard.detail,
          )
          continue
        if coverability_guard_active:
          try:
            coverability_recent_trades = bridge_recent_trades_by_ticker.get(candidate.ticker)
            if coverability_recent_trades is None:
              coverability_recent_trades = client.get_recent_trades(
                candidate.ticker,
                window_sec=resolved_settings.flow_window_sec,
              )
              bridge_recent_trades_by_ticker[candidate.ticker] = coverability_recent_trades
            flow_guard = evaluate_flow_coverability(
              coverability_recent_trades.get('yes_flow_fp'),
              coverability_recent_trades.get('no_flow_fp'),
              plan.contract_count,
              resolved_settings,
            )
          except Exception:
            coverability_recent_trades_failed = True
            flow_guard = CoverabilityGuardResult(
              ok=False,
              reason='coverability_flow_unavailable',
              message='Recent per-side flow is unavailable for coverability validation.',
            )
          if not flow_guard.ok:
            blocked_reason = str(flow_guard.reason or 'coverability_flow_blocked')
            submit_guard_blocked_count += 1
            submit_guard_block_reasons.append(blocked_reason)
            if finalist_orderbook is not None:
              capture_pair_liquidity_observation(
                client,
                connection,
                pair_id=plan.pair_id,
                ticker=candidate.ticker,
                phase='submit',
                orderbook=finalist_orderbook,
                intended_yes_price=plan.yes_price,
                intended_no_price=plan.no_price,
                intended_contract_count=plan.contract_count,
                market=candidate_market,
                settings=resolved_settings,
                recorded_at_utc=recorded_at.isoformat(),
                lane_session_id=lane_session_id,
                recent_trades=coverability_recent_trades,
                recent_trades_read_failed=coverability_recent_trades_failed,
              )
            _persist_submit_bridge_candidate_rejected_before_order(
              connection,
              recorded_at_utc=recorded_at.isoformat(),
              operation_lane=resolved_settings.operation_lane,
              lane_session_id=lane_session_id,
              phase=blocked_reason,
              exc=ValueError(flow_guard.message or blocked_reason),
              saved_set_id=saved_set_snapshot.get('saved_set_id'),
              ticker=candidate.ticker,
              detail=flow_guard.detail,
            )
            continue
        blocked_reason = None
        try:
          persist_pair_plan(
            connection,
            plan,
            created_at_utc=recorded_at.isoformat(),
            operation_lane=resolved_settings.operation_lane,
          )
        except Exception as exc:
          if bridge_profile_active:
            _persist_submit_bridge_phase_failed(
              connection,
              recorded_at_utc=recorded_at.isoformat(),
              operation_lane=resolved_settings.operation_lane,
              lane_session_id=lane_session_id,
              phase='pair_plan_persist',
              exc=exc,
              saved_set_id=saved_set_snapshot.get('saved_set_id'),
              ticker=candidate.ticker,
            )
          raise
        if finalist_orderbook is not None:
          capture_pair_liquidity_observation(
            client,
            connection,
            pair_id=plan.pair_id,
            ticker=candidate.ticker,
            phase='submit',
            orderbook=finalist_orderbook,
            intended_yes_price=plan.yes_price,
            intended_no_price=plan.no_price,
            intended_contract_count=plan.contract_count,
            market=candidate_market,
            settings=resolved_settings,
            recorded_at_utc=recorded_at.isoformat(),
            lane_session_id=lane_session_id,
            recent_trades=coverability_recent_trades,
            recent_trades_read_failed=coverability_recent_trades_failed,
          )
        try:
          persist_pair_state_transition(
            connection,
            pair_id=plan.pair_id,
            state='PLANNED',
            recorded_at_utc=recorded_at.isoformat(),
            operation_lane=resolved_settings.operation_lane,
            lane_session_id=lane_session_id,
            detail={
              'ticker': candidate.ticker,
              'mode': mode,
              'dry_run': not bridge_profile_active,
              'websocket_connected': False,
              'order_submission_blocked': not bridge_profile_active,
              'order_submission_pending': bridge_profile_active,
              'dry_run_explanation': 'No order was submitted.' if not bridge_profile_active else '',
              'yes_filled_contracts': '0',
              'no_filled_contracts': '0',
              'average_yes_price': str(plan.yes_price),
              'average_no_price': str(plan.no_price),
              'realized_fees_dollars': '0',
              'effective_density': sizing_summary['effective_density'],
              'dynamic_pair_notional_pct': sizing_summary['dynamic_pair_notional_pct'],
              'dynamic_pair_notional_cap_dollars': sizing_summary['dynamic_pair_notional_cap_dollars'],
              'dynamic_max_contracts': sizing_summary['dynamic_max_contracts'],
              'binding_limiter': sizing_summary['binding_limiter'],
              'lane_session_id': lane_session_id,
              **(
                _submit_bridge_detail_fields(
                  legacy_state='PLANNED',
                  saved_set_snapshot=saved_set_snapshot,
                  submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state='PLANNED'),
                )
                if bridge_profile_active
                else {}
              ),
            },
          )
        except Exception as exc:
          if bridge_profile_active:
            _persist_submit_bridge_phase_failed(
              connection,
              recorded_at_utc=recorded_at.isoformat(),
              operation_lane=resolved_settings.operation_lane,
              lane_session_id=lane_session_id,
              phase='pair_plan_persist',
              exc=exc,
              saved_set_id=saved_set_snapshot.get('saved_set_id'),
              ticker=candidate.ticker,
            )
          raise
        persist_operator_action(
          connection,
          action='submit-bridge-plan' if bridge_profile_active else 'dry-run-plan',
          pair_id=plan.pair_id,
          recorded_at_utc=recorded_at.isoformat(),
          operation_lane=resolved_settings.operation_lane,
          lane_session_id=lane_session_id,
          detail={
            'ticker': candidate.ticker,
            'contract_count': str(plan.contract_count),
            'dry_run_explanation': 'No order was submitted.' if not bridge_profile_active else '',
            'effective_density': sizing_summary['effective_density'],
            'dynamic_pair_notional_pct': sizing_summary['dynamic_pair_notional_pct'],
            'binding_limiter': sizing_summary['binding_limiter'],
            'active_websocket_url_tail': _websocket_label(resolved_settings.active_websocket_url),
            **(
              {
                'execution_intent_source': 'saved_set',
                'saved_set_id': saved_set_snapshot.get('saved_set_id'),
                'saved_set_actionability_status': saved_set_snapshot.get('actionability_status'),
              }
              if bridge_profile_active
              else {}
            ),
          },
        )
        persist_runtime_event(
          connection,
          level='INFO',
          event_type='pair_plan_created',
          pair_id=plan.pair_id,
          recorded_at_utc=recorded_at.isoformat(),
          operation_lane=resolved_settings.operation_lane,
          lane_session_id=lane_session_id,
          detail={
            'ticker': candidate.ticker,
            'dry_run': not bridge_profile_active,
            'no_order_submitted': not bridge_profile_active,
            'order_submission_pending': bridge_profile_active,
            'effective_density': sizing_summary['effective_density'],
            'dynamic_pair_notional_pct': sizing_summary['dynamic_pair_notional_pct'],
            'dynamic_pair_notional_cap_dollars': sizing_summary['dynamic_pair_notional_cap_dollars'],
            'dynamic_max_contracts': sizing_summary['dynamic_max_contracts'],
            'binding_limiter': sizing_summary['binding_limiter'],
            'active_websocket_url_tail': _websocket_label(resolved_settings.active_websocket_url),
            **(
              _submit_bridge_detail_fields(
                legacy_state='PLANNED',
                saved_set_snapshot=saved_set_snapshot,
                submit_response_id=_submit_bridge_response_id(blocked_reason=None, legacy_state='PLANNED'),
              )
              if bridge_profile_active
              else {}
            ),
          },
        )
        if str(execution_profile or '').strip().lower() == 'submit_order_bridge':
          if signed_evidence.load_signing_key() is None:
            # Pre-submit fail-closed gate (Lane L5b): signed money evidence is a hard precondition
            # for placing orders. With no signing key the orders are NOT placed, uniformly across
            # sandbox and live, and an explicit blocked + unsigned record is emitted.
            execution_chronology = _signing_key_blocked_execution_chronology(
              operation_lane=resolved_settings.operation_lane,
              lane_session_id=lane_session_id,
              pair_id=plan.pair_id,
            )
            persist_runtime_event(
              connection,
              level='ERROR',
              event_type='submit_blocked_signing_key_unavailable',
              pair_id=plan.pair_id,
              recorded_at_utc=recorded_at.isoformat(),
              operation_lane=resolved_settings.operation_lane,
              lane_session_id=lane_session_id,
              detail={'ticker': plan.ticker, 'blocked_reason': 'signing_key_unavailable'},
            )
          elif bridge_live_batch_collection_enabled:
            execution_chronology = _live_batch_chronology('BATCH_DISPATCH_PENDING')
            bridge_live_batch_submit_items.append(
              {
                'plan': plan,
                'candidate': candidate,
                'planned_pair_index': len(planned_pairs),
              }
            )
          else:
            if resolved_settings.operation_lane == 'live':
              try:
                execution_chronology = _place_live_pair_orders(
                  client,
                  connection,
                  plan=plan,
                  settings=resolved_settings,
                  lane_session_id=lane_session_id,
                  recorded_at=recorded_at,
                  sizing_summary=sizing_summary,
                  saved_set_snapshot=saved_set_snapshot,
                )
              except Exception as exc:
                _persist_submit_bridge_phase_failed(
                  connection,
                  recorded_at_utc=recorded_at.isoformat(),
                  operation_lane=resolved_settings.operation_lane,
                  lane_session_id=lane_session_id,
                  phase='live_order_dispatch',
                  exc=exc,
                  saved_set_id=saved_set_snapshot.get('saved_set_id'),
                  ticker=plan.ticker,
                )
                raise
            else:
              execution_chronology = _persist_submit_fill_cancel_reconcile_chronology(
                connection,
                plan=plan,
                settings=resolved_settings,
                lane_session_id=lane_session_id,
                recorded_at=recorded_at,
                sizing_summary=sizing_summary,
                saved_set_snapshot=saved_set_snapshot,
              )
            execution_chronology, signed_money_evidence_detail = _attach_money_evidence_signature_for_result_slot(
              execution_chronology,
              operation_lane=resolved_settings.operation_lane,
              lane_session_id=lane_session_id,
              pair_id=plan.pair_id,
            )
            if execution_chronology.get('enabled'):
              persist_runtime_event(
                connection,
                level='INFO',
                event_type='bridge_execution_result_slot',
                pair_id=plan.pair_id,
                recorded_at_utc=recorded_at.isoformat(),
                operation_lane=resolved_settings.operation_lane,
                lane_session_id=lane_session_id,
                detail={
                  'enabled': True,
                  'terminal_state': str(execution_chronology.get('terminal_state') or ''),
                  'profile': str(execution_chronology.get('profile') or ''),
                  'lane_session_id': lane_session_id,
                  **signed_money_evidence_detail,
                },
              )
        planned_pairs.append(
          {
            'pair_id': plan.pair_id,
            'ticker': plan.ticker,
            'contract_count': str(plan.contract_count),
            'yes_price': str(plan.yes_price),
            'no_price': str(plan.no_price),
            'edge_gross_per_contract': str(candidate.edge_gross_per_contract),
            'edge_net_per_contract': str(candidate.edge_net_per_contract),
            'effective_density': sizing_summary['effective_density'],
            'dynamic_pair_notional_pct': sizing_summary['dynamic_pair_notional_pct'],
            'dynamic_pair_notional_cap_dollars': sizing_summary['dynamic_pair_notional_cap_dollars'],
            'dynamic_max_contracts': sizing_summary['dynamic_max_contracts'],
            'binding_limiter': sizing_summary['binding_limiter'],
            'execution_intent_source': 'saved_set' if bridge_profile_active else 'candidate_scan',
            'saved_set_id': saved_set_snapshot.get('saved_set_id') if bridge_profile_active else None,
            'saved_set_recorded_at_utc': saved_set_snapshot.get('recorded_at_utc') if bridge_profile_active else None,
            'saved_set_actionability_status': saved_set_snapshot.get('actionability_status') if bridge_profile_active else None,
            'saved_set_member_ticker': _saved_set_member_ticker(saved_set_member_by_ticker.get(plan.ticker, saved_set_member or {})) if bridge_profile_active else None,
            'execution_profile': str(execution_profile or '').strip().lower() or 'dry_run_plan',
            'execution_terminal_state': str(execution_chronology.get('terminal_state') or 'PLANNED'),
            'submit_response_id': (
              _submit_bridge_response_id(
                blocked_reason=None,
                legacy_state=str(execution_chronology.get('terminal_state') or 'PLANNED'),
              )
              if bridge_profile_active
              else None
            ),
            'public_state_id': (
              _project_public_state_id(str(execution_chronology.get('terminal_state') or 'PLANNED'))
              if bridge_profile_active
              else None
            ),
          }
        )
        submit_guard_submitted_count += 1
    if bridge_live_batch_submit_items:
      if len(bridge_live_batch_submit_items) == 1:
        item = bridge_live_batch_submit_items[0]
        plan = item['plan']
        try:
          batch_chronologies = {
            plan.pair_id: _place_live_pair_orders(
              client,
              connection,
              plan=plan,
              settings=resolved_settings,
              lane_session_id=lane_session_id,
              recorded_at=recorded_at,
              sizing_summary=sizing_summary,
              saved_set_snapshot=saved_set_snapshot,
            )
          }
        except Exception as exc:
          _persist_submit_bridge_phase_failed(
            connection,
            recorded_at_utc=recorded_at.isoformat(),
            operation_lane=resolved_settings.operation_lane,
            lane_session_id=lane_session_id,
            phase='live_order_dispatch',
            exc=exc,
            saved_set_id=saved_set_snapshot.get('saved_set_id'),
            ticker=plan.ticker,
          )
          raise
      else:
        batch_plans = [item['plan'] for item in bridge_live_batch_submit_items]
        try:
          batch_chronologies = _place_live_pair_orders_batch(
            client,
            connection,
            plans=batch_plans,
            settings=resolved_settings,
            lane_session_id=lane_session_id,
            recorded_at=recorded_at,
            sizing_summary=sizing_summary,
            saved_set_snapshot=saved_set_snapshot,
          )
        except Exception as exc:
          _persist_submit_bridge_phase_failed(
            connection,
            recorded_at_utc=recorded_at.isoformat(),
            operation_lane=resolved_settings.operation_lane,
            lane_session_id=lane_session_id,
            phase='live_order_batch_dispatch',
            exc=exc,
            saved_set_id=saved_set_snapshot.get('saved_set_id'),
          )
          raise
      for item in bridge_live_batch_submit_items:
        plan = item['plan']
        actual_chronology = batch_chronologies.get(plan.pair_id) or _live_batch_chronology(
          'RECONCILE_REQUIRED',
          blocked_reason='batch_submit_result_missing',
        )
        actual_chronology, signed_money_evidence_detail = _attach_money_evidence_signature_for_result_slot(
          actual_chronology,
          operation_lane=resolved_settings.operation_lane,
          lane_session_id=lane_session_id,
          pair_id=plan.pair_id,
        )
        execution_chronology = actual_chronology
        if actual_chronology.get('enabled'):
          persist_runtime_event(
            connection,
            level='INFO',
            event_type='bridge_execution_result_slot',
            pair_id=plan.pair_id,
            recorded_at_utc=recorded_at.isoformat(),
            operation_lane=resolved_settings.operation_lane,
            lane_session_id=lane_session_id,
            detail={
              'enabled': True,
              'terminal_state': str(actual_chronology.get('terminal_state') or ''),
              'profile': str(actual_chronology.get('profile') or ''),
              'lane_session_id': lane_session_id,
              'submit_mode': 'single_create_v2' if len(bridge_live_batch_submit_items) == 1 else 'batch_create_v2',
              **signed_money_evidence_detail,
            },
          )
        planned_pair_index = int(item['planned_pair_index'])
        if 0 <= planned_pair_index < len(planned_pairs):
          terminal_state = str(actual_chronology.get('terminal_state') or 'PLANNED')
          planned_pairs[planned_pair_index]['execution_terminal_state'] = terminal_state
          planned_pairs[planned_pair_index]['submit_response_id'] = _submit_bridge_response_id(
            blocked_reason=None,
            legacy_state=terminal_state,
          )
          planned_pairs[planned_pair_index]['public_state_id'] = _project_public_state_id(terminal_state)
  elif not candidates:
    blocked_reason = 'saved_set_not_eligible' if bridge_profile_active else 'no_viable_candidates'
    persist_runtime_event(
      connection,
      level='WARN' if bridge_profile_active else 'INFO',
      event_type='submit_bridge_blocked' if bridge_profile_active else 'no_candidate',
      recorded_at_utc=recorded_at.isoformat(),
      operation_lane=resolved_settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={
        'mode': mode,
        **(
          {
            'blocked_reason': blocked_reason,
            'saved_set_id': saved_set_snapshot.get('saved_set_id'),
          }
          if bridge_profile_active
          else {}
        ),
      },
    )
  else:
    blocked_reason = 'already_active_pair' if bridge_profile_active and current_pairs else 'risk_gate_blocked_new_pair'
    persist_runtime_event(
      connection,
      level='WARN',
      event_type='submit_bridge_blocked' if bridge_profile_active else 'risk_gate_blocked',
      recorded_at_utc=recorded_at.isoformat(),
      operation_lane=resolved_settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={
        'open_pair_count': len(current_pairs),
        'timed_out_pair_count': timed_out_pair_count,
        **({'blocked_reason': blocked_reason} if bridge_profile_active else {}),
      },
    )

  submit_response_id = (
    _submit_bridge_response_id(
      blocked_reason=blocked_reason,
      legacy_state=(
        str(execution_chronology.get('terminal_state') or '')
        if execution_chronology.get('enabled')
        else str(planned_pairs[0].get('execution_terminal_state') or '') if planned_pairs else None
      ),
      has_active_pair=bool(current_pairs),
    )
    if bridge_profile_active
    else None
  )
  submit_rest_state_id = (
    _project_public_state_id(
      str(execution_chronology.get('terminal_state') or '')
      if execution_chronology.get('enabled')
      else (str(current_pairs[0].state) if blocked_reason in {'already_active_pair', 'unmatched_exposure_timeout'} and current_pairs else None),
      blocked_reason=blocked_reason,
    )
    if bridge_profile_active
    else None
  )
  top_level_failure_posture = (
    _project_failure_posture(
      public_state_id=submit_rest_state_id,
      blocked_reason=blocked_reason,
    )
    if bridge_profile_active
    else None
  )
  top_level_action_contract = (
    _project_action_contract(
      public_state_id=submit_rest_state_id,
      blocked_reason=blocked_reason,
    )
    if bridge_profile_active
    else None
  )

  persist_service_heartbeat(
    connection,
    component='runtime-loop',
    status='cycle-complete',
    recorded_at_utc=recorded_at.isoformat(),
    operation_lane=resolved_settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'candidate_count': len(candidates),
      'planned_pair_count': len(planned_pairs),
      'effective_density': sizing_summary['effective_density'],
      'dynamic_pair_notional_pct': sizing_summary['dynamic_pair_notional_pct'],
      'binding_limiter': sizing_summary['binding_limiter'],
      'mode': mode,
      'orderbook_enrichment_count': orderbook_enrichment_count,
      'reconciled_pair_count': len(reconciled_pairs),
      'timed_out_pair_count': timed_out_pair_count,
      'kalshi_alignment_terminalized_count': len(kalshi_alignment_result.terminalized),
      'kalshi_alignment_preserved_count': len(kalshi_alignment_result.preserved),
      'kalshi_alignment_degraded': kalshi_alignment_result.degraded,
      'blocked_reason': blocked_reason,
      'available_funds_snapshot': funds_posture['available_funds_snapshot'],
      'available_funds_as_of': funds_posture['available_funds_as_of'],
      'funds_refresh_status': funds_posture['funds_refresh_status'],
      'funds_refresh_reason': funds_posture['funds_refresh_reason'],
      'active_websocket_url_tail': _websocket_label(resolved_settings.active_websocket_url),
      'websocket_connected': websocket_posture['websocket_connected'],
      'websocket_status': websocket_posture['websocket_status'],
      'websocket_subscription_count': websocket_posture['websocket_subscription_count'],
      'last_websocket_event_at': websocket_posture['last_websocket_event_at'],
      'websocket_event_count': websocket_posture['websocket_event_count'],
      'analytical_outputs': analytical_outputs,
    },
  )

  return {
    'decision': 'planned',
    'command_family': 'polyventure run',
    'mode': mode,
    'dry_run': True,
    'dry_run_explanation': 'No order was submitted.',
    'balance_dollars': str(balance),
    'market_count': len(markets),
    'candidate_count': len(candidates),
    'scan_shape_summary': _scan_shape_summary(
      markets,
      candidate_markets=candidate_markets,
      orderbook_enrichment_count=orderbook_enrichment_count,
      candidate_count=len(candidates),
      websocket_orderbook_count=int(websocket_posture.get('websocket_orderbook_count') or 0),
      orderbook_review_market_count=int(websocket_posture.get('orderbook_review_market_count') or len(candidate_markets)),
      rest_fallback_count=int(websocket_posture.get('rest_fallback_count') or 0),
      orderbook_enrichment_failure_count=int(websocket_posture.get('orderbook_enrichment_failure_count') or 0),
      websocket_hit_count=int(websocket_posture.get('websocket_hit_count') or websocket_posture.get('websocket_orderbook_count') or 0),
    ),
    **sizing_summary,
    'orderbook_enrichment_count': orderbook_enrichment_count,
    'planned_pair_count': len(planned_pairs),
    'planned_pairs': planned_pairs,
    'submit_guard_summary': {
      'blocked_count': submit_guard_blocked_count,
      'submitted_count': submit_guard_submitted_count,
      'block_reasons': submit_guard_block_reasons,
    },
    'reconciled_pair_count': len(reconciled_pairs),
    'timed_out_pair_count': timed_out_pair_count,
    'reconciled_pairs': reconciled_pairs,
    'kalshi_alignment': {
      'terminalized_count': len(kalshi_alignment_result.terminalized),
      'preserved_count': len(kalshi_alignment_result.preserved),
      'degraded': kalshi_alignment_result.degraded,
      'readback_status': dict(kalshi_alignment_result.readback_status),
    },
    'blocked_reason': blocked_reason,
    'saved_set_snapshot': saved_set_snapshot if bridge_profile_active else None,
    'submit_response_id': submit_response_id,
    'submit_rest_state_id': submit_rest_state_id,
    'execution_intent_source': 'saved_set' if bridge_profile_active else 'candidate_scan',
    'funds_posture': funds_posture,
    'failure_class': top_level_failure_posture['failure_class'] if top_level_failure_posture else None,
    'failure_scope': top_level_failure_posture['failure_scope'] if top_level_failure_posture else None,
    'allowed_actions': top_level_action_contract['allowed_actions'] if top_level_action_contract else None,
    'blocked_actions': top_level_action_contract['blocked_actions'] if top_level_action_contract else None,
    'retry_allowed': top_level_action_contract['retry_allowed'] if top_level_action_contract else None,
    'execution_chronology': execution_chronology,
    'analytical_outputs': analytical_outputs,
    'account_limits': {
      'usage_tier': limits.usage_tier,
      'read': asdict(limits.read),
      'write': asdict(limits.write),
    },
    **_lane_runtime_posture(
      resolved_settings,
      lane_session_id=lane_session_id,
      connection_state=(
        'connected' if websocket_posture['websocket_connected']
        else 'skipped' if websocket_posture.get('websocket_status') == 'skipped_no_entry_window_markets'
        else 'waiting'
      ),
      websocket_connected=bool(websocket_posture['websocket_connected']),
    ),
    'settings': safe_settings_summary(resolved_settings),
    'private_key_path_tail': str(Path(private_key_path).name),
    'state_db_path_tail': _db_tail(resolved_settings.state_db_path),
    'next_action': (
      'Review the submit bridge chronology and public state projection before any later sandbox-enable consideration.'
      if bridge_profile_active and planned_pairs
      else 'Select candidates and save a final eligible set before retrying submit order.'
      if bridge_profile_active and blocked_reason in {'no_saved_set', 'saved_set_empty'}
      else 'Find candidates again, save a current eligible set, then retry submit order.'
      if bridge_profile_active and blocked_reason == 'saved_set_not_eligible'
      else 'Reconcile the existing active pair before retrying submit order.'
      if bridge_profile_active and blocked_reason in {'already_active_pair', 'unmatched_exposure_timeout'}
      else 'Review the runtime events and candidate set before the next dry-run cycle.'
    ),
  }


_LIVE_OPEN_PAIR_STATES = frozenset({
  'SUBMITTING', 'RESTING_BOTH', 'RESTING_ONE_SIDE', 'PARTIAL_ONE_SIDE', 'PARTIAL_BOTH',
  'ASYMMETRIC_EXPOSURE', 'REPAIR_LIVE', 'EXPOSURE_CAPPED', 'RECONCILE_REQUIRED',
  'SUBMIT_FAILED_RETRYABLE', 'SUBMIT_FAILED_TERMINAL',
})


def _reconcile_orphaned_in_flight(
  connection: sqlite3.Connection,
  *,
  current_operating_session_id: str,
  pairs: list[dict[str, Any]],
  recorded_at_utc: str,
  operation_lane: str,
) -> int:
  # STOP-3 SB: terminalize in_flight candidates orphaned by a PRIOR session (e.g. an
  # abrupt browser-close that could not run any teardown) -- but ONLY when no live order
  # can exist for the ticker. Fail-closed: an in_flight whose pair is in a live/open state
  # (an order may be resting) is PRESERVED, as is the current operating session's own in_flight. The
  # distinct terminal_cause keeps the ledger truthful (not operator auto_cancel, not
  # natural expired_unfilled).
  current_operating_session = str(current_operating_session_id or '').strip()
  if not current_operating_session:
    return 0
  live_ticker_state = {
    str(p.get('ticker') or ''): str(p.get('state') or '').strip().upper()
    for p in (pairs or [])
    if str(p.get('ticker') or '')
  }
  rows = connection.execute(
    '''
    SELECT cc.candidate_uid, cc.run_id, cc.ticker, r.lane_session_id
    FROM candidate_review_candidates cc
    JOIN candidate_review_runs r ON r.run_id = cc.run_id
    WHERE cc.lifecycle_stage = 'in_flight'
      AND r.operation_lane = ?
      AND r.lane_session_id IS NOT NULL
    ''',
    (operation_lane,),
  ).fetchall()
  reconciled = 0
  with connection:
    for row in rows:
      if str(row['lane_session_id'] or '').strip() == current_operating_session:
        continue
      ticker = str(row['ticker'] or '')
      if live_ticker_state.get(ticker, '') in _LIVE_OPEN_PAIR_STATES:
        continue  # fail-closed: may carry a live order -> preserve
      connection.execute(
        '''
        UPDATE candidate_review_candidates
        SET lifecycle_stage = 'terminal',
            terminal_cause = 'orphaned_teardown_reconciled',
            terminal_at_utc = ?
        WHERE candidate_uid = ? AND run_id = ? AND lifecycle_stage = 'in_flight'
        ''',
        (recorded_at_utc, row['candidate_uid'], row['run_id']),
      )
      reconciled += 1
  if reconciled > 0:
    persist_runtime_event(
      connection,
      level='INFO',
      event_type='orphaned_in_flight_reconciled',
      recorded_at_utc=recorded_at_utc,
      operation_lane=operation_lane,
      lane_session_id=current_operating_session,
      detail={'reconciled_count': reconciled},
    )
  return reconciled


def reconcile_pairs(
  settings: Settings | None = None,
  *,
  env_override: str | None = None,
  subaccount_override: int | None = None,
  client_factory: ClientFactory | None = None,
  suppress_live_funds_refresh: bool = False,
  current_operating_session_id: str | None = None,
) -> dict[str, Any]:
  resolved_settings = _resolve_settings(
    settings,
    env_override=env_override,
    subaccount_override=subaccount_override,
  )
  connection = open_database(resolved_settings.state_db_path)
  recorded_at = datetime.now(UTC).isoformat()
  lane_session_id = _lane_session_id(resolved_settings.operation_lane)
  pairs = _latest_pair_snapshots(connection, operation_lane=resolved_settings.operation_lane)
  latest_heartbeat_payload = _latest_heartbeat_payload(connection, operation_lane=resolved_settings.operation_lane)
  latest_funds_heartbeat_payload = _latest_funds_heartbeat_payload(connection, operation_lane=resolved_settings.operation_lane)
  # STOP-3 SB: each reconcile cycle, reconcile prior-session orphaned in_flight.
  # The live shell passes the stable operating-session id; latest heartbeat can be a
  # per-cycle id and must not define this preservation boundary.
  _current_operating_session = str(current_operating_session_id or '').strip()
  if _current_operating_session:
    try:
      _reconcile_orphaned_in_flight(
        connection,
        current_operating_session_id=_current_operating_session,
        pairs=pairs,
        recorded_at_utc=recorded_at,
        operation_lane=resolved_settings.operation_lane,
      )
    except Exception:
      pass
  alignment_pairs = _alignment_candidate_pairs(pairs)
  if str(resolved_settings.operation_lane or '').strip().lower() == 'live' and alignment_pairs:
    private_key_path = resolve_private_key_path(resolved_settings)
    private_key = load_private_key(private_key_path)
    alignment_client = (
      client_factory(resolved_settings, private_key)
      if client_factory is not None
      else KalshiHttpClient(resolved_settings, private_key, request_timeout=3, max_attempts=1)
    )
    kalshi_alignment_result = align_pairs_with_kalshi(
      connection,
      settings=resolved_settings,
      client=alignment_client,
      pairs=alignment_pairs,
      recorded_at_utc=recorded_at,
      operation_lane=resolved_settings.operation_lane,
      lane_session_id=lane_session_id,
      reason='reconcile_pairs_cycle',
    )
    pairs = kalshi_alignment_result.aligned_pairs
  else:
    kalshi_alignment_result = AlignmentResult(
      aligned_pairs=pairs,
      truth_by_ticker={},
      terminalized=[],
      preserved=[],
      readback_status={},
      degraded=False,
    )
  repair_close_reconciled_count = _reconcile_repair_close_exposures(
    connection,
    settings=resolved_settings,
    client_factory=client_factory,
    # Pass the full snapshot: ERROR is a terminal label but a frozen one-sided
    # exposure still needs settlement reconciliation. This function does its own
    # filtering to ERROR/REPAIR_LIVE-with-fills, so the alignment terminal filter
    # (which correctly excludes terminals from live alignment) must not apply here.
    pairs=pairs,
    recorded_at_utc=recorded_at,
    lane_session_id=lane_session_id,
  )
  if repair_close_reconciled_count:
    pairs = _latest_pair_snapshots(connection, operation_lane=resolved_settings.operation_lane)
  funds_posture = _refresh_reporting_funds_posture(
    resolved_settings,
    latest_heartbeat_payload=latest_heartbeat_payload,
    latest_funds_heartbeat_payload=latest_funds_heartbeat_payload,
    client_factory=client_factory,
    suppress_live_refresh=suppress_live_funds_refresh,
  )
  pair_runtime_summary = [
    _pair_runtime_summary(
      pair,
      fee_reserve_dollars=Decimal(str(resolved_settings.fee_reserve_dollars)),
    )
    for pair in pairs
  ]
  persist_operator_action(
    connection,
    action='reconcile',
    recorded_at_utc=recorded_at,
    operation_lane=resolved_settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'pair_count': len(pairs),
      'active_websocket_url_tail': _websocket_label(resolved_settings.active_websocket_url),
    },
  )
  # Under suppression (active-scan deck rebuild) the funds posture was served from
  # the loop's own heartbeat, not a fresh live read. The loop's reconcile remains the
  # authoritative funds-heartbeat writer; a deck-side write here would re-stamp a
  # non-fresh value and add a DB write that competes with the scan loop for the write
  # lock — the very contention being removed. Skip the funds heartbeat in that case.
  if not suppress_live_funds_refresh:
    persist_service_heartbeat(
      connection,
      component='reconcile',
      status='complete',
      recorded_at_utc=recorded_at,
      operation_lane=resolved_settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={
      'pair_count': len(pairs),
      'repair_close_reconciled_count': repair_close_reconciled_count,
      'kalshi_alignment_terminalized_count': len(kalshi_alignment_result.terminalized),
      'kalshi_alignment_preserved_count': len(kalshi_alignment_result.preserved),
      'kalshi_alignment_degraded': kalshi_alignment_result.degraded,
      'available_funds_snapshot': funds_posture['available_funds_snapshot'],
        'available_funds_as_of': funds_posture['available_funds_as_of'],
        'funds_refresh_status': funds_posture['funds_refresh_status'],
        'funds_refresh_reason': funds_posture['funds_refresh_reason'],
        'active_websocket_url_tail': _websocket_label(resolved_settings.active_websocket_url),
      },
    )
  return {
    'decision': 'planned',
    'command_family': 'polyventure reconcile',
    'pair_count': len(pairs),
    'repair_close_reconciled_count': repair_close_reconciled_count,
    'kalshi_alignment': {
      'terminalized_count': len(kalshi_alignment_result.terminalized),
      'preserved_count': len(kalshi_alignment_result.preserved),
      'degraded': kalshi_alignment_result.degraded,
      'readback_status': dict(kalshi_alignment_result.readback_status),
    },
    'pairs': pairs,
    'pair_runtime_summary': pair_runtime_summary,
    'funds_posture': funds_posture,
    **_lane_runtime_posture(resolved_settings, lane_session_id=lane_session_id),
    'state_db_path_tail': _db_tail(resolved_settings.state_db_path),
    'next_action': 'Review any partial or error states before the next runtime cycle.',
  }


def cancel_all_pairs(
  settings: Settings | None = None,
  *,
  env_override: str | None = None,
  subaccount_override: int | None = None,
) -> dict[str, Any]:
  resolved_settings = _resolve_settings(
    settings,
    env_override=env_override,
    subaccount_override=subaccount_override,
  )
  connection = open_database(resolved_settings.state_db_path)
  recorded_at = datetime.now(UTC).isoformat()
  lane_session_id = _lane_session_id(resolved_settings.operation_lane)
  pairs = _latest_pair_snapshots(connection, operation_lane=resolved_settings.operation_lane)
  terminal_states = {'CANCELED', 'LOCKED', 'ERROR', 'FILLED', 'SETTLED', 'SETTLED_EXPOSURE'}
  cancel_candidates: list[dict[str, Any]] = []
  cancelable: list[dict[str, Any]] = []
  preserved: list[dict[str, Any]] = []
  for pair in pairs:
    state = str(pair.get('state') or '').strip().upper()
    # Frozen one-sided exposure (e.g. ERROR after an uncoverable catch-up) carries
    # live unmatched contracts and must be preserved for operator handling even though
    # ERROR is otherwise a terminal label. Matched LOCKED/FILLED pairs (no unmatched
    # exposure) remain terminal.
    detail = pair.get('detail') if isinstance(pair.get('detail'), dict) else {}
    unmatched = abs(
      _decimal_from_detail(detail, 'yes_filled_contracts')
      - _decimal_from_detail(detail, 'no_filled_contracts')
    )
    if _pair_has_fill_bearing_exposure(pair, connection=connection) and unmatched > 0:
      preserved.append(pair)
      continue
    if state in terminal_states:
      continue
    cancel_candidates.append(pair)
  for pair in cancel_candidates:
    latest = connection.execute(
      '''
      SELECT state
      FROM pair_states
      WHERE pair_id = ? AND operation_lane = ?
      ORDER BY id DESC
      LIMIT 1
      ''',
      (pair['pair_id'], resolved_settings.operation_lane),
    ).fetchone()
    latest_state = str((latest['state'] if latest else pair['state']) or '').strip().upper()
    if latest_state in terminal_states:
      continue
    pair = {**pair, 'state': latest_state}
    persist_pair_state_transition(
      connection,
      pair_id=pair['pair_id'],
      state='CANCELED',
      recorded_at_utc=recorded_at,
      operation_lane=resolved_settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={'reason': 'operator_cancel_all', 'previous_state': pair['state']},
    )
    cancelable.append(pair)
  if preserved:
    persist_runtime_event(
      connection,
      level='WARN',
      event_type='cancel_all_preserved_fill_bearing_exposure',
      recorded_at_utc=recorded_at,
      operation_lane=resolved_settings.operation_lane,
      lane_session_id=lane_session_id,
      detail={
        'preserved_pair_count': len(preserved),
        'preserved_pairs': [
          {'pair_id': pair['pair_id'], 'ticker': pair['ticker'], 'state': pair['state']}
          for pair in preserved
        ],
      },
    )
  persist_operator_action(
    connection,
    action='cancel-all',
    recorded_at_utc=recorded_at,
    operation_lane=resolved_settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'canceled_pair_count': len(cancelable),
      'preserved_fill_bearing_pair_count': len(preserved),
      'active_websocket_url_tail': _websocket_label(resolved_settings.active_websocket_url),
    },
  )
  persist_runtime_event(
    connection,
    level='INFO',
    event_type='cancel_all_applied',
    recorded_at_utc=recorded_at,
    operation_lane=resolved_settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={'canceled_pair_count': len(cancelable), 'preserved_fill_bearing_pair_count': len(preserved)},
  )
  return {
    'decision': 'planned',
    'command_family': 'polyventure cancel-all',
    'canceled_pair_count': len(cancelable),
    'canceled_pairs': [
      {'pair_id': pair['pair_id'], 'ticker': pair['ticker'], 'previous_state': pair['state']}
      for pair in cancelable
    ],
    'preserved_fill_bearing_pair_count': len(preserved),
    'preserved_fill_bearing_pairs': [
      {'pair_id': pair['pair_id'], 'ticker': pair['ticker'], 'state': pair['state']}
      for pair in preserved
    ],
    **_lane_runtime_posture(resolved_settings, lane_session_id=lane_session_id),
    'state_db_path_tail': _db_tail(resolved_settings.state_db_path),
    'next_action': 'Review the updated pair states before the next runtime cycle.',
  }


def report_runtime(
  settings: Settings | None = None,
  *,
  env_override: str | None = None,
  subaccount_override: int | None = None,
  client_factory: ClientFactory | None = None,
  suppress_live_funds_refresh: bool = False,
) -> dict[str, Any]:
  resolved_settings = _resolve_settings(
    settings,
    env_override=env_override,
    subaccount_override=subaccount_override,
  )
  connection = open_database(resolved_settings.state_db_path)
  recorded_at = datetime.now(UTC).isoformat()
  lane_session_id = _lane_session_id(resolved_settings.operation_lane)
  persist_operator_action(
    connection,
    action='report',
    recorded_at_utc=recorded_at,
    operation_lane=resolved_settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={
      'state_db_path_tail': _db_tail(resolved_settings.state_db_path),
      'active_websocket_url_tail': _websocket_label(resolved_settings.active_websocket_url),
    },
  )
  persist_runtime_event(
    connection,
    level='INFO',
    event_type='report_viewed',
    recorded_at_utc=recorded_at,
    operation_lane=resolved_settings.operation_lane,
    lane_session_id=lane_session_id,
    detail={'state_db_path_tail': _db_tail(resolved_settings.state_db_path)},
  )
  summary = summarize_persistence(connection, operation_lane=resolved_settings.operation_lane)
  latest_heartbeat_payload = _latest_heartbeat_payload(connection, operation_lane=resolved_settings.operation_lane)
  latest_funds_heartbeat_payload = _latest_funds_heartbeat_payload(connection, operation_lane=resolved_settings.operation_lane)
  pair_runtime_summary = [
    _pair_runtime_summary(
      pair,
      fee_reserve_dollars=Decimal(str(resolved_settings.fee_reserve_dollars)),
    )
    for pair in _latest_pair_snapshots(connection, operation_lane=resolved_settings.operation_lane)
  ]
  analytical_outputs, analytical_captured_at = _load_latest_analytical_outputs(
    connection,
    operation_lane=resolved_settings.operation_lane,
  )
  latest_sizing_posture = _load_latest_sizing_posture(
    connection,
    operation_lane=resolved_settings.operation_lane,
  )
  funds_posture = _refresh_reporting_funds_posture(
    resolved_settings,
    latest_heartbeat_payload=latest_heartbeat_payload,
    latest_funds_heartbeat_payload=latest_funds_heartbeat_payload,
    client_factory=client_factory,
    suppress_live_refresh=suppress_live_funds_refresh,
  )
  return {
    'decision': 'planned',
    'command_family': 'polyventure report',
    'state_db_path_tail': _db_tail(resolved_settings.state_db_path),
    'table_counts': summary['table_counts'],
    'pair_state_history': summary['pair_state_history'],
    'pair_lane_session_history': summary['pair_lane_session_history'],
    'pair_runtime_summary': pair_runtime_summary,
    'saved_set_snapshot': _project_saved_set_snapshot(
      fetch_latest_candidate_saved_set(connection, operation_lane=resolved_settings.operation_lane)
    ),
    'funds_posture': funds_posture,
    'latest_sizing_posture': latest_sizing_posture,
    'parameter_surface': build_parameter_surface_payload(
      resolved_settings,
      default_settings=resolved_settings,
      report_payload={
        'pair_runtime_summary': pair_runtime_summary,
        'latest_heartbeat': latest_heartbeat_payload,
        'latest_sizing_posture': latest_sizing_posture,
      },
      analytical_outputs=analytical_outputs,
      analytical_captured_at=analytical_captured_at,
    ),
    'latest_heartbeat': latest_heartbeat_payload,
    **_lane_runtime_posture(resolved_settings, lane_session_id=lane_session_id),
    'next_action': 'Use Refresh Shell to update the current runtime view.',
  }


def fetch_system_log_entries(
  settings: Settings | None = None,
  *,
  env_override: str | None = None,
  subaccount_override: int | None = None,
  limit: int = 40,
) -> dict[str, Any]:
  resolved_settings = _resolve_settings(
    settings,
    env_override=env_override,
    subaccount_override=subaccount_override,
  )
  connection = open_database(resolved_settings.state_db_path)
  normalized = [
    _normalize_system_log_entry(row)
    for row in reversed(
      connection.execute(
        '''
        SELECT *
        FROM (
          SELECT id, recorded_at_utc, operation_lane, lane_session_id, 'service_heartbeat' AS source, component AS field_a, status AS field_b, NULL AS pair_id, detail_json
          FROM service_heartbeats
          WHERE operation_lane = ?
          UNION ALL
          SELECT id, recorded_at_utc, operation_lane, lane_session_id, 'operator_action' AS source, action AS field_a, NULL AS field_b, pair_id, detail_json
          FROM operator_actions
          WHERE operation_lane = ?
          UNION ALL
          SELECT id, recorded_at_utc, operation_lane, lane_session_id, 'runtime_event' AS source, event_type AS field_a, level AS field_b, pair_id, detail_json
          FROM runtime_events
          WHERE operation_lane = ?
        ) combined
        ORDER BY recorded_at_utc DESC, id DESC
        LIMIT ?
        ''',
        (
          resolved_settings.operation_lane,
          resolved_settings.operation_lane,
          resolved_settings.operation_lane,
          max(limit, 1),
        ),
      ).fetchall()
    )
  ]
  return {
    'decision': 'planned',
    'command_family': 'polyventure system-log',
    **_lane_runtime_posture(resolved_settings),
    'state_db_path_tail': _db_tail(resolved_settings.state_db_path),
    'entries': normalized,
    'latest_cursor': normalized[-1]['key'] if normalized else None,
  }
