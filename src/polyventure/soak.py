from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any, Callable

from .config import Settings
from .service import ClientFactory, cancel_all_pairs, reconcile_pairs, report_runtime, run_service_once


@dataclass(frozen=True)
class SoakConfig:
  cycles: int | None = None
  max_duration_seconds: float | None = None
  interval_seconds: float = 0.0
  cleanup_on_finish: bool = True
  fail_fast: bool = False


def _validate_config(config: SoakConfig) -> None:
  if config.cycles is None and config.max_duration_seconds is None:
    raise ValueError('SoakConfig requires cycles or max_duration_seconds.')
  if config.cycles is not None and config.cycles <= 0:
    raise ValueError('SoakConfig cycles must be greater than zero when provided.')
  if config.max_duration_seconds is not None and config.max_duration_seconds <= 0:
    raise ValueError('SoakConfig max_duration_seconds must be greater than zero when provided.')
  if config.interval_seconds < 0:
    raise ValueError('SoakConfig interval_seconds cannot be negative.')


def run_demo_soak(
  *,
  settings: Settings,
  config: SoakConfig,
  client_factory: ClientFactory | None = None,
  sleep_fn: Callable[[float], None] = time.sleep,
  monotonic_fn: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
  _validate_config(config)
  started_monotonic = monotonic_fn()
  cycle_payloads: list[dict[str, Any]] = []
  cycle_errors: list[dict[str, str]] = []

  while True:
    if config.cycles is not None and len(cycle_payloads) >= config.cycles:
      break
    if config.max_duration_seconds is not None and monotonic_fn() - started_monotonic >= config.max_duration_seconds:
      break

    try:
      cycle_payloads.append(
        run_service_once(
          settings=settings,
          client_factory=client_factory,
        )
      )
    except Exception as exc:
      cycle_errors.append(
        {
          'error_type': type(exc).__name__,
          'message': str(exc),
        }
      )
      if config.fail_fast:
        break

    should_continue = True
    if config.cycles is not None and len(cycle_payloads) >= config.cycles:
      should_continue = False
    if config.max_duration_seconds is not None and monotonic_fn() - started_monotonic >= config.max_duration_seconds:
      should_continue = False
    if should_continue and config.interval_seconds > 0:
      sleep_fn(config.interval_seconds)

  report = report_runtime(settings=settings)
  cancel_payload = None
  reconcile_payload = None
  if config.cleanup_on_finish:
    cancel_payload = cancel_all_pairs(settings=settings)
    reconcile_payload = reconcile_pairs(settings=settings)

  elapsed_seconds = monotonic_fn() - started_monotonic
  terminal_states = (
    [pair['state'] for pair in reconcile_payload['pairs']]
    if reconcile_payload is not None
    else []
  )
  return {
    'decision': 'pass' if not cycle_errors else 'no-go',
    'surface': 'demo-soak-dry-run',
    'cycles_requested': config.cycles,
    'max_duration_seconds': config.max_duration_seconds,
    'interval_seconds': config.interval_seconds,
    'cycles_completed': len(cycle_payloads),
    'cycle_error_count': len(cycle_errors),
    'cycle_errors': cycle_errors,
    'elapsed_seconds': elapsed_seconds,
    'cleanup_on_finish': config.cleanup_on_finish,
    'canceled_pair_count': (
      cancel_payload['canceled_pair_count']
      if cancel_payload is not None
      else None
    ),
    'terminal_states_after_cleanup': terminal_states,
    'state_db_path_tail': report['state_db_path_tail'],
    'table_counts': report['table_counts'],
    'latest_heartbeat': report['latest_heartbeat'],
    'next_action': 'Use this harness for the future long-duration demo soak gate before sandbox-enable consideration.',
  }