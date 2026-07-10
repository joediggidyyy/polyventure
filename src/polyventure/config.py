from __future__ import annotations

import os
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import urlparse

from .candidate_identity import SEED_POST_SUBMIT_PROCESSING_BUFFER_SEC


def derive_rest_api_base_url(ws_url: str) -> str:
  """Derive the REST API base URL from a WebSocket URL.

  Transform: wss://external-api-ws.<host>/trade-api/ws/v2
          -> https://external-api.<host>/trade-api/v2

  Returns '' if ws_url is empty or not a valid wss:// URL.
  Caller treats '' as a configuration error — no fallback.
  """
  url = str(ws_url or '').strip()
  if not url or not url.startswith('wss://'):
    return ''
  parsed = urlparse(url)
  hostname = parsed.hostname or ''
  # Remove '-ws' suffix from the first hostname segment before the first dot
  dot_idx = hostname.find('.')
  if dot_idx > 0:
    first_seg = hostname[:dot_idx]
    rest_host = hostname[dot_idx:]
    first_seg = first_seg[:-3] if first_seg.endswith('-ws') else first_seg
    hostname = first_seg + rest_host
  else:
    hostname = hostname[:-3] if hostname.endswith('-ws') else hostname
  # Remove '/ws' path component
  path = parsed.path.replace('/ws/', '/').replace('/ws', '')
  port_part = f':{parsed.port}' if parsed.port else ''
  return f'https://{hostname}{port_part}{path}'


REQUIRED_ENV_VARS = [
  'KALSHI_API_KEY_ID',
  'KALSHI_SCAN_INTERVAL_MS',
  'KALSHI_ENTRY_WINDOW_START_SEC',
  'KALSHI_ENTRY_WINDOW_END_SEC',
  'KALSHI_MIN_EDGE_DOLLARS',
  'KALSHI_FEE_RESERVE_DOLLARS',
  'KALSHI_MIN_PROFIT_DOLLARS',
  'KALSHI_MAX_PAIR_CONTRACTS',
  'KALSHI_MAX_OPEN_PAIRS',
  'KALSHI_MAX_UNHEDGED_SEC',
  'KALSHI_POST_SUBMIT_PROCESSING_BUFFER_SEC',
  'KALSHI_CANCEL_ON_PAUSE',
  'KALSHI_LOG_LEVEL',
  'KALSHI_STATE_DB_PATH',
]


@dataclass(frozen=True)
class Settings:
  kalshi_env: str
  api_key_id: str
  private_key_file: str | None
  private_key_inline: str | None
  private_key_path_legacy: str | None
  api_base_url: str  # deprecated — value is now derived from lane WS URL at settings-for-lane time
  websocket_url: str
  subaccount: int
  scan_interval_ms: int
  entry_window_start_sec: int
  entry_window_end_sec: int
  min_edge_dollars: float
  fee_reserve_dollars: float
  min_profit_dollars: float
  max_pair_contracts: float
  max_open_pairs: int
  max_unhedged_sec: int
  cancel_on_pause: bool
  log_level: str
  state_db_path: str
  sandbox_api_key_id: str = ''
  live_api_key_id: str = ''
  live_private_key_file: str = ''
  sandbox_private_key_file: str = ''
  min_pair_notional_pct: float = 0.05
  max_pair_notional_pct: float = 0.20
  target_deployment_pct: float = 0.60
  density_alpha: float = 0.20
  density_edge_ref: float = 0.05
  density_liquidity_ref: float = 100.0
  auto_find_candidates_cadence_ms: int = 600000
  operation_lane: str = 'sandbox'
  sandbox_websocket_url: str = ''
  live_websocket_url: str = ''
  active_websocket_url: str = ''
  sandbox_edge_relaxation_factor: float = 0.80
  sandbox_scan_return_limit: int = 50
  entry_window_fetch_padding_sec: int = 15
  post_submit_processing_buffer_sec: int = SEED_POST_SUBMIT_PROCESSING_BUFFER_SEC
  # Lane A liquidity instrumentation: trailing window (seconds) over which per-side
  # traded flow is summed from the authoritative trades endpoint. A capture-resolution
  # parameter only -- never a money-gate value (the gate seeds live in Lane B1).
  flow_window_sec: int = 300
  # One-leg exposure guard thresholds. None is intentional: these are money-path
  # thresholds with no silent default; the pre-submit guard denies until set.
  flow_participation_k: float | None = None
  max_divergence: float | None = None
  # Serial submit-prep work bound: only the top-K ranked saved-set members enter
  # per-candidate pre-submit readbacks (fresher final checks for each). An
  # efficiency bound, never a risk gate; 0 or negative disables the cap.
  submit_prep_top_k: int = 3

  def __post_init__(self) -> None:
    lane = str(self.operation_lane or 'sandbox').strip().lower() or 'sandbox'
    if lane not in {'sandbox', 'live', 'offline'}:
      raise ValueError('KALSHI_OPERATION_LANE must be sandbox, live, or offline.')

    legacy_websocket_url = str(self.websocket_url or '').strip()
    sandbox_websocket_url = str(self.sandbox_websocket_url or '').strip()
    live_websocket_url = str(self.live_websocket_url or '').strip()
    active_websocket_url = str(self.active_websocket_url or '').strip()

    if not sandbox_websocket_url and lane == 'sandbox' and legacy_websocket_url:
      sandbox_websocket_url = legacy_websocket_url
    if not live_websocket_url and lane == 'live' and legacy_websocket_url:
      live_websocket_url = legacy_websocket_url

    expected_active_websocket_url = (
      sandbox_websocket_url if lane == 'sandbox' else live_websocket_url
    )
    if not active_websocket_url:
      active_websocket_url = expected_active_websocket_url or legacy_websocket_url
    if not legacy_websocket_url:
      legacy_websocket_url = active_websocket_url

    object.__setattr__(self, 'operation_lane', lane)
    object.__setattr__(self, 'websocket_url', legacy_websocket_url)
    object.__setattr__(self, 'sandbox_websocket_url', sandbox_websocket_url)
    object.__setattr__(self, 'live_websocket_url', live_websocket_url)
    object.__setattr__(self, 'active_websocket_url', active_websocket_url)


def _candidate_dotenv_files() -> list[Path]:
  roots = []
  current = Path.cwd().resolve()
  roots.append(current)
  roots.extend(current.parents)
  module_root = Path(__file__).resolve().parents[3]
  roots.append(module_root)
  roots.extend(module_root.parents)

  seen: set[Path] = set()
  files: list[Path] = []
  for root in roots:
    if root in seen:
      continue
    seen.add(root)
    dot_env = root / '.env'
    if dot_env.exists():
      files.append(dot_env)
  return files


def _load_dotenv_values(files: Iterable[Path]) -> dict[str, str]:
  values: dict[str, str] = {}
  for file_path in files:
    for raw_line in file_path.read_text(encoding='utf-8').splitlines():
      line = raw_line.strip()
      if not line or line.startswith('#') or '=' not in line:
        continue
      key, value = line.split('=', 1)
      key = key.strip()
      if not key or key in os.environ or key in values:
        continue
      values[key] = value.strip()
  return values


def _read_dotenv_file(file_path: Path) -> dict[str, str]:
  values: dict[str, str] = {}
  for raw_line in file_path.read_text(encoding='utf-8').splitlines():
    line = raw_line.strip()
    if not line or line.startswith('#') or '=' not in line:
      continue
    key, value = line.split('=', 1)
    key = key.strip()
    if not key:
      continue
    values.setdefault(key, value.strip())
  return values


def _get_env(name: str, dotenv_values: dict[str, str]) -> str | None:
  value = os.environ.get(name)
  if value is not None and value != '':
    return value
  value = dotenv_values.get(name)
  if value is None or value == '':
    return None
  return value


def _require_values(dotenv_values: dict[str, str]) -> None:
  missing = [name for name in REQUIRED_ENV_VARS if _get_env(name, dotenv_values) is None]
  operation_lane = (_get_env('KALSHI_OPERATION_LANE', dotenv_values) or 'sandbox').strip().lower() or 'sandbox'
  legacy_websocket_url = _get_env('KALSHI_WEBSOCKET_URL', dotenv_values)
  sandbox_websocket_url = _get_env('KALSHI_SANDBOX_WEBSOCKET_URL', dotenv_values)
  live_websocket_url = _get_env('KALSHI_LIVE_WEBSOCKET_URL', dotenv_values)

  if operation_lane == 'sandbox':
    selected_websocket_url = sandbox_websocket_url or legacy_websocket_url
  elif operation_lane == 'live':
    selected_websocket_url = live_websocket_url or legacy_websocket_url
  else:
    # Offline is the boot/resting default — no lane is selected yet, so the lane
    # is chosen explicitly at runtime (web shell selection / CLI --env). Require
    # only that at least one lane endpoint exists so a lane can be selected; a
    # file with no endpoints at all still fails closed.
    selected_websocket_url = sandbox_websocket_url or live_websocket_url or legacy_websocket_url

  if selected_websocket_url is None:
    if operation_lane == 'live':
      missing.append('KALSHI_LIVE_WEBSOCKET_URL (or KALSHI_WEBSOCKET_URL compatibility alias)')
    elif operation_lane == 'sandbox':
      missing.append('KALSHI_SANDBOX_WEBSOCKET_URL (or KALSHI_WEBSOCKET_URL compatibility alias)')
    else:
      missing.append('KALSHI_SANDBOX_WEBSOCKET_URL or KALSHI_LIVE_WEBSOCKET_URL (at least one lane endpoint required)')

  if missing:
    missing_text = ', '.join(missing)
    raise ValueError(
      f'Missing required environment variables: {missing_text}'
    )


def _parse_bool(value: str) -> bool:
  return value.strip().lower() in {'1', 'true', 'yes', 'on'}


def _parse_optional_float(value: str | None) -> float | None:
  if value is None:
    return None
  text = str(value).strip()
  if not text:
    return None
  return float(text)


def _resolved_websocket_urls(
  value_lookup: Callable[[str], str | None],
  *,
  operation_lane: str,
) -> tuple[str, str, str, str]:
  lane = str(operation_lane or 'sandbox').strip().lower() or 'sandbox'
  legacy_websocket_url = value_lookup('KALSHI_WEBSOCKET_URL') or ''
  sandbox_websocket_url = value_lookup('KALSHI_SANDBOX_WEBSOCKET_URL') or ''
  live_websocket_url = value_lookup('KALSHI_LIVE_WEBSOCKET_URL') or ''

  if not sandbox_websocket_url and lane == 'sandbox' and legacy_websocket_url:
    sandbox_websocket_url = legacy_websocket_url
  if not live_websocket_url and lane == 'live' and legacy_websocket_url:
    live_websocket_url = legacy_websocket_url

  active_websocket_url = sandbox_websocket_url if lane == 'sandbox' else live_websocket_url
  if not active_websocket_url:
    active_websocket_url = legacy_websocket_url

  compatibility_websocket_url = legacy_websocket_url or active_websocket_url
  return compatibility_websocket_url, sandbox_websocket_url, live_websocket_url or '', active_websocket_url


def websocket_url_is_valid(value: str) -> bool:
  parsed = urlparse(str(value or '').strip())
  return parsed.scheme in {'ws', 'wss'} and bool(parsed.netloc)


def _truthy(value: str | None) -> bool:
  return str(value or '').strip().lower() in {'1', 'true', 'yes', 'on'}


def load_settings() -> Settings:
  dotenv_files = _candidate_dotenv_files()
  dotenv_values = _load_dotenv_values(dotenv_files)

  nearest_dotenv_values: dict[str, str] = {}
  if dotenv_files:
    nearest_dotenv_values = _read_dotenv_file(dotenv_files[0])

  inherited_legacy_private_key = str(os.environ.get('KALSHI_PRIVATE_KEY_PATH') or '').strip()
  inherited_legacy_looks_inline = inherited_legacy_private_key.startswith('-----BEGIN')
  explicit_file_key_in_env = bool(str(os.environ.get('KALSHI_PRIVATE_KEY_FILE') or '').strip())
  explicit_inline_key_in_env = bool(str(os.environ.get('KALSHI_PRIVATE_KEY_INLINE') or '').strip())
  nearest_has_file_tuple = bool(
    str(nearest_dotenv_values.get('KALSHI_PRIVATE_KEY_FILE') or '').strip()
    and str(nearest_dotenv_values.get('KALSHI_API_KEY_ID') or '').strip()
  )
  force_local_kalshi_tuple = (
    inherited_legacy_looks_inline
    and nearest_has_file_tuple
    and not explicit_file_key_in_env
    and not explicit_inline_key_in_env
  )

  if _truthy(_get_env('KALSHI_FORCE_LOCAL_DOTENV', dotenv_values)):
    force_local_kalshi_tuple = True

  local_override_names = {
    'KALSHI_API_KEY_ID',
    'KALSHI_SANDBOX_API_KEY_ID',
    'KALSHI_LIVE_API_KEY_ID',
    'KALSHI_PRIVATE_KEY_FILE',
    'KALSHI_PRIVATE_KEY_INLINE',
    'KALSHI_PRIVATE_KEY_PATH',
    'KALSHI_API_BASE_URL',
    'KALSHI_WEBSOCKET_URL',
    'KALSHI_SANDBOX_WEBSOCKET_URL',
    'KALSHI_LIVE_WEBSOCKET_URL',
    'KALSHI_STATE_DB_PATH',
    'KALSHI_LIVE_PRIVATE_KEY_FILE',
    'KALSHI_SANDBOX_PRIVATE_KEY_FILE',
  }

  def env(name: str) -> str | None:
    if force_local_kalshi_tuple and name in local_override_names:
      local_value = str(nearest_dotenv_values.get(name) or '').strip()
      if local_value:
        return local_value
    return _get_env(name, dotenv_values)

  def websocket_env(name: str) -> str | None:
    if force_local_kalshi_tuple and name in {
      'KALSHI_WEBSOCKET_URL',
      'KALSHI_SANDBOX_WEBSOCKET_URL',
      'KALSHI_LIVE_WEBSOCKET_URL',
    }:
      local_value = str(nearest_dotenv_values.get(name) or '').strip()
      if local_value:
        return local_value
      if name == 'KALSHI_WEBSOCKET_URL':
        return None
      return None
    return env(name)

  _require_values(dotenv_values)
  operation_lane = (env('KALSHI_OPERATION_LANE') or 'sandbox').strip().lower() or 'sandbox'
  websocket_url, sandbox_websocket_url, live_websocket_url, active_websocket_url = _resolved_websocket_urls(
    websocket_env,
    operation_lane=operation_lane,
  )

  return Settings(
    kalshi_env=env('KALSHI_ENV') or 'demo',
    api_key_id=env('KALSHI_API_KEY_ID') or '',
    private_key_file=env('KALSHI_PRIVATE_KEY_FILE'),
    private_key_inline=env('KALSHI_PRIVATE_KEY_INLINE'),
    private_key_path_legacy=env('KALSHI_PRIVATE_KEY_PATH'),
    api_base_url=env('KALSHI_API_BASE_URL') or '',
    websocket_url=websocket_url,
    subaccount=int(env('KALSHI_SUBACCOUNT') or '0'),
    scan_interval_ms=int(env('KALSHI_SCAN_INTERVAL_MS') or '2000'),
    entry_window_start_sec=int(
      env('KALSHI_ENTRY_WINDOW_START_SEC') or '900'
    ),
    entry_window_end_sec=int(
      env('KALSHI_ENTRY_WINDOW_END_SEC') or '60'
    ),
    flow_window_sec=int(
      env('KALSHI_FLOW_WINDOW_SEC') or '300'
    ),
    flow_participation_k=_parse_optional_float(
      env('KALSHI_FLOW_PARTICIPATION_K')
    ),
    max_divergence=_parse_optional_float(
      env('KALSHI_MAX_DIVERGENCE')
    ),
    submit_prep_top_k=int(
      env('KALSHI_SUBMIT_PREP_TOP_K') or '3'
    ),
    min_edge_dollars=float(
      env('KALSHI_MIN_EDGE_DOLLARS') or '0.03'
    ),
    fee_reserve_dollars=float(
      env('KALSHI_FEE_RESERVE_DOLLARS') or '0.02'
    ),
    min_profit_dollars=float(
      env('KALSHI_MIN_PROFIT_DOLLARS') or '0.01'
    ),
    max_pair_contracts=float(
      env('KALSHI_MAX_PAIR_CONTRACTS') or '10'
    ),
    max_open_pairs=int(env('KALSHI_MAX_OPEN_PAIRS') or '20'),
    max_unhedged_sec=int(
      env('KALSHI_MAX_UNHEDGED_SEC') or '5'
    ),
    post_submit_processing_buffer_sec=int(
      env('KALSHI_POST_SUBMIT_PROCESSING_BUFFER_SEC') or str(SEED_POST_SUBMIT_PROCESSING_BUFFER_SEC)
    ),
    cancel_on_pause=_parse_bool(
      env('KALSHI_CANCEL_ON_PAUSE') or 'true'
    ),
    log_level=env('KALSHI_LOG_LEVEL') or 'INFO',
    state_db_path=env('KALSHI_STATE_DB_PATH') or '',
    sandbox_api_key_id=env('KALSHI_SANDBOX_API_KEY_ID') or '',
    live_api_key_id=env('KALSHI_LIVE_API_KEY_ID') or '',
    live_private_key_file=env('KALSHI_LIVE_PRIVATE_KEY_FILE') or '',
    sandbox_private_key_file=env('KALSHI_SANDBOX_PRIVATE_KEY_FILE') or '',
    min_pair_notional_pct=float(
      env('KALSHI_MIN_PAIR_NOTIONAL_PCT') or '0.05'
    ),
    max_pair_notional_pct=float(
      env('KALSHI_MAX_PAIR_NOTIONAL_PCT') or '0.20'
    ),
    target_deployment_pct=float(
      env('KALSHI_TARGET_DEPLOYMENT_PCT') or '0.60'
    ),
    density_alpha=float(
      env('KALSHI_DENSITY_ALPHA') or '0.20'
    ),
    density_edge_ref=float(
      env('KALSHI_DENSITY_EDGE_REF') or '0.05'
    ),
    density_liquidity_ref=float(
      env('KALSHI_DENSITY_LIQUIDITY_REF') or '100.0'
    ),
    auto_find_candidates_cadence_ms=int(
      env('KALSHI_AUTO_FIND_CANDIDATES_CADENCE_MS') or '600000'
    ),
    sandbox_edge_relaxation_factor=float(
      env('KALSHI_SANDBOX_EDGE_RELAXATION_FACTOR') or '0.80'
    ),
    sandbox_scan_return_limit=int(
      env('KALSHI_SANDBOX_SCAN_RETURN_LIMIT') or '50'
    ),
    entry_window_fetch_padding_sec=int(
      env('KALSHI_ENTRY_WINDOW_FETCH_PADDING_SEC') or '15'
    ),
    operation_lane=operation_lane,
    sandbox_websocket_url=sandbox_websocket_url,
    live_websocket_url=live_websocket_url,
    active_websocket_url=active_websocket_url,
  )


def settings_for_lane(base: Settings, lane: str) -> Settings:
  """Resolve a concrete, lane-specific Settings from a loaded base.

  Used by non-interactive entry points (CLI batch commands) that select the
  lane explicitly. The Kalshi environment, REST base URL, and active websocket
  endpoint are derived from the lane — never carried over from a stale base.

  Fail-closed credential rule (see polyventure/CLAUDE.md): for the live lane the
  API key id and private key path come strictly from the live-scoped fields with
  no fallback to the generic/sandbox fields. The sandbox lane may use the
  generic fields, which are themselves sandbox-scoped.
  """
  normalized = str(lane or '').strip().lower()
  if normalized not in {'sandbox', 'live'}:
    raise ValueError("settings_for_lane requires an explicit 'sandbox' or 'live' lane.")

  if normalized == 'live':
    active_websocket_url = str(base.live_websocket_url or '').strip()
    api_key_id = str(base.live_api_key_id or '').strip()
    private_key_file = str(base.live_private_key_file or '').strip() or None
  else:
    active_websocket_url = str(base.sandbox_websocket_url or base.websocket_url or '').strip()
    api_key_id = str(base.sandbox_api_key_id or base.api_key_id or '').strip()
    private_key_file = str(base.sandbox_private_key_file or base.private_key_file or '').strip() or None

  api_base_url = derive_rest_api_base_url(active_websocket_url)
  kalshi_env = 'prod' if normalized == 'live' else 'demo'

  return replace(
    base,
    operation_lane=normalized,
    kalshi_env=kalshi_env,
    api_base_url=api_base_url,
    websocket_url=active_websocket_url,
    active_websocket_url=active_websocket_url,
    api_key_id=api_key_id,
    private_key_file=private_key_file,
  )


def resolve_private_key_path(settings: Settings) -> Path:
  if settings.private_key_file:
    path = Path(settings.private_key_file).expanduser()
    if not path.is_absolute():
      path = (Path.cwd() / path).resolve()
    if not path.exists():
      raise FileNotFoundError(str(path))
    return path

  if settings.private_key_path_legacy:
    legacy = Path(settings.private_key_path_legacy).expanduser()
    if legacy.exists():
      return legacy.resolve()
    raise ValueError(
      'KALSHI_PRIVATE_KEY_PATH is set but does not point to an existing file. '
      'Use KALSHI_PRIVATE_KEY_FILE for the secure steady-state path.'
    )

  if settings.private_key_inline:
    raise ValueError(
      'Inline private-key material is intentionally blocked in this first '
      'implementation slice. Move the key into KALSHI_PRIVATE_KEY_FILE.'
    )

  raise ValueError(
    'Missing private key file path. Set KALSHI_PRIVATE_KEY_FILE to a local, '
    'Git-ignored PEM file.'
  )


def safe_settings_summary(settings: Settings) -> dict[str, object]:
  return {
    'kalshi_env': settings.kalshi_env,
    'operation_lane': settings.operation_lane,
    'api_key_id_present': bool(settings.api_key_id),
    'private_key_file_present': bool(settings.private_key_file),
    'legacy_private_key_path_present': bool(settings.private_key_path_legacy),
    'inline_private_key_present': bool(settings.private_key_inline),
    'api_base_url': settings.api_base_url,
    'websocket_url': settings.websocket_url,
    'sandbox_websocket_url': settings.sandbox_websocket_url,
    'live_websocket_url': settings.live_websocket_url,
    'active_websocket_url': settings.active_websocket_url,
    'subaccount': settings.subaccount,
    'scan_interval_ms': settings.scan_interval_ms,
    'entry_window_start_sec': settings.entry_window_start_sec,
    'entry_window_end_sec': settings.entry_window_end_sec,
    'entry_window_fetch_padding_sec': settings.entry_window_fetch_padding_sec,
    'flow_window_sec': settings.flow_window_sec,
    'flow_participation_k': settings.flow_participation_k,
    'max_divergence': settings.max_divergence,
    'submit_prep_top_k': settings.submit_prep_top_k,
    'min_edge_dollars': settings.min_edge_dollars,
    'fee_reserve_dollars': settings.fee_reserve_dollars,
    'min_profit_dollars': settings.min_profit_dollars,
    'max_pair_contracts': settings.max_pair_contracts,
    'max_open_pairs': settings.max_open_pairs,
    'max_unhedged_sec': settings.max_unhedged_sec,
    'post_submit_processing_buffer_sec': settings.post_submit_processing_buffer_sec,
    'cancel_on_pause': settings.cancel_on_pause,
    'log_level': settings.log_level,
    'state_db_path': settings.state_db_path,
    'min_pair_notional_pct': settings.min_pair_notional_pct,
    'max_pair_notional_pct': settings.max_pair_notional_pct,
    'target_deployment_pct': settings.target_deployment_pct,
    'density_alpha': settings.density_alpha,
    'density_edge_ref': settings.density_edge_ref,
    'density_liquidity_ref': settings.density_liquidity_ref,
    'auto_find_candidates_cadence_ms': settings.auto_find_candidates_cadence_ms,
    'sandbox_edge_relaxation_factor': settings.sandbox_edge_relaxation_factor,
    'sandbox_scan_return_limit': settings.sandbox_scan_return_limit,
    'settings_ready': True,
    'credential_ready': bool(settings.api_key_id and (settings.private_key_file or settings.private_key_inline)),
    'environment_ready': True,
  }
