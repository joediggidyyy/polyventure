"""Pure selection/timing optimizer cores (C2 entry window, C3 thresholds).

Pure functions only: given already-labeled evidence rows they return a
recommendation. No DB, no I/O, no apply -- the caller queries evidence and
applies the result. This keeps the optimization math unit-testable in isolation
and off the money/submit path.

Design invariants (parameter-optimization BMAP):
- Fail-closed: below the evidence meter minimum, recommend no change (hold seed).
- In-support-only: recommendations stay within the range of observed outcomes;
  the optimizer never widens a gate beyond where evidence exists (extrapolation
  is disabled by default -- there is simply no data outside the traded window).
- Throttle: move the live value toward the data-optimal value by at most the
  throttle fraction per cycle, re-anchoring on the operator seed.
"""

from __future__ import annotations

from collections import defaultdict

from .flow_evidence import is_authoritative_label, is_exposure_label


def _best_contiguous_band(ordered_buckets, nets):
  """Kadane max-sum contiguous run over the net-value sequence.

  Returns (lo_bucket, hi_bucket, best_sum) for the run maximizing summed net
  (clean minus exposure). Returns (None, None, 0) if the best sum is <= 0.
  """
  best_sum = None
  best_lo = best_hi = None
  cur_sum = 0
  cur_lo = 0
  for idx, net in enumerate(nets):
    if cur_sum <= 0:
      cur_sum = net
      cur_lo = idx
    else:
      cur_sum += net
    if best_sum is None or cur_sum > best_sum:
      best_sum = cur_sum
      best_lo = cur_lo
      best_hi = idx
  if best_sum is None or best_sum <= 0:
    return None, None, 0
  return ordered_buckets[best_lo], ordered_buckets[best_hi], best_sum


def _throttle_toward(current, target, throttle):
  """Move `current` toward `target` by at most `throttle` fraction; round to int."""
  return int(round(current + throttle * (target - current)))


def optimize_entry_window(
  rows,
  *,
  current_start_sec: int,
  current_end_sec: int,
  bucket_sec: int = 15,
  min_rows: int = 30,
  throttle: float = 0.20,
  floor_end_sec: int | None = None,
):
  """Recommend entry-window bounds from timing+outcome evidence.

  floor_end_sec: ratified operational floor for the window end (e.g. the
  candidate-expiry view buffer, 75s): candidates inside it are expired from view
  regardless of the entry-window setting, so a suggested end below it would sit
  in a dead zone and contradict the ratified floor. The suggested end is clamped
  to this floor when provided.

  rows: iterable of mappings with 'seconds_to_close' (int) and 'outcome_label'.
  The window is [end, start] in seconds-to-close (end < start); a candidate is
  eligible when end <= seconds_to_close <= start. Traded evidence only exists
  inside the current window, so the recommendation can narrow / re-center but
  never widen (in-support-only, automatic).

  Returns a dict: status in {insufficient_evidence, no_change, optimized},
  suggested_start_sec, suggested_end_sec, and an evidence summary.
  """
  auth = [
    r for r in rows
    if is_authoritative_label(str(r.get('outcome_label') or ''))
    and r.get('seconds_to_close') is not None
  ]
  evidence = {
    'authoritative_rows': len(auth),
    'min_rows': min_rows,
    'exposure_rows': sum(1 for r in auth if is_exposure_label(str(r.get('outcome_label') or ''))),
  }
  if len(auth) < min_rows:
    return {
      'status': 'insufficient_evidence',
      'suggested_start_sec': current_start_sec,
      'suggested_end_sec': current_end_sec,
      'evidence': evidence,
    }

  secs = [int(r['seconds_to_close']) for r in auth]
  observed_min, observed_max = min(secs), max(secs)
  evidence['observed_min_sec'] = observed_min
  evidence['observed_max_sec'] = observed_max

  buckets = defaultdict(lambda: [0, 0])  # bucket_index -> [clean, exposure]
  for r in auth:
    b = int(r['seconds_to_close']) // bucket_sec
    if is_exposure_label(str(r['outcome_label'])):
      buckets[b][1] += 1
    else:
      buckets[b][0] += 1
  ordered = sorted(buckets)
  nets = [buckets[b][0] - buckets[b][1] for b in ordered]

  lo_bucket, hi_bucket, best_sum = _best_contiguous_band(ordered, nets)
  if lo_bucket is None:
    # No band is net-positive: hold the seed rather than pick a losing window.
    return {
      'status': 'no_change',
      'suggested_start_sec': current_start_sec,
      'suggested_end_sec': current_end_sec,
      'evidence': {**evidence, 'best_band_net': best_sum},
    }

  # Map the best band to seconds, clamped to observed support.
  band_lo = max(lo_bucket * bucket_sec, observed_min)
  band_hi = min((hi_bucket + 1) * bucket_sec - 1, observed_max)
  # Window: end = lower seconds bound, start = upper seconds bound.
  target_end = band_lo
  target_start = band_hi

  new_start = _throttle_toward(current_start_sec, target_start, throttle)
  new_end = _throttle_toward(current_end_sec, target_end, throttle)
  # Ratified floor cage: never suggest an end inside the expiry-buffer dead zone.
  if floor_end_sec is not None and new_end < int(floor_end_sec):
    new_end = int(floor_end_sec)
    evidence['floor_clamped'] = True
  # Guard the invariant end < start.
  if new_end >= new_start:
    new_end = min(new_end, new_start - 1)

  evidence['best_band_sec'] = [band_lo, band_hi]
  evidence['best_band_net'] = best_sum
  status = 'optimized' if (new_start != current_start_sec or new_end != current_end_sec) else 'no_change'
  return {
    'status': status,
    'suggested_start_sec': new_start,
    'suggested_end_sec': new_end,
    'evidence': evidence,
  }
