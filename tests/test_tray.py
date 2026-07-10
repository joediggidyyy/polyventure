from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch


def _make_tray(**kwargs):
    from polyventure.tray import ExecutionTrayIcon
    defaults = dict(
        host='127.0.0.1',
        port=8765,
        session_token='tok',
        signing_key_b64='AAAA',
        key_id='kid',
        poll_interval_sec=0.01,
    )
    defaults.update(kwargs)
    return ExecutionTrayIcon(**defaults)


def _run_poll_loop(tray, statuses):
    """Drive _poll_loop with a fixed sequence of status responses, then stop."""
    call_count = [0]
    done = threading.Event()

    def _fake_fetch(host, port):
        i = call_count[0]
        call_count[0] += 1
        if i >= len(statuses):
            tray._stop_event.set()
            done.set()
            return None
        return statuses[i]

    with patch('polyventure.tray._fetch_execution_status', side_effect=_fake_fetch):
        t = threading.Thread(target=tray._poll_loop, daemon=True)
        t.start()
        t.join(timeout=2.0)
    return call_count[0]


# ---------------------------------------------------------------------------
# G-F: notify_complete idempotency
# ---------------------------------------------------------------------------

def test_notify_complete_is_idempotent():
    tray = _make_tray()
    with patch('polyventure.popup.show_execution_result_popup') as mock_popup:
        tray.notify_complete()
        tray.notify_complete()
    mock_popup.assert_called_once()


def test_notify_complete_no_os_toast():
    tray = _make_tray()
    mock_icon = MagicMock()
    tray._icon = mock_icon
    with patch('polyventure.popup.show_execution_result_popup'):
        tray.notify_complete()
    mock_icon.notify.assert_not_called()


# ---------------------------------------------------------------------------
# G-F: deferred terminal confirmation
# ---------------------------------------------------------------------------

def test_poll_loop_fires_popup_after_two_consecutive_terminal_polls():
    tray = _make_tray()
    statuses = [
        {'in_flight_count': 3, 'drain_active': True},
        {'in_flight_count': 0, 'drain_active': False},
        {'in_flight_count': 0, 'drain_active': False},
    ]
    with patch('polyventure.popup.show_execution_result_popup') as mock_popup:
        _run_poll_loop(tray, statuses)
    mock_popup.assert_called_once()


def test_poll_loop_suppresses_popup_when_automation_restarts_between_polls():
    tray = _make_tray()
    statuses = [
        {'in_flight_count': 3, 'drain_active': True},
        {'in_flight_count': 0, 'drain_active': False},
        {'in_flight_count': 2, 'drain_active': True},
        {'in_flight_count': 0, 'drain_active': False},
        {'in_flight_count': 0, 'drain_active': False},
    ]
    with patch('polyventure.popup.show_execution_result_popup') as mock_popup:
        _run_poll_loop(tray, statuses)
    mock_popup.assert_called_once()


# ---------------------------------------------------------------------------
# FB-5: automation-active suppression
# ---------------------------------------------------------------------------

def test_poll_loop_does_not_fire_popup_while_automation_active():
    # A drain-to-zero between automation scan cycles must not be mistaken for a
    # terminal completion: with automation_active set, no popup fires even across
    # consecutive zero-in-flight / drain-inactive polls.
    tray = _make_tray()
    statuses = [
        {'in_flight_count': 2, 'drain_active': True, 'automation_active': True},
        {'in_flight_count': 0, 'drain_active': False, 'automation_active': True},
        {'in_flight_count': 0, 'drain_active': False, 'automation_active': True},
        {'in_flight_count': 0, 'drain_active': False, 'automation_active': True},
    ]
    with patch('polyventure.popup.show_execution_result_popup') as mock_popup:
        _run_poll_loop(tray, statuses)
    mock_popup.assert_not_called()


def test_poll_loop_fires_popup_after_automation_stops_and_drains():
    # Once automation is no longer active, the normal deferred-terminal path
    # resumes: two consecutive terminal polls then fire the completion popup.
    tray = _make_tray()
    statuses = [
        {'in_flight_count': 2, 'drain_active': True, 'automation_active': True},
        {'in_flight_count': 1, 'drain_active': True, 'automation_active': False},
        {'in_flight_count': 0, 'drain_active': False, 'automation_active': False},
        {'in_flight_count': 0, 'drain_active': False, 'automation_active': False},
    ]
    with patch('polyventure.popup.show_execution_result_popup') as mock_popup:
        _run_poll_loop(tray, statuses)
    mock_popup.assert_called_once()
