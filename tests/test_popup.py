from __future__ import annotations

import argparse
import sys
import threading
from typing import Any
from unittest.mock import MagicMock, patch, call


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _args(**kwargs: Any) -> argparse.Namespace:
    defaults = {'json': False}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ---------------------------------------------------------------------------
# popup_mode_active gate
# ---------------------------------------------------------------------------

def test_popup_mode_active_when_no_tty_and_no_json() -> None:
    from polyventure.popup import popup_mode_active
    with patch.dict('os.environ', {'POLYVENTURE_POPUP': '1'}):
        assert popup_mode_active(_args(json=False)) is True


def test_popup_mode_inactive_when_tty_attached() -> None:
    from polyventure.popup import popup_mode_active
    with patch.dict('os.environ', {'POLYVENTURE_POPUP': '0'}):
        assert popup_mode_active(_args(json=False)) is False


def test_popup_mode_inactive_when_json_flag() -> None:
    from polyventure.popup import popup_mode_active
    with patch.dict('os.environ', {'POLYVENTURE_POPUP': '1'}):
        assert popup_mode_active(_args(json=True)) is False


def test_popup_mode_active_by_default_when_env_absent() -> None:
    # Splash/popup is default-on for interactive launches; no flag required.
    from polyventure.popup import popup_mode_active
    with patch.dict('os.environ', {}, clear=True):
        assert popup_mode_active(_args(json=False)) is True


def test_popup_mode_inactive_by_default_when_json_and_env_absent() -> None:
    from polyventure.popup import popup_mode_active
    with patch.dict('os.environ', {}, clear=True):
        assert popup_mode_active(_args(json=True)) is False


# ---------------------------------------------------------------------------
# show_launch_splash — thread management
# ---------------------------------------------------------------------------

def test_show_launch_splash_starts_thread() -> None:
    """Thread starts and close callback is returned."""
    from polyventure.popup import show_launch_splash
    mock_tk = MagicMock()
    mock_tk.return_value.update.side_effect = Exception('no display')
    with patch('tkinter.Tk', mock_tk):
        close = show_launch_splash()
    assert callable(close)
    close()  # joins the daemon thread cleanly


def test_show_launch_splash_close_callback_fires() -> None:
    """Calling close() sets the event and joins the thread without hanging."""
    from polyventure.popup import show_launch_splash
    mock_tk = MagicMock()
    mock_tk.return_value.update.side_effect = Exception('no display')
    with patch('tkinter.Tk', mock_tk):
        close = show_launch_splash()
    # Should return without timeout (thread already dead due to mocked exception)
    close()


def test_show_launch_splash_close_fires_on_exception() -> None:
    """close() is called even when the launch path raises — simulates try/finally in cli.py."""
    from polyventure.popup import show_launch_splash
    mock_tk = MagicMock()
    mock_tk.return_value.update.side_effect = Exception('no display')
    close_called = threading.Event()

    with patch('tkinter.Tk', mock_tk):
        close_fn = show_launch_splash()

    original_close = close_fn

    def _tracked_close() -> None:
        close_called.set()
        original_close()

    try:
        raise RuntimeError('simulated launch failure')
    except RuntimeError:
        pass
    finally:
        _tracked_close()

    assert close_called.is_set()


# ---------------------------------------------------------------------------
# show_blocked_launch_popup
# ---------------------------------------------------------------------------

def test_blocked_launch_popup_instance_already_running() -> None:
    """Reason label contains 'instance_already_running'."""
    from polyventure.popup import show_blocked_launch_popup

    label_texts: list[str] = []
    mock_tk_instance = MagicMock()
    mock_tk_instance.mainloop.return_value = None
    mock_tk_instance.winfo_reqwidth.return_value = 440
    mock_tk_instance.winfo_reqheight.return_value = 240
    mock_tk_instance.winfo_screenwidth.return_value = 1920
    mock_tk_instance.winfo_screenheight.return_value = 1080

    def _capture_label(parent: Any, text: str = '', **kwargs: Any) -> MagicMock:
        label_texts.append(text)
        m = MagicMock()
        m.pack.return_value = None
        m.bind.return_value = None
        return m

    with patch('tkinter.Tk', return_value=mock_tk_instance), \
         patch('tkinter.Frame', return_value=mock_tk_instance), \
         patch('tkinter.Label', side_effect=_capture_label), \
         patch('tkinter.Button', return_value=MagicMock()), \
         patch('polyventure.popup._load_logo', return_value=None):
        show_blocked_launch_popup(
            reason='instance_already_running',
            reattach_url='http://127.0.0.1:8765/',
        )

    assert any('INSTANCE_ALREADY_RUNNING' in t for t in label_texts)


def test_blocked_launch_popup_execution_in_progress_shows_count() -> None:
    """In-flight count is shown in the reason badge label."""
    from polyventure.popup import show_blocked_launch_popup

    label_texts: list[str] = []
    mock_tk_instance = MagicMock()
    mock_tk_instance.mainloop.return_value = None
    mock_tk_instance.winfo_reqwidth.return_value = 440
    mock_tk_instance.winfo_reqheight.return_value = 240
    mock_tk_instance.winfo_screenwidth.return_value = 1920
    mock_tk_instance.winfo_screenheight.return_value = 1080

    def _capture_label(parent: Any, text: str = '', **kwargs: Any) -> MagicMock:
        label_texts.append(text)
        m = MagicMock()
        m.pack.return_value = None
        return m

    with patch('tkinter.Tk', return_value=mock_tk_instance), \
         patch('tkinter.Frame', return_value=mock_tk_instance), \
         patch('tkinter.Label', side_effect=_capture_label), \
         patch('tkinter.Button', return_value=MagicMock()), \
         patch('polyventure.popup._load_logo', return_value=None):
        show_blocked_launch_popup(
            reason='execution_in_progress',
            reattach_url='http://127.0.0.1:8765/',
            in_flight_count=3,
        )

    # count appears inline in the reason badge: "EXECUTION_IN_PROGRESS  ·  3 PAIR(S) IN-FLIGHT"
    assert any('3' in t and 'IN-FLIGHT' in t for t in label_texts)


def test_blocked_launch_popup_url_displayed() -> None:
    """Reattach URL appears as a clickable Label."""
    from polyventure.popup import show_blocked_launch_popup

    label_texts: list[str] = []
    mock_tk_instance = MagicMock()
    mock_tk_instance.mainloop.return_value = None
    mock_tk_instance.winfo_reqwidth.return_value = 440
    mock_tk_instance.winfo_reqheight.return_value = 240
    mock_tk_instance.winfo_screenwidth.return_value = 1920
    mock_tk_instance.winfo_screenheight.return_value = 1080

    def _capture_label(parent: Any, text: str = '', **kwargs: Any) -> MagicMock:
        label_texts.append(text)
        m = MagicMock()
        m.pack.return_value = None
        m.bind.return_value = None
        return m

    with patch('tkinter.Tk', return_value=mock_tk_instance), \
         patch('tkinter.Frame', return_value=mock_tk_instance), \
         patch('tkinter.Label', side_effect=_capture_label), \
         patch('tkinter.Button', return_value=MagicMock()), \
         patch('polyventure.popup._load_logo', return_value=None):
        show_blocked_launch_popup(
            reason='instance_already_running',
            reattach_url='http://127.0.0.1:8765/',
        )

    assert any('http://127.0.0.1:8765/' in t for t in label_texts)


# ---------------------------------------------------------------------------
# show_execution_result_popup
# ---------------------------------------------------------------------------

def test_execution_result_popup_complete_outcome() -> None:
    """Execution Complete label is present in the popup."""
    from polyventure.popup import show_execution_result_popup

    label_texts: list[str] = []
    mock_tk_instance = MagicMock()
    mock_tk_instance.mainloop.return_value = None
    mock_tk_instance.winfo_reqwidth.return_value = 440
    mock_tk_instance.winfo_reqheight.return_value = 200
    mock_tk_instance.winfo_screenwidth.return_value = 1920
    mock_tk_instance.winfo_screenheight.return_value = 1080

    def _capture_label(parent: Any, text: str = '', **kwargs: Any) -> MagicMock:
        label_texts.append(text)
        m = MagicMock()
        m.pack.return_value = None
        return m

    with patch('tkinter.Tk', return_value=mock_tk_instance), \
         patch('tkinter.Frame', return_value=mock_tk_instance), \
         patch('tkinter.Label', side_effect=_capture_label), \
         patch('tkinter.Button', return_value=MagicMock()), \
         patch('polyventure.popup._load_logo', return_value=None):
        show_execution_result_popup(outcome='complete', detail='3 processed / 3 cancelled')

    assert any('EXECUTION COMPLETE' in t for t in label_texts)


def test_execution_result_popup_error_outcome() -> None:
    """Execution Error label is present in the popup."""
    from polyventure.popup import show_execution_result_popup

    label_texts: list[str] = []
    mock_tk_instance = MagicMock()
    mock_tk_instance.mainloop.return_value = None
    mock_tk_instance.winfo_reqwidth.return_value = 440
    mock_tk_instance.winfo_reqheight.return_value = 200
    mock_tk_instance.winfo_screenwidth.return_value = 1920
    mock_tk_instance.winfo_screenheight.return_value = 1080

    def _capture_label(parent: Any, text: str = '', **kwargs: Any) -> MagicMock:
        label_texts.append(text)
        m = MagicMock()
        m.pack.return_value = None
        return m

    with patch('tkinter.Tk', return_value=mock_tk_instance), \
         patch('tkinter.Frame', return_value=mock_tk_instance), \
         patch('tkinter.Label', side_effect=_capture_label), \
         patch('tkinter.Button', return_value=MagicMock()), \
         patch('polyventure.popup._load_logo', return_value=None):
        show_execution_result_popup(outcome='error', detail='Connection lost')

    assert any('EXECUTION ERROR' in t for t in label_texts)
