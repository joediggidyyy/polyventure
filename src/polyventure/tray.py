from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any
from urllib import request as urllib_request


_ICON_PATH = Path(__file__).resolve().parents[2] / 'assets' / 'images' / 'polyventur_tray_icon.png'


def _load_icon_image() -> Any:
    from PIL import Image
    if _ICON_PATH.is_file():
        return Image.open(_ICON_PATH).convert('RGBA')
    return Image.new('RGBA', (16, 16), color=(0, 120, 200, 255))


def _fetch_execution_status(host: str, port: int) -> dict[str, Any] | None:
    url = f'http://{host}:{port}/api/execution-status'
    try:
        req = urllib_request.Request(url, headers={'Cache-Control': 'no-cache', 'Pragma': 'no-cache'})
        with urllib_request.urlopen(req, timeout=3.0) as response:
            return json.loads(response.read().decode('utf-8', errors='ignore'))
    except Exception:
        return None


def _sign_mutation_request(
    *,
    path: str,
    body_bytes: bytes,
    signing_key_b64: str,
    key_id: str,
) -> dict[str, str]:
    timestamp_sec = str(int(time.time()))
    nonce = base64.b64encode(os.urandom(16)).decode('ascii')
    body_hash_hex = hashlib.sha256(body_bytes).hexdigest()
    canonical = f'POST\n{path}\n{timestamp_sec}\n{nonce}\n{body_hash_hex}'
    signing_key = base64.b64decode(signing_key_b64.encode('ascii'))
    signature = base64.b64encode(
        hmac.new(signing_key, canonical.encode('utf-8'), hashlib.sha256).digest()
    ).decode('ascii')
    return {
        'X-PV-Mutation-Key-Id': key_id,
        'X-PV-Mutation-Timestamp': timestamp_sec,
        'X-PV-Mutation-Nonce': nonce,
        'X-PV-Mutation-Body-Hash': body_hash_hex,
        'X-PV-Mutation-Signature': signature,
    }


class ExecutionTrayIcon:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        session_token: str,
        signing_key_b64: str,
        key_id: str,
        poll_interval_sec: float = 10.0,
    ) -> None:
        self._host = host
        self._port = port
        self._session_token = session_token
        self._signing_key_b64 = signing_key_b64
        self._key_id = key_id
        self._poll_interval_sec = poll_interval_sec
        self._icon: Any = None
        self._stop_event = threading.Event()
        self._notified: bool = False
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        try:
            import pystray
            image = _load_icon_image()
            menu = pystray.Menu(
                pystray.MenuItem('Polyventure — execution in progress', None, enabled=False),
                pystray.MenuItem('Open Console', self._open_console),
                pystray.MenuItem('Abort Execution', self._abort_execution),
            )
            self._icon = pystray.Icon('polyventure', image, 'Polyventure — execution in progress', menu)
            self._icon.run_detached()
            threading.Thread(target=self._poll_loop, daemon=True).start()
        except Exception:
            pass

    def stop(self) -> None:
        self._stop_event.set()
        if self._icon is not None:
            try:
                self._icon.stop()
            except Exception:
                pass

    def notify_complete(self, detail: str | None = None) -> None:
        if self._notified:
            return
        self._notified = True
        try:
            from polyventure.popup import show_execution_result_popup
            show_execution_result_popup(
                outcome='complete',
                detail=detail or 'All pairs have reached terminal state.',
                reattach_url=f'http://{self._host}:{self._port}/',
            )
        except Exception:
            pass

    def notify_error(self, message: str = '') -> None:
        if self._icon is not None:
            try:
                self._icon.notify('Polyventure execution error', message or 'Execution encountered an error.')
            except Exception:
                pass
        try:
            from polyventure.popup import show_execution_result_popup
            show_execution_result_popup(
                outcome='error',
                detail=message or 'Execution encountered an error.',
                reattach_url=f'http://{self._host}:{self._port}/',
            )
        except Exception:
            pass

    def _poll_loop(self) -> None:
        prev_drain_active = True
        terminal_pending = False
        while not self._stop_event.wait(self._poll_interval_sec):
            status = _fetch_execution_status(self._host, self._port)
            if status is None:
                self.stop()
                return
            in_flight = int(status.get('in_flight_count') or 0)
            drain_active = bool(status.get('drain_active'))
            automation_active = bool(status.get('automation_active'))
            if self._icon is not None:
                title = (
                    f'Polyventure — {in_flight} pair(s) in-flight'
                    if in_flight > 0
                    else 'Polyventure — waiting for drain'
                )
                try:
                    self._icon.title = title
                except Exception:
                    pass
            # FB-5: while automation is armed, a drain-to-zero is just the gap
            # between scan cycles, not a terminal completion. Do not fire the
            # execution-complete popup; clear any pending terminal latch.
            if automation_active:
                terminal_pending = False
                prev_drain_active = drain_active
                continue
            if terminal_pending:
                if not drain_active and in_flight == 0:
                    self.notify_complete()
                    self.stop()
                    return
                terminal_pending = False
            elif prev_drain_active and not drain_active and in_flight == 0:
                terminal_pending = True
            prev_drain_active = drain_active

    def _open_console(self, icon: Any, item: Any) -> None:
        polyventure_script = Path(sys.executable).parent / 'polyventure'
        try:
            subprocess.Popen(
                [str(polyventure_script), 'console', '--no-open'],
                start_new_session=True,
            )
        except Exception:
            pass

    def _abort_execution(self, icon: Any, item: Any) -> None:
        path = '/api/run'
        body = json.dumps({'action': 'abort'}).encode('utf-8')
        signed_headers = _sign_mutation_request(
            path=path,
            body_bytes=body,
            signing_key_b64=self._signing_key_b64,
            key_id=self._key_id,
        )
        url = f'http://{self._host}:{self._port}{path}'
        req = urllib_request.Request(
            url,
            data=body,
            headers={'Content-Type': 'application/json', **signed_headers},
            method='POST',
        )
        try:
            with urllib_request.urlopen(req, timeout=3.0):
                pass
        except Exception:
            pass
