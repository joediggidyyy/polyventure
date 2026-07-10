"""Popup surfaces for no-TTY launch paths (stdlib tkinter only)."""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time
import webbrowser
from pathlib import Path
from typing import Callable

_LOGO_PATH = Path(__file__).resolve().parents[2] / 'assets' / 'images' / 'polyventure_logo.png'
_SPLASH_IMG_PATH = Path(__file__).resolve().parents[2] / 'assets' / 'images' / 'polyventure_splash.png'

# Polymath operator shell palette
_BG = '#0a0f1a'
_SURFACE = '#0f1a27'
_BORDER = '#1a2d3d'
_TEXT = '#dce8f0'
_MUTED = '#4a6b7a'
_ACCENT = '#26c6da'
_ACCENT_HOVER = '#4dd0e1'
_LINK = '#26c6da'
_BTN_BG = '#0f1a27'
_BTN_TEXT = '#90aabb'
_ERROR = '#ef5350'
_SUCCESS = '#4caf50'
_FONT = 'Consolas'


_SPLASH_GLOW_N = 60


def _build_glow_colors(n: int) -> list[str]:
    """Cyan comet gradient: bright head fading to near-dark tail over N steps."""
    out = []
    for i in range(n):
        t = i / n
        intensity = (1.0 - t) ** 1.6  # sharp bright head, long gentle tail
        r = int(10 + 28 * intensity)
        g = int(15 + 183 * intensity)
        b = int(26 + 192 * intensity)
        out.append(f'#{r:02x}{g:02x}{b:02x}')
    return out


def _build_perimeter_segs(w: int, h: int, n: int) -> list[tuple[float, float, float, float]]:
    """N line segments around a w×h rect, counterclockwise from top-left."""
    perim = 2 * (w + h)
    step = perim / n

    def _pt(s: float) -> tuple[float, float]:
        s = s % perim
        if s < h:       # left side: top → bottom
            return (0.0, s)
        s -= h
        if s < w:       # bottom: left → right
            return (s, float(h))
        s -= w
        if s < h:       # right side: bottom → top
            return (float(w), h - s)
        s -= h          # top: right → left
        return (w - s, 0.0)

    segs = []
    for i in range(n):
        x0, y0 = _pt(i * step)
        x1, y1 = _pt((i + 1) * step)
        segs.append((x0, y0, x1, y1))
    return segs


_SPLASH_GLOW_COLORS = _build_glow_colors(_SPLASH_GLOW_N)
_SPLASH_GLOW_SEGS: list[tuple[float, float, float, float]] | None = None


def _load_logo(size: int = 40) -> object | None:
    """Load the tray icon as a resized tkinter PhotoImage. Returns None on failure."""
    try:
        from PIL import Image, ImageTk
        if not _LOGO_PATH.is_file():
            return None
        img = Image.open(_LOGO_PATH).convert('RGBA').resize((size, size), Image.LANCZOS)
        return ImageTk.PhotoImage(img)
    except Exception:
        return None


def _load_splash_crest(width: int = 160) -> object | None:
    """Load the crest splash PNG scaled to width, preserving aspect ratio."""
    try:
        from PIL import Image, ImageTk
        if not _SPLASH_IMG_PATH.is_file():
            return None
        img = Image.open(_SPLASH_IMG_PATH).convert('RGBA')
        w, h = img.size
        height = round(h * width / w)
        img = img.resize((width, height), Image.LANCZOS)
        return ImageTk.PhotoImage(img)
    except Exception:
        return None


def popup_mode_active(args: argparse.Namespace) -> bool:
    """Return True when popup mode should be used.

    Popup mode is the default for interactive console launches; it is suppressed
    only when POLYVENTURE_POPUP=0 is set (e.g. headless or scripted use) or when
    the caller is in machine-readable (--json) mode.
    """
    return os.environ.get('POLYVENTURE_POPUP', '1') != '0' and (not getattr(args, 'json', False))


def show_launch_splash() -> Callable[[], None]:
    """Show a borderless launch splash in a daemon thread. Returns a close callback."""
    import tkinter as tk

    close_event = threading.Event()

    def _run() -> None:
        try:
            root = tk.Tk()
            root.overrideredirect(True)
            root.configure(bg=_BG)
            root.resizable(False, False)

            w, h = 300, 110
            sw = root.winfo_screenwidth()
            sh = root.winfo_screenheight()
            root.geometry(f'{w}x{h}+{(sw - w) // 2}+{(sh - h) // 2}')

            canvas = tk.Canvas(root, width=w, height=h, bg=_BG, highlightthickness=0)
            canvas.place(x=0, y=0)

            inner = tk.Frame(canvas, bg=_BG, padx=18, pady=14)
            canvas.create_window(2, 2, anchor='nw', window=inner, width=w - 4, height=h - 4)

            logo = _load_logo(32)
            row = tk.Frame(inner, bg=_BG)
            row.pack(anchor='w')
            if logo:
                lbl = tk.Label(row, image=logo, bg=_BG)
                lbl.image = logo
                lbl.pack(side='left', padx=(0, 10))
            title_col = tk.Frame(row, bg=_BG)
            title_col.pack(side='left')
            tk.Label(title_col, text='POLYVENTURE', font=(_FONT, 11, 'bold'),
                     fg=_TEXT, bg=_BG).pack(anchor='w')
            tk.Label(title_col, text='POLYMATH OPERATOR SHELL', font=(_FONT, 7),
                     fg=_MUTED, bg=_BG).pack(anchor='w')

            tk.Label(inner, text='INITIALIZING', font=(_FONT, 8),
                     fg=_ACCENT, bg=_BG).pack(anchor='w', pady=(10, 0))

            global _SPLASH_GLOW_SEGS
            if _SPLASH_GLOW_SEGS is None:
                _SPLASH_GLOW_SEGS = _build_perimeter_segs(w - 1, h - 1, _SPLASH_GLOW_N)
            segs = _SPLASH_GLOW_SEGS
            n_glow = _SPLASH_GLOW_N
            colors = _SPLASH_GLOW_COLORS
            glow_frame = [0]

            def _step_glow() -> None:
                canvas.delete('glow')
                f = glow_frame[0]
                for i, (x0, y0, x1, y1) in enumerate(segs):
                    canvas.create_line(x0, y0, x1, y1, fill=colors[(f - i) % n_glow],
                                       width=2, capstyle='round', tags='glow')
                glow_frame[0] = (f + 1) % n_glow

            while not close_event.is_set():
                try:
                    _step_glow()
                    root.update()
                except tk.TclError:
                    break
                time.sleep(0.04)
            try:
                root.destroy()
            except tk.TclError:
                pass
        except Exception:
            pass

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    def close() -> None:
        close_event.set()
        t.join(timeout=2.0)

    return close


def show_blocked_launch_popup(
    *,
    reason: str,
    reattach_url: str,
    message: str = '',
    in_flight_count: int | None = None,
) -> None:
    """Show a modal popup when a console session is already running."""
    import tkinter as tk

    try:
        root = tk.Tk()
        root.overrideredirect(False)
        root.title('Polyventure')
        root.configure(bg=_BG)
        root.resizable(False, False)

        tk.Frame(root, bg=_ACCENT, height=2).pack(fill='x', side='top')

        outer = tk.Frame(root, bg=_BG, padx=20, pady=16)
        outer.pack(fill='both', expand=True)

        # header row
        logo = _load_logo(34)
        title_row = tk.Frame(outer, bg=_BG)
        title_row.pack(anchor='w', pady=(0, 12))
        if logo:
            lbl = tk.Label(title_row, image=logo, bg=_BG)
            lbl.image = logo
            lbl.pack(side='left', padx=(0, 10))
        title_col = tk.Frame(title_row, bg=_BG)
        title_col.pack(side='left')
        tk.Label(title_col, text='POLYVENTURE', font=(_FONT, 11, 'bold'),
                 fg=_TEXT, bg=_BG).pack(anchor='w')
        tk.Label(title_col, text='POLYMATH OPERATOR SHELL', font=(_FONT, 7),
                 fg=_MUTED, bg=_BG).pack(anchor='w')

        # status section label
        tk.Label(outer, text='CONSOLE STATUS', font=(_FONT, 7),
                 fg=_MUTED, bg=_BG).pack(anchor='w', pady=(0, 4))

        # reason badge
        reason_display = reason or 'INSTANCE_ALREADY_RUNNING'
        if in_flight_count and in_flight_count > 0:
            reason_display = f'{reason_display.upper()}  ·  {in_flight_count} PAIR(S) IN-FLIGHT'
        else:
            reason_display = reason_display.upper()
        tk.Label(outer, text=reason_display, font=(_FONT, 9, 'bold'),
                 fg=_ACCENT, bg=_BG).pack(anchor='w', pady=(0, 10))

        tk.Frame(outer, bg=_BORDER, height=1).pack(fill='x', pady=(0, 10))

        # reattach label + clickable URL
        tk.Label(outer, text='REATTACH', font=(_FONT, 7),
                 fg=_MUTED, bg=_BG).pack(anchor='w', pady=(0, 2))

        url = reattach_url or 'http://127.0.0.1:8765/'

        def _open_url(event: object = None) -> None:
            try:
                webbrowser.open(url)
            except Exception:
                pass

        link = tk.Label(outer, text=url, font=(_FONT, 9, 'underline'),
                        fg=_LINK, bg=_BG, cursor='hand2')
        link.pack(anchor='w', pady=(0, 14))
        link.bind('<Button-1>', _open_url)
        link.bind('<Enter>', lambda e: link.configure(fg=_ACCENT_HOVER))
        link.bind('<Leave>', lambda e: link.configure(fg=_LINK))

        btn_row = tk.Frame(outer, bg=_BG)
        btn_row.pack(anchor='e')
        tk.Button(btn_row, text='DISMISS', command=root.destroy,
                  font=(_FONT, 8), bg=_BTN_BG, fg=_BTN_TEXT,
                  activebackground=_BORDER, activeforeground=_TEXT,
                  relief='flat', padx=14, pady=4, cursor='hand2',
                  highlightthickness=1, highlightbackground=_BORDER).pack()

        root.lift()
        root.attributes('-topmost', True)
        root.update_idletasks()
        w = root.winfo_reqwidth()
        h = root.winfo_reqheight()
        x = (root.winfo_screenwidth() - w) // 2
        y = (root.winfo_screenheight() - h) // 2
        root.geometry(f'{w}x{h}+{x}+{y}')
        root.mainloop()
    except Exception:
        pass


def show_execution_result_popup(
    *,
    outcome: str,
    detail: str = '',
    reattach_url: str = '',
) -> None:
    """Show a timed execution result popup near the system tray. Auto-dismisses after 10s."""
    import tkinter as tk

    try:
        root = tk.Tk()
        root.overrideredirect(False)
        is_error = outcome == 'error'
        root.title('Polyventure')
        root.configure(bg=_BG)
        root.resizable(False, False)

        accent_color = _ERROR if is_error else _SUCCESS
        tk.Frame(root, bg=accent_color, height=2).pack(fill='x', side='top')

        outer = tk.Frame(root, bg=_BG, padx=20, pady=16)
        outer.pack(fill='both', expand=True)

        # header row
        logo = _load_logo(34)
        title_row = tk.Frame(outer, bg=_BG)
        title_row.pack(anchor='w', pady=(0, 12))
        if logo:
            lbl = tk.Label(title_row, image=logo, bg=_BG)
            lbl.image = logo
            lbl.pack(side='left', padx=(0, 10))
        title_col = tk.Frame(title_row, bg=_BG)
        title_col.pack(side='left')
        tk.Label(title_col, text='POLYVENTURE', font=(_FONT, 11, 'bold'),
                 fg=_TEXT, bg=_BG).pack(anchor='w')
        tk.Label(title_col, text='POLYMATH OPERATOR SHELL', font=(_FONT, 7),
                 fg=_MUTED, bg=_BG).pack(anchor='w')

        # outcome section
        tk.Label(outer, text='EXECUTION OUTCOME', font=(_FONT, 7),
                 fg=_MUTED, bg=_BG).pack(anchor='w', pady=(0, 4))
        outcome_text = 'EXECUTION ERROR' if is_error else 'EXECUTION COMPLETE'
        tk.Label(outer, text=outcome_text, font=(_FONT, 9, 'bold'),
                 fg=accent_color, bg=_BG).pack(anchor='w', pady=(0, 10))

        if detail:
            tk.Frame(outer, bg=_BORDER, height=1).pack(fill='x', pady=(0, 8))
            tk.Label(outer, text=detail, font=(_FONT, 8),
                     fg=_TEXT, bg=_BG, wraplength=340, justify='left').pack(anchor='w', pady=(0, 12))

        btn_row = tk.Frame(outer, bg=_BG)
        btn_row.pack(anchor='e')

        if reattach_url:
            def _view() -> None:
                try:
                    webbrowser.open(reattach_url)
                except Exception:
                    pass
                root.destroy()
            tk.Button(btn_row, text='VIEW CONSOLE', command=_view,
                      font=(_FONT, 8), bg=_BTN_BG, fg=_ACCENT,
                      activebackground=_BORDER, activeforeground=_ACCENT_HOVER,
                      relief='flat', padx=14, pady=4, cursor='hand2',
                      highlightthickness=1, highlightbackground=_BORDER).pack(side='left', padx=(0, 8))

        tk.Button(btn_row, text='OK', command=root.destroy,
                  font=(_FONT, 8), bg=_BTN_BG, fg=_BTN_TEXT,
                  activebackground=_BORDER, activeforeground=_TEXT,
                  relief='flat', padx=14, pady=4, cursor='hand2',
                  highlightthickness=1, highlightbackground=_BORDER).pack(side='left')

        root.lift()
        root.attributes('-topmost', True)
        root.update_idletasks()
        w = root.winfo_reqwidth()
        h = root.winfo_reqheight()
        margin = 24
        x = root.winfo_screenwidth() - w - margin
        y = root.winfo_screenheight() - h - margin - 48
        root.geometry(f'{w}x{h}+{x}+{y}')

        root.after(10_000, root.destroy)
        root.mainloop()
    except Exception:
        pass
