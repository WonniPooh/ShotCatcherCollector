"""
Terminal dashboard for the ShotCatcher collector.

Split-screen curses UI:
  ┌─ Header ────────────────────────────────────────────────────┐
  │ Uptime / network / WS status / rate limit                  │
  ├─ Symbols ───────────────────┬─ Trades Stream ──────────────┤
  │ Watched symbols list        │ WS conns / received / buffer │
  ├─ Active Loading ────────────┼─ Order Events ───────────────┤
  │ Symbols being loaded (REST) │ Persisted DB count           │
  ├─ Live Logs ─────────────────────────────────────────────────┤
  │ (scrolling log tail — fills remaining space)                │
  └─────────────────────────────────────────────────────────────┘

Usage:
    dashboard = Dashboard(...)
    dashboard.start()      # spawns background refresh task
    dashboard.stop()       # restores terminal
"""
from __future__ import annotations

import asyncio
import curses
import logging
import time
from collections import deque
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from collector.order_event_persister import OrderEventPersister
    from collector.symbol_tracker import SymbolTracker
    from collector.user_data_ws import UserDataWS
    from data_manager.binance_rate_limiter import BinanceRateLimiter
    from data_manager.trades_manager.trades_loader import TradesLoader


class _LogCapture(logging.Handler):
    """Captures log records into a deque for the dashboard log panel."""

    def __init__(self, maxlen: int = 500):
        super().__init__()
        self.records: deque[str] = deque(maxlen=maxlen)
        self.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname).1s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        ))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.records.append(self.format(record))
        except Exception:
            pass


class Dashboard:
    """Curses-based terminal dashboard for the collector."""

    REFRESH_HZ = 2  # updates per second

    def __init__(
        self,
        user_data_ws: Optional[UserDataWS] = None,
        symbol_tracker: Optional[SymbolTracker] = None,
        trades_loader: Optional[TradesLoader] = None,
        persister: Optional[OrderEventPersister] = None,
        rate_limiter: Optional[BinanceRateLimiter] = None,
    ):
        self._ws = user_data_ws
        self._tracker = symbol_tracker
        self._loader = trades_loader
        self._persister = persister
        self._rate_limiter = rate_limiter
        self._start_time = time.monotonic()

        self._stdscr: Optional[curses.window] = None
        self._task: Optional[asyncio.Task] = None
        self._log_capture = _LogCapture(maxlen=2000)
        self._stopped = False

    def start(self) -> None:
        """Attach log capture and spawn the curses refresh loop."""
        root = logging.getLogger()
        # Detach any StreamHandlers that write to the terminal so they don't
        # corrupt the curses display. They are re-attached in _restore_terminal.
        self._suspended_handlers: list[logging.Handler] = [
            h for h in root.handlers
            if isinstance(h, logging.StreamHandler)
            and not isinstance(h, logging.FileHandler)
        ]
        for h in self._suspended_handlers:
            root.removeHandler(h)
        root.addHandler(self._log_capture)

        self._stdscr = curses.initscr()
        curses.noecho()
        curses.cbreak()
        curses.curs_set(0)
        self._stdscr.nodelay(True)
        self._stdscr.keypad(True)

        if curses.has_colors():
            curses.start_color()
            curses.use_default_colors()
            curses.init_pair(1, curses.COLOR_GREEN, -1)   # connected / ok
            curses.init_pair(2, curses.COLOR_RED, -1)      # disconnected / error
            curses.init_pair(3, curses.COLOR_YELLOW, -1)   # warning / loading
            curses.init_pair(4, curses.COLOR_CYAN, -1)     # header / labels
            curses.init_pair(5, curses.COLOR_WHITE, -1)    # normal text

        self._task = asyncio.get_running_loop().create_task(self._refresh_loop())

    async def stop(self) -> None:
        """Stop the refresh loop and restore the terminal."""
        self._stopped = True
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._restore_terminal()

    def _restore_terminal(self) -> None:
        try:
            curses.nocbreak()
            curses.echo()
            curses.endwin()
        except Exception:
            pass
        root = logging.getLogger()
        root.removeHandler(self._log_capture)
        # Re-attach the console handlers that were suspended during curses.
        for h in getattr(self, '_suspended_handlers', []):
            root.addHandler(h)
        self._suspended_handlers = []

    # ── Main loop ────────────────────────────────────────────────────

    async def _refresh_loop(self) -> None:
        _draw_logger = logging.getLogger("dashboard")
        try:
            while not self._stopped:
                try:
                    self._draw()
                except curses.error:
                    pass
                except Exception as exc:
                    # Log to the capture handler so it surfaces in the log panel,
                    # but keep the loop alive rather than silently dying.
                    try:
                        _draw_logger.error("Dashboard draw error: %s", exc, exc_info=True)
                    except Exception:
                        pass
                # Handle 'q' to quit
                try:
                    ch = self._stdscr.getch()
                    if ch == ord('q'):
                        break
                except curses.error:
                    pass
                await asyncio.sleep(1.0 / self.REFRESH_HZ)
        except asyncio.CancelledError:
            pass

    # ── Drawing ──────────────────────────────────────────────────────

    def _draw(self) -> None:
        scr = self._stdscr
        scr.erase()
        max_y, max_x = scr.getmaxyx()
        if max_y < 10 or max_x < 40:
            scr.addstr(0, 0, "Terminal too small")
            scr.refresh()
            return

        row = 0

        # ── Header bar ──────────────────────────────────────────────
        row = self._draw_header(scr, row, max_x)

        # ── Split panels ────────────────────────────────────────────
        mid = max_x // 2
        panel_top = row

        # Left: watched symbols
        row_l = self._draw_symbols(scr, panel_top, 0, mid - 1)
        # Right: trades stream stats
        row_r = self._draw_trades_stream(scr, panel_top, mid, max_x - mid)

        row = max(row_l, row_r)

        # Left: active loading
        row_l2 = self._draw_loading(scr, row, 0, mid - 1)
        # Right: order events
        row_r2 = self._draw_order_events(scr, row, mid, max_x - mid)

        row = max(row_l2, row_r2)

        # ── Log panel (fills rest) ──────────────────────────────────
        self._draw_logs(scr, row, max_y, max_x)

        scr.refresh()

    def _draw_header(self, scr, row: int, width: int) -> int:
        # Title bar
        uptime = self._format_uptime()
        net = "mainnet"
        title = f" ShotCatcher Collector — {net} — up {uptime} "
        self._safe_addstr(scr, row, 0, title.center(width, "─")[:width],
                          curses.A_BOLD | curses.color_pair(4))
        row += 1

        # UserData WS status
        ws_ok = self._ws and self._ws.connected
        ws_lbl = "● connected" if ws_ok else "○ disconnected"
        ws_color = curses.color_pair(1) if ws_ok else curses.color_pair(2)
        self._safe_addstr(scr, row, 1, "UserData WS: ")
        self._safe_addstr(scr, row, 14, ws_lbl, ws_color | curses.A_BOLD)

        # Rate limit
        if self._rate_limiter:
            used = self._rate_limiter.used_weight
            limit = 2400
            rl_str = f"Rate: {used}/{limit} weight"
            rl_color = curses.color_pair(1) if used < 1800 else (
                curses.color_pair(3) if used < 2200 else curses.color_pair(2))
            self._safe_addstr(scr, row, width // 2, rl_str, rl_color)

        row += 1
        self._safe_addstr(scr, row, 0, "─" * width, curses.color_pair(4))
        row += 1
        return row

    def _draw_symbols(self, scr, row: int, col: int, width: int) -> int:
        symbols = sorted(self._tracker.watched_symbols) if self._tracker else []
        hdr = f" Symbols ({len(symbols)}) "
        self._safe_addstr(scr, row, col, hdr.ljust(width, "─")[:width],
                          curses.A_BOLD | curses.color_pair(4))
        row += 1

        if not symbols:
            self._safe_addstr(scr, row, col + 1, "(none)", curses.color_pair(5))
            row += 1
        else:
            # Pack symbols in rows, ~12 chars each
            per_row = max(1, (width - 2) // 12)
            for i in range(0, len(symbols), per_row):
                chunk = symbols[i:i + per_row]
                line = "  ".join(f"{s:10s}" for s in chunk)
                self._safe_addstr(scr, row, col + 1, line[:width - 2],
                                  curses.color_pair(5))
                row += 1
        return row

    def _draw_trades_stream(self, scr, row: int, col: int, width: int) -> int:
        hdr = " Trades Stream "
        self._safe_addstr(scr, row, col, hdr.ljust(width, "─")[:width],
                          curses.A_BOLD | curses.color_pair(4))
        row += 1

        ws_stream = getattr(self._loader, '_ws_stream', None) if self._loader else None
        if ws_stream:
            live = self._loader.live_symbols if self._loader else []
            items = [
                ("Live syms", str(len(live))),
                ("WS conns", str(ws_stream.connection_count)),
                ("Received", f"{ws_stream.trades_received:,}"),
                ("Flushed", f"{ws_stream.trades_flushed:,}"),
                ("Buffer", f"{ws_stream.buffer_size():,}"),
            ]
        else:
            items = [("Live syms", "—"), ("WS conns", "—"), ("Received", "—"),
                     ("Flushed", "—"), ("Buffer", "—")]

        for label, val in items:
            self._safe_addstr(scr, row, col + 1, f"{label:>10s}: ", curses.color_pair(5))
            self._safe_addstr(scr, row, col + 13, val, curses.color_pair(1))
            row += 1
        return row

    def _draw_loading(self, scr, row: int, col: int, width: int) -> int:
        active = self._loader.active_symbols if self._loader else []
        hdr = f" Loading ({len(active)}) "
        self._safe_addstr(scr, row, col, hdr.ljust(width, "─")[:width],
                          curses.A_BOLD | curses.color_pair(4))
        row += 1

        if not active:
            self._safe_addstr(scr, row, col + 1, "(idle)", curses.color_pair(5))
            row += 1
        else:
            for sym in active[:5]:  # show up to 5
                self._safe_addstr(scr, row, col + 1, f"{sym}: loading...",
                                  curses.color_pair(3))
                row += 1
            if len(active) > 5:
                self._safe_addstr(scr, row, col + 1, f"  +{len(active) - 5} more",
                                  curses.color_pair(5))
                row += 1
        return row

    def _draw_order_events(self, scr, row: int, col: int, width: int) -> int:
        hdr = " Order Events "
        self._safe_addstr(scr, row, col, hdr.ljust(width, "─")[:width],
                          curses.A_BOLD | curses.color_pair(4))
        row += 1

        if self._persister:
            n_order = len(self._persister._order_dbs)
            n_trade = len(self._persister._trade_dbs)
            self._safe_addstr(scr, row, col + 1, f"Order DBs: {n_order}",
                              curses.color_pair(5))
            row += 1
            self._safe_addstr(scr, row, col + 1, f"Trade DBs: {n_trade}",
                              curses.color_pair(5))
            row += 1
        else:
            self._safe_addstr(scr, row, col + 1, "(not active)", curses.color_pair(5))
            row += 1

        return row

    def _draw_logs(self, scr, row: int, max_y: int, max_x: int) -> None:
        hdr = " Live Logs (press 'q' to quit) "
        self._safe_addstr(scr, row, 0, hdr.center(max_x, "─")[:max_x],
                          curses.A_BOLD | curses.color_pair(4))
        row += 1

        log_lines = max_y - row - 1  # leave 1 line bottom margin
        if log_lines <= 0:
            return

        records = list(self._log_capture.records)
        visible = records[-log_lines:] if len(records) > log_lines else records

        for i, line in enumerate(visible):
            y = row + i
            if y >= max_y - 1:
                break
            # Color based on log level marker
            color = curses.color_pair(5)
            if "[E]" in line:
                color = curses.color_pair(2)
            elif "[W]" in line:
                color = curses.color_pair(3)

            self._safe_addstr(scr, y, 0, line[:max_x - 1], color)

    # ── Helpers ──────────────────────────────────────────────────────

    def _format_uptime(self) -> str:
        elapsed = int(time.monotonic() - self._start_time)
        h, rem = divmod(elapsed, 3600)
        m, s = divmod(rem, 60)
        if h > 0:
            return f"{h}h {m:02d}m {s:02d}s"
        return f"{m}m {s:02d}s"

    @staticmethod
    def _safe_addstr(scr, y: int, x: int, text: str, attr: int = 0) -> None:
        """Write text at (y, x), silently ignoring out-of-bounds."""
        try:
            max_y, max_x = scr.getmaxyx()
            if y < max_y and x < max_x:
                scr.addnstr(y, x, text, max_x - x - 1, attr)
        except curses.error:
            pass
