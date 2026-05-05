"""Progress display for image and model transfers.

A :class:`TransferProgress` context manager renders one progress bar
per host while transfers run in parallel.  When stderr is a TTY and
verbose logging isn't active, ``rich.progress`` draws live multi-row
bars; otherwise progress is reported as periodic log lines so CI
output stays clean.

Two channels feed the progress display:

- **Byte pump** (image transfer): a Python loop streams bytes from
  ``docker save`` to ``ssh ... docker load`` and calls
  :meth:`TransferProgress.advance` per chunk.
- **Rsync stderr parser** (model transfer): rsync is launched with
  ``--info=progress2 --no-inc-recursive`` and its stderr is parsed
  for cumulative-bytes / total / pct lines.
- **Remote progress lines** (from-head distribution): the embedded
  shell scripts emit ``__SR_PROGRESS host=... bytes=... total=...``
  markers which the controller streams from SSH and forwards.

The module is deliberately small — the rendering layer is decoupled
from the transfer code so non-TTY callers see ordinary log lines.
"""

from __future__ import annotations

import logging
import re
import sys
import threading
import time
from contextlib import AbstractContextManager
from dataclasses import dataclass, field

from sparkrun.core.progress import PROGRESS

logger = logging.getLogger(__name__)

# Marker emitted by from-head shell scripts and parsed back on the controller.
# Format: __SR_PROGRESS host=<h> bytes=<n> total=<m>   (total is optional)
PROGRESS_MARKER_PREFIX = "__SR_PROGRESS"

_RE_PROGRESS_MARKER = re.compile(r"__SR_PROGRESS\s+host=(?P<host>\S+)\s+bytes=(?P<bytes>\d+)(?:\s+total=(?P<total>\d+))?")

# Rsync --info=progress2 line shape:
#   "      1,234,567,890  45%  123.45MB/s    0:01:23"
# We extract bytes and percent; commas and unit suffixes are stripped.
_RE_RSYNC_PROGRESS = re.compile(
    r"^\s*([\d,]+)\s+(\d+)%",
)


def parse_rsync_progress_line(line: str) -> tuple[int, int] | None:
    """Parse a single ``--info=progress2`` line.

    Returns ``(bytes, percent)`` or ``None`` when the line isn't a
    progress line (rsync emits filenames and a final summary too).
    """
    m = _RE_RSYNC_PROGRESS.match(line)
    if not m:
        return None
    raw_bytes = m.group(1).replace(",", "")
    try:
        return int(raw_bytes), int(m.group(2))
    except ValueError:
        return None


def parse_remote_progress_line(line: str) -> tuple[str, int, int | None] | None:
    """Parse a ``__SR_PROGRESS`` marker emitted by a from-head shell script.

    Returns ``(host, bytes, total_or_None)`` or ``None`` when the line
    isn't a marker.
    """
    m = _RE_PROGRESS_MARKER.search(line)
    if not m:
        return None
    host = m.group("host")
    bytes_done = int(m.group("bytes"))
    total_raw = m.group("total")
    total = int(total_raw) if total_raw else None
    return host, bytes_done, total


def _human_bytes(n: int) -> str:
    """Format byte counts as a short human-readable string."""
    units = ["B", "KB", "MB", "GB", "TB"]
    f = float(n)
    for unit in units:
        if abs(f) < 1024.0:
            return f"{f:.1f}{unit}" if unit != "B" else f"{int(f)}B"
        f /= 1024.0
    return f"{f:.1f}PB"


@dataclass
class _HostState:
    """Per-host progress bookkeeping for the non-TTY (log) renderer."""

    host: str
    total: int | None
    completed: int = 0
    last_logged_at: float = 0.0
    last_logged_pct: int = -1
    done: bool = False
    label: str = ""
    task_id: object | None = None  # rich TaskID when in TTY mode
    extra: dict = field(default_factory=dict)


class TransferProgress(AbstractContextManager):
    """Multi-host transfer progress display.

    Use as a context manager.  Inside the block, call :meth:`add_host`
    for each transfer, then :meth:`advance` / :meth:`set_completed` /
    :meth:`set_total` from the worker thread, and :meth:`finish` when
    the host's transfer ends.

    The class is thread-safe — all rich calls go through an internal
    lock so multiple worker threads can update concurrently.

    Parameters
    ----------
    label:
        Short prefix for log messages (e.g. ``"image"`` or ``"model"``).
    enable:
        Force-enable or force-disable the rich renderer.  ``None``
        (default) auto-detects based on TTY and verbosity.
    """

    def __init__(self, label: str = "transfer", enable: bool | None = None) -> None:
        self.label = label
        self._lock = threading.Lock()
        self._hosts: dict[str, _HostState] = {}
        self._rich_progress = None  # type: ignore[var-annotated]
        self._enabled = enable if enable is not None else self._auto_enable()

    @staticmethod
    def _auto_enable() -> bool:
        """Use rich bars only when stderr is a TTY and DEBUG isn't on.

        DEBUG output is line-oriented and would clobber the live bars,
        so we fall back to plain log lines at -vvv.
        """
        if not sys.stderr.isatty():
            return False
        root_level = logging.getLogger().getEffectiveLevel()
        if root_level <= logging.DEBUG:
            return False
        return True

    # -- Lifecycle ----------------------------------------------------------

    def __enter__(self) -> TransferProgress:
        if self._enabled:
            try:
                from rich.progress import (
                    BarColumn,
                    DownloadColumn,
                    Progress,
                    TextColumn,
                    TimeRemainingColumn,
                    TransferSpeedColumn,
                )

                self._rich_progress = Progress(
                    TextColumn("[bold blue]{task.fields[host]:<20}", justify="left"),
                    BarColumn(bar_width=None),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    DownloadColumn(),
                    TransferSpeedColumn(),
                    TimeRemainingColumn(),
                    transient=False,
                )
                self._rich_progress.__enter__()
            except Exception as e:  # pragma: no cover - defensive
                logger.debug("Failed to start rich progress (%s); falling back to logs", e)
                self._rich_progress = None
                self._enabled = False
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._rich_progress is not None:
            try:
                self._rich_progress.__exit__(exc_type, exc, tb)
            except Exception:  # pragma: no cover - defensive
                pass
            self._rich_progress = None

    # -- Host registration --------------------------------------------------

    def add_host(self, host: str, total: int | None = None, action: str = "syncing") -> str:
        """Register a host transfer.

        ``host`` doubles as the lookup key for subsequent updates.
        ``total`` is the expected byte count (may be ``None`` for
        rsync where it's only known once rsync starts streaming).
        """
        with self._lock:
            state = _HostState(host=host, total=total, label=action)
            if self._rich_progress is not None:
                state.task_id = self._rich_progress.add_task(
                    description=action,
                    total=total or 0,
                    host=host,
                )
            else:
                size_str = f" ({_human_bytes(total)})" if total else ""
                logger.log(PROGRESS, "  %s %s -> %s%s", self.label, action, host, size_str)
            self._hosts[host] = state
        return host

    # -- Updates (called from worker threads) -------------------------------

    def advance(self, host: str, n: int) -> None:
        """Add ``n`` bytes of progress to *host*."""
        if n <= 0:
            return
        with self._lock:
            state = self._hosts.get(host)
            if state is None or state.done:
                return
            state.completed += n
            self._render(state)

    def set_completed(self, host: str, completed: int) -> None:
        """Set absolute completed bytes for *host*."""
        with self._lock:
            state = self._hosts.get(host)
            if state is None or state.done:
                return
            state.completed = completed
            self._render(state)

    def set_total(self, host: str, total: int) -> None:
        """Set or update the total byte count for *host*.

        Used by rsync where the total is reported in the first
        progress line.
        """
        with self._lock:
            state = self._hosts.get(host)
            if state is None or state.done:
                return
            state.total = total
            if self._rich_progress is not None and state.task_id is not None:
                self._rich_progress.update(state.task_id, total=total)

    def finish(self, host: str, success: bool = True) -> None:
        """Mark *host* as finished."""
        with self._lock:
            state = self._hosts.get(host)
            if state is None or state.done:
                return
            state.done = True
            if self._rich_progress is not None and state.task_id is not None:
                if success:
                    final_total = state.total or state.completed or 0
                    self._rich_progress.update(
                        state.task_id,
                        completed=final_total,
                        total=final_total,
                    )
                else:
                    self._rich_progress.update(state.task_id, description=f"{state.label} (FAILED)")
            else:
                tag = "OK" if success else "FAILED"
                size_str = _human_bytes(state.completed) if state.completed else ""
                logger.log(PROGRESS, "  %s %s -> %s %s%s", self.label, state.label, host, tag, f" ({size_str})" if size_str else "")

    # -- Internal -----------------------------------------------------------

    def _render(self, state: _HostState) -> None:
        """Render a single host's current state.  Caller holds the lock."""
        if self._rich_progress is not None and state.task_id is not None:
            update_kwargs: dict[str, object] = {"completed": state.completed}
            if state.total:
                update_kwargs["total"] = state.total
            self._rich_progress.update(state.task_id, **update_kwargs)
            return

        # Non-TTY: throttle log lines so we don't flood output.
        now = time.monotonic()
        pct = -1
        if state.total:
            pct = int(state.completed * 100 / state.total) if state.total > 0 else 0
        time_elapsed = now - state.last_logged_at
        pct_advanced = pct - state.last_logged_pct
        if time_elapsed >= 5.0 or (pct >= 0 and pct_advanced >= 10):
            state.last_logged_at = now
            state.last_logged_pct = pct
            if state.total:
                logger.log(
                    PROGRESS,
                    "  %s -> %s: %s / %s (%d%%)",
                    self.label,
                    state.host,
                    _human_bytes(state.completed),
                    _human_bytes(state.total),
                    pct,
                )
            else:
                logger.log(
                    PROGRESS,
                    "  %s -> %s: %s",
                    self.label,
                    state.host,
                    _human_bytes(state.completed),
                )
