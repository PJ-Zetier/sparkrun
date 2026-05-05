"""Tests for the transfer-progress helpers.

Covers:
- ``__SR_PROGRESS`` marker parsing (used by from-head distribution)
- rsync ``--info=progress2`` line parsing
- TransferProgress non-TTY (log) rendering — no ANSI escapes leak out
- Byte-pump pipeline (run_pipeline_to_remote_with_progress) wires bytes
  through and invokes the per-chunk callback
- Line-callback streaming (run_remote_script_with_line_callback) forwards
  lines while preserving the captured stdout
"""

from __future__ import annotations

import logging
from unittest import mock

from sparkrun.orchestration.progress_transfer import (
    TransferProgress,
    parse_remote_progress_line,
    parse_rsync_progress_line,
)


# ---------------------------------------------------------------------------
# parse_remote_progress_line
# ---------------------------------------------------------------------------


class TestParseRemoteProgressLine:
    def test_full_marker(self):
        out = parse_remote_progress_line("__SR_PROGRESS host=node-1 bytes=12345 total=99999")
        assert out == ("node-1", 12345, 99999)

    def test_marker_without_total(self):
        out = parse_remote_progress_line("__SR_PROGRESS host=10.0.0.5 bytes=42")
        assert out == ("10.0.0.5", 42, None)

    def test_embedded_marker_in_log_line(self):
        # Markers may appear after a leading prefix when stderr is merged.
        out = parse_remote_progress_line("[head] __SR_PROGRESS host=h bytes=1 total=2")
        assert out == ("h", 1, 2)

    def test_non_marker(self):
        assert parse_remote_progress_line("Sending image -> node-1") is None
        assert parse_remote_progress_line("") is None


# ---------------------------------------------------------------------------
# parse_rsync_progress_line
# ---------------------------------------------------------------------------


class TestParseRsyncProgressLine:
    def test_typical_line(self):
        # Standard --info=progress2 cumulative-bytes line
        out = parse_rsync_progress_line("    1,234,567,890  45%  123.45MB/s    0:01:23")
        assert out == (1234567890, 45)

    def test_no_commas(self):
        out = parse_rsync_progress_line("12345 100% 1.0MB/s 0:00:00")
        assert out == (12345, 100)

    def test_filename_line_returns_none(self):
        assert parse_rsync_progress_line("models/Llama/snapshots/abc/model.safetensors") is None

    def test_summary_line_returns_none(self):
        assert parse_rsync_progress_line("sent 12345 bytes  received 99 bytes  100.00 bytes/sec") is None


# ---------------------------------------------------------------------------
# TransferProgress (non-TTY log path)
# ---------------------------------------------------------------------------


class TestTransferProgressNonTTY:
    """Forced non-TTY mode: progress should emit log lines, not ANSI."""

    def test_basic_lifecycle_emits_log_lines(self, caplog):
        caplog.set_level(logging.INFO, logger="sparkrun.orchestration.progress_transfer")
        with TransferProgress(label="image", enable=False) as p:
            p.add_host("node-1", total=1000, action="docker save→load")
            p.advance("node-1", 500)
            p.advance("node-1", 500)
            p.finish("node-1", success=True)

        joined = "\n".join(r.getMessage() for r in caplog.records)
        # No ANSI escapes leak out in non-TTY mode.
        assert "\x1b[" not in joined
        # Start, finish lines visible.
        assert "node-1" in joined
        assert "OK" in joined

    def test_failed_finish_logs_failed(self, caplog):
        caplog.set_level(logging.INFO, logger="sparkrun.orchestration.progress_transfer")
        with TransferProgress(label="model", enable=False) as p:
            p.add_host("node-1", total=None, action="rsync")
            p.finish("node-1", success=False)
        joined = "\n".join(r.getMessage() for r in caplog.records)
        assert "FAILED" in joined

    def test_advance_after_finish_is_noop(self, caplog):
        caplog.set_level(logging.INFO, logger="sparkrun.orchestration.progress_transfer")
        with TransferProgress(label="image", enable=False) as p:
            p.add_host("h", total=100)
            p.finish("h", success=True)
            p.advance("h", 50)  # should not crash, not re-render
        # No exceptions = pass

    def test_unknown_host_advance_is_safe(self):
        with TransferProgress(label="image", enable=False) as p:
            p.advance("never-registered", 999)  # must not raise


# ---------------------------------------------------------------------------
# run_pipeline_to_remote_with_progress (byte pump)
# ---------------------------------------------------------------------------


class TestPipelineWithProgress:
    """Verify the byte pump invokes the progress callback per chunk."""

    def test_callback_sees_total_bytes(self):
        from sparkrun.orchestration.ssh import run_pipeline_to_remote_with_progress

        # Use 'cat' as the local producer with a known input; redirect via
        # 'dd of=/dev/null' on the remote side.  We don't actually SSH —
        # we monkey-patch Popen so the producer reads our test data and
        # the consumer is a no-op sink.
        chunks_seen: list[int] = []

        with mock.patch("sparkrun.orchestration.ssh.subprocess.Popen") as PopenMock:
            # Producer: read three chunks then EOF
            producer = mock.MagicMock()
            producer.stdout.read.side_effect = [b"a" * 1024, b"b" * 1024, b"c" * 512, b""]
            producer.stderr.read.return_value = b""
            producer.wait.return_value = 0

            consumer = mock.MagicMock()
            consumer.stdin.write.return_value = None
            consumer.stdout.read.return_value = b""
            consumer.stderr.read.return_value = b""
            consumer.wait.return_value = 0

            PopenMock.side_effect = [producer, consumer]

            def cb(n: int) -> None:
                chunks_seen.append(n)

            result = run_pipeline_to_remote_with_progress(
                host="h",
                local_cmd=["docker", "save", "img"],
                remote_cmd="docker load",
                progress_cb=cb,
                chunk_size=1024,
            )

        assert result.success
        assert chunks_seen == [1024, 1024, 512]


# ---------------------------------------------------------------------------
# run_remote_script_with_line_callback
# ---------------------------------------------------------------------------


class TestLineCallbackSSH:
    def test_callback_invoked_per_line(self):
        from sparkrun.orchestration.ssh import run_remote_script_with_line_callback

        with mock.patch("sparkrun.orchestration.ssh.subprocess.Popen") as PopenMock:
            proc = mock.MagicMock()
            # iter() over Popen.stdout yields lines (newline-terminated)
            proc.stdout.__iter__.return_value = iter(["one\n", "__SR_PROGRESS host=h bytes=10 total=100\n", "two\n"])
            proc.stdin = mock.MagicMock()
            proc.wait.return_value = 0
            PopenMock.return_value = proc

            received: list[str] = []
            result = run_remote_script_with_line_callback(
                "head",
                "echo hi",
                received.append,
            )

        assert result.success
        assert received == [
            "one",
            "__SR_PROGRESS host=h bytes=10 total=100",
            "two",
        ]
        # Captured stdout preserves the lines too.
        assert "__SR_PROGRESS" in result.stdout

    def test_callback_exception_does_not_break_stream(self):
        from sparkrun.orchestration.ssh import run_remote_script_with_line_callback

        with mock.patch("sparkrun.orchestration.ssh.subprocess.Popen") as PopenMock:
            proc = mock.MagicMock()
            proc.stdout.__iter__.return_value = iter(["a\n", "b\n", "c\n"])
            proc.stdin = mock.MagicMock()
            proc.wait.return_value = 0
            PopenMock.return_value = proc

            seen: list[str] = []

            def cb(line: str) -> None:
                seen.append(line)
                if line == "b":
                    raise RuntimeError("kaboom")

            result = run_remote_script_with_line_callback("head", "x", cb)

        assert result.success
        # All three lines were attempted despite the exception in the middle.
        assert seen == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# Integration: progress markers from script -> TransferProgress
# ---------------------------------------------------------------------------


class TestEndToEndProgressForwarding:
    """Simulate the from-head pipeline: marker lines -> progress updates."""

    def test_markers_route_to_correct_host(self, caplog):
        caplog.set_level(logging.INFO, logger="sparkrun.orchestration.progress_transfer")
        with TransferProgress(label="image", enable=False) as p:
            for h in ("node-2", "node-3"):
                p.add_host(h, total=None, action="docker save→load")

            for raw in [
                "Distributing image",
                "__SR_PROGRESS host=node-2 bytes=512 total=1024",
                "__SR_PROGRESS host=node-2 bytes=1024 total=1024",
                "__SR_PROGRESS host=node-3 bytes=200 total=400",
            ]:
                parsed = parse_remote_progress_line(raw)
                if parsed is None:
                    continue
                host, bytes_done, total = parsed
                if total is not None:
                    p.set_total(host, total)
                p.set_completed(host, bytes_done)

            for h in ("node-2", "node-3"):
                p.finish(h, success=True)

        msgs = "\n".join(r.getMessage() for r in caplog.records)
        assert "node-2" in msgs and "node-3" in msgs


# ---------------------------------------------------------------------------
# Auto-enable detection
# ---------------------------------------------------------------------------


class TestAutoEnable:
    def test_disabled_when_stderr_not_tty(self):
        with mock.patch("sparkrun.orchestration.progress_transfer.sys.stderr") as stderr:
            stderr.isatty.return_value = False
            assert TransferProgress._auto_enable() is False

    def test_disabled_at_debug_level(self):
        with (
            mock.patch("sparkrun.orchestration.progress_transfer.sys.stderr") as stderr,
            mock.patch.object(logging.getLogger(), "getEffectiveLevel", return_value=logging.DEBUG),
        ):
            stderr.isatty.return_value = True
            assert TransferProgress._auto_enable() is False

    def test_enabled_when_tty_and_info_level(self):
        with (
            mock.patch("sparkrun.orchestration.progress_transfer.sys.stderr") as stderr,
            mock.patch.object(logging.getLogger(), "getEffectiveLevel", return_value=logging.INFO),
        ):
            stderr.isatty.return_value = True
            assert TransferProgress._auto_enable() is True
