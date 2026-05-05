"""SSH remote execution via bash -s stdin piping.

All remote operations in sparkrun are executed by generating scripts
as Python strings and piping them to `ssh <host> bash -s` via stdin.
No files are ever copied to remote hosts.
"""

from __future__ import annotations

import logging
import subprocess
import time
from dataclasses import dataclass

from sparkrun.utils.shell import quote, quote_list, args_list_to_shell_str

logger = logging.getLogger(__name__)

_DEFAULT_RSYNC_OPTIONS = ["-az", "--no-times", "--mkpath", "--partial", "--links"]


@dataclass
class RemoteResult:
    """Result of a remote script execution."""

    host: str
    returncode: int
    stdout: str
    stderr: str

    @property
    def success(self) -> bool:
        return self.returncode == 0

    @property
    def last_line(self) -> str:
        """Get the last non-empty line of stdout (useful for extracting IPs etc)."""
        lines = [line for line in self.stdout.strip().splitlines() if line.strip()]
        return lines[-1] if lines else ""


def _run_subprocess(
    cmd: list[str] | str,
    host: str,
    label: str,
    timeout: int | None = None,
    input_data: str | None = None,
    shell: bool = False,
    quiet: bool = False,
) -> RemoteResult:
    """Run a subprocess and return a RemoteResult with standard error handling.

    Centralizes the try/subprocess.run/TimeoutExpired/Exception pattern
    used by all SSH, rsync, and pipeline execution functions.

    Args:
        cmd: Command to execute (list or string for shell=True).
        host: Host identifier for the result and log messages.
        label: Human-readable label for log messages (e.g. "SSH script", "Rsync").
        timeout: Execution timeout in seconds.
        input_data: Optional stdin data.
        shell: Whether to use shell=True.
        quiet: If True, downgrade failure logging from WARNING to DEBUG.
            Used for expected-failure probes (e.g. NOPASSWD sudo checks).

    Returns:
        RemoteResult with returncode, stdout, stderr.
    """
    t0 = time.monotonic()
    try:
        proc = subprocess.run(
            cmd,
            input=input_data,
            capture_output=True,
            text=True,
            timeout=timeout,
            shell=shell,
        )
        elapsed = time.monotonic() - t0
        result = RemoteResult(
            host=host,
            returncode=proc.returncode,
            stdout=proc.stdout,
            stderr=proc.stderr,
        )
        if result.success:
            logger.debug("  %s <- %s OK (%.1fs)", label, host, elapsed)
        else:
            log_fn = logger.debug if quiet else logger.warning
            log_fn(
                "  %s <- %s FAILED rc=%d (%.1fs): %s",
                label,
                host,
                proc.returncode,
                elapsed,
                proc.stderr.strip()[:200],
            )
        return result
    except subprocess.TimeoutExpired:
        elapsed = time.monotonic() - t0
        logger.error("  %s <- %s TIMEOUT after %.0fs", label, host, elapsed)
        return RemoteResult(host=host, returncode=-1, stdout="", stderr="Execution timed out")
    except Exception as e:
        elapsed = time.monotonic() - t0
        logger.error("  %s <- %s ERROR (%.1fs): %s", label, host, elapsed, e)
        return RemoteResult(host=host, returncode=-1, stdout="", stderr=str(e))


def build_ssh_cmd(
    host: str,
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    connect_timeout: int = 10,
) -> list[str]:
    """Build the base SSH command with standard options.

    Args:
        host: Remote hostname or IP address.
        ssh_user: Optional SSH username (prepended as user@host).
        ssh_key: Optional path to SSH private key file.
        ssh_options: Additional SSH command-line options.
        connect_timeout: SSH connection timeout in seconds.

    Returns:
        List of command parts suitable for subprocess.
    """
    cmd = ["ssh", "-o", "BatchMode=yes", "-o", f"ConnectTimeout={connect_timeout}"]
    if ssh_key:
        cmd.extend(["-i", ssh_key])
    if ssh_options:
        cmd.extend(ssh_options)
    target = f"{ssh_user}@{host}" if ssh_user else host
    cmd.append(target)
    return cmd


def run_remote_script(
    host: str,
    script: str,
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    connect_timeout: int = 10,
    timeout: int | None = None,
    dry_run: bool = False,
    quiet: bool = False,
) -> RemoteResult:
    """Execute a script on a remote host via stdin piping.

    The script is generated in-process and piped directly to
    ``ssh <host> bash -s`` on the remote. No files are copied.

    Args:
        host: Remote hostname or IP.
        script: Bash script content to execute.
        ssh_user: Optional SSH username.
        ssh_key: Optional path to SSH private key.
        ssh_options: Additional SSH options.
        connect_timeout: SSH connection timeout in seconds.
        timeout: Overall execution timeout in seconds.
        dry_run: If True, log the script but don't execute.
        quiet: If True, downgrade failure logging from WARNING to DEBUG.

    Returns:
        RemoteResult with returncode, stdout, stderr.
    """
    script_lines = script.count("\n")
    if dry_run:
        logger.info("[dry-run] Would execute on %s (%d lines, %d bytes)", host, script_lines, len(script))
        return RemoteResult(host=host, returncode=0, stdout="[dry-run]", stderr="")

    cmd = build_ssh_cmd(host, ssh_user, ssh_key, ssh_options, connect_timeout)
    cmd.extend(["bash", "-s"])

    logger.debug("  SSH script -> %s (%d bytes)%s", host, len(script), f" [timeout={timeout}s]" if timeout else "")
    logger.debug("SSH command: %s", " ".join(cmd))
    logger.debug("Script: %d lines, %d bytes", script_lines, len(script))

    result = _run_subprocess(quote_list(cmd), host, "SSH script", timeout=timeout, input_data=script, quiet=quiet)
    if result.success:
        if result.stdout.strip():
            logger.debug("Remote script stdout on %s:\n%s", host, result.stdout.strip())
        if result.stderr.strip():
            logger.debug("Remote script stderr on %s:\n%s", host, result.stderr.strip())
    else:
        if result.stdout.strip():
            logger.debug("Remote script stdout on %s:\n%s", host, result.stdout.strip())
    return result


def run_remote_script_streaming(
    host: str,
    script: str,
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    connect_timeout: int = 10,
    timeout: int | None = None,
    dry_run: bool = False,
    quiet: bool = False,
) -> RemoteResult:
    """Execute a script on a remote host with real-time stdout/stderr.

    Like :func:`run_remote_script` but connects the remote process's
    stdout and stderr directly to the terminal so output streams in
    real time.  Useful for long-running operations like container builds.

    When *quiet* is True, stdout/stderr are captured instead of
    streamed to the terminal.  Captured output is logged at DEBUG
    level.

    Args:
        host: Remote hostname or IP.
        script: Bash script content to execute.
        ssh_user: Optional SSH username.
        ssh_key: Optional path to SSH private key.
        ssh_options: Additional SSH options.
        connect_timeout: SSH connection timeout in seconds.
        timeout: Overall execution timeout in seconds.
        dry_run: If True, log the script but don't execute.
        quiet: If True, capture output instead of streaming to terminal.

    Returns:
        RemoteResult with returncode (stdout/stderr are empty when
        streaming, or captured when quiet).
    """
    if dry_run:
        logger.info("[dry-run] Would execute (streaming) on %s (%d bytes)", host, len(script))
        return RemoteResult(host=host, returncode=0, stdout="[dry-run]", stderr="")

    cmd = build_ssh_cmd(host, ssh_user, ssh_key, ssh_options, connect_timeout)
    cmd.extend(["bash", "-s"])

    logger.debug("  SSH script (streaming) -> %s (%d bytes)%s", host, len(script), " [timeout=%ds]" % timeout if timeout else "")

    t0 = time.monotonic()
    try:
        if quiet:
            proc = subprocess.run(
                cmd,
                input=script,
                text=True,
                timeout=timeout,
                capture_output=True,
            )
        else:
            proc = subprocess.run(
                cmd,
                input=script,
                text=True,
                timeout=timeout,
                # stdout/stderr go to terminal (no capture)
                stdout=None,
                stderr=None,
            )
        elapsed = time.monotonic() - t0
        if proc.returncode == 0:
            logger.debug("  SSH script (streaming) <- %s OK (%.1fs)", host, elapsed)
        else:
            logger.warning("  SSH script (streaming) <- %s FAILED rc=%d (%.1fs)", host, proc.returncode, elapsed)
        stdout = getattr(proc, "stdout", "") or ""
        stderr = getattr(proc, "stderr", "") or ""
        if quiet and stdout:
            logger.debug("Captured stdout on %s:\n%s", host, stdout[-2000:])
        if quiet and stderr:
            logger.debug("Captured stderr on %s:\n%s", host, stderr[-2000:])
        return RemoteResult(host=host, returncode=proc.returncode, stdout=stdout, stderr=stderr)
    except subprocess.TimeoutExpired:
        elapsed = time.monotonic() - t0
        logger.error("  SSH script (streaming) <- %s TIMEOUT after %.0fs", host, elapsed)
        return RemoteResult(host=host, returncode=-1, stdout="", stderr="Execution timed out")
    except Exception as e:
        elapsed = time.monotonic() - t0
        logger.error("  SSH script (streaming) <- %s ERROR (%.1fs): %s", host, elapsed, e)
        return RemoteResult(host=host, returncode=-1, stdout="", stderr=str(e))


def run_remote_script_with_line_callback(
    host: str,
    script: str,
    line_cb,
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    connect_timeout: int = 10,
    timeout: int | None = None,
    dry_run: bool = False,
) -> RemoteResult:
    """Execute a script on a remote host and invoke *line_cb* per output line.

    The script's stdout and stderr are merged and read line-by-line
    so a controller can react to progress markers in real time
    (used by from-head distribution).  Lines that ``line_cb``
    consumes are still appended to :attr:`RemoteResult.stdout`.

    Args:
        host: Remote hostname or IP.
        script: Bash script content to execute.
        line_cb: Callable invoked as ``cb(line)`` for each output
            line (no trailing newline).  Errors in the callback are
            swallowed so they can't disrupt the SSH stream.
        ssh_user, ssh_key, ssh_options, connect_timeout, timeout, dry_run:
            See :func:`run_remote_script`.
    """
    if dry_run:
        logger.info("[dry-run] Would execute (line-cb) on %s (%d bytes)", host, len(script))
        return RemoteResult(host=host, returncode=0, stdout="[dry-run]", stderr="")

    cmd = build_ssh_cmd(host, ssh_user, ssh_key, ssh_options, connect_timeout)
    cmd.extend(["bash", "-s"])

    logger.debug("  SSH script (line-cb) -> %s (%d bytes)", host, len(script))

    t0 = time.monotonic()
    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # merge so we get progress markers in order
            text=True,
            bufsize=1,
        )
    except Exception as e:
        elapsed = time.monotonic() - t0
        logger.error("  SSH script (line-cb) <- %s ERROR (%.1fs): %s", host, elapsed, e)
        return RemoteResult(host=host, returncode=-1, stdout="", stderr=str(e))

    assert proc.stdin is not None and proc.stdout is not None
    try:
        proc.stdin.write(script)
    except (BrokenPipeError, OSError):
        pass
    finally:
        try:
            proc.stdin.close()
        except OSError:
            pass

    captured: list[str] = []
    try:
        for raw in proc.stdout:
            line = raw.rstrip("\r\n")
            captured.append(line)
            try:
                line_cb(line)
            except Exception:  # pragma: no cover - defensive
                pass
    except Exception as e:
        logger.error("  SSH script (line-cb) <- %s read error: %s", host, e)

    try:
        rc = proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        elapsed = time.monotonic() - t0
        logger.error("  SSH script (line-cb) <- %s TIMEOUT after %.0fs", host, elapsed)
        proc.kill()
        return RemoteResult(host=host, returncode=-1, stdout="\n".join(captured), stderr="Execution timed out")

    elapsed = time.monotonic() - t0
    if rc == 0:
        logger.debug("  SSH script (line-cb) <- %s OK (%.1fs)", host, elapsed)
    else:
        logger.warning("  SSH script (line-cb) <- %s FAILED rc=%d (%.1fs)", host, rc, elapsed)
    return RemoteResult(host=host, returncode=rc, stdout="\n".join(captured), stderr="")


def run_remote_command(
    host: str,
    command: str,
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    connect_timeout: int = 10,
    timeout: int | None = None,
    dry_run: bool = False,
    quiet: bool = False,
) -> RemoteResult:
    """Execute a single command on a remote host (not via bash -s).

    For simple one-liners where piping a script is overkill.

    Args:
        host: Remote hostname or IP.
        command: Command string to execute remotely.
        ssh_user: Optional SSH username.
        ssh_key: Optional path to SSH private key.
        ssh_options: Additional SSH options.
        connect_timeout: SSH connection timeout in seconds.
        timeout: Overall execution timeout in seconds.
        dry_run: If True, log the command but don't execute.
        quiet: If True, downgrade failure logging from WARNING to DEBUG.

    Returns:
        RemoteResult with returncode, stdout, stderr.
    """
    if dry_run:
        logger.info("[dry-run] Would run on %s: %s", host, command)
        return RemoteResult(host=host, returncode=0, stdout="[dry-run]", stderr="")

    cmd = build_ssh_cmd(host, ssh_user, ssh_key, ssh_options, connect_timeout)
    cmd.append(command)

    logger.debug("  SSH cmd -> %s: %s", host, command[:80])
    logger.debug("SSH command: %s", " ".join(cmd))

    result = _run_subprocess(cmd, host, "SSH cmd", timeout=timeout, quiet=quiet)
    if result.stdout.strip():
        logger.debug("Remote command stdout on %s:\n%s", host, result.stdout.strip())
    if result.stderr.strip():
        logger.debug("Remote command stderr on %s:\n%s", host, result.stderr.strip())
    return result


def stream_remote_logs(
    host: str,
    container_name: str,
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    tail: int = 100,
    dry_run: bool = False,
) -> None:
    """Stream ``docker logs -f`` output to the terminal.

    For remote hosts, runs ``ssh <host> docker logs -f --tail N <container>``.
    For local hosts, runs ``docker logs -f --tail N <container>`` directly.

    The process's stdout/stderr are connected directly to the terminal
    (no capture), so log output flows in real time.  A ``KeyboardInterrupt``
    is caught so the user can press Ctrl-C to stop following without a
    traceback.

    Args:
        host: Target hostname or IP.  ``"localhost"``, ``"127.0.0.1"``,
            or ``""`` are treated as local.
        container_name: Name of the Docker container to follow.
        ssh_user: Optional SSH username.
        ssh_key: Optional path to SSH private key.
        ssh_options: Additional SSH options.
        tail: Number of existing log lines to show before following.
        dry_run: If True, print the command that would run and return.
    """
    from sparkrun.orchestration.docker import docker_logs_cmd
    from sparkrun.orchestration.primitives import should_run_locally

    logs_cmd = docker_logs_cmd(container_name, follow=True, tail=tail)

    if should_run_locally(host, ssh_user):
        cmd = logs_cmd.split()
    else:
        ssh_base = build_ssh_cmd(host, ssh_user, ssh_key, ssh_options)
        cmd = ssh_base + logs_cmd.split()

    if dry_run:
        logger.info("[dry-run] Would stream logs: %s", " ".join(cmd))
        return

    logger.info("Following logs for container '%s' on %s (Ctrl-C to stop)...", container_name, host or "localhost")
    try:
        subprocess.run(cmd)
    except KeyboardInterrupt:
        logger.info("\nLog following stopped.")


def stream_container_file_logs(
    host: str,
    container_name: str,
    log_file: str = "/tmp/sparkrun_serve.log",
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    tail: int = 100,
    dry_run: bool = False,
) -> None:
    """Stream a log file from inside a running container.

    Runs ``docker exec <container> tail -f --lines <N> <file>``.
    Used for runtimes that exec the serve command inside a long-running
    container (e.g. vLLM's ``sleep infinity`` + ``nohup serve``).

    Args:
        host: Target hostname or IP.
        container_name: Name of the Docker container.
        log_file: Path to the log file inside the container.
        ssh_user: Optional SSH username.
        ssh_key: Optional path to SSH private key.
        ssh_options: Additional SSH options.
        tail: Number of existing log lines to show before following.
        dry_run: If True, print the command that would run and return.
    """
    tail_cmd = [
        "docker",
        "exec",
        container_name,
        "tail",
        "-f",
        "--lines",
        str(tail),
        log_file,
    ]

    from sparkrun.orchestration.primitives import should_run_locally

    if should_run_locally(host, ssh_user):
        cmd = tail_cmd
    else:
        ssh_base = build_ssh_cmd(host, ssh_user, ssh_key, ssh_options)
        cmd = ssh_base + tail_cmd

    if dry_run:
        logger.info("[dry-run] Would stream container file logs: %s", " ".join(cmd))
        return

    logger.info("Following serve logs in container '%s' on %s (Ctrl-C to stop)...", container_name, host or "localhost")
    try:
        subprocess.run(cmd)
    except KeyboardInterrupt:
        logger.info("\nLog following stopped.")


def start_log_capture(
    host: str,
    container_name: str,
    ssh_kwargs: dict,
    tail: int = 200,
) -> subprocess.Popen | None:
    """Start a background ``docker logs -f`` process, capturing output.

    Returns the Popen handle (or ``None`` if the process couldn't start).
    The caller should later pass this to :func:`stop_log_capture`.

    Args:
        host: Target hostname or IP.
        container_name: Name of the Docker container to follow.
        ssh_kwargs: SSH connection kwargs (ssh_user, ssh_key, ssh_options).
        tail: Number of existing log lines to include.

    Returns:
        A :class:`subprocess.Popen` handle, or ``None`` on failure.
    """
    from sparkrun.orchestration.docker import docker_logs_cmd
    from sparkrun.orchestration.primitives import should_run_locally

    logs_cmd = docker_logs_cmd(container_name, follow=True, tail=tail)

    if should_run_locally(host, ssh_kwargs.get("ssh_user")):
        cmd = logs_cmd.split()
    else:
        ssh_base = build_ssh_cmd(
            host,
            ssh_user=ssh_kwargs.get("ssh_user"),
            ssh_key=ssh_kwargs.get("ssh_key"),
            ssh_options=ssh_kwargs.get("ssh_options"),
        )
        cmd = ssh_base + logs_cmd.split()

    try:
        return subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except OSError:
        logger.debug("Failed to start background log capture for %s", container_name)
        return None


def stop_log_capture(proc: subprocess.Popen | None) -> list[str]:
    """Terminate a background log capture and return captured lines.

    Args:
        proc: The Popen handle returned by :func:`start_log_capture`,
            or ``None`` (in which case an empty list is returned).

    Returns:
        List of captured log lines.
    """
    if proc is None:
        return []
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)

    lines: list[str] = []
    if proc.stdout:
        try:
            raw = proc.stdout.read()
            lines = raw.splitlines()
        except (OSError, ValueError):
            pass
        finally:
            proc.stdout.close()
    return lines


def run_remote_scripts_parallel(
    hosts: list[str],
    script: str,
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    timeout: int | None = None,
    dry_run: bool = False,
    quiet: bool = False,
) -> list[RemoteResult]:
    """Execute the same script on multiple hosts in parallel using threads.

    Args:
        hosts: List of remote hostnames or IPs.
        script: Bash script content to execute on each host.
        ssh_user: Optional SSH username.
        ssh_key: Optional path to SSH private key.
        ssh_options: Additional SSH options.
        timeout: Per-host execution timeout in seconds.
        dry_run: If True, log the script but don't execute.
        quiet: If True, downgrade failure logging from WARNING to DEBUG.

    Returns:
        List of RemoteResult, one per host (order not guaranteed).
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    logger.info("  Running script in parallel on %d hosts: %s", len(hosts), ", ".join(hosts))

    t0 = time.monotonic()
    results: list[RemoteResult] = []
    with ThreadPoolExecutor(max_workers=len(hosts)) as executor:
        futures = {
            executor.submit(
                run_remote_script,
                host,
                script,
                ssh_user=ssh_user,
                ssh_key=ssh_key,
                ssh_options=ssh_options,
                timeout=timeout,
                dry_run=dry_run,
                quiet=quiet,
            ): host
            for host in hosts
        }
        for future in as_completed(futures):
            result = future.result()
            results.append(result)

    elapsed = time.monotonic() - t0
    ok = sum(1 for r in results if r.success)
    logger.info("  Parallel execution done: %d/%d OK (%.1fs total)", ok, len(results), elapsed)

    return results


def run_remote_sudo_script(
    host: str,
    script: str,
    password: str,
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    timeout: int = 60,
    dry_run: bool = False,
) -> RemoteResult:
    """Execute a script on a remote host via ``sudo -S bash -s``.

    Prepends the sudo password to stdin so ``sudo -S`` can read it,
    then the remaining stdin is consumed by ``bash -s`` as the script.

    Only use this for hosts that do NOT have passwordless sudo.
    For NOPASSWD hosts, use :func:`run_remote_script` instead — ``sudo -S``
    on a NOPASSWD host would leave the password line in stdin for bash
    to misinterpret as a command.

    Args:
        host: Remote hostname or IP.
        script: Bash script content to execute.
        password: Sudo password for the remote user.
        ssh_user: Optional SSH username.
        ssh_key: Optional path to SSH private key.
        ssh_options: Additional SSH options.
        timeout: Overall execution timeout in seconds.
        dry_run: If True, log the script but don't execute.

    Returns:
        RemoteResult with returncode, stdout, stderr.
    """
    if dry_run:
        logger.info("[dry-run] Would execute with sudo on %s", host)
        return RemoteResult(host=host, returncode=0, stdout="[dry-run]", stderr="")

    cmd = build_ssh_cmd(host, ssh_user=ssh_user, ssh_key=ssh_key, ssh_options=ssh_options)
    cmd.extend(["sudo", "-S", "bash", "-s"])
    full_input = password + "\n" + script

    logger.debug("  SSH sudo script -> %s (%d bytes)", host, len(script))

    result = _run_subprocess(cmd, host, "SSH sudo script", timeout=timeout, input_data=full_input)
    # Upgrade success log to INFO for sudo operations
    if result.success:
        logger.info("  SSH sudo script <- %s OK", host)
    return result


def detect_sudo_on_hosts(
    hosts: list[str],
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    dry_run: bool = False,
) -> set[str]:
    """Detect which hosts have passwordless sudo.

    Runs ``sudo -n true`` on each host in parallel to check whether
    the SSH user can execute sudo commands without a password prompt.

    Args:
        hosts: List of remote hostnames or IPs.
        ssh_user: Optional SSH username.
        ssh_key: Optional path to SSH private key.
        ssh_options: Additional SSH options.
        dry_run: If True, return empty set without executing.

    Returns:
        Set of hostnames that have passwordless (NOPASSWD) sudo.
    """
    if not hosts:
        return set()

    script = 'sudo -n true 2>/dev/null && echo "SUDO_OK=1" || echo "SUDO_OK=0"'
    results = run_remote_scripts_parallel(
        hosts,
        script,
        ssh_user=ssh_user,
        ssh_key=ssh_key,
        ssh_options=ssh_options,
        timeout=15,
        dry_run=dry_run,
    )

    nopasswd_hosts: set[str] = set()
    for r in results:
        if r.success and "SUDO_OK=1" in r.stdout:
            nopasswd_hosts.add(r.host)
            logger.debug("  %s: passwordless sudo available", r.host)
        else:
            logger.debug("  %s: passwordless sudo NOT available", r.host)

    return nopasswd_hosts


def build_ssh_opts_string(
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    connect_timeout: int = 10,
) -> str:
    """Build a flat SSH options string for embedding in bash script templates.

    Unlike :func:`build_ssh_cmd`, this returns a single string of options
    (without the ``ssh`` command or target host) suitable for interpolation
    into shell scripts that construct their own ``ssh`` or ``rsync -e`` calls.

    Args:
        ssh_user: Optional SSH username (not included here — handle in the script).
        ssh_key: Optional path to SSH private key file.
        ssh_options: Additional SSH command-line options.
        connect_timeout: SSH connection timeout in seconds.

    Returns:
        Space-separated options string, e.g.
        ``"-o BatchMode=yes -o ConnectTimeout=10 -i /path/key"``.
    """
    parts = ["-o", "BatchMode=yes", "-o", f"ConnectTimeout={connect_timeout}"]
    if ssh_key:
        parts.extend(["-i", ssh_key])
    if ssh_options:
        parts.extend(ssh_options)
    return args_list_to_shell_str(parts)


def run_pipeline_to_remote(
    host: str,
    local_cmd: str,
    remote_cmd: str,
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    connect_timeout: int = 10,
    timeout: int | None = None,
    dry_run: bool = False,
) -> RemoteResult:
    """Run a shell pipeline that streams data from a local command to a remote command.

    Executes ``{local_cmd} | ssh {host} '{remote_cmd}'`` as a single shell
    pipeline via :func:`subprocess.run`.  Useful for streaming transfers like
    ``docker save img | ssh host 'docker load'``.

    Args:
        host: Remote hostname or IP.
        local_cmd: Command to run locally (producer side of pipe).
        remote_cmd: Command to run on the remote host (consumer side).
        ssh_user: Optional SSH username.
        ssh_key: Optional path to SSH private key.
        ssh_options: Additional SSH options.
        connect_timeout: SSH connection timeout in seconds.
        timeout: Overall execution timeout in seconds.
        dry_run: If True, log the pipeline but don't execute.

    Returns:
        RemoteResult with returncode, stdout, stderr.
    """
    ssh_opts = build_ssh_opts_string(
        ssh_user=ssh_user,
        ssh_key=ssh_key,
        ssh_options=ssh_options,
        connect_timeout=connect_timeout,
    )
    target = f"{ssh_user}@{host}" if ssh_user else host
    pipeline = f"{local_cmd} | ssh {ssh_opts} {quote(target)} {quote(remote_cmd)}"

    if dry_run:
        logger.info("[dry-run] Would run pipeline to %s: %s", host, pipeline)
        return RemoteResult(host=host, returncode=0, stdout="[dry-run]", stderr="")

    logger.info("  Pipeline -> %s%s", host, f" [timeout={timeout}s]" if timeout else "")
    logger.debug("Pipeline command: %s", pipeline)

    result = _run_subprocess(pipeline, host, "Pipeline", timeout=timeout, shell=True)
    if result.success:
        logger.info("  Pipeline <- %s OK", host)
    return result


def _run_rsync_impl(
    source: str,
    dest: str,
    host: str,
    direction: str,
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    connect_timeout: int = 10,
    rsync_options: list[str] | None = None,
    timeout: int | None = None,
    dry_run: bool = False,
) -> RemoteResult:
    """Shared rsync implementation for both push and pull directions.

    Args:
        source: Source path (with trailing ``/`` for directory contents).
        dest: Destination path.
        host: Remote hostname (for logging and result).
        direction: ``"->"`` for push, ``"<-"`` for pull (for log messages).
        ssh_user: Optional SSH username.
        ssh_key: Optional path to SSH private key.
        ssh_options: Additional SSH options.
        connect_timeout: SSH connection timeout in seconds.
        rsync_options: Override rsync flags.
        timeout: Overall execution timeout in seconds.
        dry_run: If True, log the command but don't execute.

    Returns:
        RemoteResult with returncode, stdout, stderr.
    """
    if rsync_options is None:
        rsync_options = list(_DEFAULT_RSYNC_OPTIONS)

    ssh_opts = build_ssh_opts_string(
        ssh_user=ssh_user,
        ssh_key=ssh_key,
        ssh_options=ssh_options,
        connect_timeout=connect_timeout,
    )

    cmd = ["rsync"] + rsync_options + ["-e", f"ssh {ssh_opts}", source, dest]

    if dry_run:
        logger.info("[dry-run] Would rsync %s %s: %s", direction, host, " ".join(cmd))
        return RemoteResult(host=host, returncode=0, stdout="[dry-run]", stderr="")

    logger.info("  Rsync %s %s%s", direction, host, f" [timeout={timeout}s]" if timeout else "")
    logger.debug("Rsync command: %s", " ".join(cmd))

    result = _run_subprocess(cmd, host, "Rsync", timeout=timeout)
    if result.success:
        logger.info("  Rsync %s %s OK", direction, host)
    return result


def run_rsync(
    source_path: str,
    host: str,
    dest_path: str,
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    connect_timeout: int = 10,
    rsync_options: list[str] | None = None,
    timeout: int | None = None,
    dry_run: bool = False,
) -> RemoteResult:
    """Rsync a local path to a remote host.

    Runs ``rsync {rsync_options} -e "ssh {opts}" source user@host:dest``.
    Default *rsync_options* are ``["-az", "--no-times", "--mkpath", "--partial", "--links"]``
    which create the destination path and preserve symlinks (important for
    HuggingFace cache layout).
    """
    src = source_path.rstrip("/") + "/"
    target = f"{ssh_user}@{host}:{dest_path}" if ssh_user else f"{host}:{dest_path}"
    return _run_rsync_impl(
        src,
        target,
        host,
        "->",
        ssh_user=ssh_user,
        ssh_key=ssh_key,
        ssh_options=ssh_options,
        connect_timeout=connect_timeout,
        rsync_options=rsync_options,
        timeout=timeout,
        dry_run=dry_run,
    )


def run_pipeline_to_remote_with_progress(
    host: str,
    local_cmd: list[str],
    remote_cmd: str,
    progress_cb=None,
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    connect_timeout: int = 10,
    timeout: int | None = None,
    chunk_size: int = 1024 * 1024,
    dry_run: bool = False,
) -> RemoteResult:
    """Stream a local producer's stdout into a remote consumer with byte tracking.

    Like :func:`run_pipeline_to_remote` but the byte stream goes
    through Python so a *progress_cb(n_bytes)* can be invoked per
    chunk read.  Used for ``docker save | ssh ... docker load``
    when we need a progress bar.

    Args:
        host: Remote hostname or IP.
        local_cmd: Local producer as a list (e.g. ``["docker", "save", image]``).
        remote_cmd: Command to run on the remote host (consumer side).
        progress_cb: Callable invoked with the size of each chunk
            written to the remote.  Errors in the callback are
            swallowed so they can't break the transfer.
        ssh_user: Optional SSH username.
        ssh_key: Optional path to SSH private key.
        ssh_options: Additional SSH options.
        connect_timeout: SSH connection timeout in seconds.
        timeout: Overall execution timeout in seconds.
        chunk_size: Read/write chunk size in bytes.
        dry_run: If True, log the plan but don't execute.

    Returns:
        RemoteResult with returncode, stdout, stderr.
    """
    if dry_run:
        logger.info("[dry-run] Would pump-pipeline to %s: %s | %s", host, " ".join(local_cmd), remote_cmd)
        return RemoteResult(host=host, returncode=0, stdout="[dry-run]", stderr="")

    ssh_cmd = build_ssh_cmd(host, ssh_user, ssh_key, ssh_options, connect_timeout)
    ssh_cmd.append(remote_cmd)

    logger.info("  Pump-pipeline -> %s%s", host, f" [timeout={timeout}s]" if timeout else "")
    logger.debug("Producer: %s", " ".join(local_cmd))
    logger.debug("SSH consumer: %s", " ".join(ssh_cmd))

    t0 = time.monotonic()
    producer = None
    consumer = None
    stderr_chunks: list[str] = []

    try:
        producer = subprocess.Popen(
            local_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        consumer = subprocess.Popen(
            ssh_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        assert producer.stdout is not None and consumer.stdin is not None
        try:
            while True:
                chunk = producer.stdout.read(chunk_size)
                if not chunk:
                    break
                consumer.stdin.write(chunk)
                if progress_cb is not None:
                    try:
                        progress_cb(len(chunk))
                    except Exception:  # pragma: no cover - defensive
                        pass
        finally:
            try:
                consumer.stdin.close()
            except (BrokenPipeError, OSError):
                pass

        prod_rc = producer.wait(timeout=timeout)
        cons_rc = consumer.wait(timeout=timeout)

        cons_stdout = consumer.stdout.read().decode(errors="replace") if consumer.stdout else ""
        cons_stderr = consumer.stderr.read().decode(errors="replace") if consumer.stderr else ""
        prod_stderr = producer.stderr.read().decode(errors="replace") if producer.stderr else ""
        if prod_stderr:
            stderr_chunks.append(prod_stderr)
        if cons_stderr:
            stderr_chunks.append(cons_stderr)

        elapsed = time.monotonic() - t0
        rc = prod_rc if prod_rc != 0 else cons_rc
        if rc == 0:
            logger.info("  Pump-pipeline <- %s OK (%.1fs)", host, elapsed)
        else:
            logger.warning(
                "  Pump-pipeline <- %s FAILED prod_rc=%d cons_rc=%d (%.1fs): %s",
                host,
                prod_rc,
                cons_rc,
                elapsed,
                "".join(stderr_chunks).strip()[:200],
            )
        return RemoteResult(
            host=host,
            returncode=rc,
            stdout=cons_stdout,
            stderr="".join(stderr_chunks),
        )
    except subprocess.TimeoutExpired:
        elapsed = time.monotonic() - t0
        logger.error("  Pump-pipeline <- %s TIMEOUT after %.0fs", host, elapsed)
        for proc in (producer, consumer):
            if proc is not None:
                try:
                    proc.kill()
                except OSError:
                    pass
        return RemoteResult(host=host, returncode=-1, stdout="", stderr="Execution timed out")
    except Exception as e:
        elapsed = time.monotonic() - t0
        logger.error("  Pump-pipeline <- %s ERROR (%.1fs): %s", host, elapsed, e)
        for proc in (producer, consumer):
            if proc is not None:
                try:
                    proc.kill()
                except OSError:
                    pass
        return RemoteResult(host=host, returncode=-1, stdout="", stderr=str(e))


def run_pipeline_to_remotes_parallel(
    hosts: list[str],
    local_cmd: str,
    remote_cmd: str,
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    connect_timeout: int = 10,
    timeout: int | None = None,
    dry_run: bool = False,
) -> list[RemoteResult]:
    """Run a local-to-remote pipeline on multiple hosts in parallel.

    Wrapper over :func:`run_pipeline_to_remote` using a thread pool,
    matching the pattern of :func:`run_remote_scripts_parallel`.

    Args:
        hosts: List of remote hostnames or IPs.
        local_cmd: Command to run locally (producer side).
        remote_cmd: Command to run on each remote host (consumer side).
        ssh_user: Optional SSH username.
        ssh_key: Optional path to SSH private key.
        ssh_options: Additional SSH options.
        connect_timeout: SSH connection timeout in seconds.
        timeout: Per-host execution timeout in seconds.
        dry_run: If True, log but don't execute.

    Returns:
        List of RemoteResult, one per host.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    logger.info("  Running pipeline in parallel to %d hosts: %s", len(hosts), ", ".join(hosts))

    t0 = time.monotonic()
    results: list[RemoteResult] = []
    with ThreadPoolExecutor(max_workers=len(hosts)) as executor:
        futures = {
            executor.submit(
                run_pipeline_to_remote,
                host,
                local_cmd,
                remote_cmd,
                ssh_user=ssh_user,
                ssh_key=ssh_key,
                ssh_options=ssh_options,
                connect_timeout=connect_timeout,
                timeout=timeout,
                dry_run=dry_run,
            ): host
            for host in hosts
        }
        for future in as_completed(futures):
            results.append(future.result())

    elapsed = time.monotonic() - t0
    ok = sum(1 for r in results if r.success)
    logger.info("  Parallel pipeline done: %d/%d OK (%.1fs total)", ok, len(results), elapsed)
    return results


def run_pipeline_to_remotes_parallel_with_progress(
    hosts: list[str],
    local_cmd: list[str],
    remote_cmd: str,
    progress_cb_factory=None,
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    connect_timeout: int = 10,
    timeout: int | None = None,
    chunk_size: int = 1024 * 1024,
    dry_run: bool = False,
) -> list[RemoteResult]:
    """Pump-pipeline variant of :func:`run_pipeline_to_remotes_parallel`.

    For each host, a fresh producer subprocess (``local_cmd``) is
    started and its stdout is streamed to ``ssh <host> <remote_cmd>``.
    *progress_cb_factory(host)* (when given) returns a per-host
    callback ``cb(n_bytes)`` invoked for each chunk written to that
    host's SSH consumer.

    The producer is *not* shared between hosts — Docker's ``docker save``
    is fast and pumping it twice is simpler than fanning out a single
    stream, especially since the pipe order matters for progress
    accuracy.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    logger.info("  Running pump-pipeline in parallel to %d hosts: %s", len(hosts), ", ".join(hosts))

    t0 = time.monotonic()
    results: list[RemoteResult] = []
    with ThreadPoolExecutor(max_workers=len(hosts)) as executor:
        futures = {
            executor.submit(
                run_pipeline_to_remote_with_progress,
                host,
                local_cmd,
                remote_cmd,
                progress_cb=progress_cb_factory(host) if progress_cb_factory else None,
                ssh_user=ssh_user,
                ssh_key=ssh_key,
                ssh_options=ssh_options,
                connect_timeout=connect_timeout,
                timeout=timeout,
                chunk_size=chunk_size,
                dry_run=dry_run,
            ): host
            for host in hosts
        }
        for future in as_completed(futures):
            results.append(future.result())

    elapsed = time.monotonic() - t0
    ok = sum(1 for r in results if r.success)
    logger.info("  Parallel pump-pipeline done: %d/%d OK (%.1fs total)", ok, len(results), elapsed)
    return results


def run_rsync_from_remote(
    host: str,
    source_path: str,
    dest_path: str,
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    connect_timeout: int = 10,
    rsync_options: list[str] | None = None,
    timeout: int | None = None,
    dry_run: bool = False,
) -> RemoteResult:
    """Rsync a remote path to the local machine.

    Inverse of :func:`run_rsync` — pulls ``user@host:source/`` to local
    *dest_path*.
    """
    remote_src = source_path.rstrip("/") + "/"
    remote = f"{ssh_user}@{host}:{remote_src}" if ssh_user else f"{host}:{remote_src}"
    return _run_rsync_impl(
        remote,
        dest_path,
        host,
        "<-",
        ssh_user=ssh_user,
        ssh_key=ssh_key,
        ssh_options=ssh_options,
        connect_timeout=connect_timeout,
        rsync_options=rsync_options,
        timeout=timeout,
        dry_run=dry_run,
    )


def _run_rsync_streaming(
    source: str,
    dest: str,
    host: str,
    direction: str,
    progress_cb=None,
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    connect_timeout: int = 10,
    rsync_options: list[str] | None = None,
    timeout: int | None = None,
    dry_run: bool = False,
) -> RemoteResult:
    """Run rsync with ``--info=progress2`` and forward progress lines.

    Lines parseable as ``"<bytes> <pct>%"`` are converted to two
    callback signals:

    - ``progress_cb("total", N)``  — declared total (only the first line)
    - ``progress_cb("bytes", N)``  — current cumulative bytes

    Other rsync output is captured and returned in
    :attr:`RemoteResult.stdout`.
    """
    from sparkrun.orchestration.progress_transfer import parse_rsync_progress_line

    if rsync_options is None:
        rsync_options = list(_DEFAULT_RSYNC_OPTIONS)

    # Augment with progress-emitting flags (idempotent if already present).
    extra = ["--info=progress2", "--no-inc-recursive"]
    for opt in extra:
        if opt not in rsync_options:
            rsync_options = rsync_options + [opt]

    ssh_opts = build_ssh_opts_string(
        ssh_user=ssh_user,
        ssh_key=ssh_key,
        ssh_options=ssh_options,
        connect_timeout=connect_timeout,
    )
    cmd = ["rsync"] + rsync_options + ["-e", f"ssh {ssh_opts}", source, dest]

    if dry_run:
        logger.info("[dry-run] Would rsync %s %s: %s", direction, host, " ".join(cmd))
        return RemoteResult(host=host, returncode=0, stdout="[dry-run]", stderr="")

    logger.info("  Rsync (progress) %s %s%s", direction, host, f" [timeout={timeout}s]" if timeout else "")
    logger.debug("Rsync command: %s", " ".join(cmd))

    t0 = time.monotonic()
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
    except Exception as e:
        elapsed = time.monotonic() - t0
        logger.error("  Rsync %s %s ERROR (%.1fs): %s", direction, host, elapsed, e)
        return RemoteResult(host=host, returncode=-1, stdout="", stderr=str(e))

    captured: list[str] = []
    last_bytes = 0
    total_seen = False
    assert proc.stdout is not None
    try:
        # rsync emits progress on stdout when --info=progress2 is set;
        # filenames and a final summary appear there too.  Iterating
        # in unbuffered/line-buffered mode keeps callback latency low.
        for raw in proc.stdout:
            line = raw.rstrip("\r\n")
            captured.append(line)
            parsed = parse_rsync_progress_line(line)
            if parsed is None:
                continue
            cur_bytes, pct = parsed
            if progress_cb is not None:
                # rsync emits progress as cumulative bytes, but the
                # *total* needs to be inferred from cumulative ÷ pct.
                if not total_seen and pct > 0:
                    total = int(cur_bytes * 100 / pct) if pct > 0 else 0
                    if total > 0:
                        try:
                            progress_cb("total", total)
                            total_seen = True
                        except Exception:  # pragma: no cover - defensive
                            pass
                try:
                    progress_cb("bytes", cur_bytes)
                except Exception:  # pragma: no cover - defensive
                    pass
            last_bytes = cur_bytes
    except Exception as e:
        elapsed = time.monotonic() - t0
        logger.error("  Rsync %s %s read error (%.1fs): %s", direction, host, elapsed, e)

    try:
        rc = proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        elapsed = time.monotonic() - t0
        logger.error("  Rsync %s %s TIMEOUT after %.0fs", direction, host, elapsed)
        proc.kill()
        return RemoteResult(host=host, returncode=-1, stdout="", stderr="Execution timed out")

    err = proc.stderr.read() if proc.stderr else ""
    elapsed = time.monotonic() - t0
    if rc == 0:
        logger.info("  Rsync %s %s OK (%.1fs, %d bytes)", direction, host, elapsed, last_bytes)
    else:
        logger.warning(
            "  Rsync %s %s FAILED rc=%d (%.1fs): %s",
            direction,
            host,
            rc,
            elapsed,
            err.strip()[:200],
        )

    return RemoteResult(host=host, returncode=rc, stdout="\n".join(captured), stderr=err)


def run_rsync_with_progress(
    source_path: str,
    host: str,
    dest_path: str,
    progress_cb=None,
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    connect_timeout: int = 10,
    rsync_options: list[str] | None = None,
    timeout: int | None = None,
    dry_run: bool = False,
) -> RemoteResult:
    """Like :func:`run_rsync` but parses ``--info=progress2`` for callbacks."""
    src = source_path.rstrip("/") + "/"
    target = f"{ssh_user}@{host}:{dest_path}" if ssh_user else f"{host}:{dest_path}"
    return _run_rsync_streaming(
        src,
        target,
        host,
        "->",
        progress_cb=progress_cb,
        ssh_user=ssh_user,
        ssh_key=ssh_key,
        ssh_options=ssh_options,
        connect_timeout=connect_timeout,
        rsync_options=rsync_options,
        timeout=timeout,
        dry_run=dry_run,
    )


def run_rsync_parallel_with_progress(
    source_path: str,
    hosts: list[str],
    dest_path: str,
    progress_cb_factory=None,
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    connect_timeout: int = 10,
    rsync_options: list[str] | None = None,
    timeout: int | None = None,
    dry_run: bool = False,
) -> list[RemoteResult]:
    """Parallel rsync to multiple hosts with per-host progress callbacks."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    logger.info("  Running rsync (progress) in parallel to %d hosts: %s", len(hosts), ", ".join(hosts))

    t0 = time.monotonic()
    results: list[RemoteResult] = []
    with ThreadPoolExecutor(max_workers=len(hosts)) as executor:
        futures = {
            executor.submit(
                run_rsync_with_progress,
                source_path,
                host,
                dest_path,
                progress_cb=progress_cb_factory(host) if progress_cb_factory else None,
                ssh_user=ssh_user,
                ssh_key=ssh_key,
                ssh_options=ssh_options,
                connect_timeout=connect_timeout,
                rsync_options=rsync_options,
                timeout=timeout,
                dry_run=dry_run,
            ): host
            for host in hosts
        }
        for future in as_completed(futures):
            results.append(future.result())

    elapsed = time.monotonic() - t0
    ok = sum(1 for r in results if r.success)
    logger.info("  Parallel rsync (progress) done: %d/%d OK (%.1fs total)", ok, len(results), elapsed)
    return results


def run_rsync_parallel(
    source_path: str,
    hosts: list[str],
    dest_path: str,
    ssh_user: str | None = None,
    ssh_key: str | None = None,
    ssh_options: list[str] | None = None,
    connect_timeout: int = 10,
    rsync_options: list[str] | None = None,
    timeout: int | None = None,
    dry_run: bool = False,
) -> list[RemoteResult]:
    """Rsync a local path to multiple hosts in parallel.

    Wrapper over :func:`run_rsync` using a thread pool,
    matching the pattern of :func:`run_remote_scripts_parallel`.

    Args:
        source_path: Local source directory.
        hosts: List of remote hostnames or IPs.
        dest_path: Remote destination directory.
        ssh_user: Optional SSH username.
        ssh_key: Optional path to SSH private key.
        ssh_options: Additional SSH options.
        connect_timeout: SSH connection timeout in seconds.
        rsync_options: Override rsync flags.
        timeout: Per-host execution timeout in seconds.
        dry_run: If True, log but don't execute.

    Returns:
        List of RemoteResult, one per host.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    logger.info("  Running rsync in parallel to %d hosts: %s", len(hosts), ", ".join(hosts))

    t0 = time.monotonic()
    results: list[RemoteResult] = []
    with ThreadPoolExecutor(max_workers=len(hosts)) as executor:
        futures = {
            executor.submit(
                run_rsync,
                source_path,
                host,
                dest_path,
                ssh_user=ssh_user,
                ssh_key=ssh_key,
                ssh_options=ssh_options,
                connect_timeout=connect_timeout,
                rsync_options=rsync_options,
                timeout=timeout,
                dry_run=dry_run,
            ): host
            for host in hosts
        }
        for future in as_completed(futures):
            results.append(future.result())

    elapsed = time.monotonic() - t0
    ok = sum(1 for r in results if r.success)
    logger.info("  Parallel rsync done: %d/%d OK (%.1fs total)", ok, len(results), elapsed)
    return results
