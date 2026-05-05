#!/bin/bash
set -uo pipefail

# Distribute a Docker image from this host to target hosts via docker save/load.
# Emits __SR_PROGRESS markers so the controller can render progress bars.
# Placeholders filled by Python: {image}, {targets}, {ssh_opts}, {ssh_user}

IMAGE="{image}"
TARGETS="{targets}"
SSH_OPTS="{ssh_opts}"
SSH_USER="{ssh_user}"

# Determine total bytes for progress display (best-effort).
SIZE=$(docker image inspect --format '{{{{.Size}}}}' "$IMAGE" 2>/dev/null || echo 0)

printf "Distributing image %s (%s bytes) to targets: %s\n" "$IMAGE" "$SIZE" "$TARGETS"

FAILED=0
for TARGET in $TARGETS; do
    if [ -n "$SSH_USER" ]; then
        DEST="$SSH_USER@$TARGET"
    else
        DEST="$TARGET"
    fi
    printf "  Sending %s -> %s ...\n" "$IMAGE" "$TARGET"
    # Pipe save -> python byte counter -> ssh load.
    # The byte counter emits __SR_PROGRESS markers on stderr (which
    # the controller reads via merged stdout+stderr).
    if docker save "$IMAGE" \
        | SR_TARGET="$TARGET" SR_TOTAL="$SIZE" python3 -u -c '
import os, sys, time
host = os.environ["SR_TARGET"]
total = os.environ.get("SR_TOTAL", "0")
n = 0
last = time.monotonic()
while True:
    chunk = sys.stdin.buffer.read(1024 * 1024)
    if not chunk:
        break
    sys.stdout.buffer.write(chunk)
    n += len(chunk)
    now = time.monotonic()
    if now - last >= 1.0:
        sys.stderr.write(f"__SR_PROGRESS host={{host}} bytes={{n}} total={{total}}\n")
        sys.stderr.flush()
        last = now
sys.stderr.write(f"__SR_PROGRESS host={{host}} bytes={{n}} total={{total}}\n")
sys.stderr.flush()
' \
        | ssh $SSH_OPTS "$DEST" 'docker load'; then
        printf "  OK: %s\n" "$TARGET"
    else
        printf "  FAILED: %s\n" "$TARGET" >&2
        FAILED=$((FAILED + 1))
    fi
done

if [ "$FAILED" -gt 0 ]; then
    printf "ERROR: %s target(s) failed\n" "$FAILED" >&2
    exit 1
fi
printf "Image distributed successfully to all targets\n"
