#!/bin/bash
set -uo pipefail

# Distribute an HF model cache directory from this host to target hosts via rsync.
# Emits __SR_PROGRESS markers so the controller can render progress bars.
# Placeholders filled by Python: {model_path}, {targets}, {ssh_opts}, {ssh_user}
#
# Progress strategy:
#   1. Pre-compute source size (TOTAL_BYTES) once. This is the bar denominator.
#   2. Emit a 0/TOTAL marker upfront so the bar can render % immediately.
#   3. Run rsync in the background, capture its PID.
#   4. Heartbeat poller: every 1s read /proc/<rsync-pid>/io wchar and emit
#      a fresh __SR_PROGRESS marker. wchar counts bytes written via syscalls
#      (mostly: rsync sender → SSH socket), which grows whenever rsync ships
#      data, regardless of whether source files came from page cache or disk.
#      This decouples bar updates from rsync's sparse --info=progress2
#      cadence, which can be silent for tens of seconds during file-list
#      build or all-skip runs.
#   5. On rsync success, emit a definitive bytes=TOTAL marker so the bar
#      always lands at 100%, including the zero-transfer case where every
#      file matched via --size-only quick-check.

MODEL_PATH="{model_path}"
TARGETS="{targets}"
SSH_OPTS="{ssh_opts}"
SSH_USER="{ssh_user}"

printf "Distributing model %s to targets: %s\n" "$MODEL_PATH" "$TARGETS"

# Source size — the denominator for every peer's progress bar. `du -sb` walks
# the tree once locally; cheap relative to the rsync that follows.
# Brace doubling: this script is rendered through Python str.format, so
# any literal curly braces (awk programs, bash parameter expansions like
# var:-default) must be doubled to survive rendering. Single-brace placeholders
# (model_path, targets, ssh_opts, ssh_user) are intentional substitutions.
TOTAL_BYTES=$(du -sb "$MODEL_PATH" 2>/dev/null | awk '{{print $1}}')
: "${{TOTAL_BYTES:=0}}"
printf "  source size: %s bytes\n" "$TOTAL_BYTES"

FAILED=0
for TARGET in $TARGETS; do
    if [ -n "$SSH_USER" ]; then
        DEST="$SSH_USER@$TARGET:$MODEL_PATH/"
    else
        DEST="$TARGET:$MODEL_PATH/"
    fi
    printf "  Syncing -> %s ...\n" "$TARGET"

    # Initial 0/TOTAL marker so the bar renders right away with a real
    # denominator instead of "0/0 bytes".
    printf "__SR_PROGRESS host=%s bytes=0 total=%s\n" "$TARGET" "$TOTAL_BYTES" >&2

    # Background rsync. --info=progress2 left in for human-readable rsync
    # output captured by the controller; we no longer parse it (the heartbeat
    # below is more reliable). --size-only because HF blobs are content-
    # addressed by filename+size; -a keeps mtimes for future quick-checks.
    rsync -a --mkpath --partial --links --size-only \
        --info=progress2 \
        -e "ssh $SSH_OPTS" "$MODEL_PATH/" "$DEST" &
    RSYNC_PID=$!

    # Heartbeat poller. Reads /proc/$RSYNC_PID/io once per second and emits
    # a marker using `wchar` (bytes written to syscalls — i.e. rsync's stdout
    # to the SSH child / socket). Caps reported bytes at TOTAL so a momentary
    # over-read doesn't push the bar past 100%.
    (
        while kill -0 "$RSYNC_PID" 2>/dev/null; do
            if [ -r "/proc/$RSYNC_PID/io" ]; then
                bytes=$(awk '/^wchar:/{{print $2; exit}}' "/proc/$RSYNC_PID/io" 2>/dev/null)
                bytes=${{bytes:-0}}
                if [ "$TOTAL_BYTES" -gt 0 ] && [ "$bytes" -gt "$TOTAL_BYTES" ]; then
                    bytes="$TOTAL_BYTES"
                fi
                printf "__SR_PROGRESS host=%s bytes=%s total=%s\n" "$TARGET" "$bytes" "$TOTAL_BYTES" >&2
            fi
            sleep 1
        done
    ) &
    POLLER_PID=$!

    wait "$RSYNC_PID"
    RC=$?
    kill "$POLLER_PID" 2>/dev/null
    wait "$POLLER_PID" 2>/dev/null

    if [ "$RC" -eq 0 ]; then
        # Definitive 100% marker. Required because under --size-only the
        # final wchar is well below TOTAL_BYTES (most files are stat'd, not
        # read or written), so the heartbeat alone never reaches 100%.
        printf "__SR_PROGRESS host=%s bytes=%s total=%s\n" "$TARGET" "$TOTAL_BYTES" "$TOTAL_BYTES" >&2
        printf "  OK: %s\n" "$TARGET"
    else
        printf "  FAILED: %s (rc=%s)\n" "$TARGET" "$RC" >&2
        FAILED=$((FAILED + 1))
    fi
done

if [ "$FAILED" -gt 0 ]; then
    printf "ERROR: %s target(s) failed\n" "$FAILED" >&2
    exit 1
fi
printf "Model distributed successfully to all targets\n"
