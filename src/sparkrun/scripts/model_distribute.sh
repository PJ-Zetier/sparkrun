#!/bin/bash
set -uo pipefail

# Distribute an HF model cache directory from this host to target hosts via rsync.
# Emits __SR_PROGRESS markers so the controller can render progress bars.
# Placeholders filled by Python: {model_path}, {targets}, {ssh_opts}, {ssh_user}

MODEL_PATH="{model_path}"
TARGETS="{targets}"
SSH_OPTS="{ssh_opts}"
SSH_USER="{ssh_user}"

printf "Distributing model %s to targets: %s\n" "$MODEL_PATH" "$TARGETS"

FAILED=0
for TARGET in $TARGETS; do
    if [ -n "$SSH_USER" ]; then
        DEST="$SSH_USER@$TARGET:$MODEL_PATH/"
    else
        DEST="$TARGET:$MODEL_PATH/"
    fi
    printf "  Syncing %s -> %s ...\n" "$MODEL_PATH" "$TARGET"
    # Emit an initial 0-byte marker so the bar renders immediately —
    # rsync's file-list scan can take 5-30s for large model caches
    # before it produces the first --info=progress2 line.
    printf "__SR_PROGRESS host=%s bytes=0\n" "$TARGET" >&2
    printf "  rsync: scanning files (may take a moment for large caches)...\n"
    # rsync --info=progress2 emits cumulative-bytes lines.  Pipe them
    # through python to convert to __SR_PROGRESS markers tagged with
    # the target host so the controller can route updates.
    if rsync -az --no-times --mkpath --partial --links \
        --info=progress2 --no-inc-recursive \
        -e "ssh $SSH_OPTS" "$MODEL_PATH/" "$DEST" \
        | SR_TARGET="$TARGET" python3 -u -c '
import os, re, sys
host = os.environ["SR_TARGET"]
pat = re.compile(r"^\s*([\d,]+)\s+(\d+)%")
total_seen = False
for line in sys.stdin:
    line = line.rstrip("\r\n")
    sys.stdout.write(line + "\n")
    sys.stdout.flush()
    m = pat.match(line)
    if not m:
        continue
    cur = int(m.group(1).replace(",", ""))
    pct = int(m.group(2))
    if not total_seen and pct > 0:
        total = int(cur * 100 / pct)
        sys.stderr.write(f"__SR_PROGRESS host={{host}} bytes={{cur}} total={{total}}\n")
        total_seen = True
    else:
        sys.stderr.write(f"__SR_PROGRESS host={{host}} bytes={{cur}}\n")
    sys.stderr.flush()
'; then
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
printf "Model distributed successfully to all targets\n"
