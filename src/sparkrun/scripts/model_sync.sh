#!/bin/bash
set -uo pipefail
echo "Checking model cache for {model_id}..."
# HuggingFace cache uses DOUBLE dashes between org and repo, e.g.
# "models--MiniMaxAI--MiniMax-M2.7". `tr '/' '--'` silently emits a single
# dash because tr is character-for-character. Use sed for the multi-char
# replacement.
SAFE_NAME=$(echo "{model_id}" | sed 's|/|--|g')
CACHE_PATH="{cache}/hub/models--$SAFE_NAME"

# Check for actual weight files (not just config.json from VRAM auto-detect)
FOUND_WEIGHTS=false
if [ -d "$CACHE_PATH/snapshots" ]; then
    for pattern in "*.safetensors" "*.bin" "*.pt" "*.gguf"; do
        if find "$CACHE_PATH/snapshots" -name "$pattern" -print -quit 2>/dev/null | grep -q .; then
            FOUND_WEIGHTS=true
            break
        fi
    done
fi

if [ "$FOUND_WEIGHTS" = true ]; then
    echo "Model already cached: {model_id}"
    exit 0
fi

echo "Downloading model: {model_id}..."
# Priority: `hf` (current huggingface_hub CLI) > deprecated `huggingface-cli`
# > uvx-installed `hf`. Recent huggingface_hub releases removed
# `huggingface-cli download` entirely (the binary still exists but errors
# with "deprecated and no longer works"), so we prefer `hf` when present
# and only fall back to `huggingface-cli` for very old containers.
if command -v hf &>/dev/null; then
    hf download "{model_id}" {revision_flag}--cache-dir "{cache}/hub"
elif command -v huggingface-cli &>/dev/null; then
    huggingface-cli download "{model_id}" {revision_flag}--cache-dir "{cache}/hub"
elif command -v uvx &>/dev/null; then
    uvx hf download "{model_id}" {revision_flag}--cache-dir "{cache}/hub"
else
    echo "Installing uv for model download access..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
    if command -v uvx &>/dev/null; then
        uvx hf download "{model_id}" {revision_flag}--cache-dir "{cache}/hub"
    else
        echo "ERROR: failed to install uv; cannot download model on this host" >&2
        exit 1
    fi
fi
