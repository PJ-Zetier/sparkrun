#!/bin/bash
set -uo pipefail
echo "Checking GGUF model cache for {repo_id} (quant: {quant})..."
# HuggingFace cache uses DOUBLE dashes between org and repo. `tr '/' '--'`
# silently emits a single dash; use sed for the multi-char replacement.
SAFE_NAME=$(echo "{repo_id}" | sed 's|/|--|g')
CACHE_PATH="{cache}/hub/models--$SAFE_NAME"

# Check if GGUF file matching quant already exists
if [ -d "$CACHE_PATH/snapshots" ]; then
    MATCH=$(find "$CACHE_PATH/snapshots" -name "*{quant}*.gguf" -print -quit 2>/dev/null)
    if [ -n "$MATCH" ]; then
        echo "GGUF model already cached: $MATCH"
        exit 0
    fi
fi

echo "Downloading GGUF model: {repo_id} (quant: {quant})..."
# Priority: `hf` > deprecated `huggingface-cli` > uvx-installed `hf`.
if command -v hf &>/dev/null; then
    hf download "{repo_id}" --include "*{quant}*" {revision_flag}--cache-dir "{cache}/hub"
elif command -v huggingface-cli &>/dev/null; then
    huggingface-cli download "{repo_id}" --include "*{quant}*" {revision_flag}--cache-dir "{cache}/hub"
elif command -v uvx &>/dev/null; then
    uvx hf download "{repo_id}" --include "*{quant}*" {revision_flag}--cache-dir "{cache}/hub"
else
    echo "Installing uv for model download access..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
    if command -v uvx &>/dev/null; then
        uvx hf download "{repo_id}" --include "*{quant}*" {revision_flag}--cache-dir "{cache}/hub"
    else
        echo "ERROR: failed to install uv; cannot download model on this host" >&2
        exit 1
    fi
fi
