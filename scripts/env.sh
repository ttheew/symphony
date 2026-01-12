#!/usr/bin/env bash
set -e

ENV_NAME="symphony"
PYTHON_VERSION="3.11"
REQ_FILE="requirements.txt"

# ---- Ensure conda is available ----
if ! command -v conda >/dev/null 2>&1; then
    echo "conda not found. Please install Miniconda/Anaconda first."
    return 1 2>/dev/null || exit 1
fi

# ---- Init conda for this shell ----
# Needed when script is sourced
eval "$(conda shell.bash hook)"

# ---- Check if env exists ----
if conda env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
    echo "Conda environment '$ENV_NAME' already exists."
else
    echo "Creating conda environment '$ENV_NAME' (Python $PYTHON_VERSION)..."
    conda create -y -n "$ENV_NAME" python="$PYTHON_VERSION"
fi

# ---- Activate env ----
echo "Activating environment '$ENV_NAME'..."
conda activate "$ENV_NAME"

# ---- Install dependencies ----
if [[ -f "$REQ_FILE" ]]; then
    echo "Installing requirements from $REQ_FILE..."
    pip install --upgrade pip
    pip install -r "$REQ_FILE"
else
    echo "No requirements.txt found. Skipping dependency install."
fi

echo "Environment '$ENV_NAME' is ready and active."
