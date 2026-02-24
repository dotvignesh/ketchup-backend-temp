#!/usr/bin/env bash
set -euo pipefail

echo "Initializing DVC workspace for ketchup-backend..."

if ! command -v dvc >/dev/null 2>&1; then
  echo "dvc not found. Install with: pip install dvc (add dvc-gs only if using GCS remote)"
  exit 1
fi

if command -v python >/dev/null 2>&1; then
  if ! python - <<'PY'
from pathspec.patterns import gitwildmatch
raise SystemExit(0 if hasattr(gitwildmatch, "_DIR_MARK") else 1)
PY
  then
    echo "Detected incompatible pathspec for dvc (missing _DIR_MARK)."
    echo "Re-pinning pathspec to 0.11.2..."
    if command -v pip >/dev/null 2>&1; then
      pip install --upgrade "pathspec==0.11.2"
    elif command -v uv >/dev/null 2>&1; then
      uv pip install --upgrade "pathspec==0.11.2"
    else
      echo "Neither pip nor uv found to repair pathspec. Install pathspec==0.11.2 manually."
      exit 1
    fi
  fi
fi

if ! DVC_VERSION_OUTPUT="$(dvc --version 2>&1)"; then
  echo "dvc is installed but failed to start."
  echo "$DVC_VERSION_OUTPUT"
  if echo "$DVC_VERSION_OUTPUT" | grep -q "_DIR_MARK"; then
    echo
    echo "Detected dvc/pathspec incompatibility."
    echo "Fix with one of:"
    echo "  pip install --upgrade 'pathspec==0.11.2'"
    echo "  uv pip install --upgrade 'pathspec==0.11.2'"
  fi
  exit 1
fi

echo "Using dvc ${DVC_VERSION_OUTPUT}"

if [ ! -d ".dvc" ]; then
  dvc init
fi

mkdir -p \
  data/raw \
  data/processed \
  data/metrics \
  data/reports \
  data/statistics \
  data/analysis/plots

for path in \
  data/raw/.gitkeep \
  data/processed/.gitkeep \
  data/metrics/.gitkeep \
  data/reports/.gitkeep \
  data/statistics/.gitkeep \
  data/analysis/plots/.gitkeep; do
  touch "$path"
done

echo "Done."
echo "Next steps:"
echo "  1) Configure remote: dvc remote add -d myremote gs://<bucket>/<prefix>"
echo "  2) Run pipeline: dvc repro"
echo "  3) Push artifacts: dvc push"
