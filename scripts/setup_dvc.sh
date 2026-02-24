#!/usr/bin/env bash
set -euo pipefail

echo "Initializing DVC workspace for ketchup-backend..."

if ! command -v dvc >/dev/null 2>&1; then
  echo "dvc not found. Install with: pip install dvc (add dvc-gs only if using GCS remote)"
  exit 1
fi

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
