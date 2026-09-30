#!/bin/sh
# Container entrypoint: generate the input once (in the background), then serve the page.
set -e
mkdir -p "$DATA_DIR/raw"
if [ ! -f "$DATA_DIR/raw/manifest.json" ]; then
  echo "No dataset yet: generating with seed $SEED in the background (log: $DATA_DIR/generate.log)"
  python -m generator.generate --seed "$SEED" --out "$DATA_DIR/raw" --overwrite > "$DATA_DIR/generate.log" 2>&1 &
fi
echo "Open http://localhost:8000"
exec python -m web.app
