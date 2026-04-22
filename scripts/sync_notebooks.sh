#!/usr/bin/env bash
# Sync the src/new_data_impact/ notebook track into the user-facing Workspace
# folder so data scientists / actuaries can open and run the notebooks directly.
#
# Destination: /Users/<you>@databricks.com/pricing-workbench/new_data_impact/
#
# Run this after editing any notebook locally. The bundle deploy also syncs
# the notebooks, but to the hidden .bundle/ folder — this puts them where
# users can actually find them.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
USER_EMAIL="$(databricks current-user me --output json | python3 -c 'import json,sys; print(json.load(sys.stdin)["userName"])')"
DEST="/Users/${USER_EMAIL}/pricing-workbench/new_data_impact"

echo "Syncing notebooks from ${REPO_ROOT}/src/new_data_impact"
echo "                  to ${DEST}"

databricks workspace mkdirs "/Users/${USER_EMAIL}/pricing-workbench" 2>/dev/null || true
databricks workspace mkdirs "${DEST}" 2>/dev/null || true

databricks workspace import-dir \
  "${REPO_ROOT}/src/new_data_impact" \
  "${DEST}" \
  --overwrite

echo ""
echo "Done. Open the folder in the Workspace UI:"
echo "  ${DEST}"
