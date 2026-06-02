#!/bin/bash
# Setup script for running the CIFS dev-testing scripts.
# Works both inside a K8s pod (Alpine/Debian) and on a local workstation.
# Installs Python3, pip, venv, and the Python dependencies + proto_py package.
#
# Usage:
#   chmod +x setup.sh && ./setup.sh
#   source .venv/bin/activate

set -euo pipefail

echo "=== Ensuring Python3 + venv ==="
if command -v python3 &>/dev/null; then
    echo "python3 already present: $(python3 --version)"
elif command -v apk &>/dev/null; then
    apk add --no-cache python3 py3-pip py3-virtualenv
elif command -v apt-get &>/dev/null; then
    apt-get update && apt-get install -y python3 python3-pip python3-venv
elif command -v dnf &>/dev/null; then
    dnf install -y python3 python3-pip python3-virtualenv
else
    echo "ERROR: Unknown package manager. Install python3 + pip manually."
    exit 1
fi

echo ""
echo "=== Creating virtualenv (.venv) ==="
python3 -m venv .venv
# shellcheck disable=SC1091
source .venv/bin/activate

echo ""
echo "=== Installing Python dependencies ==="
pip install --no-cache-dir -r requirements.txt
# Install the generated protobuf stubs as an editable package.
pip install --no-cache-dir -e ./proto_py

echo ""
echo "=== Setup complete ==="
echo "Activate the venv:  source .venv/bin/activate"
echo "Run scan:           python kickoff.py cifs"
echo "Run augment:        python kickoff.py cifs-augment"
echo "Validate:           python validate.py"
