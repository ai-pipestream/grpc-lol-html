#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Self-contained runner for the Python demo client: creates a virtualenv,
# installs dependencies, generates the gRPC stubs from ../../proto (the single
# source of truth), and runs the client.
#
#   ./run.sh <file.html> [client.py options]

set -euo pipefail
cd "$(dirname "$0")"

if [ ! -d .venv ]; then
    python3 -m venv .venv
    .venv/bin/pip install --quiet --upgrade pip
    .venv/bin/pip install --quiet -r requirements.txt
fi

# Regenerated every run, deliberately.
#
# The obvious optimisation is to skip this when the .proto files are older
# than the generated code. Do not: the generated stubs are gitignored, so a
# checkout with a stale `gen/` produces a client that silently ignores any
# oneof arm added since. `WhichOneof` returns None for a variant the stubs do
# not know about, so the event vanishes with no error anywhere. Regenerating
# costs about a second and removes the whole failure mode.
mkdir -p gen
touch gen/__init__.py
.venv/bin/python -m grpc_tools.protoc \
    -I ../../proto \
    --python_out=gen --grpc_python_out=gen --pyi_out=gen \
    lolhtml/v1/types.proto lolhtml/v1/lolhtml_service.proto
touch gen/lolhtml/__init__.py gen/lolhtml/v1/__init__.py

exec .venv/bin/python client.py "$@"
