#!/bin/bash
# Wrapper for launchd — starts the Prospecting Toolkit web server.
# exec replaces the shell with the Python process so launchd tracks
# the real server PID and KeepAlive restarts it correctly on exit.
cd /Users/nick/Prospecting || exit 1

LOG_DIR="/Users/nick/Prospecting/logs"
mkdir -p "$LOG_DIR"

exec /Users/nick/Prospecting/.venv/bin/python server.py --no-reload
