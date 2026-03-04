#!/bin/bash
# Wrapper for launchd — runs meeting_prep.py on weekdays only.

# Skip weekends (0=Sunday, 6=Saturday)
DOW=$(date +%w)
if [ "$DOW" -eq 0 ] || [ "$DOW" -eq 6 ]; then
    echo "$(date): Weekend — skipping."
    exit 0
fi

cd /Users/nick/Prospecting || exit 1

LOG_DIR="/Users/nick/Prospecting/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/meeting_prep_$(date +%Y-%m-%d).log"

echo "$(date): Starting meeting prep" >> "$LOG_FILE"

/Users/nick/Prospecting/.venv/bin/python meeting_prep.py --days 1 --email >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "$(date): Finished (exit $EXIT_CODE)" >> "$LOG_FILE"
exit $EXIT_CODE
