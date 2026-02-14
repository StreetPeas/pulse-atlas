#!/bin/zsh
cd /Users/johnnymahony/Projects/atlas || exit 1

/usr/bin/python3 fetch_rss.py >> data/logs/rss.log 2>&1
/usr/bin/python3 analyze_signal.py >> data/logs/analyze.log 2>&1
