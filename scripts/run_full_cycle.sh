#!/usr/bin/env bash
set -euo pipefail
cd /Users/ethanheinrick/.openclaw/workspace/bait-engine-v2
export PYTHONPATH=src
MODEL="qwen3.5:4b"
BASE_URL="http://localhost:11434/v1"

/opt/homebrew/opt/python@3.14/bin/python3.14 -m bait_engine.cli.main hunt-cycle reddit_listing \
  --subreddit changemyview --sort hot --limit 20 \
  --persona auto --model "$MODEL" --base-url "$BASE_URL" \
  --stage-emit --approve-emit --dispatch-approved --dispatch-limit 5 \
  --notes "scheduled-8x" || true

/opt/homebrew/opt/python@3.14/bin/python3.14 -m bait_engine.cli.main hunt-cycle reddit_listing \
  --subreddit unpopularopinion --sort hot --limit 20 \
  --persona auto --model "$MODEL" --base-url "$BASE_URL" \
  --stage-emit --approve-emit --dispatch-approved --dispatch-limit 5 \
  --notes "scheduled-8x" || true

if [[ -f "$HOME/Desktop/BAITED_RETARDS/feeds/azfamily.jsonl" ]]; then
  /opt/homebrew/opt/python@3.14/bin/python3.14 -m bait_engine.cli.main hunt-cycle jsonl_file \
    --file-path "$HOME/Desktop/BAITED_RETARDS/feeds/azfamily.jsonl" \
    --persona auto --model "$MODEL" --base-url "$BASE_URL" \
    --stage-emit --approve-emit --dispatch-approved --dispatch-limit 5 \
    --notes "scheduled-8x-azfamily" || true
fi

if [[ -f "$HOME/Desktop/BAITED_RETARDS/feeds/livingston_rants_raves.jsonl" ]]; then
  /opt/homebrew/opt/python@3.14/bin/python3.14 -m bait_engine.cli.main hunt-cycle jsonl_file \
    --file-path "$HOME/Desktop/BAITED_RETARDS/feeds/livingston_rants_raves.jsonl" \
    --persona auto --model "$MODEL" --base-url "$BASE_URL" \
    --stage-emit --approve-emit --dispatch-approved --dispatch-limit 5 \
    --notes "scheduled-8x-livingston" || true
fi
