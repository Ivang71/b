#!/usr/bin/env bash
set -euo pipefail

NAME="Your Name"
EMAIL="you@example.com"
MSG="${1:-update}"

git rev-parse --is-inside-work-tree >/dev/null

if git diff --cached --quiet; then
  echo "nothing to commit"
  exit 0
fi

GIT_AUTHOR_NAME="$NAME" GIT_AUTHOR_EMAIL="$EMAIL" GIT_COMMITTER_NAME="$NAME" GIT_COMMITTER_EMAIL="$EMAIL" \
  git commit -m "$MSG"
