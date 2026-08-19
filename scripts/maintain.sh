#!/usr/bin/env bash
#
# Periodic maintenance for repos backed by @keeb/mongodb-datastore.
#
# Exists because the failure this repo was built around was not a bug in the
# sync protocol — it was drift. `autoGc: true` in .swamp.yaml did NOT keep up:
# a model type declaring `garbageCollection: 10` had accumulated 5,400-7,400
# versions per data-name, 229,598 files under data/, and a 6.8 MB dirty
# sidecar. Nothing enforced retention between runs, so it compounded until
# every push timed out.
#
# Two passes, in order:
#   1. `swamp data gc`  — enforces each model type's declared retention.
#      This is what actually keeps the corpus small; everything else is
#      cleaning up after it.
#   2. `blob-gc`        — reclaims tombstones past grace and blobs no live
#      path references. Only meaningful *after* gc has produced deletions.
#
# Disk is NOT returned to the filesystem by either pass — see --compact.
#
# Usage:
#   scripts/maintain.sh --dry-run                 # report only
#   scripts/maintain.sh --confirm                 # gc + sweep
#   scripts/maintain.sh --confirm --compact       # ...and reclaim disk
#   scripts/maintain.sh --confirm --repos "/a /b" # explicit repo list
#
# Repos default to every checkout under $SWAMP_REPO_ROOT (default ~/git)
# whose .swamp.yaml selects this datastore.

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="${SWAMP_REPO_ROOT:-$HOME/git}"
MODE="dry"
COMPACT=0
REPOS=""

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) MODE="dry" ;;
    --confirm) MODE="confirm" ;;
    --compact) COMPACT=1 ;;
    --repos) REPOS="$2"; shift ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
  shift
done

if [ -z "$REPOS" ]; then
  REPOS="$(
    find "$REPO_ROOT" -maxdepth 3 -name '.swamp.yaml' 2>/dev/null |
      while read -r f; do
        grep -q 'keeb/mongodb-datastore' "$f" 2>/dev/null && dirname "$f"
      done | sort
  )"
fi

if [ -z "$REPOS" ]; then
  echo "no repos found under $REPO_ROOT using @keeb/mongodb-datastore" >&2
  exit 1
fi

echo "mode: $MODE"
echo "repos:"
for r in $REPOS; do echo "  $r"; done
echo

for r in $REPOS; do
  echo "===================================================================="
  echo "== $r"
  echo "===================================================================="

  # A repo with no .env cannot authenticate; skip rather than fail the run.
  if [ ! -f "$r/.env" ]; then
    echo "  SKIP — no .env (cannot authenticate)"
    continue
  fi

  echo "-- swamp data gc"
  if [ "$MODE" = "confirm" ]; then
    ( cd "$r" && swamp data gc --force --json )
  else
    ( cd "$r" && swamp data gc --dry-run --json )
  fi

  echo "-- blob-gc"
  if [ "$MODE" = "confirm" ]; then
    ( cd "$HERE" && deno task blob-gc --repo "$r" --confirm )
  else
    ( cd "$HERE" && deno task blob-gc --repo "$r" --dry-run )
  fi
  echo
done

if [ "$COMPACT" = "1" ]; then
  echo "===================================================================="
  echo "== compact"
  echo "===================================================================="
  # Deleting documents returns space to WiredTiger's free list, not to the
  # filesystem. Without this the cluster still reports its pre-sweep size.
  # `force: true` is required on a replica-set primary and slows concurrent
  # operations, which is why it is opt-in and belongs in a quiet window.
  echo "Run against your primary (adjust namespace list):"
  echo "  mongosh <uri> --eval 'db.getCollectionNames()" \
    ".filter(n => /_(paths|blobs)\$/.test(n))" \
    ".forEach(n => print(n, JSON.stringify(db.runCommand({compact: n, force: true}))))'"
fi
