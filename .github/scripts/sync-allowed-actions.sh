#!/usr/bin/env bash
##
## Copyright contributors to Besu.
##
## Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
## the License. You may obtain a copy of the License at
##
## http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
## an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
## specific language governing permissions and limitations under the License.
##
## SPDX-License-Identifier: Apache-2.0
##

# Applies .github/allowed-actions.txt to the repository's "Allow specified actions" setting.
# Prints the changes and only writes them with --apply. Needs gh, logged in as a repository
# admin, and jq.
#
# Usage: sync-allowed-actions.sh [--mode add|exact] [--apply] [allowlist-file]
#
#   --mode add    (default) add the file's entries to the live list and remove nothing. Use it to
#                 allow the actions an approved PR introduces, so that its workflows can run
#                 before it merges.
#   --mode exact  make the live list match the file, removing anything not listed. Use it with
#                 the file from main.
#
# Set REPO to target a repository other than besu-eth/besu.

set -euo pipefail

repo=${REPO:-besu-eth/besu}
mode=add
apply=false
allowlist=.github/allowed-actions.txt

while [ $# -gt 0 ]; do
  case "$1" in
    --mode) mode=$2; shift 2 ;;
    --apply) apply=true; shift ;;
    -h | --help) sed -n '/^# Applies/,/^# Set REPO/p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*) echo "Unknown option: $1" >&2; exit 2 ;;
    *) allowlist=$1; shift ;;
  esac
done
case "$mode" in
  add | exact) ;;
  *) echo "--mode must be add or exact" >&2; exit 2 ;;
esac

wanted=$(sed -e 's/#.*//' -e 's/[[:space:]]//g' -e '/^$/d' "$allowlist" | sort -u)
invalid=$(grep -vE '^[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+@[0-9a-f]{40}$' <<<"$wanted" || true)
if [ -n "$invalid" ]; then
  echo "Entries in $allowlist must be owner/repo[/path]@<full commit SHA>:" >&2
  echo "$invalid" >&2
  exit 1
fi

allowed_actions=$(gh api "repos/$repo/actions/permissions" --jq .allowed_actions)
if [ "$allowed_actions" != "selected" ]; then
  echo "$repo allows '$allowed_actions' actions, not 'selected'; refusing to change the list" >&2
  exit 1
fi
current_json=$(gh api "repos/$repo/actions/permissions/selected-actions")
current=$(jq -r '.patterns_allowed[]' <<<"$current_json" | sort -u)

if [ "$mode" = exact ]; then
  target=$wanted
else
  target=$(printf '%s\n%s\n' "$current" "$wanted" | sed '/^$/d' | sort -u)
fi

added=$(comm -13 <(sed '/^$/d' <<<"$current") <(echo "$target"))
removed=$(comm -23 <(sed '/^$/d' <<<"$current") <(echo "$target"))
[ -n "$added" ] && echo "$added" | sed 's/^/+ /'
[ -n "$removed" ] && echo "$removed" | sed 's/^/- /'
if [ -z "$added" ] && [ -z "$removed" ]; then
  echo "$repo already matches $allowlist (mode: $mode)"
  exit 0
fi

if [ "$apply" != true ]; then
  echo "Dry run; rerun with --apply to update $repo"
  exit 0
fi

# PUT replaces the whole setting, so carry over the flags this file doesn't manage
jq -n --argjson current "$current_json" --arg patterns "$target" \
  '{github_owned_allowed: $current.github_owned_allowed,
    verified_allowed: $current.verified_allowed,
    patterns_allowed: ($patterns | split("\n") | map(select(length > 0)))}' |
  gh api -X PUT "repos/$repo/actions/permissions/selected-actions" --input - >/dev/null
echo "Updated $repo"
