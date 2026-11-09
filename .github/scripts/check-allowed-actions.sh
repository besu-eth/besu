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

# Fails if a workflow or action under .github uses an action that is not listed in
# .github/allowed-actions.txt, or that is not pinned to a full commit SHA.
#
# Usage: check-allowed-actions.sh [allowlist-file] [dir-to-scan]

set -euo pipefail

allowlist=${1:-.github/allowed-actions.txt}
scan_dir=${2:-.github}

allowed=$(sed -e 's/#.*//' -e 's/[[:space:]]//g' -e '/^$/d' "$allowlist" | sort -u)

status=0
used=""
# file:line:uses: <ref>, for every YAML file under scan_dir
while IFS=: read -r file line ref; do
  ref=$(echo "$ref" | sed -E "s/^[[:space:]-]*uses:[[:space:]]*['\"]?//; s/['\"].*//; s/[[:space:]].*//")
  # Local actions and reusable workflows, and Docker images, are not covered by the allowlist
  case "$ref" in
    ./* | docker://*) continue ;;
  esac
  used="$used$ref"$'\n'
  if ! [[ "$ref" =~ @[0-9a-f]{40}$ ]]; then
    echo "::error file=$file,line=$line::$ref is not pinned to a full commit SHA"
    status=1
  elif ! grep -qxF "$ref" <<<"$allowed"; then
    echo "::error file=$file,line=$line::$ref is not in $allowlist"
    status=1
  fi
done < <(grep -rnE --include='*.yml' --include='*.yaml' "^[[:space:]-]*uses:" "$scan_dir")

# Entries that only other branches use are expected, so this is informational
unused=$(comm -23 <(echo "$allowed") <(echo -n "$used" | sort -u))
if [ -n "$unused" ]; then
  echo "Listed in $allowlist but not used on this branch:"
  echo "$unused" | sed 's/^/  /'
fi

if [ "$status" -eq 0 ]; then
  echo "All actions used under $scan_dir are listed in $allowlist"
else
  echo "Add missing actions to $allowlist; a maintainer then applies it with .github/scripts/sync-allowed-actions.sh"
fi
exit $status
