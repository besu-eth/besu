#!/usr/bin/env python3
# Copyright contributors to Besu.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# SPDX-License-Identifier: Apache-2.0
"""
Promote Besu Maven artifacts from a staging Artifactory repo to production.

Used by publish-release.yml to avoid rebuilding artifacts that were already
validated during RC burn-in. Mirrors the Docker retag approach: move, don't
rebuild.

Usage:
    python3 promote_artifacts.py \\
        --besu_version 26.5.0 \\
        --src_repo besu-maven-rc \\
        --dst_repo besu-maven

Credentials are read from ARTIFACTORY_USER and ARTIFACTORY_KEY env vars.
"""

import argparse
import os
import sys
import requests

ARTIFACTORY_BASE = "https://hyperledger.jfrog.io/hyperledger"

# Top-level Maven group paths that contain Besu artifacts.
# Internal artifacts live one level deeper (org/hyperledger/besu/internal).
GROUP_PATHS = [
    "org/hyperledger/besu",
]


def artifactory_session(user: str, key: str) -> requests.Session:
    s = requests.Session()
    s.auth = (user, key)
    return s


def list_modules(session: requests.Session, repo: str, group_path: str) -> list[str]:
    """Return all direct child folder names under group_path in repo."""
    url = f"{ARTIFACTORY_BASE}/api/storage/{repo}/{group_path}"
    r = session.get(url, params={"list": None, "deep": 0, "listFolders": 1})
    if r.status_code == 404:
        return []
    r.raise_for_status()
    return [f["uri"].lstrip("/") for f in r.json().get("files", [])]


def version_exists(session: requests.Session, repo: str, path: str) -> bool:
    """Return True if an artifact folder exists at path in repo."""
    url = f"{ARTIFACTORY_BASE}/api/storage/{repo}/{path}"
    return session.get(url).status_code == 200


def copy_folder(
    session: requests.Session, src_repo: str, src_path: str, dst_repo: str, dst_path: str
) -> None:
    """Copy a folder from src to dst using the Artifactory copy REST API."""
    url = f"{ARTIFACTORY_BASE}/api/copy/{src_repo}/{src_path}"
    r = session.post(url, params={"to": f"/{dst_repo}/{dst_path}", "failFast": 1, "overwrite": 0})
    if r.status_code == 400 and "exists" in r.text.lower():
        print(f"  Already exists, skipping: {dst_repo}/{dst_path}")
        return
    r.raise_for_status()
    print(f"  Promoted: {src_repo}/{src_path} -> {dst_repo}/{dst_path}")


def promote(version: str, src_repo: str, dst_repo: str) -> None:
    user = os.environ.get("ARTIFACTORY_USER")
    key = os.environ.get("ARTIFACTORY_KEY")
    if not user or not key:
        print("ERROR: ARTIFACTORY_USER and ARTIFACTORY_KEY must be set.", file=sys.stderr)
        sys.exit(1)

    session = artifactory_session(user, key)
    errors: list[str] = []

    for group_path in GROUP_PATHS:
        print(f"Scanning {src_repo}/{group_path} for version {version} ...")
        modules = list_modules(session, src_repo, group_path)
        if not modules:
            print(f"  No modules found under {group_path} in {src_repo} — "
                  "was draft-release.yml run with -RC tag?")
            errors.append(f"No modules found under {group_path}")
            continue

        for module in modules:
            # Recurse one level into 'internal' sub-group
            if module == "internal":
                sub_modules = list_modules(session, src_repo, f"{group_path}/internal")
                for sub in sub_modules:
                    _promote_module(
                        session, version, src_repo, dst_repo,
                        f"{group_path}/internal/{sub}", errors
                    )
            else:
                _promote_module(
                    session, version, src_repo, dst_repo,
                    f"{group_path}/{module}", errors
                )

    if errors:
        print("\nERRORS encountered during promotion:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        sys.exit(1)

    print(f"\nPromotion complete: {src_repo} -> {dst_repo} for version {version}")


def _promote_module(
    session: requests.Session,
    version: str,
    src_repo: str,
    dst_repo: str,
    module_path: str,
    errors: list[str],
) -> None:
    src_version_path = f"{module_path}/{version}"
    if not version_exists(session, src_repo, src_version_path):
        # Module exists but has no folder for this version — expected for
        # modules that are version-independent or were not part of this build.
        return
    try:
        copy_folder(session, src_repo, src_version_path, dst_repo, src_version_path)
    except requests.HTTPError as e:
        msg = f"Failed to promote {src_repo}/{src_version_path}: {e}"
        print(f"  ERROR: {msg}", file=sys.stderr)
        errors.append(msg)


def main() -> None:
    parser = argparse.ArgumentParser(description="Promote Besu Maven artifacts in Artifactory")
    parser.add_argument("--besu_version", required=True, help="Release version, e.g. 26.5.0")
    parser.add_argument("--src_repo", default="besu-maven-rc", help="Source (staging) repo key")
    parser.add_argument("--dst_repo", default="besu-maven", help="Destination (production) repo key")
    args = parser.parse_args()

    promote(args.besu_version, args.src_repo, args.dst_repo)


if __name__ == "__main__":
    main()
