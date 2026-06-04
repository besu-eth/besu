#!/usr/bin/env python3
"""
Rotate CHANGELOG.md after a release.

Usage:
    python3 scripts/rotate-changelog.py <version> [--repo owner/repo] [--dry-run]

Example:
    python3 scripts/rotate-changelog.py 26.6.0

What it does:
  1. Fetches CHANGELOG.md at the release tag from GitHub
  2. Fetches CHANGELOG.md from the release branch (release-<major>.<minor>.x)
  3. Reads the local CHANGELOG.md (current main)
  4. Identifies post-tag entries  = main Unreleased minus tag Unreleased
  5. Identifies burnin entries    = release branch Unreleased minus tag Unreleased
  6. Writes:
     - New ## Unreleased: all Upcoming Breaking Changes from the released version
       (they are still upcoming) + any truly new post-tag entries
     - ## <version>: the tag Unreleased content + any burnin entries from release branch
     - Remainder of file unchanged (26.5.0 and older)

Requires: gh CLI (authenticated).
Run from the repo root.
"""

import argparse
import base64
import subprocess
import sys


# The subsection headers we recognise, in order.
SUBSECTIONS = [
    "### Breaking Changes",
    "### Upcoming Breaking Changes",
    "### Performance",
    "### Bug fixes",
    "### Additions and Improvements",
]


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

def gh_fetch_changelog(ref: str, repo: str) -> str | None:
    result = subprocess.run(
        ["gh", "api", f"repos/{repo}/contents/CHANGELOG.md?ref={ref}", "--jq", ".content"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        return None
    return base64.b64decode(result.stdout.strip()).decode("utf-8")


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_unreleased(text: str) -> dict[str, list[str]]:
    """
    Return a dict mapping subsection header → list of bullet strings.
    Each bullet string is the full text of one list item (including any
    indented continuation lines), with a trailing newline stripped.
    """
    sections: dict[str, list[str]] = {h: [] for h in SUBSECTIONS}
    current_sub: str | None = None
    current_bullet: str | None = None
    in_unreleased = False

    for line in text.splitlines():
        # Enter the Unreleased block
        if line.startswith("## Unreleased"):
            in_unreleased = True
            continue
        # Leave the Unreleased block at the next ## heading
        if in_unreleased and line.startswith("## "):
            break
        if not in_unreleased:
            continue

        # Subsection header
        if line in SUBSECTIONS:
            if current_bullet is not None and current_sub is not None:
                sections[current_sub].append(current_bullet)
                current_bullet = None
            current_sub = line
            continue

        if current_sub is None:
            continue

        if line.startswith("- "):
            # New top-level bullet
            if current_bullet is not None:
                sections[current_sub].append(current_bullet)
            current_bullet = line
        elif (line.startswith("  ") or line.startswith("\t")) and current_bullet is not None:
            # Continuation / nested line
            current_bullet += "\n" + line
        else:
            # Blank line or unrecognised — flush current bullet
            if current_bullet is not None:
                sections[current_sub].append(current_bullet)
                current_bullet = None

    if current_bullet is not None and current_sub is not None:
        sections[current_sub].append(current_bullet)

    return sections


def get_remainder(text: str) -> str:
    """Everything from the first ## X.Y.Z heading onwards (skips ## Unreleased)."""
    lines = text.splitlines(keepends=True)
    collecting = False
    result: list[str] = []
    for line in lines:
        if not collecting:
            if line.startswith("## ") and not line.startswith("## Unreleased"):
                collecting = True
                result.append(line)
        else:
            result.append(line)
    return "".join(result)


# ---------------------------------------------------------------------------
# Diffing
# ---------------------------------------------------------------------------

def new_bullets(base: dict[str, list[str]], updated: dict[str, list[str]]) -> dict[str, list[str]]:
    """Return bullets present in `updated` but not in `base`, per subsection."""
    result: dict[str, list[str]] = {}
    for sub in SUBSECTIONS:
        base_set = set(base.get(sub, []))
        result[sub] = [b for b in updated.get(sub, []) if b not in base_set]
    return result


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def render_unreleased(
    released_sections: dict[str, list[str]],
    post_tag: dict[str, list[str]],
) -> str:
    """
    Build the new ## Unreleased block.

    - Breaking Changes: only genuinely new post-tag entries.
    - Upcoming Breaking Changes: carry ALL entries forward from the released
      version (they are still upcoming) plus any new post-tag ones.
    - Other sections: only new post-tag entries.
    """
    lines = ["## Unreleased", ""]

    for sub in SUBSECTIONS:
        if sub == "### Upcoming Breaking Changes":
            # Carry forward all from the released version + new post-tag ones
            carried = released_sections.get(sub, [])
            new = [b for b in post_tag.get(sub, []) if b not in set(carried)]
            bullets = carried + new
        else:
            bullets = post_tag.get(sub, [])

        lines.append(sub)
        for b in bullets:
            lines.append(b)
        lines.append("")

    return "\n".join(lines)


def render_version(
    version: str,
    tag_sections: dict[str, list[str]],
    burnin: dict[str, list[str]],
) -> str:
    """Build the ## <version> block."""
    lines = [f"## {version}", ""]

    for sub in SUBSECTIONS:
        bullets = tag_sections.get(sub, []) + burnin.get(sub, [])
        if not bullets:
            continue
        lines.append(sub)
        for b in bullets:
            lines.append(b)
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("version", help="Release version, e.g. 26.6.0")
    parser.add_argument("--repo", default="besu-eth/besu")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the new CHANGELOG to stdout instead of writing it")
    args = parser.parse_args()

    version = args.version
    repo = args.repo
    major, minor, *_ = version.split(".")
    release_branch = f"release-{major}.{minor}.x"

    print(f"Rotating changelog for {version} (repo: {repo})", file=sys.stderr)

    # --- Fetch ---
    tag_text = gh_fetch_changelog(version, repo)
    if tag_text is None:
        sys.exit(f"ERROR: could not fetch CHANGELOG at tag '{version}' from {repo}")

    release_text = gh_fetch_changelog(release_branch, repo)
    if release_text is None:
        print(f"WARNING: could not fetch CHANGELOG from {release_branch}; "
              f"assuming no burnin entries", file=sys.stderr)
        release_text = tag_text

    with open("CHANGELOG.md") as fh:
        main_text = fh.read()

    # --- Parse ---
    tag_sections     = parse_unreleased(tag_text)
    release_sections = parse_unreleased(release_text)
    main_sections    = parse_unreleased(main_text)

    # --- Diff ---
    post_tag_entries = new_bullets(tag_sections, main_sections)
    burnin_entries   = new_bullets(tag_sections, release_sections)

    n_post = sum(len(v) for v in post_tag_entries.values())
    n_burn = sum(len(v) for v in burnin_entries.values())
    print(f"  Post-tag entries  (→ new Unreleased):   {n_post}", file=sys.stderr)
    print(f"  Burnin entries    (→ {version} section): {n_burn}", file=sys.stderr)

    for sub, bullets in post_tag_entries.items():
        for b in bullets:
            print(f"    [post-tag] {sub}: {b[:70].splitlines()[0]}", file=sys.stderr)
    for sub, bullets in burnin_entries.items():
        for b in bullets:
            print(f"    [burnin]   {sub}: {b[:70].splitlines()[0]}", file=sys.stderr)

    # --- Render ---
    unreleased_block = render_unreleased(release_sections, post_tag_entries)
    version_block    = render_version(version, tag_sections, burnin_entries)
    remainder        = get_remainder(main_text)

    new_changelog = "# Changelog\n\n" + unreleased_block + "\n" + version_block + "\n" + remainder

    if args.dry_run:
        print(new_changelog)
    else:
        with open("CHANGELOG.md", "w") as fh:
            fh.write(new_changelog)
        print("CHANGELOG.md updated.", file=sys.stderr)
        print(f"\nNext steps:", file=sys.stderr)
        print(f"  git diff CHANGELOG.md          # review the result", file=sys.stderr)
        print(f"  git add CHANGELOG.md && git commit -s -m 'chore: rotate changelog for {version}'",
              file=sys.stderr)


if __name__ == "__main__":
    main()
