#!/usr/bin/env python3
"""
Rotate CHANGELOG.md after a release.

Usage:
    python3 scripts/rotate-changelog.py <version> [commit] [--repo owner/repo] [--dry-run]

Examples:
    python3 scripts/rotate-changelog.py 26.6.1 baa7b0b   # pre-tag: use specific commit
    python3 scripts/rotate-changelog.py 26.6.0            # post-tag: uses existing tag (or HEAD)

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
    tag_sections: dict[str, list[str]],
    post_tag: dict[str, list[str]],
    release_sections: dict[str, list[str]] | None = None,
) -> str:
    """
    Build the new ## Unreleased block.

    - Breaking Changes: only genuinely new post-tag entries.
    - Upcoming Breaking Changes: carry ALL entries forward from the tag
      (they are still upcoming) plus any from the release and new post-tag ones.
    - Performance: only emitted when there are entries (optional section).
    - Bug fixes / Additions and Improvements: always emitted as scaffolding.
    """
    if release_sections is None:
        release_sections = {}

    # Sections always included as scaffolding even when empty
    SCAFFOLD = {"### Breaking Changes", "### Bug fixes", "### Additions and Improvements"}

    lines = ["## Unreleased", ""]

    for sub in SUBSECTIONS:
        if sub == "### Upcoming Breaking Changes":
            # Carry forward all from the tag, plus any from the release, plus new post-tag ones
            carried = list(tag_sections.get(sub, []))
            carried_set = set(carried)
            for b in release_sections.get(sub, []):
                if b not in carried_set:
                    carried.append(b)
                    carried_set.add(b)
            new = [b for b in post_tag.get(sub, []) if b not in carried_set]
            bullets = carried + new
        else:
            bullets = post_tag.get(sub, [])

        if not bullets and sub not in SCAFFOLD:
            continue  # skip optional empty sections (e.g. ### Performance)

        lines.append(sub)
        for b in bullets:
            lines.append(b)
        lines.append("")

    return "\n".join(lines)


def render_version(
    version: str,
    tag_sections: dict[str, list[str]],
    burnin: dict[str, list[str]],
    prev_tag_sections: dict[str, list[str]] | None = None,
) -> str:
    """Build the ## <version> block."""
    if prev_tag_sections is None:
        prev_tag_sections = {}

    lines = [f"## {version}", ""]

    for sub in SUBSECTIONS:
        if sub == "### Upcoming Breaking Changes":
            # Carry forward all upcoming breaking changes from the previous release
            # plus any new ones in this release and burnin
            carried = list(prev_tag_sections.get(sub, []))
            carried_set = set(carried)
            for b in tag_sections.get(sub, []) + burnin.get(sub, []):
                if b not in carried_set:
                    carried.append(b)
                    carried_set.add(b)
            bullets = carried
        else:
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
    parser.add_argument("commit", nargs="?",
                        help="Commit SHA to use as the release ref (for pre-tag rotation). "
                             "Omit to use the existing tag, or HEAD if the tag does not exist yet.")
    parser.add_argument("--repo", default="besu-eth/besu")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the new CHANGELOG to stdout instead of writing it")
    args = parser.parse_args()

    version = args.version
    repo = args.repo
    major, minor, patch = version.split(".")
    release_branch = f"release-{major}.{minor}.x"

    with open("CHANGELOG.md", encoding="utf-8") as fh:
        main_text = fh.read()

    if f"\n## {version}\n" in main_text:
        sys.exit(f"ERROR: ## {version} already exists in CHANGELOG.md — already rotated?")

    print(f"Rotating changelog for {version} (repo: {repo})", file=sys.stderr)

    # --- Fetch ---
    pre_tag = False
    if args.commit:
        ref = args.commit
        print(f"  Using commit {ref} as release ref (pre-tag)", file=sys.stderr)
        tag_text = gh_fetch_changelog(ref, repo)
        if tag_text is None:
            sys.exit(f"ERROR: could not fetch CHANGELOG at commit '{ref}' from {repo}")
        pre_tag = True
    else:
        tag_text = gh_fetch_changelog(version, repo)
        if tag_text is None:
            # Tag doesn't exist yet — the local CHANGELOG is the pre-tag state.
            # Main is the release branch; whatever is in Unreleased now becomes the release.
            print(f"  Tag '{version}' not found; using local CHANGELOG as pre-tag state", file=sys.stderr)
            tag_text = main_text
            pre_tag = True

    if pre_tag:
        # No separate release branch — no burnin entries possible yet
        release_text = main_text
    else:
        release_text = gh_fetch_changelog(release_branch, repo)
        if release_text is None:
            print(f"WARNING: could not fetch CHANGELOG from {release_branch}; "
                  f"assuming no burnin entries", file=sys.stderr)
            release_text = tag_text

    # --- Parse ---
    tag_sections     = parse_unreleased(tag_text)
    release_sections = parse_unreleased(release_text)
    main_sections    = parse_unreleased(main_text)

    # For patch releases the release branch is never rotated between patches, so
    # tag_sections contains the previous patch's content plus new burnin entries.
    # Fetch the previous patch tag and subtract it so the version block only shows
    # what's new in this release.
    empty_sections: dict[str, list[str]] = {h: [] for h in SUBSECTIONS}
    if int(patch) > 0:
        prev_version = f"{major}.{minor}.{int(patch) - 1}"
        print(f"  Patch release — fetching {prev_version} tag as baseline", file=sys.stderr)
        prev_tag_text = gh_fetch_changelog(prev_version, repo)
        if prev_tag_text is None:
            print(f"  WARNING: could not fetch {prev_version} tag; version block may contain duplicates",
                  file=sys.stderr)
            prev_tag_sections = empty_sections
        else:
            prev_tag_sections = parse_unreleased(prev_tag_text)
    else:
        prev_tag_sections = empty_sections

    new_in_release = new_bullets(prev_tag_sections, tag_sections)

    # --- Diff ---
    post_tag_raw   = new_bullets(tag_sections, main_sections)
    burnin_entries = new_bullets(tag_sections, release_sections)
    # Entries that appear in both post-tag and burnin belong to the release, not to Unreleased
    post_tag_entries = new_bullets(burnin_entries, post_tag_raw)

    n_new  = sum(len(v) for v in new_in_release.values())
    n_post = sum(len(v) for v in post_tag_entries.values())
    n_burn = sum(len(v) for v in burnin_entries.values())
    n_tag  = sum(len(v) for v in tag_sections.values())
    n_prev = sum(len(v) for v in prev_tag_sections.values())
    print(f"  tag entries (raw):                       {n_tag}", file=sys.stderr)
    print(f"  prev-tag entries (baseline):             {n_prev}", file=sys.stderr)
    print(f"  New in release   (→ {version} section): {n_new}", file=sys.stderr)
    print(f"  Post-tag entries (→ new Unreleased):     {n_post}", file=sys.stderr)
    print(f"  Burnin entries   (→ {version} section):  {n_burn}", file=sys.stderr)

    for sub, bullets in post_tag_entries.items():
        for b in bullets:
            print(f"    [post-tag] {sub}: {b[:70].splitlines()[0]}", file=sys.stderr)
    for sub, bullets in burnin_entries.items():
        for b in bullets:
            print(f"    [burnin]   {sub}: {b[:70].splitlines()[0]}", file=sys.stderr)

    # --- Render ---
    unreleased_block = render_unreleased(tag_sections, post_tag_entries, new_in_release)
    version_block    = render_version(version, new_in_release, burnin_entries, prev_tag_sections)
    remainder        = get_remainder(main_text)

    new_changelog = "# Changelog\n\n" + unreleased_block + "\n" + version_block + "\n" + remainder

    if args.dry_run:
        print(unreleased_block)
        print(version_block)
    else:
        with open("CHANGELOG.md", "w", encoding="utf-8") as fh:
            fh.write(new_changelog)
        print("CHANGELOG.md updated.", file=sys.stderr)
        print(f"\nNext steps:", file=sys.stderr)
        print(f"  git diff CHANGELOG.md          # review the result", file=sys.stderr)
        print(f"  git add CHANGELOG.md && git commit -s -m 'chore: rotate changelog for {version}'",
              file=sys.stderr)


if __name__ == "__main__":
    main()
