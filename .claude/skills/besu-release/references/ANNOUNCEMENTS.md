# Release Announcement Templates

Use these as skeletons. Fill in highlights by extracting the most user-visible
entries from the `## <version>` section of `CHANGELOG.md` — prioritize in this
order:

1. **Breaking changes** (users need to know before they upgrade)
2. **New protocol / EIP support** (network participation signal)
3. **New user-facing features** (CLI flags, RPCs, subscriptions)
4. **Perf wins with measurable impact**
5. **Notable bug fixes** (only if they affect operators)

Skip: internal refactors, test-only changes, build/CI work, dependency bumps
without behavior change.

## Discord (#besu-release or #announcements)

```
🚀 **Besu <version> is live**
https://github.com/besu-eth/besu/releases/tag/<version>

<optional 1-line context — repo moves, major deprecations, upgrade urgency>

**Highlights**
• <3-6 bullets, lead with protocol/EIP support, then features, then perf>

**⚠️ Breaking**
• <breaking changes — only if present in this release>

Full changelog: https://github.com/besu-eth/besu/blob/main/CHANGELOG.md
```

## Twitter / X

Keep under 280 characters. Structure:

```
🚀 Besu <version> is out

• <top 3-4 bullets, very terse>
• ⚠️ <breaking change, if any>

<repo or link hook>
https://github.com/besu-eth/besu/releases/tag/<version>
```

Tip: if breaking changes dominate, lead with them. If the release is mostly
perf/features, lead with those and note breaking at the end.

## Composition workflow

1. Read `CHANGELOG.md`, extract `## <version>` section
2. Group entries into Highlights / Breaking
3. Draft both texts, show to the release engineer for edits before posting
4. After posting, record the links in the `#besu-release` thread so future
   releases can see prior announcement style
