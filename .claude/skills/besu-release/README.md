# besu-release

An [Agent Skill](https://agentskills.io) that guides a release engineer through the full Besu release lifecycle — from changelog prep and RC tagging through burn-in analysis, artifact publishing, and post-release verification.

## Installation

### Claude Code

**Project-level** (recommended for team use — commit to version control):

```bash
# From your project root
cp -r besu-release .claude/skills/besu-release
```

**Personal** (available across all your projects):

```bash
cp -r besu-release ~/.claude/skills/besu-release
```

Once installed, Claude will automatically activate the skill when you mention releasing Besu, or you can invoke it directly:

```
/besu-release 26.4.0
```

### Hermes Agent

**From a local copy:**

```bash
cp -r besu-release ~/.hermes/skills/besu-release
```

**From GitHub** (once published to a repo):

```bash
hermes skills install <org>/<repo>/besu-release
```

**Via external directory** — add to `~/.hermes/config.yaml`:

```yaml
skills:
  external_dirs:
    - /path/to/directory/containing/besu-release
```

## Permissions

This skill orchestrates a multi-step release process that involves git operations, GitHub API calls, AWS infrastructure, and Ansible playbooks. Grant permissions deliberately based on which phases you are performing.

### Recommended permission strategy

**Start restrictive, approve as you go.** Both Claude Code and Hermes will prompt you before executing any tool that hasn't been pre-approved. This is the safest approach — you review each action in context before it runs.

If you want to pre-approve certain tools to reduce prompts, scope them narrowly:

#### Claude Code

Add `allowed-tools` to the skill frontmatter or your permission settings. Only approve tools you're comfortable with the agent running unsupervised:

```yaml
# In SKILL.md frontmatter — read-only tools only
allowed-tools: Read Grep Glob Bash(git status *) Bash(git log *) Bash(git diff *) Bash(gh pr view *) Bash(gh run view *)
```

For broader automation (e.g. tagging and pushing), you can extend this, but understand the implications:

```yaml
# Extends to write operations — review carefully
allowed-tools: Read Grep Glob Bash(git *)  Bash(gh *)
```

You can also manage this in `/permissions` rules:

```
# Allow read-only git and GitHub operations for this skill
Skill(besu-release *)
Bash(git status *)
Bash(git log *)
Bash(gh run view *)
```

#### Hermes Agent

Hermes uses a trust-level system. Since this is a custom/local skill, it falls under `community` trust. Hermes will prompt for approval on tool use. To pre-approve specific tools, configure your `config.yaml` accordingly.

### What each phase needs

| Phase | Tools used | Risk level |
|-------|-----------|------------|
| 1. Pre-release coordination | Read, Grep, Bash(git) | Low — read-only |
| 2. Create RC tag | Bash(git tag, git push) | **Medium** — creates a public tag |
| 3. Burn-in | External (Jenkins) | N/A — done outside the agent |
| 4. Burn-in analysis | Bash(ansible, aws, git) | **Medium** — runs playbooks on remote nodes |
| 5. Release simulation | Bash(git tag, git push) | Low — sandbox repo only |
| 6. Create full release tag | Bash(git tag, git push) | **Medium** — creates the release tag |
| 7. Publish release | External (GitHub Actions) | **High** — triggers artifact publishing |
| 8. Publish and verify | External (GitHub UI) | **High** — makes the release public |
| 9. Post-release | Bash(brew), External (GitHub Actions) | Low |

### What NOT to pre-approve

- **`Bash(*)`** — never grant unrestricted shell access
- **`Bash(git push --force *)`** — destructive, can overwrite history
- **`Bash(rm *)`** — no reason for this skill to delete local files
- **`Bash(aws *)`** — too broad; the skill only needs AWS through Ansible scripts

### Sensitive data

This skill references internal URLs (Jenkins, Grafana, AWS inventories) but does not require secrets to be embedded. Ensure:

- AWS SSO session is active before Phase 4 (`aws sso login --profile protocols`)
- GitHub CLI is authenticated (`gh auth status`)
- No API keys, tokens, or passwords are stored in skill files
- Hosts files generated during analysis are ephemeral — do not commit them to repositories (IPs are recycled when nodes are deleted)

## Skill structure

```
besu-release/
├── SKILL.md                       # 9-phase release process (main instructions)
└── references/
    ├── BURN-IN.md                 # Burn-in fleet setup and troubleshooting
    ├── NODE-ANALYSIS.md           # Analysis tooling (protocols-node-analysis)
    ├── SANDBOX.md                 # Release simulation in sandbox repo
    └── TROUBLESHOOTING.md         # Known issues and failure recovery
```

Reference files are loaded on demand — they don't consume context until the agent needs them.

## Prerequisites

- `git` with push access to [besu-eth/besu](https://github.com/besu-eth/besu)
- `gh` CLI authenticated with GitHub
- AWS CLI with `protocols` profile (for burn-in analysis)
- `ansible` (`brew install ansible`)
- Access to Consensys Jenkins and Grafana (for burn-in phases)
- Discord access to **#besu-release** channel
