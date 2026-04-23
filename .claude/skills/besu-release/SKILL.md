---
name: besu-release
description: Conducts a Besu software release. Guides the release engineer through the full release lifecycle including changelog prep, RC tagging, burn-in, artifact publishing, and post-release verification. Use when performing a Besu release or when the user mentions releasing Besu.
compatibility: Requires git, gh CLI, AWS CLI, ansible, access to GitHub (besu-eth/besu), and Discord for team coordination. Consensys staff access required for Jenkins burn-in jobs and AWS (protocols profile) for node analysis.
---

# Besu Release Process

Guide the release engineer through each phase of a Besu release. Prompt for the release version up front (calver format, e.g. `26.1.0`) and use it throughout.

## Phase 1: Pre-Release Coordination

- [ ] Confirm at least 24 hours prior anything outstanding for release with other maintainers on **#besu-release** in Discord
- [ ] Update changelog if necessary, and merge a PR for it to main
  - [ ] Notify maintainers about updating changelog for in-flight PRs

**If this is a hotfix release:**
- [ ] Create a release branch and cherry-pick, e.g. `release-<version>-hotfix`
- [ ] Create a PR into main from the hotfix branch to see CI checks pass

## Phase 2: Create Release Candidate Tag

**Preflight:** verify the `upstream` remote points at `besu-eth/besu` (the current
official repo). Some older checkouts still have `upstream` pointing at the
pre-migration `hyperledger/besu`:

```bash
git remote get-url upstream
# expect: git@github.com:besu-eth/besu.git (or https://github.com/besu-eth/besu.git)
```

If it's wrong, fix it before tagging: `git remote set-url upstream git@github.com:besu-eth/besu.git`.

On the appropriate branch/commit, create a calver tag for the release candidate:

```bash
git tag <version>-RC1
git push upstream <version>-RC1
```

- [ ] Sign-off with team; announce the tag in **#besu-release** in Discord
  - Post: "Targeting this tag for the burn-in: https://github.com/besu-eth/besu/releases/tag/<version>-RC1"

## Phase 3: Burn-In

Consensys staff start burn-in using the RC tag. See [burn-in reference](references/BURN-IN.md) for detailed instructions.

- [ ] Start a new burn-in fleet with comparison nodes from the previous release
- [ ] **Verify all expected stacks provisioned.** The `elc-yml-create-burnin-nodes`
      Jenkins job creates 10 nodes by default: 4 `burn-*` (functional burn-in on
      hoodi/mainnet) + 3 `perf-test-*` + 3 `perf-control-*` (mainnet SNAP for
      regression comparison). A green Jenkins run is **not sufficient** —
      enumerate actual AWS instances and confirm all 10 are present. If perf
      nodes are missing, Phase 4 Step 3's "compare perf-test vs perf-control"
      cannot happen, and the release will ship without perf regression data.
      Re-run the Jenkins job for any missing stacks before proceeding.
- [ ] Update existing canaries and validators on Sepolia, Hoodi, and Mainnet
- [ ] Once nodes have synced and run for a sufficient period, proceed to Phase 4 for analysis

## Phase 4: Burn-In Analysis

Use the [protocols-node-analysis](https://github.com/Consensys/protocols-node-analysis) tooling to analyze the burn-in fleet. See [node analysis reference](references/NODE-ANALYSIS.md) for detailed instructions.

**Prerequisites:** AWS CLI installed, `ansible` installed (`brew install ansible`), logged into AWS SSO with protocols profile.

### Step 1: Generate hosts file
Filter AWS instances by the burn-in fleet name. **Use a regex that includes
both `burn` and `perf` stacks** — otherwise the perf-test / perf-control nodes
are silently excluded and the performance regression comparison can't run:
```bash
./create-hosts-file.sh "(burn|perf).*<version>-RC1" <version>-RC1-hosts.ini
```
If you specifically need the functional burn-in nodes only (e.g. for the
`bonsaitreeverifier` step in Step 4), use the narrower filter `"burn.*<version>-RC1"`
into a separate file.

### Step 2: Run node analysis report
```bash
./run-ansible-playbook.sh <version>-RC1-burn-in-hosts.ini node-analysis-playbook.yml
```
This generates an HTML report in `docs/` with a Grafana link at the top. Commit and push to publish to the [reports site](https://crispy-train-plzljoq.pages.github.io/).

### Step 3: Analyze the report
- [ ] Investigate and account for all Besu and CL **errors**
- [ ] Ideally account for all Besu and CL **warnings**
- [ ] Check sync times across burn-in nodes
- [ ] Check peer connections
- [ ] Check JVM memory usage (both during sync and after sync) via Grafana
- [ ] Compare perf-test nodes against perf-control nodes for regressions

### Step 4: Database integrity verification
**Do not run until performance analysis is complete** — this shuts down the clients.
```bash
./run-ansible-playbook.sh <version>-RC1-burn-in-hosts.ini bonsaitreeverifier-playbook.yml
```
View results:
```bash
./run-ansible-adhoc.sh <version>-RC1-burn-in-hosts.ini 'tail /tmp/bonsaitreeverifier.log'
```

### Step 5: Seek sign-off
- [ ] Share the analysis report with the team
- [ ] Seek sign-off for burn-in results

**If burn-in passes:** proceed to Phase 5.
**If burn-in fails:**
- Post in **#besu-release** that the release is aborted
- **Record the failure mode** in the `#besu-release` thread and in the
  node-analysis report notes: which check failed (Besu errors, CL errors,
  sync time, JVM memory, perf regression, bonsai verifier, etc.), and the
  root cause if known. This is how recurring problems become visible across
  releases
- Either skip this version (note in CHANGELOG.md) or build RC2 by restarting
  at Phase 2 with the new RC number

**Parallelism note:** Phases 5–7 (release simulation, tag creation, and draft release workflow + release notes) can be started in parallel with burn-in (Phases 3–4). The draft release and its notes can be prepared while burn-in is running — only the final *publish* (Phase 8) must wait for burn-in sign-off.

## Phase 5: Optional Release Simulation

Test the release workflows in the sandbox before the real release. See [sandbox reference](references/SANDBOX.md) for details.

### First RC

```bash
# In protocols-release-sandbox (sync fork first)
git checkout <sha of version-RC1>
git tag <version>
git push <your-remote> <version>
```

1. Manually run the [draft-release workflow](https://github.com/Consensys/protocols-release-sandbox/actions/workflows/draft-release.yml) using `main` branch and the release tag
2. Once the workflow creates the draft, immediately read the `Upcoming Release` section from `CHANGELOG.md`, replace the heading with `## <version>`, and update the draft release body with it
3. Present the composed release notes to the release engineer for review

### Subsequent RCs (RC2, RC3, etc.)

A subsequent RC reuses the existing draft release — do NOT create a second release for the same version. Instead, update the existing draft in place:

1. Move the release tag to the new RC commit and force-push it:
   ```bash
   git tag -f <version> <version>-RCn
   git push <your-remote> -f <version>
   ```
2. Re-run the draft-release workflow — since the tag already has an associated release, the workflow will update the existing draft (not create a new one)
3. Update the draft release description with the latest `Upcoming Release` section from `CHANGELOG.md` to capture any new entries since the previous RC

## Phase 6: Create Full Release Tag

Using the sha of the **RC that passed burn-in** (not necessarily RC1 — for a
multi-RC cycle, use the highest-numbered RC that got sign-off), create the
final calver tag:

```bash
# Find the latest RC tag, e.g. for 26.4.0 with RC1/RC2/RC3 this prints 26.4.0-RC3
git tag -l '<version>-RC*' | sort -V | tail -n 1

# Use that tag
git checkout <version>-RC<n>
git tag <version>
git push upstream <version>
```

If an earlier RC tag has already been force-pushed to the final version tag in
a prior cycle (from a previous release simulation), use `git push -f upstream <version>`.

## Phase 7: Publish Release

- [ ] Manually run the [draft-release workflow](https://github.com/besu-eth/besu/actions/workflows/draft-release.yml) using `main` branch and the full release tag name (e.g. `26.1.0`)
  - **Note:** Always run from `main` branch even for hotfix tags
  - This publishes artifacts and version-specific docker tags but does NOT fully publish the GitHub release
- [ ] Once the workflow creates the draft, immediately update the release description with the changelog:
  1. Read `CHANGELOG.md` from the besu repo and extract the `Upcoming Release` section
  2. Replace the heading with `## <version>` and update the draft release body
  3. Present the composed release notes to the release engineer for review and editing
- [ ] Verify all draft-release workflow jobs went green
- [ ] Check binary SHAs are correct on the release page
- [ ] Check artifacts exist in [Artifactory](https://besu-eth.jfrog.io/ui/repos/tree/General/besu-maven/org/besu-eth/besu/bom)
- [ ] Finalize the notes with the release engineer, save draft and sign-off with team

## Phase 8: Publish and Verify

**CRITICAL — GATE CHECK:** Publishing a release is irreversible. It notifies all subscribers, promotes Docker `latest` tags, and triggers downstream workflows. Before proceeding, explicitly confirm with the release engineer that:
1. Burn-in analysis (Phase 4) is complete and signed off
2. The release engineer has explicitly approved moving the release out of draft status

Do NOT publish the release based on workflow success alone. The draft-release workflow (Phase 7) can complete well before burn-in is finished — these are independent processes. Always wait for burn-in sign-off before publishing.

**IMPORTANT:** Confirm the tag name is the ONLY text in the "Release title". Any extra text breaks the Docker Promote workflow. See [known issues](references/TROUBLESHOOTING.md) for details.

- [ ] Confirm burn-in sign-off has been received (Phase 4, Step 5)
- [ ] Obtain explicit approval from the release engineer to publish (take the release out of draft)
- [ ] Publish draft release ensuring it is marked as **latest release** (if appropriate)
  - This notifies subscribed users, makes the release "latest" in GitHub, and publishes the docker `latest` tag variants
- [ ] Verify the [Docker Promote workflow](https://github.com/besu-eth/besu/actions/workflows/docker-promote.yml) went green

## Phase 9: Post-Release

- [ ] Create homebrew release PR using the [update-version workflow](https://github.com/besu-eth/homebrew-besu/actions/workflows/update-version.yml)
  - If the PR was not automatically created, create it manually using the branch `update-<version>`
- [ ] Verify homebrew release once the PR has merged:
  ```bash
  brew tap besu-eth/besu && brew install besu
  ```
- [ ] Burn-in nodes: **no action required by default.** Burn-in instances
      self-delete after 7 days via the `Besu/scheduled-delete-vm` cron job
      (runs daily at 10:00 UTC). Only delete manually if you need capacity
      back sooner or need to free quota — use the
      `Besu/elc-delete-yml-n-nodes` Jenkins job with a YAML payload listing
      the `vm_name` entries to terminate.
- [ ] Social announcements — see [announcements reference](references/ANNOUNCEMENTS.md)
      for Discord and Twitter/X templates. Fill in from the `## <version>`
      section of `CHANGELOG.md` and present to the release engineer for
      review before posting.

## Phase 10: If Something Goes Wrong

If a critical issue surfaces after the release is published — consensus bug,
data corruption, silent validator failure, remote crash — see
[rollback reference](references/ROLLBACK.md) for:
- Hotfix vs. rollback decision
- Communication cadence (#besu-release, ethPandaOps, GitHub release body)
- Mechanics of demoting the release and repointing `latest` docker tags
- What **not** to do (delete tags, force-move published tags, silently
  remove docker tags)
