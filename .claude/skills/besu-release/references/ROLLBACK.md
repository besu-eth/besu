# Rollback & Incident Response

If a critical issue is discovered after a release has been published. "Critical"
means: consensus bug, data corruption, silent validator failure, remote crash,
or anything that warrants telling operators to not upgrade / to downgrade.

## Decision: rollback vs. hotfix

- **Hotfix forward** is almost always preferred — operators have already started
  upgrading, and asking them to downgrade is disruptive. Use a hotfix when the
  fix is small and can be shipped within hours.
- **Rollback** (mark the release as not-latest, pull `latest` docker tag) is
  only appropriate when the issue is severe enough that *no one* should run the
  new version, and a hotfix is not imminent.

Loop in the release engineer and the on-call responder before deciding.

## Communication first

1. Post in **#besu-release** on Discord with: the symptom, known impact,
   affected versions, and current recommendation (downgrade / hold / hotfix ETA)
2. Update the GitHub release body with a prominent ⚠️ warning at the top
3. If operators may be hitting it in production, reach out to ethPandaOps /
   EF devops channels (coordinate with the RE on wording)
4. Pin / update the Discord announcement message from the original release

## Hotfix procedure

1. Create a branch from the released tag:
   ```bash
   git checkout -b release-<version>.1-hotfix <version>
   ```
2. Cherry-pick the fix commit(s); keep the scope minimal
3. Open a PR into `main` to run CI (merge only if the team agrees the fix
   should also go to main — sometimes it does, sometimes main has already
   moved on)
4. Restart the release flow from **Phase 2** using the hotfix branch:
   - Tag `<version>.1-RC1`
   - Full burn-in cycle (abbreviated is acceptable for small fixes — get RE
     sign-off on scope)
   - Publish as `<version>.1`

## Rollback: mark release not-latest

This stops new users from pulling the broken version via `docker pull besu:latest`
and from seeing it as the "Latest" release on GitHub.

```bash
# Demote the broken release — pick the last known-good version as the new latest
gh release edit <previous-good-version> --repo besu-eth/besu --latest
gh release edit <broken-version> --repo besu-eth/besu --prerelease
```

Then repoint the `latest` docker tags back to the previous good version:

```bash
for TAG in latest latest-amd64 latest-arm64; do
  docker buildx imagetools create \
    -t docker.io/hyperledger/besu:${TAG} \
    docker.io/hyperledger/besu:<previous-good-version>${TAG#latest}
done
```

(Where `${TAG#latest}` strips `latest` from `latest-amd64` → `-amd64` etc.)

**Note:** the version-specific docker tag (`besu:<broken-version>`) cannot be
practically revoked — operators who pulled it already have it. Rollback is
about preventing *new* pulls from hitting the broken version.

## Post-incident

- Add a postmortem note in the next release's announcement or the CHANGELOG
  under Bug fixes
- If the root cause was a process gap (not a code bug), update this skill's
  checklists — that's what it's for

## What not to do

- Do **not** delete the git tag or the GitHub release — archaeologists need
  them, and downstream automation (homebrew, distro packagers) may already
  reference them
- Do **not** force-move a published tag to a different sha — breaks everyone
  who already pulled it, causes signature verification failures
- Do **not** silently delete docker tags — breaks anyone pinned to them
