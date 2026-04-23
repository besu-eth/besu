# Release Simulation (Sandbox)

The sandbox repository https://github.com/Consensys/protocols-release-sandbox is a fork of besu-eth/besu used to simulate releases before performing them on the real repository.

## When to Use

- If the `draft-release` workflow has been updated since the last release
- If you haven't released in a while and want extra confidence

## Steps

1. Go to https://github.com/Consensys/protocols-release-sandbox and click **"Sync fork"** to pull in all latest changes from upstream
2. Create the release tag on the same commit as the RC:
   ```bash
   git checkout <sha of version-RC1>
   git tag <version>
   git push <your-remote> <version>
   ```
3. Manually run the [draft-release workflow](https://github.com/Consensys/protocols-release-sandbox/actions/workflows/draft-release.yml) using `main` branch and the release tag

## Expected Results

- Build binaries are added to the GitHub release
- Docker images are uploaded to Dockerhub (`devopsconsensys/besu`) — this is a private repository requiring authentication
- **Artifactory publish will FAIL** — this is expected as there is no test Artifactory instance configured

---

## Testing on a Personal Fork

You can also run the release simulation on your own GitHub fork (e.g. `jflo/besu`). This requires additional setup since your fork needs the right permissions and secrets.

### GitHub Personal Access Token (PAT) Scopes

To trigger and run the release workflows from the CLI, your `gh` token (or PAT) needs these scopes:

| Scope | Required By | Purpose |
|-------|-------------|---------|
| `actions:write` | `gh workflow run` | Trigger the `draft-release` workflow via the API |
| `contents:write` | `draft-release` job | Push tags and create draft GitHub releases with attached artifacts |
| `contents:read` | All jobs | Checkout code and verify tags |

> **Note:** The `actions:read` scope is implied by `actions:write` and lets you monitor workflow runs with `gh run list` / `gh run watch`.

### Repository Secrets

The draft-release workflow references several secrets. Jobs that depend on missing secrets will fail — this is expected for a test release on a fork.

| Secret | Used By | Purpose | Required? |
|--------|---------|---------|-----------|
| `DOCKER_USER_RW` | `docker-publish`, `docker-manifest`, `docker-verify`, `docker-promote` | Docker Hub username | No — Docker jobs will fail without it |
| `DOCKER_PASSWORD_RW` | Same as above | Docker Hub password/token | No — Docker jobs will fail without it |
| `DOCKER_ORG` | Same as above | Docker Hub organization (e.g. `hyperledger`) | No — Docker jobs will fail without it |
| `BESU_ARTIFACTORY_USER` | `artifactory` | Artifactory username | No — Artifactory publish will fail without it |
| `BESU_ARTIFACTORY_TOKEN` | `artifactory` | Artifactory API token | No — Artifactory publish will fail without it |

For a **minimal test** (build, test, create draft GitHub release), no secrets are needed beyond the automatic `GITHUB_TOKEN`. The `release-draft`, `build`, `test-linux`, and `test-windows` jobs will succeed without any repository secrets.

### Steps for Fork-Based Test Release

1. Ensure your fork is synced with upstream
2. Create and push the RC and release tags to your fork:
   ```bash
   git tag <version>-RC1
   git push origin <version>-RC1
   git tag <version> <version>-RC1
   git push origin <version>
   ```
3. Trigger the draft-release workflow:
   ```bash
   gh workflow run "Draft Release" -R <your-user>/besu --ref main -f tag=<version>
   ```
4. Monitor the run:
   ```bash
   gh run list -R <your-user>/besu -w "Draft Release" --limit 5
   gh run watch -R <your-user>/besu <run-id>
   ```

### Expected Results on a Fork

| Job | Expected Result |
|-----|-----------------|
| `validate` | Pass |
| `build` | Pass |
| `test-linux` | Pass |
| `test-windows` | Pass |
| `docker-lint` | Pass |
| `release-draft` | Pass — creates a draft release on your fork |
| `docker-publish` | **Fail** unless Docker secrets are configured |
| `docker-manifest` | **Fail** (depends on `docker-publish`) |
| `docker-verify` | **Fail** (depends on `docker-manifest`) |
| `artifactory` | **Fail** unless Artifactory secrets are configured |
| `verify_artifactory` | **Fail** (depends on `artifactory`) |

### Cleanup

After testing, delete the tags from your fork:
```bash
git push origin --delete <version>-RC1 <version>
git tag -d <version>-RC1 <version>
```
