# Troubleshooting and Known Issues

## Release Title Must Be Exact Tag Name

The GitHub release title **must** contain ONLY the release version (e.g. `26.1.0`), with no additional text like `- P2P Blob Transaction Hotfix`.

**Why:** The release title is piped directly into Gradle commands. Extra text causes build failures:

```
./gradlew -Prelease.releaseVersion=24.3.3 - P2P Blob Transaction Hotfix Pre-release ...
```

This broke in practice: https://github.com/besu-eth/besu/actions/runs/8640564590/job/23688673464

Background on why the title is used: https://github.com/besu-eth/besu/pull/6866

## Burn-In Failure Recovery

If the burn-in fails, the release is effectively cancelled — nothing needs to happen to "undo" it since no public release was created. Options:

1. **Skip the version** — note the outcome in CHANGELOG.md
2. **Build RC2** — restart the burn-in candidate steps at Phase 2 with the new RC number (e.g. `26.1.0-RC2`)

## Hotfix Cherry-Pick Friction

The release process leaves a cherry-pick escape hatch for hotfixes with minimum friction for the latest release. However, a PR merging main into the release branch will need to handle the merge conflict created by the cherry-pick. Cherry-picking should be a rare event, done with care.

## Artifactory Publish Fails in Sandbox

When using the sandbox repo (protocols-release-sandbox), the Artifactory publish job is expected to fail because there is no test Artifactory instance configured. Docker and binary artifact publishing should still succeed.

## Draft-Release Workflow Branch

The draft-release workflow should **always** be run from the `main` branch, even for hotfix releases. Hotfix tags will still be released correctly regardless of which branch they were created on.
