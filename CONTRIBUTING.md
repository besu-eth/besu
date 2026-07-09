# Contributing to Besu

## :tada: Thanks for taking the time to contribute! :tada:

Welcome to the Besu repository! The following is a set of guidelines for contributing to this repo and its packages. These are mostly guidelines, not rules. Use your best judgement, and feel free to propose changes to this document in a pull request. Contributions come in the form of code submissions, writing documentation, raising issues, helping others in chat, and any other actions that help develop Besu.

This document covers everything you need to know to decide whether and how you want to contribute. For deeper, area-specific guides, we link out to the [Besu wiki](https://github.com/besu-eth/besu/wiki).

## Contents

- [GitHub and Discord accounts](#github-and-discord-accounts)
- [I just have a quick question](#i-just-have-a-quick-question)
- [Where to start?](#where-to-start)
- [Contribution workflow](#contribution-workflow)
- [Reporting bugs](#reporting-bugs)
- [Suggesting enhancements](#suggesting-enhancements)
- [Pull requests](#pull-requests)
- [Changelog](#changelog)
- [Code reviews](#code-reviews)
- [Developer Certificate of Origin (DCO)](#developer-certificate-of-origin-dco)
- [Copyright and license](#copyright-and-license)
- [Security contributions](#security-contributions)
- [Guidelines for non-code and other trivial contributions](#guidelines-for-non-code-and-other-trivial-contributions)
- [Other important information](#other-important-information)
- [Contributing to specific areas](#contributing-to-specific-areas)

## GitHub and Discord accounts

A GitHub account is required for code and issue contributions. Discord is optional, but useful for asking questions and coordinating with maintainers.

- Create a [GitHub account](https://github.com) if you don't already have one.
- Join our [Discord](https://discord.com/invite/hyperledger) to ask questions or chat with us.

## I just have a quick question

You might find the answer in the [Besu documentation](https://docs.besu-eth.org/), or you can ask in the [Besu Discord](https://discord.com/invite/hyperledger) or browse the [wiki](https://github.com/besu-eth/besu/wiki).

> [!NOTE]
> Please don't file an issue to ask a question. You'll get faster results by using the resources above.

## Where to start?

The first step is deciding what to work on! We use the "good first issue" label to identify issues that we think are [a good place to start](https://github.com/besu-eth/besu/contribute).

- Browse the [good first issues](https://github.com/besu-eth/besu/labels/good%20first%20issue). These should only require a few lines of code or documentation, and maybe a test or two.
- Browse the [help wanted issues](https://github.com/besu-eth/besu/labels/help%20wanted). These are a bit more involved than good first issues.

If you find an issue you'd like to work on, reach out on [Discord](https://discord.com/invite/hyperledger) in the **#besu** channel and we can assign it to you. If you have a different idea, reach out and we can discuss it.

## Contribution workflow

The codebase and documentation are maintained using the same _contributor workflow_, where everyone without exception contributes change proposals using pull requests (PRs). This facilitates social contribution, easy testing, and peer review.

To contribute changes, use the following workflow:

1. [**Fork the repository**](https://github.com/besu-eth/besu/fork). Make sure you also [add an upstream remote](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/syncing-a-fork) so you can keep your fork up to date.
2. **Clone your fork** to your computer.
3. **Create a topic branch** and name it appropriately. Starting the branch name with the issue number is a good practice and a reminder to fix only one issue per PR.
4. **Make your changes** adhering to the coding conventions described in the [Besu wiki](https://github.com/besu-eth/besu/wiki). In general a commit serves a single purpose, and diffs should be easily comprehensible. For this reason, do not mix formatting fixes or code moves with actual code changes.
5. **Commit your changes**. See the [How to Write a Git Commit Message](https://cbea.ms/git-commit/) article by Chris Beams. Make sure to add a DCO sign-off to each commit (for example, `git commit -s -m "..."`); see [Developer Certificate of Origin (DCO)](#developer-certificate-of-origin-dco) below.
6. **Test your changes** locally before pushing to ensure you are not breaking another part of the software. Running `./gradlew clean check test` locally helps you be confident that your changes will pass CI once pushed as a PR.
7. **Push your changes** to your remote fork (usually labeled `origin`).
8. **Create a pull request** on the Besu repository. If it's not ready for review, make it a `Draft` PR. If the PR addresses an existing issue, link it in the PR description using GitHub keywords such as `fixes #1234` or `refs #1234`.
9. **Add labels** to identify the type of your PR, if you have permission. For example, if your PR fixes a bug, add the "bug" label. If you don't have permission, maintainers will label the PR during triage.
10. **Ensure your changes are reviewed**. Let us know on Discord that your PR is ready for review. If you are a maintainer, you can choose reviewers; otherwise this is done by one of the maintainers.
11. **Make any required changes** based on reviewer feedback. Make the changes, commit to your branch, and push to your remote fork.
12. **When your PR is approved and validated**, all tests pass, and your branch has no conflicts, it can be merged. This is done by a maintainer, usually the same person who approves also merges it.

You contributed to Besu! Thanks!

## Reporting bugs

This section guides you through submitting a bug report. Following these guidelines helps maintainers and the community understand your report, reproduce the behavior, and find related reports.

Bugs are tracked as [GitHub issues](https://github.com/besu-eth/besu/issues). Before submitting a bug report:

- **Confirm the problem** is reproducible in the latest version of the software.
- **Check the [Besu documentation](https://docs.besu-eth.org/)**. You might be able to find the cause of the problem and fix it yourself.
- **Search [existing issues](https://github.com/besu-eth/besu/issues)** to see if the problem has already been reported. If it has and the issue is still open, add a comment to the existing issue instead of opening a new one. If you find a closed issue that seems like the same thing you're experiencing, open a new issue and include a link to the original issue.

When you create a bug report, please include as many details as possible:

- **Use a clear and descriptive summary** to identify the problem.
- **Describe the exact steps that reproduce the problem** in as much detail as possible. For example, explain how you started Besu, including the exact command you used.
- **Provide specific examples to demonstrate the steps**. Include links to files or GitHub projects, or copy-pasteable snippets. If you're providing snippets in the issue, use Markdown code blocks.
- **Describe the behavior you observed** and point out exactly what the problem is.
- **Explain which behavior you expected to see instead and why.**
- **Include screenshots** where they help demonstrate the problem.

Provide more context by answering these questions:

- **Did the problem start happening recently** (for example, after updating to a new version) or was it always a problem? If recent, can you reproduce it in an older version? What is the most recent version in which the problem doesn't happen?
- **Can you reliably reproduce the issue?** If not, provide details about how often it happens and under which conditions.

Include details about your configuration and environment:

- **Which version of Besu are you using?** Get the exact version by running `besu -v`.
- **What OS and version are you running?** For Linux, include the kernel (`uname -a`).
- **Are you running in a virtual machine, Docker container, or cloud?** If so, include the relevant software, versions, and instance type.
- **What version of Java are you running?** You can find it in the Besu log file at startup.

## Suggesting enhancements

This section guides you through submitting an enhancement suggestion. Following these guidelines helps maintainers and the community understand your suggestion and find related ones.

Enhancement suggestions are tracked as [GitHub issues](https://github.com/besu-eth/besu/issues). Before submitting:

- **Check the [Besu documentation](https://docs.besu-eth.org/)**. The enhancement might already exist.
- **Search [existing issues](https://github.com/besu-eth/besu/issues)** to see if it has already been suggested. If it has and the issue is still open, add a comment instead of opening a new one.

When you create an enhancement suggestion, please include:

- **A clear and descriptive title** to identify the suggestion.
- **A step-by-step description of the suggested enhancement** in as much detail as possible.
- **Specific examples to demonstrate the steps**, using Markdown code blocks for any snippets.
- **A description of the current behavior** and an explanation of which behavior you expected to see instead, and why.
- **An explanation of why this enhancement would be useful** to most users.
- **Whether this enhancement exists in other clients.**
- **The version of Besu you're using** (`besu --version`) and the name and version of your OS.

## Pull requests

The pull request process has several goals:

- Maintain product quality.
- Fix problems that are important to users.
- Engage the community in working toward the best possible product.
- Enable a sustainable system for maintainers to review contributions.

Please follow these steps to have your contribution considered by the approvers:

1. Ensure all commits have a sign-off for the DCO, as described in [Developer Certificate of Origin (DCO)](#developer-certificate-of-origin-dco).
2. Follow all instructions in the [pull request template](https://github.com/besu-eth/besu/blob/main/.github/pull_request_template.md).
3. Include appropriate test coverage. Testing is 100% automated; all submissions must be testable in an automated fashion.
4. Follow the coding conventions in the [Developing and Conventions](https://github.com/besu-eth/besu/wiki/Developing-and-Conventions) wiki page.
5. After you submit your pull request, verify that all [status checks](https://docs.github.com/articles/about-status-checks) are passing.

### What makes a good pull request?

#### One pull request, one change

- This limits the surface area of the change and makes it easier to identify root causes when issues arise.
- Make sure your PR doesn't include commits that are not part of it. This can happen if [your fork is not up to date](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/syncing-a-fork).

#### Minimize lines of code (LOC) per PR

- PRs get near-exponentially longer to review as the number of lines of code increases. Ideally, keep your changes under 300 LOC. If that's not possible, try to break your PR into smaller ones for reviewers to review sequentially.

#### Write meaningful commit messages

- Your commit messages should be meaningful, and the PR description should link to the related issue and comprehensively describe the changes.

#### Be responsive

- Don't let a PR sit idle with unaddressed comments until it needs a full rebase. If you are pausing work on an issue, indicate it in the PR comments or change the PR to draft status.

#### What if the status checks are failing?

- If a status check is failing and you believe the failure is unrelated to your change, leave a comment on the pull request explaining why. A maintainer will re-run the status check for you. If we conclude that the failure was a false positive, we will open an issue to track the problem with our status check suite.

## Changelog

Besu maintains a [`CHANGELOG.md`](CHANGELOG.md) so users can see what changed between releases. Include a changelog entry in the same PR as your change when it introduces any of the following:

- Future breaking changes or deprecated functionality.
- A new feature, with a short description of the functionality.
- A fixed bug, with a description of the user impact.

Add your entry under the `## Unreleased` section, in the appropriate subsection (for example, **Breaking Changes**, **Additions and Improvements**, or **Bug fixes**), as a single line ending with a link to your PR or issue. Where several PRs contribute to the same feature, the feature developer can add a single entry covering the whole feature.

## Code reviews

All changes must be code reviewed, preferably (and, for non-trivial changes, obligatorily) from someone who knows the areas the change touches. For non-trivial changes we may want two reviewers. The primary reviewer makes this decision and nominates a second reviewer if needed. Except for trivial changes, PRs should not be merged until relevant parties (for example, owners of the affected subsystem) have had a reasonable chance to look at the PR in their local business hours.

Most PRs will find reviewers organically. If an approver intends to be the primary reviewer of a PR, they should set themselves as the assignee on GitHub. Only the primary approver of a change should do the merge, except in rare cases (for example, they are unavailable in a reasonable timeframe).

If a PR has gone five working days without a reviewer emerging, you can ask on [Discord](https://discord.com/invite/hyperledger), however please don't ping or message individual maintainers.

## Developer Certificate of Origin (DCO)

All commits must be signed off to satisfy the [Developer Certificate of Origin](https://developercertificate.org/) (DCO). This certifies that you are able to submit your contribution under the license of the repository, and for it to be redistributed under that same license.

**TL;DR:** ensure all your commits have a sign-off. Git has a built-in mechanism for this via the `-s` (or `--signoff`) argument to `git commit`, provided your `user.name` and `user.email` are set up correctly:

```bash
git config user.name "FIRST_NAME LAST_NAME"
git config user.email "MY_NAME@example.com"
```

The sign-off must use your legal name, not a pseudonym. If you use the GitHub web UI for commits, make sure the `Signed-off-by` line uses the same email address as the commit author. This can be your GitHub `users.noreply.github.com` email if you keep your email address private.

For a git alias to sign off automatically, and for what to do when the DCO check fails on your PR, see [Working with DCO](https://github.com/besu-eth/besu/wiki/Contributing-Working-with-DCO) in the wiki.

### Guidelines for submitting agentic contributions

DCO sign-offs are required for all contributions, and only a human may sign off on a commit. Agents are encouraged to use the `Co-Authored-By` or `Assisted-By` keys in DCO statements, and to include their model name, version, and context size. Example:

```text
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
Signed-off-by: jflo <justin+github@florentine.us>
```

## Copyright and license

All new code submitted to Besu must be under the Apache License, Version 2.0, and all new documentation must be under the Creative Commons Attribution 4.0 International License, in line with the [Linux Foundation Decentralized Trust (LF Decentralized Trust) charter](https://www.lfdecentralizedtrust.org/about/charter). You may maintain copyright to your works under these clauses.

When you commit code, ensure you:

- Add your copyright if required.
- Include the SPDX license header using Apache 2.0. Otherwise, the build fails the license header check. This is enforced by `spotlessCheck`, and `spotlessApply` will add it automatically.

```java
/* SPDX-License-Identifier: Apache-2.0 */
```

Example header:

```java
/*
 * Copyright contributors to Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
```

## Security contributions

If you think you have discovered a security issue in Besu, please follow the responsible disclosure process described in [SECURITY.md](SECURITY.md). Please do not open a public issue for security vulnerabilities.

## Guidelines for non-code and other trivial contributions

Please keep in mind that we do not accept non-code contributions like fixing comments, typos, or other trivial fixes. Although we appreciate the extra help, managing lots of these small contributions is unfeasible and puts extra pressure on our continuous delivery systems (running all tests, and so on). Feel free to open an issue pointing out any of these errors, and we will batch them into a single change.

## Other important information

- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Governance](CHARTER.md)

## Contributing to specific areas

For deeper, area-specific guides, see the [Besu wiki](https://github.com/besu-eth/besu/wiki), including:

- [Besu CLI Style Guide](https://github.com/besu-eth/besu/wiki/Developing-and-Conventions-Besu-CLI-Style-Guide)
- [Testing](https://github.com/besu-eth/besu/wiki/Developing-and-Conventions-Testing)
- [Logging](https://github.com/besu-eth/besu/wiki/Developing-and-Conventions-Logging)
- [Building from source](https://github.com/besu-eth/besu/wiki/Developing-and-Conventions-Building-from-Source)
- [Working with DCO](https://github.com/besu-eth/besu/wiki/Contributing-Working-with-DCO)
