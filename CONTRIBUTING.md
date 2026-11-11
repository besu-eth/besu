# Contributing to Besu

## :tada: Thanks for taking the time to contribute! :tada:

Welcome to the Besu repository! The following is a set of guidelines for contributing to this repo and its packages. These are mostly guidelines, not rules. Use your best judgment, and feel free to propose changes to this document in a pull request. Contributions come in the form of code submissions, writing documentation, raising issues, helping others in chat, and any other actions that help develop Besu.

This document covers everything you need to know to decide whether and how you want to contribute. For deeper, area-specific guides, we link out to the [Besu wiki](https://github.com/besu-eth/besu/wiki).

## Contents

- [Where to start?](#where-to-start)
- [Reporting bugs](#reporting-bugs)
- [Suggesting enhancements](#suggesting-enhancements)
- [Pull requests](#pull-requests)
- [Changelog](#changelog)
- [Code reviews](#code-reviews)
- [Developer Certificate of Origin (DCO)](#developer-certificate-of-origin-dco)
- [AI-assisted contributions](#ai-assisted-contributions)
- [Copyright and license](#copyright-and-license)
- [Security contributions](#security-contributions)
- [Other important information](#other-important-information)
- [Contributing to specific areas](#contributing-to-specific-areas)

## Where to start?
A [GitHub account](https://github.com) is required for code and issue contributions. Join our [Discord](https://discord.com/invite/hyperledger) to ask questions or chat with maintainers, contributors, operators, and the broader Besu community.

Questions? You might find the answer in the [Besu documentation](https://docs.besu-eth.org/), or you can ask in the [Besu Discord](https://discord.com/invite/hyperledger) or browse the [wiki](https://github.com/besu-eth/besu/wiki). Please don't file an issue to ask a question. You'll get faster results by using the resources above.

The first step is deciding what to work on! We use labels to identify issues that we think are [a good place to start](https://github.com/besu-eth/besu/contribute).

First-time contributors should start with smaller PRs to understand the process and build trust.

- Browse the [good first issues](https://github.com/besu-eth/besu/labels/good%20first%20issue). These should only require a few lines of code or documentation, and maybe a test or two.
- Browse the [help wanted issues](https://github.com/besu-eth/besu/labels/help%20wanted). These are a bit more involved than good first issues.

If you find an issue you'd like to work on, reach out on [Discord](https://discord.com/invite/hyperledger) in the **#besu** channel, and we can assign it to you. If you have a different idea, reach out, and we can discuss it.

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

- **Which version of Besu are you using?** Get the exact version by running `besu --version`.
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

The basics:
1. Ensure all commits have a sign-off for the DCO, as described in [Developer Certificate of Origin (DCO)](#developer-certificate-of-origin-dco).
2. Follow all instructions in the [pull request template](https://github.com/besu-eth/besu/blob/main/.github/pull_request_template.md).
3. Follow the coding conventions in the [Developing and Conventions](https://github.com/besu-eth/besu/wiki/Developing-and-Conventions) wiki page.

### What makes a good pull request?
There's a lot more to a PR than just the code: starting with a clear description of the change, and including testing, verification, evidence that there was a problem and the PR is addressing it. Writing the code is often the quickest part of the PR to produce.
Look at your PR through the eyes of a reviewer. Human review is without doubt the bottleneck in this process right now. If you make life easier for reviewers, this will expedite review of your PR.

### Pitch your PR
- The PR title should be short and descriptive.
- The PR description should succinctly describe the changes. If there is a related issue, link to it with a GitHub keyword such as `fixes #1234` so the issue closes when the PR is merged, or `refs #1234` if it doesn't fully resolve it. If there is no issue, for example for a test-only change or a small refactor, the pitch below should explain the motivation instead.
- A short one sentence "pitch" at the top of the PR description, explaining why reviewers should prioritize your PR is helpful. What's the impact, what's the problem you're solving?
- Then also include evidence to justify your PR
  - evidence that the problem exists - logs, errors, metrics - this adds weight to your PR.
  - evidence that you have fixed the problem - tests run (auto or manual), reproduction steps, any steps you have done to verify that the fix is correct.
- We prioritize PRs that have impact on production scenarios.

Please keep in mind that we reserve the right to reject trivial PRs, such as fixing comments, typos, or other trivial fixes. Although we appreciate the extra help, managing lots of these small contributions is unfeasible and puts extra pressure on our continuous delivery systems (running all tests, and so on). Feel free to open an issue pointing out any of these errors, and we will batch them into a single change.

#### Keep it simple
- One pull request, one change - this limits the surface area of the change and makes it easier to identify root causes when issues arise.
- Don't mix formatting fixes or code moves with actual code changes; they make the diff hard to review. Put them in a separate PR.
- Make sure your PR doesn't include unrelated commits. This can happen if [your fork is not up to date](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/syncing-a-fork).
- Minimize lines of code (LOC) per PR. Ideally, keep your changes under 300 LOC. Break up larger changes into smaller PRs for reviewers to review sequentially.
- Your commit messages should be meaningful.

#### Engage in the process
- We expect you to be accountable for the changes you are proposing. Be ready to answer questions and address feedback.
- Include appropriate test coverage. Testing is 100% automated; all submissions must be testable in an automated fashion.
- Run relevant [status checks](https://docs.github.com/articles/about-status-checks) locally: at a minimum, run spotless and the tests related to your change. Don't let a reviewer find that your code doesn't pass basic checks.
- If a status check fails and you believe the failure is unrelated to your change, say so in a PR comment, and a maintainer will re-run it.
- If your PR is not ready for review, open it as a draft.
- Don't let a PR sit idle with unaddressed comments until it needs a complex rebase. If you are pausing work on an issue, indicate it in the PR comments or change the PR to draft status.
- Once review has started, prefer merging `main` into your branch over rebasing and force-pushing: after a force-push it's harder for reviewers to tell which changes they have already reviewed.
- If you allow maintainers to push to your branch, this makes merging easier once your PR has been approved.

### Further considerations
- Security - please consider security implications of your PR e.g. if you're adding a new RPC, make sure limits are bounded. Reviewers will be looking for this.
- Performance - if your PR potentially improves performance, provide justification for any claims. Refer to [benchmarking](BENCHMARKING.md) for docs on running benchmarks.
- Performance - on the other hand, if your PR could detrimentally affect performance, provide proof that there is no regression.
- If you want to work on something more involved than a good first issue, reach out to us on Discord. The [Ethereum Protocol Fellowship (EPF)](https://epf.wiki/#/wiki/epf) and the [LF Decentralized Trust Mentorship Program](https://lf-decentralized-trust-mentorships.github.io/mentorship-program/latest/) are opportunities for project-based work.
- Issues labeled P1 or P2 are urgent and are handled by maintainers, so please pick a different issue.

## Changelog

Besu maintains a [`CHANGELOG.md`](CHANGELOG.md) so users can see what changed between releases. Include a changelog entry in the same PR as your change when it introduces any of the following:

- Future breaking changes or deprecated functionality.
- A new feature, with a short description of the functionality.
- A fixed bug, with a description of the user impact.

Add your entry under the `## Unreleased` section, in the appropriate subsection (for example, **Breaking Changes**, **Additions and Improvements**, or **Bug fixes**), as a single line ending with a link to your PR or issue. Where several PRs contribute to the same feature, the feature developer can add a single entry covering the whole feature.

## Code reviews

All changes must be code reviewed, preferably from someone who knows the areas the change touches. For non-trivial changes we may want multiple reviewers. The primary reviewer can make this decision and nominates a second reviewer if needed. We also have CODEOWNERS which mandates specific reviewers for specific parts of the codebase.

Except for trivial or urgent changes, PRs should not be merged until all relevant parties have had a reasonable chance to look at the PR in their local business hours. As a general rule, PRs remain open for at least 24 hours.

Most PRs will find reviewers organically. If an approver intends to be the primary reviewer of a PR, they should set themselves as the assignee on GitHub. Only the primary approver of a change should do the merge, except in rare cases (for example, they are unavailable in a reasonable timeframe).

If a PR has gone five working days without a reviewer emerging, you can ask on [Discord](https://discord.com/invite/hyperledger), however please don't ping or message individual maintainers.

Expect your PR to go through automated agentic checks and code reviews, and respond to these comments as you would for a human reviewer. This helps reduce the review burden on maintainers.

Reviewers (maintainers) will give feedback, ask questions, make suggestions, and can also convert PRs to draft, request changes and/or assign PRs back to the author. This is all public and makes the status of the PRs transparent for you as the author and other reviewers.

Reviewers will be using AI to assist code reviews, but we do also expect that reviewers develop an understanding of the code they are reviewing.

Note that a maintainer must approve the status checks workflows to run in CI, this normally happens as part of PR review.

Some parts of the codebase require more scrutiny than others, e.g. the EVM hot path and GitHub Actions. If your PR touches these areas, you can expect the review process to be more thorough and time-consuming.

## Developer Certificate of Origin (DCO)

All commits must be signed off to satisfy the [Developer Certificate of Origin](https://developercertificate.org/) (DCO). This certifies that you are able to submit your contribution under the license of the repository, and for it to be redistributed under that same license.

**TL;DR:** ensure all your commits have a sign-off. Git has a built-in mechanism for this via the `-s` (or `--signoff`) argument to `git commit`, provided your `user.name` and `user.email` are set up correctly:

```bash
git config user.name "FIRST_NAME LAST_NAME"
git config user.email "MY_NAME@example.com"
```

The sign-off must use your legal name, not a pseudonym. If you use the GitHub web UI for commits, make sure the `Signed-off-by` line uses the same email address as the commit author. This can be your GitHub `users.noreply.github.com` email if you keep your email address private.

For a git alias to sign off automatically, and for what to do when the DCO check fails on your PR, see [Working with DCO](https://github.com/besu-eth/besu/wiki/Contributing-Working-with-DCO) in the wiki.

## AI-assisted contributions

At this point we assume all contributors are using AI to assist in implementation and review tasks. We do still expect that you are a human in the loop who is adding value, and you are taking responsibility for the code that you as the PR author have submitted.

Use AI as a tool to produce quality output (as opposed to quantity). For example, run an adversarial agentic code review, and fix any findings, before pushing your PR.

DCO sign-offs are required for all contributions, and only a human may sign off on a commit. Agents are encouraged to add a `Co-Authored-By` or `Assisted-By` trailer to the commit message, and to include their model name, version, and context size. Example:

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

If you think you have discovered a security issue in Besu, please follow the responsible disclosure process described in [SECURITY.md](SECURITY.md). Please do not open a public issue or PR for security vulnerabilities.

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

You contributed to Besu! Thanks!
