# Security Policy

## Reporting a Vulnerability

If you believe you have found a security vulnerability in Besu, please report it privately.
**Do not open a public issue.**

### Preferred method — GitHub private vulnerability reporting

Use the **[Report a vulnerability](https://github.com/besu-eth/besu/security/advisories/new)** button
in the Security tab of this repository. This opens a private draft advisory visible only to
maintainers. It is the fastest path to triage and allows structured, confidential collaboration
between you and the team throughout the disclosure process.

GitHub's private vulnerability reporting is the recommended approach for most reports.

### Alternative — email

For highly sensitive reports where you prefer not to use GitHub, you can contact the team by email:

- **[security-besu@lists.hyperledger.org](mailto:security-besu@lists.hyperledger.org)** — reaches a
  subset of Besu maintainers and LF Decentralized Trust staff. Use this for reports that are
  particularly sensitive.
- **[security@hyperledger.org](mailto:security@hyperledger.org)** — reaches security staff across all
  LF Decentralized Trust projects. Note that maintainers outside of Besu may have access to this list.

When reporting by email, include a description of the vulnerability and any relevant detail:
reproduction steps, affected versions, and any known active exploitation.

### How we handle reports

- Acknowledgement — we aim to acknowledge receipt within 5 business days.
- Triage — the security list assesses severity using the Defect Prioritization Guide. We will keep you informed of our assessment.
- Fix development — fixes are developed privately. For issues that affect other Ethereum clients or require ecosystem-wide coordination, we follow the Ethereum security disclosure process and work with relevant teams before releasing.
- Release — the fix ships in a Besu release. We will notify you before the release if possible.
- Disclosure — after a fixed version has been available for a reasonable period, we publish a GitHub Security Advisory (GHSA) with the vulnerability details, affected versions, fix version, and reporter credit (with your consent).
- We do not publish vulnerability details before a fix is available in a released version.

For more detail on how the security team handles reports, see our
[Security Policy](https://github.com/besu-eth/besu/wiki/Security-Policy).
