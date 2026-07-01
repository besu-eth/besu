# Besu Ethereum Client
 [![CodeQL](https://github.com/besu-eth/besu/actions/workflows/codeql.yml/badge.svg)](https://github.com/besu-eth/besu/actions/workflows/codeql.yml)
 [![OpenSSF Best Practices](https://www.bestpractices.dev/projects/3174/badge)](https://www.bestpractices.dev/en/projects/3174)
 [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/besu-eth/besu/blob/main/LICENSE)
 [![Discord](https://img.shields.io/discord/905194001349627914?logo=Hyperledger&style=plastic)](https://discord.com/invite/hyperledger)
 [![X Follow](https://img.shields.io/twitter/follow/Besu_eth)](https://x.com/Besu_eth)

[Download](https://github.com/besu-eth/besu/releases)

Besu is an Apache 2.0 licensed, Mainnet compatible, Ethereum client written in Java.

## Useful Links

* [Besu User Documentation]
* [Besu Issues]
* [Besu Wiki](https://github.com/besu-eth/besu/wiki)
* [How to Contribute to Besu][Contributing Guidelines]

## Community and support

If you have a question, need help, or want to engage with the Besu community, the following resources are available.

### Documentation

The [Besu documentation](https://docs.besu-eth.org/) answers many common questions. Some useful starting points:

* [System requirements](https://docs.besu-eth.org/public-networks/get-started/system-requirements)
* [Troubleshoot peering](https://docs.besu-eth.org/public-networks/how-to/troubleshoot/peering)
* [Troubleshoot performance](https://docs.besu-eth.org/public-networks/how-to/troubleshoot/performance)
* [Configure ports](https://docs.besu-eth.org/public-networks/how-to/connect/configure-ports)
* [Understand metrics](https://docs.besu-eth.org/public-networks/how-to/monitor/understand-metrics)
* [Configure the JVM](https://docs.besu-eth.org/public-networks/how-to/configure-jvm)

### Chat

* Join the [Besu Discord](https://discord.com/invite/hyperledger): `#besu` to interact with the dev team and get support, and `#besu-contributors` if you are interested in contributing to the client.
* Besu is an execution client and must be paired with a consensus client. If you are also running the [Teku](https://github.com/Consensys/teku) consensus client, the [Consensys Discord](https://discord.com/invite/consensys) is useful too (Mainnet Clients -> `#teku`).

### GitHub

The [Besu GitHub repository](https://github.com/besu-eth/besu) tracks recent releases, patch notes, and known issues with up-to-date status on fixes and mitigations. If Discord can't resolve your problem, [search the existing issues](https://github.com/besu-eth/besu/issues) and, if needed, [open a new issue](https://github.com/besu-eth/besu/issues/new/choose) with relevant logs and context.

### Announcements

Version announcements are posted in the Discord announcements channel. Occasionally, the team also posts emergency alerts and support information on Discord and [X](https://x.com/Besu_eth).

## Issues 

Besu issues are tracked [in the github issues tab][Besu Issues].
See our [contributing guidelines][Contributing Guidelines] for more details on searching and creating issues.

If you have any questions, queries or comments, [Besu channel on Discord] is the place to find us.


## Besu Users

To install the Besu binary, follow [these instructions](https://docs.besu-eth.org/public-networks/get-started/install/binary-distribution).    

## Besu Developers

* [Contributing Guidelines]
* [Coding Conventions](https://github.com/besu-eth/besu/wiki/Developing-and-Conventions)
* [Command Line Interface (CLI) Style Guide](https://github.com/besu-eth/besu/wiki/Developing-and-Conventions-Besu-CLI-Style-Guide)
* [Besu User Documentation] for running and using Besu


### Development

Instructions for how to get started with developing on the Besu codebase. Please also read the
[contributing guidelines][Contributing Guidelines] for more details on how to submit a pull request (PR).  

* [Checking Out and Building](https://github.com/besu-eth/besu/wiki/Developing-and-Conventions-Building-from-Source)
* [Logging](https://github.com/besu-eth/besu/wiki/Developing-and-Conventions-Logging) or the [Documentation's Logging section](https://docs.besu-eth.org/public-networks/how-to/monitor/logging)

#### Dependency Verification

This project uses [Gradle dependency verification](https://docs.gradle.org/current/userguide/dependency_verification.html). When adding or updating dependencies, regenerate `gradle/verification-metadata.xml` with:

```shell
./gradlew --write-verification-metadata sha256 resolveSourceArtifacts
```

The `resolveSourceArtifacts` task ensures source JARs are included in the metadata, which is required for IDE sync (e.g. IntelliJ automatically downloads sources).

### Profiling Besu

Besu supports performance profiling using [Async Profiler](https://github.com/async-profiler/async-profiler), a low-overhead sampling profiler.  
You can find setup and usage instructions in the [Profiling Guide](docs/PROFILING.md).

Profiling can help identify performance bottlenecks in block processing, transaction validation, and EVM execution.  
Please ensure the profiler is run as the same user that started the Besu process.

## Release Notes

[Release Notes](CHANGELOG.md)

## Reference Tests and JSON Tracing

Besu includes support for running Ethereum reference tests and generating detailed EVM execution traces.

To learn how to run the tests and enable opcode-level JSON tracing for debugging and correctness verification, see the [Reference Test Execution and Tracing Guide](REFERENCE_TESTS.md).

[Besu Issues]: https://github.com/besu-eth/besu/issues
[Besu User Documentation]: https://docs.besu-eth.org
[Besu channel on Discord]: https://discord.com/invite/hyperledger
[Contributing Guidelines]: CONTRIBUTING.md
