---
name: Bug Report
about: necessary information to help us resolve the issue
title: ''
labels: 'bug'
assignees: ''

---

<!-- Have you done the following? -->
<!--   * read the Code of Conduct? By filing an Issue, you are expected to -->  
<!--     comply with it, including treating everyone with respect: -->
<!--     https://github.com/besu-eth/besu/blob/main/CODE_OF_CONDUCT.md -->
<!--   * Reproduced the issue in the latest version of the software -->
<!--   * Read the debugging docs: https://besu.hyperledger.org/private-networks/how-to -->
<!--   * Duplicate Issue check:  https://github.com/search?q=+is%3Aissue+repo%3Abesu-eth/besu -->

### Steps to Reproduce
1. [Step 1]
2. [Step 2]
3. [Step ...]

**Expected behavior:** [What you expect to happen]

**Actual behavior:** [What actually happens]

**Frequency:** [What percentage of the time does it occur?]

### Logs
Please post relevant logs from Besu (and the consensus client, if running proof of stake) from before and after the issue.

### Versions (Add all that apply)
* Software version: [`besu --version`]
* Java version: [`java -version`]
* OS Name & Version: [`cat /etc/*release`]
* Kernel Version: [`uname -a`]
* Virtual Machine software & version: [`vmware -v`]
* Docker Version: [`docker version`]
* Cloud VM, type, size: [Amazon Web Services I3-large]
* Consensus Client & Version if using Proof of Stake: [e.g. Teku, Lighthouse, Prysm, Nimbus, Lodestar]

### Smart contract information (If you're reporting an issue arising from deploying or calling a smart contract, please supply related information)
* Solidity version [`solc --version`]
* Repo with minimal set of deployable/reproducible contract code - please provide a link
* Please include specifics on how you are deploying/calling the contract
* Have you reproduced the issue on other eth clients

### Additional Information (Add any of the following or anything else that may be relevant)
* Besu setup info - genesis file, config options
* System info - memory, CPU
