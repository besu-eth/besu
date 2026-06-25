# Contributing to Besu
## :tada: Thanks for taking the time to contribute! :tada:

Welcome to the Besu repository! The following links are a set of guidelines for contributing to this repo and its packages. These are mostly guidelines, not rules. Use your best judgement, and feel free to propose changes to this document in a pull request. Contributions come in the form of code submissions, writing documentation, raising issues, helping others in chat, and any other actions that help develop Besu.

### GitHub/Discord Accounts

Having the following accounts is necessary for contributing code/issues to Besu.  
* If you want to contribute code, you can make a [github account here](https://github.com).  
* If you want to raise an issue, do so [in the issues tab](https://github.com/besu-eth/besu/issues).
* To ask questions or chat with us, join our [Discord](https://discord.com/invite/hyperledger)

### Useful contributing links

* [I just have a quick question](https://docs.besu-eth.org/)
* [First contribution workflow](https://github.com/besu-eth/besu/wiki/Contributing-First-Contribution)
* [Did you find a bug?](https://wiki.hyperledger.org/display/BESU/Reporting+Bugs)
* [Issues](https://wiki.hyperledger.org/display/BESU/Issues)
* [DCO](DCO.md)
* [Suggesting Enhancements](https://wiki.hyperledger.org/display/BESU/Suggesting+Enhancements)
* [Pull Requests](https://wiki.hyperledger.org/display/BESU/Pull+Requests)
* [Code Reviews](https://wiki.hyperledger.org/display/BESU/Code+Reviews)


* [Security contributions](https://wiki.hyperledger.org/display/BESU/Security)

### Other important information

* [Code of Conduct](CODE_OF_CONDUCT.md)
* [Governance](CHARTER.md)

### Guidelines for PR Quality

With the advent of agentic coding tools it has become far easier to create and submit a pull request, than it is to responsibly review it. In order to protect maintainers time, we must set a very high quality bar for PRs to meet. 

The PR description and summary are critical. Agentic summaries tend to be overly verbose; care should be taken to write these as concisely as possible, while still directing reviewers to the highest impact parts of the code to pay attention to. 

CI/CD will not be run on incoming PRs till after review for security reasons. Passing CI/CD should be a foregone conclusion, and contributors are responsible for running tests locally before submission.

Please keep in mind that we do not accept non-code contributions like fixing comments, typos or some other trivial fixes. Although we appreciate the extra help, managing lots of these small contributions is unfeasible, and puts extra pressure in our continuous delivery systems (running all tests, etc). Feel free to open an issue pointing any of those errors, and we will batch them into a single change.

The Github ids of contributors who repeatedly submit PRs that fail to meet these standards will be noted, and deprioritized for review.

### Guidelines for submitting agentic contributions.
DCO sign-offs are required for all contributions, and only a human may sign off on a commit. Agents are encouraged to use the Co-Author or Assisted-By keys in DCO statements, and to include their model name, version and context size. example:

```
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
Signed-off-by: jflo <justin+github@florentine.us>
```

