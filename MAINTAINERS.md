# Maintainers

<!-- Please keep all lists sorted alphabetically by github -->

## Active Maintainers

<!-- besu-maintainers group has maintainer access to besu repo -->

| Name             | Github           | LFID             |
| ---------------- | ---------------- | ---------------- |
| Ameziane Hamlat   | ahamlat         | ahamlat          |
| Daniel Lehrner   | daniellehrner    | daniellehrner    |
| Diego López León | diega            | diega            |
| Fabio Di Fabio   | fab-10           | fab-10           |
| Gabriel Trintinalia | gabriel-trintinalia | gabrieltrintinalia |
| Gary Schulte     | garyschulte      | GarySchulte      | 
| Gabriel Fukushima| gfukushima       | gfukushima       |
| Justin Florentine| jflo             | RoboCopsGoneMad  |
| Jason Frame      | jframe           | jframe           |
| Luis Pinto       | lu-pinto         | lu-pinto         |
| Lucas Saldanha   | lucassaldanha    | lucassaldanha    |
| Sally MacFarlane | macfarla         | macfarla         |
| Matilda Clerke   | Matilda-Clerke   | MatildaClerke    |
| Karim Taam       | matkt            | matkt            |
| Matthew Whitehead| matthew1001      | matthew.whitehead      |
| Miroslav Kovář   | mirgee           | mirgee           |
| Stefan Pingel    | pinges           | pinges           |
| Simon Dudley     | siladu           | siladu           |
| Usman Saleem     | usmansaleem      | usmansaleem      |


## Emeritus Maintainers

| Name             | Github           | LFID             |
|------------------|------------------|------------------|
| Abdel Bakhta     | abdelhamidbakhta | abdelhamidbakhta |
| Adrian Sutton    | ajsutton         | ajsutton         |
| Antony Denyer    | antonydenyer     | antonydenyer     |
| Antoine Toulme   | atoulme          | atoulme          |
| Byron Gravenorst | bgravenorst      | bgravenorst      |
| Chaminda Divitotawela | cdivitotawela    | cdivitotawela    |
| Chris Hare       | CjHare           | cjhare           |
| David Mechler    | davemec          | davemec          |
| Edward Evans     | EdJoJob          | EdJoJob          |
| Edward Mack      | edwardmack       | mackcom          | 
| Frank Li         | frankisawesome   | frankliawesome   |
| Jiri Peinlich    | gezero           | JiriPeinlich     |
| Ivaylo Kirilov   | iikirilov        | iikirilov        |
| Joshua Fernandes | joshuafernandes  | joshuafernandes  |
| Madeline Murray  | MadelineMurray   | madelinemurray   |
| Mark Terry       | mark-terry       | m.terry          |
| Meredith Baxter  | mbaxter          | mbaxter          |
| Nicolas Massart  | NicolasMassart   | NicolasMassart   |
| Bhanu Pulluri    | pullurib         | bpulluri         |
| Trent Mohay      | rain-on          | trent.mohay      |
| Rai Sur          | RatanRSur        | ratanraisur      |
| Rob Dawson       | rojotek          | RobDawson        |
| Sajida Zouarhi   | sajz             | SajidaZ          |
| Danno Ferrin     | shemnon          | shemnon          |
| Taccat Isid      | taccatisid       | taccatisid       |
| Tim Beiko        | timbeiko         | timbeiko         |
| Vijay Michalik   | vmichalik        | VijayMichalik    |
| Zhenyang Shi     | wcgcyx           | wcgcyx           |

## Becoming a Maintainer

Besu welcomes community contribution.
Each community member may progress to become a maintainer.

How to become a maintainer:

- Contribute significantly to the code in this repository.
  
### Maintainers contribution requirement

The requirement to be able to be proposed as a maintainer is:

- 5 significant changes on code have been authored in this repos by the proposed maintainer and accepted (merged PRs).
  
### Maintainers approval process

The following steps must occur for a contributor to be "upgraded" as a maintainer:

### PR proposed
- The proposed maintainer has the sponsorship of at least one other maintainer.
  - This sponsoring maintainer will create a proposal PR modifying the list of
    maintainers. (see [proposal PR template](#proposal-pr-template).)
  - The proposed maintainer accepts the nomination and expresses a willingness
    to be a long-term (more than 6 month) committer by adding a comment in the proposal PR.
  - The PR will be communicated in all appropriate communication channels
    including at least [besu-contributors channel on Discord](https://discord.com/invite/hyperledger),
    the [mailing list](https://lists.hyperledger.org/g/besu)
    and any maintainer/community call.

### Voting
- Approval by at least 3 current maintainers within two weeks of the proposal or
  an absolute majority (half the total + 1) of current maintainers.
  - Maintainers vote by approving the proposal PR.
- No veto raised by another maintainer within the voting timeframe.
  - All vetoes must be accompanied by a public explanation as a comment on the
    proposal PR.
  - The explanation of the veto must be reasonable and follow the [Besu code of conduct](https://wiki.hyperledger.org/display/BESU/Code+of+Conduct).
  - A veto can be retracted, in that case, the voting timeframe is reset, and all approvals are removed.
  - It is bad form to veto, retract, and veto again.
  
The proposed maintainer becomes a maintainer either:

  - when two weeks have passed without a veto since the third approval of the proposal PR,
  - or an absolute majority of maintainers approved the proposal PR.

In either case, no maintainer raised and stood by a veto.

## Removing Maintainers

Being a maintainer is not a status symbol or a title to be maintained indefinitely.

It will occasionally be necessary and appropriate to move a maintainer to emeritus status.

This can occur in the following situations:

- Resignation of a maintainer.
- Violation of the Code of Conduct warranting removal.
- Inactivity.
  - A general measure of inactivity will be no commits or code review comments
    for two reporting quarters, although this will not be strictly enforced if
    the maintainer expresses a reasonable intent to continue contributing.
  - Reasonable exceptions to inactivity will be granted for known long term
    leave such as parental leave and medical leave.
- Other unspecified circumstances.

### PR proposed
- A PR listing the justification for moving the maintainer to emeritus (most commonly inactivity) is proposed.

### Voting
The voting process for moving a maintainer to emeritus status is the same as for adding a new maintainer.

Returning to active status from emeritus status uses the same steps as adding a
new maintainer.

Note that the emeritus maintainer always already has the required significant contributions.
There is no contribution prescription delay.

## Proposal PR template

```markdown
I propose to add [maintainer github handle] as a Besu project maintainer.

[maintainer github handle] contributed with many high quality commits:

- [list significant achievements]

Here are [their past contributions on Besu project](https://github.com/besu-eth/besu/commits?author=[user github handle]).

Voting ends two weeks from today.

For more information on this process, see the MAINTAINERS.md file.
```
## Modifying this file
Other than adding and removing maintainers, any changes to this file (MAINTAINERS.md) must be proposed in a PR and agreed upon by 2/3 of current maintainers.
