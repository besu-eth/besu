DCO
===

All code submitted to Besu must have a [Developer Certificate of Origin](https://developercertificate.org/) (DCO) sign-off, in line with the [Linux Foundation Decentralized Trust (LF Decentralized Trust) charter](https://www.lfdecentralizedtrust.org/about/charter).

The sign-off must use your legal name, not a pseudonym. Git has a built-in mechanism to add it via the `-s` or `--signoff` argument to `git commit`, provided your `user.name` and `user.email` have been set up correctly.

TL;DR:

If you don't want to break the DCO check, ensure all your commits have a sign-off.

`git config user.name "FIRST_NAME LAST_NAME"`
`git config user.email "MY_NAME@example.com"`

If you use the GitHub web UI for commits, make sure the `Signed-off-by` line uses the same email address as the commit author. This can be your GitHub `users.noreply.github.com` email if you keep your email address private.

You can also set up a git global alias.

More info, including how to set up an alias and what to do if the DCO check is failing on your PR, is in the [Working with DCO](https://github.com/besu-eth/besu/wiki/Contributing-Working-with-DCO) wiki page.

If you have any questions, you can reach us on [Discord](https://discord.com/invite/hyperledger).
