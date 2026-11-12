# How we would like patches made

## Size and scope of a fix
- Fix the root cause, as narrowly as possible. If the same faulty pattern appears at other call sites, fix them in the same patch and list them.
- Bound input before it is decoded or allocated, not after. When Besu already has a limit for the same data, reuse it rather than adding a new constant.
- When a fix touches transaction execution or the world state, make it hold for both sequential and parallel transaction processing, and say how you checked.
- Do not change behaviour that the Ethereum specifications require. Do not reject data that the other execution clients accept, unless the specification requires it.
- Avoid new command-line options. If one is needed, give it a safe default and describe it.
- No new dependencies, and no changes to `plugin-api` unless needed: its compatibility is checked against the last release.
- Target the `main` branch.

## Code style
- Follow the style of the surrounding code. Format with `./gradlew spotlessApply`.
- The code must compile with Error Prone and NullAway, whose warnings are errors in this build.
- New files start with the Apache 2.0 header used in the repository, with the line `Copyright contributors to Besu.`.
- Concise comments explain why the code does what it does. Describe the behaviour, not the attack.

## Tests
- Add a regression test next to the existing tests of the changed class (JUnit 5, AssertJ, Mockito). It must fail without the fix and pass with it.
- Run the tests of the changed module, e.g. `./gradlew --offline :ethereum:eth:test`, and report the result.
