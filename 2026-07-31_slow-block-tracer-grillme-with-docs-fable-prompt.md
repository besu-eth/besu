/mattpocock-skills:grill-with-docs I am implementing a new slow block tracing feature in Besu. I already have a PR which takes a minimalist approach and implements metrics only for serial transaction processing: https://github.com/besu-eth/besu/pull/10746
That's a good initial reference that shows the scope of the feature, but based on feedback from a meeting with Karim, I want to plan a new POC implementation taking a different
approach to that PR.
I've got an updated requirements doc, @2026-07-31_slow-block-tracing-requirements-take2.md  and also some meeting notes from a meeting with Karim
@2026-07-30_Slow-Block-Tracer-PR-Meeting-with-Karim.md
It is crucial to read both these docs.
The main difference with the new approach exploration is to record slow block tracer metrics when BAL feature (and therefore parallel transaction execution) is enabled, more details in
the requirements doc. We want to reuse the code that is already tracking state changes for BAL. In order to track some metrics such as repeated rather than unique reads, we may need
to further customise/decorate existing BAL behaviour for this tracing use case. Some non-state related features may remain the same as my original PR unless BAL approach is superior.
