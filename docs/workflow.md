# Workflow

This procedure governs all changes to the system — bug fixes and new features
alike. Every change is either correcting behavior we thought was right (bug fix)
or providing behavior we didn't have before (new feature). In both cases, the
process is the same: demonstrate that the system doesn't exhibit the desired
behavior, then make it so.

## Why this process exists

Without a failing test asserting correct behavior, there is no evidence that a
change is needed. Without evidence that a change is needed, there is no way to
tell whether the changes being made actually contribute to achieving the desired
behavior. This is the scientific method applied to software: state a hypothesis
("the system should do X"), run an experiment (the test), and observe whether
the hypothesis holds. Only when the hypothesis is rejected (test fails) do we
have justification to change the system.

A failing test that asserts correct behavior proves one of two things: either
the system has a bug, or the system lacks logic that should exist. A passing
test that asserts correct behavior proves the system already behaves as expected
— no change is justified for that behavior.

Tests must always assert correct behavior and nothing else. A test that asserts
wrong behavior tells us nothing about the system. Even following this process
mechanically without understanding the reasoning leads to mistakes like thinking
"asserting wrong behavior" achieves the purpose of writing failing tests first.
It does not — it just produces noise that validates nothing.

## Starting level

- **New feature**: Start with the spec. Define the desired behavior in SPEC.md
  first. Then write an e2e test asserting that the system exhibits the specified
  behavior. This fails, proving the system doesn't yet do what the spec says it
  should.
- **Production bug**: Start with an e2e test reproducing the exact failure path
  observed in production, asserting the correct behavior the system should have
  exhibited. This fails, confirming the bug exists.
- **Bug identified at unit or integration level that doesn't (yet) produce
  incorrect system-level behavior**: Start at the level where the incorrect
  behavior exists. Write a failing integration or unit test reproducing the
  issue, then drill down to the root cause and build back up with logic changes.

## The process

### Reproduce with a failing test

Write a test at the appropriate starting level (see above) that asserts the
correct behavior the system should exhibit. This test must fail with the current
code. If it passes, the system already behaves correctly and no change is
justified — either the test doesn't reproduce the issue or the issue doesn't
exist. This failing test is the top-level hypothesis: "the system should behave
this way, but it doesn't."

Sometimes an existing test already has the scenario you need but doesn't assert
enough, or needs a few extra steps at the beginning or end to cover the case. In
that situation, extend the existing test rather than writing a new one from
scratch. However, only extend if the scenario remains the same — if you need a
different scenario, add a new test case. Modifying an existing test's scenario
risks reducing coverage surface and introducing regressions.

### Identify the source of incorrect behavior

Analyze why the test fails. The root cause will be at one of two levels:

- **Unit level**: A component has incorrect logic, or a component that should
  exist doesn't.
- **Integration level**: Individual components work correctly in isolation, but
  they aren't connected properly.

### Unit-level fix (when root cause is at the unit level)

Define any new types, traits, and function signatures needed. Use `todo!()` for
bodies. `cargo check` must pass. No behavioral logic yet.

Write unit tests for the component in question, asserting what it should do.
These fail because the logic doesn't exist yet (new unit) or is buggy (existing
unit). Each failing test is a hypothesis: "this unit should behave this way." As
above, extend existing tests if they already have the right scenario but lack
sufficient assertions.

Implement the unit logic. Fill in the `todo!()` stubs or fix the buggy logic.
Unit tests now pass, confirming the hypothesis. The top-level test still fails
because the unit isn't integrated into the system yet.

### Integration-level fix (when root cause is at the integration level)

If individual components already work correctly but aren't connected, there are
no unit-level hypotheses to validate — skip straight to integration.

Adjust signatures, add parameters, update wiring types. Use `todo!()` for new
wiring logic. `cargo check` must pass.

Write integration tests that verify the components are connected properly,
asserting the correct integrated behavior. These fail because the wiring doesn't
exist yet.

Wire the components together. Fill in the integration logic. Integration tests
now pass, and the top-level test should pass too.

### Iterate if needed

Sometimes the top-level test still fails after fixing one slice. This means
another unit or integration is also contributing to the incorrect behavior.
Apply the same approach: hypothesize which unit or integration is the next
culprit or missing piece, write a failing test asserting correct behavior to
validate that hypothesis, then fix it. Only make changes after rejecting the
null hypothesis ("this component is NOT the problem / missing piece").

### Cleanup

**Correct ordering:** SPEC.md describes the desired behavior and must be updated
_before_ implementation begins — not after. The sequence is:

1. **Update SPEC.md first** with any new or changed behavior
2. **Implement** the changes following the process above (failing tests, then
   logic)
3. **Run the top-level test** to confirm the system exhibits the specified
   behavior, then run the **full test suite** to ensure nothing else broke
4. **Refine and clean up**: refactor, fix clippy warnings, improve code quality.
   Re-validate SPEC.md — refine any details that became clearer during
   implementation. Update ROADMAP.md and AGENTS.md to reflect what changed
