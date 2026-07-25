<!--
Performance claims are settled by the bench comment below, not by local
timings. A change under about fifteen percent needs a second run to mean
anything.
-->

## What this changes

<!-- One paragraph. What was wrong, and what it does instead. -->

## Why

<!-- Closes #NNN, or the reasoning if there is no issue. -->

## How it was checked

- [ ] `cargo test --workspace`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo fmt --all`
- [ ] Tests added for the behaviour that changed

## Anything a reviewer should look at closely

<!--
Durability and concurrency changes are the ones that hurt later. If this
touches the commit path, recovery, or page reuse, say what ordering it
relies on.
-->
