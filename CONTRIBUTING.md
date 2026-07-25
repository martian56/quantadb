# Contributing

Thanks for looking. QuantaDB is a young database and the useful contributions
right now are small, measured, and reviewable.

## Before you write code

Open an issue first for anything beyond a fix. It saves you building something
that does not fit, and it saves a review that has to say no.

If you are looking for somewhere to start, the issue tracker carries the
current performance work with its measurements attached, which is usually the
clearest statement of what the project needs.

## Getting set up

Rust 1.96 or newer.

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
```

All four have to be clean; CI runs them as separate jobs so a formatting slip
does not hide behind a compile.

## What a good change looks like

**One issue per pull request**, with several small commits rather than one
large one. Each commit should build and pass tests on its own.

**Tests that would have caught the bug.** For storage and MVCC work that
usually means a test that survives a reopen, or one that drives the exact
interleaving that broke.

**Comments only where the code cannot speak.** Keep them short. Explain why
something is the way it is, not what the next line does.

**Commit messages that say what changed and why.** A subject line under about
seventy characters, a blank line, then prose. No trailing metadata.

## Performance changes

This is the part that is different here, and it is not negotiable.

**A performance claim needs numbers from the bench matrix, not from your
laptop.** Every pull request is benched on five machine profiles against the
previous build running on the same runner, and the result is posted as a
comment. Local timings have repeatedly pointed the opposite way to the truth
on this project, so they do not decide anything.

**One run is not evidence.** Runner speed varies. A change under about fifteen
percent needs a second run before anyone believes it, and the report says so.

**Negative results are worth submitting.** Several issues here exist only to
record what did not work and why, so nobody spends the idea twice. If you tried
something reasonable and the matrix rejected it, that is a useful pull request
description even if the code never merges.

## Licensing

QuantaDB is Apache-2.0. By opening a pull request you agree that your
contribution is licensed under the same terms, and you confirm you have the
right to contribute it. Commits should carry a `Signed-off-by` line asserting
the [Developer Certificate of Origin](https://developercertificate.org):

```bash
git commit -s
```
