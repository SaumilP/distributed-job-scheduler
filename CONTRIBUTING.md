# Contributing

This is a reference implementation written to be read. That goal shapes what a good contribution looks like here more than any style guide would: a change that makes the system harder to follow is a regression even if it makes it faster, and a change that closes one of the gaps named in [Limitations](README.md#limitations) is welcome even if it is small.

If you are here to understand the design first, read [ARCHITECTURE.md](ARCHITECTURE.md). If you are here to build and run it, read [DEVELOPERS.md](DEVELOPERS.md). This file is the part in between: how to get a change accepted.

---

## The gate

Three commands. CI runs exactly these, and a pull request that fails any of them will not be reviewed until it passes:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --locked
```

`--workspace` is not decorative. A package-scoped gate once reported clean while the workspace was not, which is why nothing in `.github/workflows/ci.yml` names a crate — a new crate under `crates/adapters/` or `crates/bin/` is picked up by the `members` globs with no change to CI.

CI additionally builds `deploy/Dockerfile` and asserts all three binaries are present and runnable inside the image. That job is the standing check on the no-`protoc` constraint below; it builds in `rust:1.88.0-bookworm`, which ships no protobuf compiler.

The toolchain is pinned in `rust-toolchain.toml` (1.88.0). Do not bump it in a change that is about something else.

---

## Architectural constraints

These are not preferences. Each one is argued in [ARCHITECTURE.md](ARCHITECTURE.md), and a pull request that breaks one needs to argue against that reasoning rather than around it.

**`scheduler-domain` depends on no async runtime, no database driver and no broker client.** Its entire dependency set is `thiserror`, `time` and `uuid`, and that is checkable in one line:

```sh
cargo tree -p scheduler-domain --edges normal
```

A commit that reaches for `sqlx` or `tokio` inside the domain shows up in that output. If the domain needs a capability from the outside, it gets a trait in `crates/scheduler-domain/src/ports.rs` and an implementation in an adapter crate.

**Nothing in the application layer reads the system clock.** `Clock` is a port and tests inject `FixedClock`. A `now()` call inside a use case is a bug, and a quiet one — a whole class of scheduling regression is invisible under a fixed clock, which is how the [per-job anchoring bug](ARCHITECTURE.md#run-instants-are-anchored-per-job) shipped green.

**Validation lives in the domain constructors, not in the adapters.** REST, GraphQL and gRPC are three driving adapters over the same ports, and an empty tenant or an out-of-range interval must be rejected identically on all three. A check added to one surface is a bug in the other two.

**Ports are not object-safe, deliberately.** They use return-position `impl Trait` in trait methods, which is what keeps them zero-cost; the price is that nothing can hold a `Box<dyn RunRepository>` and every consumer is generic over its ports. If you find yourself wanting a trait object, you are fighting a trade that is [argued in full](ARCHITECTURE.md#why-hexagonal), not an oversight.

**There is no `protoc` in this repository, and there must not be.** `api-grpc` compiles its `.proto` with `protox`, a pure-Rust compiler, so a fresh clone builds with nothing but a Rust toolchain. Switching to `tonic_build::compile_protos`, which shells out, would break that.

**The `Metric` enum is closed on purpose.** A new metric is a variant in the domain plus its name and buckets in `adapter-metrics`, so a typo cannot silently mis-record.

**The three tuning constants have to agree.** `LEASE_SECS`, `MAX_ATTEMPTS` and `REAPER_INTERVAL` are correct in relation to each other, not individually — the inequality against the NATS `ACK_WAIT` and `MAX_DELIVER` is enforced by a compile-time assertion in `adapter-nats`. Changing one means re-arguing [the section that ties them together](README.md#tuning-the-three-constants-that-have-to-agree).

Where a given kind of change goes is tabulated in [DEVELOPERS.md](DEVELOPERS.md#where-things-go).

---

## Tests

Integration tests start real Postgres, NATS and Redis containers through testcontainers, so a container runtime has to be available. One container is shared per test binary rather than per test; the fixtures are in `crates/test-support/`.

**On macOS with Docker Desktop** the socket is not where testcontainers looks:

```sh
export DOCKER_HOST="unix:///Users/$USER/.docker/run/docker.sock"
```

**testcontainers 0.23 has no reaper**, so containers outlive the test process and accumulate. [DEVELOPERS.md](DEVELOPERS.md#running-the-tests) has the cleanup command that spares a running `deploy/` demo using the same images.

To run only the parts that need no containers:

```sh
cargo test -p scheduler-domain -p scheduler-application
```

What a change is expected to bring with it:

| Change | Test |
|---|---|
| A domain invariant | A unit test in `scheduler-domain` — no containers, both the accepting and the rejecting case |
| A use case | A test in `scheduler-application` against the in-memory fakes behind its `testing` feature |
| A query, or a change to the claim | An integration test in `adapter-postgres/tests/`, against real Postgres |
| Anything touching delivery | An integration test in `adapter-nats/tests/` |
| A new or changed API surface | A case in `crates/e2e/`, which asserts all three surfaces report the same run with the same id and the same state |

The in-memory fakes are behind a `testing` feature on `scheduler-application` so they are available to other crates' tests without ever compiling into a production binary. Keep them there.

---

## Where help is most useful

The [Not built](README.md#not-built) list is the backlog, and it is deliberate rather than accidental — each item was deferred with a reason, so a contribution that closes one should engage with that reason. In rough order of how much they would add:

- **A dead-letter path.** `Dead` is currently a terminal state, not a queue: nothing republishes a dead run and there is no replay. The interesting part is not the table, it is deciding what replay means when the original schedule has moved on.
- **Cursor pagination for `Job.runs`.** GraphQL cannot reach history older than the most recent 50 — the field takes no arguments, no cursor, no `before`. This is the most self-contained item on the list.
- **A fencing token.** The reaper can return a run to `Pending` while its original worker is still executing, and nothing stops that worker's side effect. This is the one gap that changes the delivery story rather than just filling it in.
- **Batched NATS publishing.** The bench found the synchronous per-run publish to be 97% of the claim loop's time. Batching is the change the measurement actually points at, and it should come with a bench run showing what it moved.
- **An authentication adapter.** There is no auth on any surface, stated plainly and on purpose. A credible contribution is a driving-side adapter that leaves the domain untouched and works identically across REST, GraphQL and gRPC — not a check bolted onto one of them.
- **A concurrent-claimer benchmark.** The published numbers are a single claimer on a laptop with Docker. The measurement that would justify the Redis hot index — many concurrent claimers against a non-containerised database, where `SKIP LOCKED` contention would first appear — has not been run.

Small things are welcome too: a doc correction, a comment that explains a *why* the code cannot, a test that pins behaviour currently only asserted by reading.

**What is unlikely to be merged:** production-hardening that contradicts the didactic goal, an abstraction added for a second implementation that does not exist, unrelated refactoring bundled into a feature change, and any change that makes the domain crate depend on infrastructure.

If a change is large or reshapes a decision, open a discussion or an issue before writing it. That is cheaper for both of us than a rejected branch.

---

## Pull requests

Branch from `main`. Keep one concern per pull request — a rename, a fix and a feature in one branch is three reviews wearing a trench coat.

Commit messages: a short imperative subject with the existing prefix convention (`feat:`, `fix:`, `docs:`, `chore:`), and a body only where the *why* is not obvious from the subject. The log is part of what this repository is for.

In the pull request description, say what changed and why, name any constraint above that the change touches, and paste the bench output if the change claims a performance effect. "It should be faster" is not a claim this repository accepts without numbers — the [Redis hot index](ARCHITECTURE.md#the-redis-hot-index--built-then-measured) was built, measured, found 21% slower, and documented as such.

**Documentation is part of the change, not a follow-up.** The split is load-bearing: reasoning goes in ARCHITECTURE.md, operational detail in DEVELOPERS.md, and the shared overview in README.md. A new limitation belongs in the README's table. Prose is written one line per paragraph — do not hard-wrap at 80 columns, it makes diffs unreadable.

If you touch a C4 diagram, edit the `.puml` source and regenerate the SVG with `docs/diagrams/render.sh`, then commit both.

---

## Reporting bugs and asking questions

An issue for something reproducibly wrong; a [Discussion](https://github.com/SaumilP/distributed-job-scheduler/discussions) for a question about the design, or an idea you want to think through before it becomes a branch.

For a bug, the useful minimum is what you ran, what happened, what you expected, and the relevant slice of `docker compose logs engine worker`. Note that several surprising behaviours are documented rather than broken — a run executed twice, a batch waiting out its lease, `Job.runs` stopping at 50 — so [Limitations](README.md#limitations) is worth a look first.

---

## Code of Conduct

Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md).
