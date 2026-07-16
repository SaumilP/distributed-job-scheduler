# Demo image, not a hardened production one.
#
# One build produces all three binaries and the runtime stage picks which to
# run via the compose `command`. That keeps the images identical across roles,
# which is what you want for a demo: one build, three deployments.
#
# Deliberately absent, and listed so the omissions are choices rather than
# oversights: no distroless base, no vendored dependencies, no SBOM, no
# multi-arch manifest, no static musl link.

# Both builder stages below pin `rust:1.88.0-bookworm`, not `rust:latest`. The
# workspace pins 1.88.0 in rust-toolchain terms; a floating tag would silently
# break that pin from the outside. They must stay on the same tag as each
# other, or the skeleton stage pulls a second toolchain image for nothing.

# --- manifest skeleton ------------------------------------------------------
#
# This stage exists to turn a glob copy of `crates/` into just the manifests
# plus stub sources, so the dependency build below has a stable input.
#
# It replaces a hand-maintained list of ten `COPY crates/<name>/Cargo.toml`
# lines and three matching lists (mkdir, stub-lib, cleanup). That list was four
# coordinated edits per new crate and had already drifted once: Phase 2b added
# `crates/e2e`, missed it here, and the workspace failed to load inside the
# image while every test on the host stayed green. Phase 2c adds three more
# crates. A glob cannot drift.
#
# **The trade.** The old list was keyed on ten specific files, so editing any
# source left the dependency layer cached. This stage is keyed on all of
# `crates/`, so it re-runs whenever any source changes. It is cheap (a find and
# a copy, no compilation), and what it emits -- manifests and empty stubs --
# only changes when a manifest does. Whether that keeps the expensive layer
# below cached depends on BuildKit hashing `COPY --from` by content rather than
# by producing-layer identity; measured on this repo, it does (see
# deploy/README.md, "Dependency caching"). If a future BuildKit stops doing so,
# the failure mode is a slower rebuild, not a wrong image.
FROM rust:1.88.0-bookworm AS manifests

WORKDIR /src
COPY crates crates

# `-mindepth 2` so a stray `crates/Cargo.toml` could never be mistaken for a
# member. Every package gets both stub targets rather than the build guessing
# which are libs and which are binaries -- an unused stub target costs a few
# milliseconds, and guessing wrong is what a hand-maintained list did.
#
# `build.rs` and `proto/` come along when a crate has them. Without them cargo
# never compiles that crate's build-dependencies, which for `api-grpc` is the
# whole protox and tonic-build tree -- it would land in the source layer
# instead and rebuild on every edit, which is the cost this stage exists to
# avoid.
RUN find crates -mindepth 2 -name Cargo.toml -printf '%h\n' \
  | while read -r dir; do \
      mkdir -p "/skeleton/$dir/src" || exit 1; \
      cp "$dir/Cargo.toml" "/skeleton/$dir/Cargo.toml" || exit 1; \
      : > "/skeleton/$dir/src/lib.rs"; \
      echo 'fn main() {}' > "/skeleton/$dir/src/main.rs"; \
      for extra in build.rs proto; do \
        if [ -e "$dir/$extra" ]; then cp -r "$dir/$extra" "/skeleton/$dir/" || exit 1; fi; \
      done; \
    done \
 && test -d /skeleton/crates

# --- builder ----------------------------------------------------------------
FROM rust:1.88.0-bookworm AS builder

WORKDIR /build

# Dependency layer first: manifests and stubs only, build, and let Docker cache
# the result. Without this, editing one line of source rebuilds every
# dependency in the graph -- minutes per iteration.
#
# No `protoc` is installed, here or anywhere in this file. `api-grpc` compiles
# its `.proto` with protox, a pure-Rust protobuf compiler, and this build
# succeeding is the check that keeps it that way: if someone swaps protox for
# `tonic_build::compile_protos`, which shells out to `protoc`, this layer fails
# rather than the constraint quietly eroding.
COPY Cargo.toml Cargo.lock ./
COPY --from=manifests /skeleton/crates crates

RUN cargo build --release --locked \
      -p scheduler-api -p scheduler-engine -p scheduler-worker \
 && rm -rf crates

# Real sources. Only this layer and beyond rebuild on a source change.
COPY crates crates
# Cargo skips rebuilding when mtimes look unchanged; touching the roots forces
# the real sources to win over the stubs above.
RUN find crates -name '*.rs' -exec touch {} + \
 && cargo build --release --locked \
      -p scheduler-api -p scheduler-engine -p scheduler-worker

FROM debian:bookworm-slim AS runtime

# ca-certificates so a future executor can make TLS calls; the Phase 2b
# executor does not, but the image should not need rebuilding to gain that.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Non-root. The processes bind 8080, not a privileged port, so there is no
# reason for them to run as root.
RUN useradd --system --uid 10001 --create-home scheduler
USER scheduler
WORKDIR /home/scheduler

COPY --from=builder /build/target/release/scheduler-api    /usr/local/bin/
COPY --from=builder /build/target/release/scheduler-engine /usr/local/bin/
COPY --from=builder /build/target/release/scheduler-worker /usr/local/bin/

# No default CMD: the role is always chosen explicitly by compose or the
# deployment, so an image started without one should fail loudly rather than
# silently become whichever role happened to be the default.
