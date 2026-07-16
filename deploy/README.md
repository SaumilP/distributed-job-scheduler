# Local demo stack

Starts Postgres, NATS (JetStream), and the three scheduler roles.

```sh
docker compose -f deploy/docker-compose.yml up --build -d
```

Create a job that fires every 5 seconds:

```sh
curl -sS -XPOST localhost:8080/jobs \
  -H 'content-type: application/json' \
  -d '{"tenant":"acme","target":"http://example.invalid/run",
       "schedule":{"type":"interval","every_secs":5}}'
```

Watch it flow through:

```sh
docker compose -f deploy/docker-compose.yml logs -f engine worker
```

Then inspect a run (the id comes from the engine/worker logs):

```sh
curl -sS localhost:8080/runs/<run-id>
```

Tear down, including volumes:

```sh
docker compose -f deploy/docker-compose.yml down -v
```

## Ports

Two client surfaces are published: **8080** (REST, and GraphQL at `/graphql`) and **50051** (gRPC). Three **admin** ports carry Prometheus `/metrics`, one per role: **9090** (api), **9091** (engine), **9092** (worker). The interesting numbers are on the engine (claim throughput, due-lag, reclaim and burial rates) and the worker (execution time); the API's registry is empty because it does no scheduling.

```sh
curl -sS localhost:9091/metrics   # engine
curl -sS localhost:9092/metrics   # worker
```

Each role binds `9090` inside its own container; the host mapping is what keeps them distinct. Note this is an **unauthenticated admin surface** that discloses tenant counts and traffic shape — published here only because it is a local demo. In a real deployment it belongs on an internal network or behind auth, scraped in-cluster, not mapped to a host port.

**Tracing.** Set `OTEL_EXPORTER_OTLP_ENDPOINT` on the roles to export spans to an OTLP/HTTP collector (e.g. an OpenTelemetry Collector or Jaeger); a run's dispatch and execution then appear under one trace, since the trace context rides the NATS message. Unset (the default here), tracing stays in the logs and the only cost is the free context propagation.

**Postgres and NATS are not published.** They are reachable from inside the compose network but not from your machine, so the stack does not collide with a Postgres or NATS you already run — which it did, the first time this was started. Both services carry a commented-out `ports:` line using a non-default host port if you want to inspect them.

The consequence is that a binary you build on the host cannot reach the compose Postgres or NATS. That is the intended trade: the demo is meant to be driven through its published API surfaces, not by pointing a host process at its database. If you do need a host-built binary against this stack, uncomment those two `ports:` lines and point it at the mapped ports:

```sh
DATABASE_URL=postgres://postgres:postgres@localhost:55432/scheduler \
NATS_URL=nats://localhost:54222 \
  cargo run -p scheduler-api
```

## The other two surfaces

GraphQL, with the playground on `GET localhost:8080/graphql` in a browser and queries POSTed to the same path:

```sh
curl -sS -XPOST localhost:8080/graphql \
  -H 'content-type: application/json' \
  -d '{"query":"mutation { createJob(tenant:\"acme\", target:\"http://example.invalid/run\", everySecs:5) { id } }"}'

curl -sS -XPOST localhost:8080/graphql \
  -H 'content-type: application/json' \
  -d '{"query":"query($id: ID!) { job(id:$id) { id tenant schedule runs { id state } } }","variables":{"id":"<job-id>"}}'
```

gRPC on 50051. The service is `scheduler.v1.Scheduler`; the schema is `crates/adapters/api-grpc/proto/scheduler.proto`. Server reflection is **not** enabled, so a client needs the `.proto` — with [`grpcurl`](https://github.com/fullstorydev/grpcurl):

```sh
grpcurl -plaintext -import-path crates/adapters/api-grpc/proto \
  -proto scheduler.proto \
  -d '{"tenant":"acme","target":"http://example.invalid/run","every_secs":5}' \
  localhost:50051 scheduler.v1.Scheduler/CreateJob
```

## Dependency caching

The image builds the workspace twice: once with stub sources to compile dependencies into a cacheable layer, then once for real. The stub skeleton is produced by globbing `crates/` in a separate stage rather than by a hand-maintained list of `COPY crates/<name>/Cargo.toml` lines. The list was four coordinated edits per new crate and had already drifted once — Phase 2b added `crates/e2e`, missed it here, and the workspace failed to load inside the image while every test on the host stayed green.

The glob stage re-runs on any source change, but it only copies files, and its *output* — manifests and empty stubs — changes only when a manifest does. BuildKit hashes `COPY --from` by content, so the expensive dependency layer below stays cached across ordinary source edits. Measured on this repo: after changing a `.rs` file, `docker compose build` reports both the `COPY --from=manifests` step and the dependency `cargo build` step `CACHED`, and rebuilds only the source layer: 11m38s cold, 3m41s for a source-only change on this machine. (Measure with an actual content change, not `touch` — BuildKit hashes content, so `touch` alone rebuilds nothing and proves nothing.)

**No `protoc` anywhere.** `api-grpc` compiles its `.proto` with protox, a pure-Rust protobuf compiler, so a clone builds with nothing but a Rust toolchain. Neither this Dockerfile nor CI installs a protobuf compiler, and the image build failing is what would catch a regression to `protoc`.

## What this is and is not

**Throwaway credentials only.** `postgres/postgres` is hardcoded because this stack is meant to run on a laptop and be deleted. There are no secrets here and none should be added — the moment this needs a real credential it needs a real secret store, not an environment variable in a committed file.

**A demo Dockerfile, not a hardened one.** No distroless base, no vendored dependencies, no SBOM, no multi-arch manifest. It does run as a non-root user and pin the toolchain, because those cost nothing.

**`Job.runs` shows execution history, not the upcoming schedule.** The engine materializes runs `HORIZON_SECS` ahead, so an active job always has future `pending` runs. The batched read is bounded at `now`, so the nested field returns what has already been scheduled up to this moment, newest first.

This was originally a defect found by running this stack rather than by any test: without the time bound the window filled entirely with future runs and a completed run could never appear. It is fixed in the read port, and covered by tests at both the adapter and the GraphQL layer.

**The worker simulates execution.** It logs the run and reports success; it does not call `target`. Real outbound calls need timeouts, retries, TLS, and SSRF protection, none of which this phase covers.

**No partition maintenance runs in the demo.** `job_runs` is range-partitioned by day, and the `0005` migration seeds only a starter week of partitions. This stack does not run `ensure_partitions`, so a demo left running past that week routes new runs into the `DEFAULT` partition — correct, but unpruned. To keep a long-running demo on real range partitions, create them ahead of time; the helper is `adapter_postgres::ensure_partitions(&pool, from, to)`, callable from a host-built binary against the mapped Postgres port (uncomment the `ports:` line as above). Production automates this on a schedule — see `ARCHITECTURE.md`.

## Startup ordering

Two dependencies are real and worth knowing about, because they are why the services use `restart: on-failure` rather than pretending startup is instantaneous:

- **The API owns migrations.** Three roles racing the same migration against a fresh database is a real, reproducible failure, so exactly one of them runs them. The engine's queries fail until the schema lands; its loops tolerate that.
- **The engine creates the `RUNS` stream.** The worker binds a consumer to that stream at startup and exits if it does not exist yet. Restarting is the honest fix; a retry loop inside the binary would hide the ordering dependency rather than express it.

## Scaling a role

```sh
docker compose -f deploy/docker-compose.yml up -d --scale worker=3
```

Workers share one durable consumer name, so they form a queue group: a run goes to exactly one of them.

Scaling the **engine** needs one change first — remove `SCHEDULER_OWNER` from the service so each replica falls back to its container hostname. Two engines sharing an owner string makes lease attribution meaningless.
