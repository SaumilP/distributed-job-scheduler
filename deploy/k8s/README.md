# Kubernetes manifests

The three roles (`api`, `engine`, `worker`) plus demo Postgres and NATS, and the autoscaling that ties the metrics from Phase 3b to replica counts.

```
scheduler.yaml     namespace, config, secret, postgres, nats, and the three roles
autoscaling.yaml   worker CPU HPA + engine KEDA ScaledObject on due-lag
```

## Deploy

The image is a placeholder (`distributed-job-scheduler:latest`). Build it and make it available to your cluster:

```sh
docker build -f deploy/Dockerfile -t distributed-job-scheduler:latest .
# kind:     kind load docker-image distributed-job-scheduler:latest
# minikube: minikube image load distributed-job-scheduler:latest
# a registry: docker tag/push, and set imagePullPolicy: Always
```

Then:

```sh
kubectl apply -f deploy/k8s/scheduler.yaml
kubectl apply -f deploy/k8s/autoscaling.yaml   # HPA applies anywhere; the KEDA
                                               # object needs KEDA installed
kubectl -n scheduler get pods
kubectl -n scheduler port-forward svc/scheduler-api 8080:8080 50051:50051
```

> These manifests are plain YAML validated for well-formedness and standard
> schemas; they were authored on a host where `kubectl` could not run, so apply
> them against a real cluster (kind/minikube) before relying on them.

## Design notes

**Engine owner uniqueness comes free.** Each engine replica needs a distinct lease owner or the reaper cannot attribute an expired lease. `SCHEDULER_OWNER` is deliberately left unset; `Config` falls back to `HOSTNAME`, which Kubernetes sets to the pod name — unique per replica. So `kubectl scale deploy/scheduler-engine --replicas=N` just works, and `SKIP LOCKED` keeps the concurrent claimers from colliding.

**Graceful shutdown is wired to the drain.** The engine and worker set `terminationGracePeriodSeconds: 30`; the engine drains its unpublished claims on SIGTERM within `DRAIN_BUDGET` (5s), comfortably inside the grace period, so a rollout does not strand a batch waiting out the lease.

**`/metrics` is the probe and the scrape target.** Every role serves its admin `/metrics` on 9090; the readiness/liveness probes hit it, and the pods carry `prometheus.io/scrape` annotations so a Prometheus with the standard pod-scrape config picks them up. The interesting metrics are on the engine and worker; the API's registry is empty.

**Autoscaling.**
- *worker* → a CPU `HorizontalPodAutoscaler` (needs only metrics-server). Workers share one durable NATS queue group, so replicas are drop-in consumers.
- *engine* → a KEDA `ScaledObject` on `due_lag_seconds` p95: when runs are claimed late, the due queue is backing up faster than one dispatcher drains it, and more claimers is the answer. Needs KEDA and a reachable Prometheus.
- A commented KEDA NATS-JetStream scaler in `autoscaling.yaml` shows the more direct worker signal — RUNS consumer depth — to apply *instead of* the worker CPU HPA (never both on one Deployment; KEDA runs its own HPA underneath).

## What this is NOT — production caveats

- **Postgres and NATS are demo Deployments on `emptyDir`.** They lose all data on restart. Production uses a managed Postgres (or an operator with real `PersistentVolume`s) and a NATS cluster/operator with JetStream file storage.
- **`DATABASE_URL` is a throwaway literal in a `Secret`.** A real deployment sources it from the managed-database secret, never from a committed file.
- **No `NetworkPolicy`, no TLS, no auth, no `PodDisruptionBudget`, no `ServiceMonitor`.** `/metrics` is unauthenticated and discloses tenant counts and traffic shape — it must stay on the cluster network, never on an Ingress.
- **Migrations run from the API.** At first boot the engine and worker may start before the schema lands; their loops tolerate that by retrying, so no init container orders them here — but a stricter deployment would gate on a migration Job.
