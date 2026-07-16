//! The contract between the publisher and the Phase 2b consumer.
//!
//! This is deliberately its own module with no NATS types in it: the payload
//! shape and the subject naming are a *protocol*, and a protocol you can test
//! without infrastructure is a protocol you can reason about.

use scheduler_domain::{JobId, JobRun, RunId, RunState, TenantId};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

/// A claimed run, on the wire.
///
/// Note what is *absent*: `state`. A run is only ever published at the moment
/// it is claimed, so the state is implied by the event's existence. Putting it
/// on the wire would invite a consumer to trust a value that was already stale
/// when it was serialized -- the database is the authority on run state, not
/// the message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunEvent {
    pub run_id: Uuid,
    pub job_id: Uuid,
    pub tenant: String,
    /// RFC 3339, so the payload stays human-readable in `nats sub` output and
    /// survives a `time` version bump -- the default `time` serde encoding is
    /// a non-obvious struct-ish form that is far easier to break accidentally.
    #[serde(with = "time::serde::rfc3339")]
    pub scheduled_at: OffsetDateTime,
    pub attempt: i32,
}

impl RunEvent {
    pub fn from_domain(run: &JobRun) -> RunEvent {
        RunEvent {
            run_id: run.id.0,
            job_id: run.job_id.0,
            tenant: run.tenant.0.clone(),
            scheduled_at: run.scheduled_at,
            attempt: run.attempt,
        }
    }

    /// Reconstructs the domain run.
    ///
    /// `state` is `Claimed` by construction -- see the type-level note on why
    /// state is not carried on the wire.
    pub fn to_domain(&self) -> JobRun {
        JobRun {
            id: RunId(self.run_id),
            job_id: JobId(self.job_id),
            tenant: TenantId(self.tenant.clone()),
            scheduled_at: self.scheduled_at,
            state: RunState::Claimed,
            attempt: self.attempt,
        }
    }
}

/// The JetStream subject a run for `tenant` is published to.
///
/// ## Why the tenant is escaped rather than sanitized
///
/// NATS subjects are dot-delimited, and `.`, `*`, `>` and whitespace are
/// structural. A tenant id is caller-supplied, so it cannot be interpolated
/// raw: `subject_for("a.b")` would silently create a two-token subject and a
/// consumer bound to `runs.a.>` would start receiving another tenant's runs.
/// That is a cross-tenant data leak, not a formatting bug.
///
/// The obvious fix -- replacing illegal characters with `_` -- trades the leak
/// for a quieter version of the same bug: `"a.b"` and `"a_b"` would collapse
/// to one subject. So this escapes instead, percent-encoding every byte
/// outside `[A-Za-z0-9_-]` (including `%` itself). That mapping is injective,
/// so distinct tenants always get distinct subjects, and it emits exactly one
/// subject token.
///
/// ## The empty tenant
///
/// Escaping fixes the identity of bytes, but it cannot invent bytes that are
/// not there: an empty tenant would produce `"runs."`, a trailing empty token
/// that is not a legal NATS subject and that the `runs.>` stream does not
/// match. Every such publish fails.
///
/// So empty is mapped to the literal token `%empty`. That is unambiguous
/// rather than merely unlikely: this encoder only ever emits `%` followed by
/// exactly two *uppercase* hex digits, and `em` is not two uppercase hex
/// digits, so no non-empty tenant can produce `%empty`. Injectivity holds
/// across the whole input domain, empty included.
///
/// An empty tenant id is still a data-integrity problem worth rejecting where
/// tenants enter the system (Phase 2b's API surface). Handling it here is what
/// keeps it from becoming an *unroutable message* problem as well.
pub fn subject_for(tenant: &str) -> String {
    if tenant.is_empty() {
        return "runs.%empty".to_string();
    }
    let mut s = String::from("runs.");
    for b in tenant.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' => s.push(b as char),
            other => s.push_str(&format!("%{other:02X}")),
        }
    }
    s
}

#[cfg(test)]
mod tests {
    // `use super::*` already brings in this module's own imports
    // (`JobRun`, `RunId`, `Uuid`, ...), so nothing further is needed here.
    use super::*;

    #[test]
    fn run_event_round_trips_through_json() {
        let run = JobRun {
            id: RunId(Uuid::new_v4()),
            job_id: JobId(Uuid::new_v4()),
            tenant: TenantId("acme".into()),
            scheduled_at: time::macros::datetime!(2026-07-18 10:00:00 UTC),
            state: RunState::Claimed,
            attempt: 2,
        };

        let json = serde_json::to_vec(&RunEvent::from_domain(&run)).unwrap();
        let back: RunEvent = serde_json::from_slice(&json).unwrap();
        let restored = back.to_domain();

        assert_eq!(restored.id, run.id);
        assert_eq!(restored.job_id, run.job_id);
        assert_eq!(restored.tenant, run.tenant);
        assert_eq!(restored.scheduled_at, run.scheduled_at);
        assert_eq!(restored.attempt, run.attempt);
        assert_eq!(restored.state, RunState::Claimed);
    }

    #[test]
    fn subject_is_namespaced_per_tenant() {
        let s = subject_for("acme");
        assert!(
            s.starts_with("runs."),
            "subject must live under the runs.* hierarchy: {s}"
        );
        assert!(s.contains("acme"));
    }

    /// A tenant id cannot smuggle subject structure. Without escaping, this
    /// tenant would produce `runs.a.>` -- a wildcard subscription to every
    /// tenant under `runs.a`.
    #[test]
    fn tenant_cannot_inject_subject_structure() {
        let s = subject_for("a.>");
        let token = s.strip_prefix("runs.").unwrap();
        assert!(
            !token.contains('.') && !token.contains('>') && !token.contains('*'),
            "escaped tenant must be a single literal token: {s}"
        );
    }

    /// The escaping must not collapse distinct tenants onto one subject --
    /// that is the failure mode a naive `replace('.', "_")` introduces.
    #[test]
    fn distinct_tenants_get_distinct_subjects() {
        assert_ne!(subject_for("a.b"), subject_for("a_b"));
        assert_ne!(subject_for("a b"), subject_for("a-b"));
    }

    /// An empty tenant must still yield a routable subject.
    ///
    /// `"runs."` has a trailing empty token: illegal as a NATS subject, and
    /// unmatched by the `runs.>` stream, so publishing it fails outright.
    /// Combined with batch dispatch, one such run used to strand every run
    /// behind it.
    #[test]
    fn empty_tenant_still_yields_a_routable_subject() {
        let s = subject_for("");
        let token = s.strip_prefix("runs.").expect("must keep the runs. prefix");
        assert!(!token.is_empty(), "empty token is not a legal subject: {s}");
        assert!(!token.contains('.') && !token.contains(' '));
    }

    /// The empty-tenant sentinel must not be reachable from a real tenant,
    /// or two different tenants would share a subject.
    #[test]
    fn empty_sentinel_does_not_collide_with_a_real_tenant() {
        let sentinel = subject_for("");
        for candidate in ["%empty", "empty", "%25empty", "", " "] {
            if candidate.is_empty() {
                continue;
            }
            assert_ne!(
                subject_for(candidate),
                sentinel,
                "tenant {candidate:?} collides with the empty-tenant sentinel"
            );
        }
    }

    /// A payload that no longer deserializes is a silent outage between the
    /// engine and the worker, so the encoding is pinned by example, not just
    /// by round-trip. `scheduled_at` in particular must stay RFC 3339.
    #[test]
    fn scheduled_at_is_encoded_as_rfc3339() {
        let run = JobRun {
            id: RunId(Uuid::nil()),
            job_id: JobId(Uuid::nil()),
            tenant: TenantId("acme".into()),
            scheduled_at: time::macros::datetime!(2026-07-18 10:00:00 UTC),
            state: RunState::Claimed,
            attempt: 0,
        };
        let json = serde_json::to_value(RunEvent::from_domain(&run)).unwrap();
        assert_eq!(json["scheduled_at"], "2026-07-18T10:00:00Z");
    }
}
