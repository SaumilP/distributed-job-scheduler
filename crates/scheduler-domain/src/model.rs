use crate::ports::{DomainError, DomainResult};
use time::OffsetDateTime;
use uuid::Uuid;

/// Upper bound on an interval period, about ten years.
///
/// Chosen to sit far below the point where `now + period` leaves
/// `OffsetDateTime`'s range while staying well above any plausible real
/// schedule. The exact value matters less than having one: unbounded is what
/// let a single request stall the engine.
pub const MAX_INTERVAL_SECS: i64 = 10 * 366 * 24 * 60 * 60;

/// How long a claim holds its lease, in seconds.
///
/// `claim_due` stamps `lease_expires_at = now + LEASE_SECS`; once that instant
/// passes, `reclaim_expired` is entitled to hand the run back to `Pending` on
/// the assumption its owner died. The value is therefore a bound on *recovery
/// latency* for an engine that dies mid-batch, and simultaneously a bound on
/// how long a legitimate execution may take before the reaper starts causing
/// the duplicate execution it exists to repair.
///
/// It lives in the domain rather than in the Postgres adapter so the adapter
/// and the in-memory fake cannot drift apart on it — a fake that leased for a
/// different duration than the adapter would make every expiry test agree with
/// a broken adapter.
///
/// # Why 120 and not the 30 this used to be
///
/// The lease starts at the **claim**, not at delivery, and it has to still be
/// held when the run reaches a terminal state. Follow the whole path, with `T0`
/// the instant `claim_due` commits:
///
/// | leg | cost | bounded by |
/// |---|---|---|
/// | claim → publish | `p` | batch position; `ClaimAndDispatch` publishes the batch serially |
/// | publish → a worker fetches it | `q` | **nothing** — queue wait is backlog depth over worker count |
/// | worst-case redelivery window | `ACK_WAIT * (MAX_DELIVER - 1)` = **20s** | `adapter-nats/src/consumer.rs` |
/// | the execution itself | `x` | nothing; the target is arbitrary |
/// | `complete()` round trip | `c` | a single indexed `UPDATE` |
///
/// For the reaper never to fire on a run that is legitimately still being
/// worked:
///
/// ```text
/// LEASE_SECS > p + q + ACK_WAIT * (MAX_DELIVER - 1) + x + c
/// ```
///
/// **At 30 this was wrong.** The broker's own redelivery window is 20 seconds
/// and is entirely normal behaviour — a worker restart during a rollout reaches
/// it. That left 10 seconds for publish, queue wait, execution and completion
/// combined. 120 leaves 100, so the term the repository actually controls is
/// dominated by a factor of six rather than by 1.5.
///
/// **It is still not a proof, and this comment does not claim one.** `q` is
/// unbounded: with a deep enough backlog and few enough workers, no finite
/// lease is safe. What the value buys is that expiry stops being reachable
/// through ordinary operation and becomes a symptom of genuine saturation.
///
/// # What it costs, and why that is the right trade
///
/// This value is simultaneously the bound on *recovery latency* for an engine
/// that dies mid-batch: its stranded runs wait out the lease before
/// `reclaim_expired` may touch them. Raising it to 120 makes a crashed engine's
/// batch up to two minutes late.
///
/// The two failures are not symmetric. A late run is a liveness cost that
/// resolves itself; a duplicated execution against a non-idempotent target is a
/// correctness cost that does not — `complete()` keeps the *recording* single
/// (it reports `false` for the loser), but nothing un-runs the work. So the
/// lease is tuned to make duplicate execution rare at the price of slower
/// recovery.
///
/// The graceful drain in `scheduler-engine`'s shutdown path is what keeps that
/// price small in practice: a *planned* stop (SIGTERM, a rollout) releases this
/// engine's unpublished claims immediately and waits out no lease at all. The
/// 120 seconds applies only to a genuine crash, which is the case the reaper
/// exists for.
///
/// # Enforcement
///
/// The inequality against `ACK_WAIT` and `MAX_DELIVER` is checked at compile
/// time in `adapter-nats/src/consumer.rs`, which is the crate that can see both
/// sides of it. It is a `const _: () = assert!(..)` and not a comment here
/// precisely because a comment asserting a property is an obligation, not
/// evidence.
pub const LEASE_SECS: i64 = 120;

/// How many times a run may be attempted before it is given up on as
/// [`RunState::Dead`].
///
/// `claim_due` increments `attempt` on every claim, so a run that has been
/// claimed `MAX_ATTEMPTS` times has had every attempt it is going to get: the
/// next claim buries it instead. Five, because the failures this retries are
/// transient by assumption -- a broker that was briefly unreachable, an engine
/// that died mid-batch -- and a fault that survives five attempts is not
/// transient. The exact number matters less than the cap existing.
///
/// **Why there is a cap at all.** Every retry path returns a run to `Pending`
/// and nothing decrements `attempt`: a publish failure releases it, and an
/// engine that dies has its lease reclaimed. Without a ceiling, a run that can
/// never be published -- an unroutable subject, a permanently poisoned payload
/// -- cycles through claim, fail, retry forever, and the Phase 3a reaper makes
/// that loop tighter rather than looser. `Dead` is where such a run stops.
///
/// `Dead` is a terminal *state*, not a queue. Nothing republishes a dead run;
/// there is no dead-letter stream in this repository, and the attempt count on
/// the row is the record of why it died.
pub const MAX_ATTEMPTS: i32 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JobId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RunId(pub Uuid);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TenantId(pub String);

#[derive(Debug, Clone, PartialEq)]
pub enum Schedule {
    Interval { every_secs: i64 },
    OneShot { at: OffsetDateTime },
}

impl Schedule {
    /// Construct an interval schedule, the supported path for building one
    /// from untrusted input (e.g. an HTTP request body in Phase 2).
    ///
    /// Rejects `every_secs <= 0`: a non-positive period would make
    /// `next_after` non-advancing (zero) or move the cursor backward
    /// (negative), which the materializer loop cannot safely tolerate.
    ///
    /// Rejects periods above [`MAX_INTERVAL_SECS`] too. `now + every_secs`
    /// must land inside `OffsetDateTime`'s representable range; past that it
    /// overflows, and the overflow used to reach `next_after` as an unchecked
    /// add -- one API call with `every_secs = 1e12` (a milliseconds-for-seconds
    /// slip, not an attack) panicked the materializer and stalled scheduling
    /// for every tenant.
    ///
    /// The `Interval { every_secs }` variant itself remains directly
    /// constructible (e.g. for tests exercising `next_after` in isolation),
    /// but this constructor is the supported way to build one from external
    /// input.
    pub fn interval(every_secs: i64) -> DomainResult<Schedule> {
        if every_secs <= 0 {
            return Err(DomainError::Invalid(format!(
                "interval every_secs must be positive, got {every_secs}"
            )));
        }
        if every_secs > MAX_INTERVAL_SECS {
            return Err(DomainError::Invalid(format!(
                "interval every_secs must be at most {MAX_INTERVAL_SECS} \
                 (about 10 years), got {every_secs}"
            )));
        }
        Ok(Schedule::Interval { every_secs })
    }

    /// Next fire time strictly after `after`, on the fixed grid anchored at
    /// `anchor`. `None` = no more runs.
    ///
    /// **The grid is a function of `anchor` and the period, never of `after`.**
    /// An interval schedule fires at `anchor + k * every_secs` for integer `k`;
    /// this returns the smallest such instant strictly greater than `after`.
    /// Moving `after` forward can only *drop* instants that have gone past, it
    /// can never shift the remaining ones onto new values.
    ///
    /// That property is the whole point. This used to be
    /// `after.checked_add(period)`, which anchors the grid on whatever instant
    /// the caller passed in. The materializer passes `clock.now()`, and `now`
    /// moves every tick, so every tick proposed a *different* set of instants:
    /// `UNIQUE (job_id, scheduled_at)` never collided, `ON CONFLICT DO NOTHING`
    /// absorbed nothing, and a job accumulated roughly one run per poll
    /// interval instead of one per schedule period. A 5-second job reached 888
    /// runs in about two minutes on the demo stack. See
    /// `interval_grid_is_stable_as_the_cursor_moves`.
    ///
    /// `OneShot` ignores `anchor`: it has one instant, and it is already fixed.
    ///
    /// A non-positive period yields `None` rather than dividing by zero or
    /// walking backwards. `Schedule::interval` already rejects one, but this
    /// has to hold for variants built directly and for rows read back from
    /// storage.
    ///
    /// Overflow yields `None` rather than panicking, for the same reason: an
    /// unchecked add here panics inside the engine's materialize loop, and a
    /// panic there is not caught by the loop's error handling. "No more runs"
    /// is the safe reading of a time we cannot represent.
    pub fn next_after(
        &self,
        after: OffsetDateTime,
        anchor: OffsetDateTime,
    ) -> Option<OffsetDateTime> {
        match self {
            Schedule::Interval { every_secs } => {
                // Non-positive periods have no grid. Returning `None` also
                // keeps the `div_euclid` below from dividing by zero.
                if *every_secs <= 0 {
                    return None;
                }

                // Smallest integer `k` with `anchor + k * period > after`, i.e.
                // `k = floor((after - anchor) / period) + 1`.
                //
                // Done in nanoseconds and `i128` so the sub-second part of the
                // difference is not silently truncated (`whole_seconds` rounds
                // toward zero, which is not `floor` for a negative difference —
                // that is the `after < anchor` case, and getting it wrong there
                // would put the first run one period late).
                let period_nanos = i128::from(*every_secs) * 1_000_000_000;
                let elapsed_nanos = (after - anchor).whole_nanoseconds();
                let k = elapsed_nanos.div_euclid(period_nanos) + 1;

                // Every step checked. `k * period` can leave `i64` for a
                // far-future `after` even when the period itself is in range.
                let offset_secs = k.checked_mul(i128::from(*every_secs))?;
                let offset_secs = i64::try_from(offset_secs).ok()?;
                anchor.checked_add(time::Duration::seconds(offset_secs))
            }
            Schedule::OneShot { at } => {
                if *at > after {
                    Some(*at)
                } else {
                    None
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Job {
    pub id: JobId,
    pub tenant: TenantId,
    pub schedule: Schedule,
    pub target: String,
    /// When this job was created — and, because of that, the origin of its
    /// interval grid.
    ///
    /// This is not audit metadata that happens to be on the struct. It is
    /// load-bearing scheduling input: `MaterializeDueRuns` passes it to
    /// `Schedule::next_after` as the anchor, so a job's runs land on
    /// `created_at + k * every_secs` forever. It must therefore be **stable for
    /// the life of the job** — round-tripped exactly through storage, never
    /// re-stamped on read, never defaulted to `now()` on a path the
    /// materializer can see. Changing it re-phases every future run of the job.
    ///
    /// Anchoring on the job's own creation instant rather than on the Unix
    /// epoch is deliberate: a shared epoch would put every job of the same
    /// period on one grid, so all of them would fire on the same second. See
    /// the module docs on `next_after` for what anchoring on a *moving* value
    /// did instead.
    pub created_at: OffsetDateTime,
}

impl Job {
    /// The supported way to build a `Job` from untrusted input.
    ///
    /// Until this existed the REST adapter built `Job` as a struct literal and
    /// only the schedule was validated, so an empty tenant and an empty target
    /// were both accepted. Neither is harmless: a job with no target is
    /// materialized forever and can never be delivered anywhere, and an empty
    /// tenant flows into the per-tenant NATS subject.
    ///
    /// Fields stay public so tests and adapters can still construct a `Job`
    /// directly; what this adds is one obvious place for input to go through.
    ///
    /// `created_at` is supplied by the caller rather than read from the system
    /// clock here, both to keep this crate free of ambient time and because it
    /// anchors the job's interval grid (see [`Job::created_at`]) — a value that
    /// load-bearing has to come from the same clock the rest of the request
    /// used, not from a second read taken microseconds later.
    pub fn new(
        id: JobId,
        tenant: impl Into<String>,
        schedule: Schedule,
        target: impl Into<String>,
        created_at: OffsetDateTime,
    ) -> DomainResult<Job> {
        let tenant = tenant.into();
        let target = target.into();

        // Trimmed, so "   " is rejected the same as "". Whitespace-only input
        // is a typo, not a tenant.
        if tenant.trim().is_empty() {
            return Err(DomainError::Invalid("tenant must not be empty".into()));
        }
        if target.trim().is_empty() {
            return Err(DomainError::Invalid("target must not be empty".into()));
        }

        Ok(Job {
            id,
            tenant: TenantId(tenant),
            schedule,
            target,
            created_at,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    Pending,
    Claimed,
    Running,
    Succeeded,
    Failed,
    Dead,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JobRun {
    pub id: RunId,
    pub job_id: JobId,
    pub tenant: TenantId,
    pub scheduled_at: OffsetDateTime,
    pub state: RunState,
    pub attempt: i32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    /// The anchor is a real anchor: on the grid, `next_after` lands on the next
    /// grid point, not on `after + period`.
    #[test]
    fn interval_next_after_advances_by_period() {
        let s = Schedule::Interval { every_secs: 60 };
        let anchor = datetime!(2026-07-16 10:00:00 UTC);
        assert_eq!(
            s.next_after(anchor, anchor),
            Some(datetime!(2026-07-16 10:01:00 UTC))
        );
    }

    /// An `after` that sits *between* grid points must snap forward to the next
    /// grid point, not start a new grid of its own. This is the assertion that
    /// distinguishes an anchored schedule from the old `after + period`: under
    /// the old code this returned 10:00:37, which is not on the job's grid at
    /// all.
    #[test]
    fn interval_next_after_snaps_to_the_grid_not_to_the_cursor() {
        let anchor = datetime!(2026-07-16 09:59:37 UTC);
        let s = Schedule::Interval { every_secs: 60 };

        // Grid: ...09:59:37, 10:00:37, 10:01:37...
        assert_eq!(
            s.next_after(datetime!(2026-07-16 10:00:00 UTC), anchor),
            Some(datetime!(2026-07-16 10:00:37 UTC))
        );
        // Half a second later, still the same grid point.
        assert_eq!(
            s.next_after(datetime!(2026-07-16 10:00:00.5 UTC), anchor),
            Some(datetime!(2026-07-16 10:00:37 UTC))
        );
        // Exactly on a grid point: strictly-after, so the next one.
        assert_eq!(
            s.next_after(datetime!(2026-07-16 10:00:37 UTC), anchor),
            Some(datetime!(2026-07-16 10:01:37 UTC))
        );
    }

    /// The grid extends backwards through the anchor too. `after` before the
    /// anchor must floor correctly — `whole_seconds` truncates toward zero
    /// rather than flooring, which would put the answer a whole period late.
    #[test]
    fn interval_next_after_handles_a_cursor_before_the_anchor() {
        let anchor = datetime!(2026-07-16 10:00:00 UTC);
        let s = Schedule::Interval { every_secs: 60 };

        assert_eq!(
            s.next_after(datetime!(2026-07-16 09:58:30 UTC), anchor),
            Some(datetime!(2026-07-16 09:59:00 UTC))
        );
        // A sub-second offset must not round the wrong way.
        assert_eq!(
            s.next_after(datetime!(2026-07-16 09:59:00.25 UTC), anchor),
            Some(datetime!(2026-07-16 10:00:00 UTC))
        );
    }

    /// **The regression, at the domain level.**
    ///
    /// Walking the cursor forward — which is exactly what a materializer tick
    /// does with a moving `now` — must not invent new instants. The set of
    /// instants a cursor sees may *shrink* as instants go past, but every
    /// instant it does see must already be on the grid the anchor fixed.
    #[test]
    fn interval_grid_is_stable_as_the_cursor_moves() {
        let anchor = datetime!(2026-07-16 10:00:00 UTC);
        let s = Schedule::Interval { every_secs: 60 };

        let mut seen = std::collections::BTreeSet::new();
        // 120 cursors one second apart: two full periods of clock movement.
        for i in 0..120 {
            let cursor = anchor + time::Duration::seconds(i);
            let next = s.next_after(cursor, anchor).unwrap();
            assert!(next > cursor, "must advance strictly past the cursor");
            assert_eq!(
                (next - anchor).whole_seconds() % 60,
                0,
                "{next} is not on the grid anchored at {anchor}"
            );
            seen.insert(next);
        }

        // Two periods of cursor movement can only expose two distinct grid
        // points. Under the old `after + period` this produced 120.
        assert_eq!(
            seen.len(),
            2,
            "the grid moved with the cursor: got {seen:?}"
        );
    }

    /// A non-positive period has no grid. It must yield `None` rather than
    /// dividing by zero or walking backwards — the materializer's loop calls
    /// this in a `while let`.
    #[test]
    fn interval_next_after_is_none_for_a_non_positive_period() {
        let t = datetime!(2026-07-16 10:00:00 UTC);
        assert_eq!(Schedule::Interval { every_secs: 0 }.next_after(t, t), None);
        assert_eq!(
            Schedule::Interval { every_secs: -60 }.next_after(t, t),
            None
        );
    }

    #[test]
    fn oneshot_next_after_is_none_once_past() {
        let at = datetime!(2026-07-16 10:00:00 UTC);
        let s = Schedule::OneShot { at };
        // The anchor is irrelevant to a one-shot; pass a nonsense one to say so.
        let anchor = datetime!(1999-01-01 00:00:00 UTC);
        assert_eq!(s.next_after(at, anchor), None);
        assert_eq!(
            s.next_after(at - time::Duration::seconds(1), anchor),
            Some(at)
        );
    }

    #[test]
    fn interval_constructor_accepts_positive() {
        let s = Schedule::interval(60).unwrap();
        assert_eq!(s, Schedule::Interval { every_secs: 60 });
    }

    #[test]
    fn interval_constructor_rejects_zero() {
        let err = Schedule::interval(0).unwrap_err();
        assert!(matches!(err, DomainError::Invalid(_)));
    }

    #[test]
    fn interval_constructor_rejects_negative() {
        let err = Schedule::interval(-1).unwrap_err();
        assert!(matches!(err, DomainError::Invalid(_)));
    }

    /// The remote-stall regression.
    ///
    /// `every_secs = 1e12` is a milliseconds-for-seconds slip, not an attack.
    /// It used to be accepted, and `next_after` then panicked on an unchecked
    /// add -- inside the engine's materialize loop, which catches `Err` and not
    /// panics. The task died, its sibling kept running, `join!` never returned,
    /// and scheduling stopped for every tenant while the container still
    /// reported healthy.
    #[test]
    fn interval_constructor_rejects_a_period_that_would_overflow_time() {
        let err = Schedule::interval(1_000_000_000_000).unwrap_err();
        assert!(matches!(err, DomainError::Invalid(_)), "got {err:?}");
        assert!(Schedule::interval(i64::MAX).is_err());
        assert!(
            Schedule::interval(MAX_INTERVAL_SECS).is_ok(),
            "the bound itself must be allowed"
        );
    }

    /// Defence in depth: even a variant built directly (bypassing the
    /// constructor, as old rows and tests do) must not panic.
    #[test]
    fn next_after_returns_none_instead_of_panicking_on_overflow() {
        let t = datetime!(2026-07-19 10:00:00 UTC);
        let s = Schedule::Interval {
            every_secs: i64::MAX,
        };
        assert_eq!(s.next_after(t, t), None);

        // Anchoring moved the overflow surface: the add is now
        // `anchor + k * period`, not `after + period`. A period well inside
        // `MAX_INTERVAL_SECS` still walks off the end of representable time if
        // the anchor is late enough, and that must be `None` rather than a
        // panic inside the engine's materialize loop.
        let s = Schedule::Interval {
            every_secs: MAX_INTERVAL_SECS,
        };
        assert_eq!(
            s.next_after(
                datetime!(9999-12-01 00:00:00 UTC),
                datetime!(9999-01-01 00:00:00 UTC)
            ),
            None,
            "a grid point past the end of representable time must be None, not a panic"
        );
    }

    #[test]
    fn job_constructor_rejects_empty_tenant_and_target() {
        let created_at = datetime!(2026-07-16 10:00:00 UTC);
        let ok = Job::new(
            JobId(uuid::Uuid::nil()),
            "acme",
            Schedule::Interval { every_secs: 60 },
            "http://svc/run",
            created_at,
        );
        assert!(ok.is_ok());

        for (tenant, target) in [
            ("", "http://x"),
            ("   ", "http://x"),
            ("acme", ""),
            ("acme", "  "),
        ] {
            let err = Job::new(
                JobId(uuid::Uuid::nil()),
                tenant,
                Schedule::Interval { every_secs: 60 },
                target,
                created_at,
            )
            .unwrap_err();
            assert!(
                matches!(err, DomainError::Invalid(_)),
                "tenant={tenant:?} target={target:?} must be rejected"
            );
        }
    }
}
