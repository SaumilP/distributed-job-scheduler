//! The GraphQL schema: a driving adapter over the same ports REST and gRPC use.
//!
//! Resolvers go through the domain constructors (`Job::new`,
//! `Schedule::interval`) rather than building values directly. A second
//! transport is a second chance to make the same validation mistake, and the
//! Phase 2b review found exactly that on the REST surface — an empty tenant,
//! an empty target, and an overflowing interval all accepted.

use crate::loader::RunsLoader;
use async_graphql::dataloader::DataLoader;
use async_graphql::{
    Context, EmptySubscription, ID, Object, Result as GqlResult, Schema, SimpleObject,
};
use scheduler_domain::{
    Clock, DomainError, Job, JobId, JobRepository, JobRun, RunId, RunRepository, RunState, Schedule,
};
use std::marker::PhantomData;
use uuid::Uuid;

/// Maximum nesting depth accepted.
///
/// This bounds **introspection**, not data queries. `RunNode` is a
/// `SimpleObject` of scalars, so nothing below `Job.runs` returns an object and
/// the deepest reachable data query — `{ jobs { runs { id } } }` — is three
/// levels. No data query can reach 8. Introspection is the recursive part:
/// `__schema { types { fields { type { fields { ... } } } } }` nests without
/// limit, which is what `excessive_query_depth_is_rejected` actually exercises.
///
/// The bound that does the work on data queries is [`MAX_COMPLEXITY`], with
/// [`MAX_PAGE`] capping the row count behind a single `jobs` field. Depth is
/// kept because the schema is not frozen: the moment any field returns an
/// object type, arbitrary nesting becomes reachable, and a limit added then is
/// a limit added after the fact.
pub const MAX_DEPTH: usize = 8;

/// Maximum query complexity (roughly, field count weighted by nesting).
///
/// This is the effective bound on data queries. Depth cannot stop a wide one:
/// several dozen aliases of `jobs { ... }` in a single document stay three
/// levels deep and each one is a full lookup. Complexity counts the fields, so
/// it rejects that shape.
pub const MAX_COMPLEXITY: usize = 256;

/// Cap on `jobs(limit:)` regardless of what is asked for.
///
/// Only `jobs` takes a client-supplied limit; the nested `runs` field is
/// bounded by the loader's per-job cap instead, which the client cannot
/// influence — and consequently cannot page through. That limitation is
/// deliberate and documented in ARCHITECTURE.md ("What `Job.runs` cannot
/// reach").
pub const MAX_PAGE: i32 = 200;

fn clamp_limit(requested: Option<i32>, default: i32) -> i64 {
    requested.unwrap_or(default).clamp(1, MAX_PAGE) as i64
}

fn to_gql_error(e: DomainError) -> async_graphql::Error {
    match e {
        // Client errors are safe to echo: the client caused them.
        DomainError::Invalid(m) => async_graphql::Error::new(m),
        DomainError::NotFound => async_graphql::Error::new("not found"),
        // Storage/publish detail carries connection strings and table names.
        other => {
            tracing::error!(error = %other, "graphql request failed");
            async_graphql::Error::new("internal error")
        }
    }
}

#[derive(SimpleObject)]
pub struct RunNode {
    pub id: ID,
    pub job_id: ID,
    pub state: String,
    pub attempt: i32,
    pub scheduled_at: String,
}

impl From<JobRun> for RunNode {
    fn from(r: JobRun) -> Self {
        RunNode {
            id: ID(r.id.0.to_string()),
            job_id: ID(r.job_id.0.to_string()),
            state: state_name(r.state).to_string(),
            attempt: r.attempt,
            scheduled_at: r
                .scheduled_at
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
        }
    }
}

fn state_name(s: RunState) -> &'static str {
    match s {
        RunState::Pending => "pending",
        RunState::Claimed => "claimed",
        RunState::Running => "running",
        RunState::Succeeded => "succeeded",
        RunState::Failed => "failed",
        RunState::Dead => "dead",
    }
}

/// A job, with a `runs` field that resolves through the batch loader.
///
/// Generic in `R` and `C` only so the `runs` resolver can name the loader's
/// concrete type in the context. The GraphQL type name is pinned to `Job` so
/// the published schema does not leak Rust generics.
pub struct JobNode<R: RunRepository, C: Clock> {
    inner: Job,
    _runs: PhantomData<R>,
    _clock: PhantomData<C>,
}

impl<R: RunRepository, C: Clock> JobNode<R, C> {
    fn new(inner: Job) -> Self {
        Self {
            inner,
            _runs: PhantomData,
            _clock: PhantomData,
        }
    }
}

#[Object(name = "Job")]
impl<R: RunRepository, C: Clock> JobNode<R, C> {
    async fn id(&self) -> ID {
        ID(self.inner.id.0.to_string())
    }

    async fn tenant(&self) -> &str {
        &self.inner.tenant.0
    }

    async fn target(&self) -> &str {
        &self.inner.target
    }

    /// `"interval:60"` or `"oneshot:<rfc3339>"`.
    ///
    /// A string rather than a union because the schedule shape is already
    /// modelled twice (domain enum, database discriminant) and a third
    /// encoding is a third thing to keep in step. A union is the right answer
    /// once clients need to branch on it.
    async fn schedule(&self) -> String {
        match &self.inner.schedule {
            Schedule::Interval { every_secs } => format!("interval:{every_secs}"),
            Schedule::OneShot { at } => format!(
                "oneshot:{}",
                at.format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_default()
            ),
        }
    }

    /// Resolved through `DataLoader`, so N jobs cost one lookup rather than N.
    async fn runs(&self, ctx: &Context<'_>) -> GqlResult<Vec<RunNode>> {
        let loader = ctx.data_unchecked::<DataLoader<RunsLoader<R, C>>>();
        // The loader's error is an `Arc<Error>` (it is shared across every
        // resolver that batched into the same call), so unwrap it explicitly
        // rather than via `?`.
        let runs = loader
            .load_one(self.inner.id)
            .await
            .map_err(|e| async_graphql::Error::new(e.message.clone()))?
            .unwrap_or_default();
        Ok(runs.into_iter().map(RunNode::from).collect())
    }
}

pub struct QueryRoot<J: JobRepository, R: RunRepository, C: Clock> {
    _jobs: PhantomData<J>,
    _runs: PhantomData<R>,
    _clock: PhantomData<C>,
}

impl<J: JobRepository, R: RunRepository, C: Clock> Default for QueryRoot<J, R, C> {
    fn default() -> Self {
        Self {
            _jobs: PhantomData,
            _runs: PhantomData,
            _clock: PhantomData,
        }
    }
}

#[Object(name = "Query")]
impl<J: JobRepository, R: RunRepository, C: Clock> QueryRoot<J, R, C> {
    /// `null` rather than an error for an absent job: "you asked for something
    /// that is not there" is a normal GraphQL result, and turning it into an
    /// error makes partial responses harder for clients to handle.
    async fn job(&self, ctx: &Context<'_>, id: ID) -> GqlResult<Option<JobNode<R, C>>> {
        let Some(uuid) = parse_uuid(&id) else {
            return Ok(None);
        };
        match ctx.data_unchecked::<J>().get(JobId(uuid)).await {
            Ok(job) => Ok(Some(JobNode::new(job))),
            Err(DomainError::NotFound) => Ok(None),
            Err(e) => Err(to_gql_error(e)),
        }
    }

    async fn jobs(&self, ctx: &Context<'_>, limit: Option<i32>) -> GqlResult<Vec<JobNode<R, C>>> {
        let limit = clamp_limit(limit, 50);
        let jobs = ctx
            .data_unchecked::<J>()
            .list_active(limit)
            .await
            .map_err(to_gql_error)?;
        Ok(jobs.into_iter().map(JobNode::new).collect())
    }

    async fn run(&self, ctx: &Context<'_>, id: ID) -> GqlResult<Option<RunNode>> {
        let Some(uuid) = parse_uuid(&id) else {
            return Ok(None);
        };
        match ctx.data_unchecked::<R>().get(RunId(uuid)).await {
            Ok(run) => Ok(Some(RunNode::from(run))),
            Err(DomainError::NotFound) => Ok(None),
            Err(e) => Err(to_gql_error(e)),
        }
    }
}

pub struct MutationRoot<J: JobRepository, R: RunRepository, C: Clock> {
    _jobs: PhantomData<J>,
    _runs: PhantomData<R>,
    _clock: PhantomData<C>,
}

impl<J: JobRepository, R: RunRepository, C: Clock> Default for MutationRoot<J, R, C> {
    fn default() -> Self {
        Self {
            _jobs: PhantomData,
            _runs: PhantomData,
            _clock: PhantomData,
        }
    }
}

#[Object(name = "Mutation")]
impl<J: JobRepository, R: RunRepository, C: Clock> MutationRoot<J, R, C> {
    async fn create_job(
        &self,
        ctx: &Context<'_>,
        tenant: String,
        target: String,
        every_secs: i64,
    ) -> GqlResult<JobNode<R, C>> {
        // Both constructors, not struct literals. `Schedule::interval` bounds
        // the period (an unbounded one overflowed and stalled the engine), and
        // `Job::new` rejects an empty tenant or target.
        //
        // `created_at` comes from the system clock at the edge, as in the REST
        // and gRPC adapters, and anchors the job's interval grid — see
        // `scheduler_domain::Job::created_at`.
        let schedule = Schedule::interval(every_secs).map_err(to_gql_error)?;
        let job = Job::new(
            JobId(Uuid::new_v4()),
            tenant,
            schedule,
            target,
            time::OffsetDateTime::now_utc(),
        )
        .map_err(to_gql_error)?;

        ctx.data_unchecked::<J>()
            .insert(&job)
            .await
            .map_err(to_gql_error)?;

        Ok(JobNode::new(job))
    }
}

fn parse_uuid(id: &ID) -> Option<Uuid> {
    Uuid::parse_str(id.as_str()).ok()
}

pub type SchedulerSchema<J, R, C> =
    Schema<QueryRoot<J, R, C>, MutationRoot<J, R, C>, EmptySubscription>;

/// Builds the schema with the batch loader installed and the depth/complexity
/// limits applied.
///
/// The limits are applied here rather than left to the caller so every
/// composition gets them — a caller that forgets is a caller with an
/// unbounded query surface.
/// `clock` is threaded down to the batch loader, which reads it on every load
/// to bound nested `Job.runs` at the present instant. It is a parameter rather
/// than a hardcoded `SystemClock` so a test can pin time; `scheduler-domain`
/// owns the `Clock` port precisely so this adapter need not know about wall
/// clocks.
pub fn build<J, R, C>(jobs: J, runs: R, clock: C, runs_per_job: i64) -> SchedulerSchema<J, R, C>
where
    J: JobRepository,
    R: RunRepository + Clone,
    C: Clock,
{
    let loader = DataLoader::new(
        RunsLoader::new(runs.clone(), clock, runs_per_job),
        tokio::spawn,
    );
    Schema::build(
        QueryRoot::<J, R, C>::default(),
        MutationRoot::<J, R, C>::default(),
        EmptySubscription,
    )
    .data(jobs)
    .data(runs)
    .data(loader)
    .limit_depth(MAX_DEPTH)
    .limit_complexity(MAX_COMPLEXITY)
    .finish()
}
