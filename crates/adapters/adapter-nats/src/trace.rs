//! W3C trace-context propagation across the RUNS stream.
//!
//! Dispatch (engine) and execution (worker) are two processes joined only by a
//! NATS message. Without carrying the trace context across that gap, a run's
//! dispatch span and its execution span belong to two unrelated traces, and the
//! one question distributed tracing exists to answer — *where did this run's
//! time go, end to end* — cannot be. These helpers inject the current span's
//! W3C `traceparent` into the published headers and extract it on consume, so
//! the execution span can be parented to the dispatch that caused it.
//!
//! They use the globally configured propagator (set once in
//! `scheduler-common`'s telemetry init), so if tracing is not configured the
//! inject is a no-op and the extract yields the empty context — the transport
//! is unchanged and nothing breaks.

use async_nats::HeaderMap;
use opentelemetry::Context;
use opentelemetry::propagation::{Extractor, Injector};
use std::collections::HashMap;

/// The W3C fields a text-map propagator uses. Reading just these avoids
/// depending on `HeaderMap`'s iteration shape or its private value accessor.
const W3C_FIELDS: [&str; 2] = ["traceparent", "tracestate"];

/// Writes propagation fields into NATS headers.
struct HeaderInjector<'a>(&'a mut HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key, value.as_str());
    }
}

/// Reads propagation fields out of a small materialized copy, so the borrow the
/// `Extractor` trait requires (`&str`) comes from a `String` we own rather than
/// from a `HeaderValue` whose `&str` view is private.
struct HeaderExtractor(HashMap<String, String>);

impl HeaderExtractor {
    fn from_headers(headers: &HeaderMap) -> Self {
        let mut map = HashMap::new();
        for field in W3C_FIELDS {
            if let Some(value) = headers.get(field) {
                map.insert(field.to_string(), value.to_string());
            }
        }
        Self(map)
    }
}

impl Extractor for HeaderExtractor {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

/// Injects `cx` into `headers` using the globally configured text-map
/// propagator. A no-op if no propagator is set.
pub fn inject_context(cx: &Context, headers: &mut HeaderMap) {
    opentelemetry::global::get_text_map_propagator(|prop| {
        prop.inject_context(cx, &mut HeaderInjector(headers));
    });
}

/// Extracts a parent context from `headers`. Returns the empty context when no
/// propagation fields are present.
pub fn extract_context(headers: &HeaderMap) -> Context {
    let extractor = HeaderExtractor::from_headers(headers);
    opentelemetry::global::get_text_map_propagator(|prop| prop.extract(&extractor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
    };
    use opentelemetry_sdk::propagation::TraceContextPropagator;

    /// A traceparent injected on publish must be the same one extracted on
    /// consume — otherwise the two ends of a run are on different traces and the
    /// propagation buys nothing. Exercised end to end through a real
    /// `HeaderMap`, with the same W3C propagator the binaries configure.
    #[test]
    fn trace_context_round_trips_through_headers() {
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

        let sc = SpanContext::new(
            TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap(),
            SpanId::from_hex("b7ad6b7169203331").unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );
        let cx = Context::new().with_remote_span_context(sc.clone());

        let mut headers = HeaderMap::new();
        inject_context(&cx, &mut headers);

        // The wire carries the W3C header, not our internal types.
        assert!(
            headers.get("traceparent").is_some(),
            "inject must write a traceparent header"
        );

        let extracted = extract_context(&headers);
        let got = extracted.span().span_context().clone();
        assert_eq!(
            got.trace_id(),
            sc.trace_id(),
            "the extracted trace id must match the injected one"
        );
        assert_eq!(got.span_id(), sc.span_id(), "and the span id");
    }

    /// Headers with no propagation fields yield the empty context — an
    /// un-traced publisher must not crash or fabricate a trace on the consumer.
    #[test]
    fn missing_headers_yield_the_empty_context() {
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        let extracted = extract_context(&HeaderMap::new());
        assert!(
            !extracted.span().span_context().is_valid(),
            "no headers must mean no parent, not a bogus one"
        );
    }
}
