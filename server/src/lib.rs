// Library entry point for the server crate. Exposes flush_events so it can be
// used by examples (e.g. examples/prefill.rs) and integration tests without
// going through the HTTP API.

pub use flush::flush_events;

mod flush;
