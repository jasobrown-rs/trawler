//! This crate provides a workload generator that emulates the traffic to
//! [lobste.rs](https://lobste.rs). It is a mostly open-loop benchmark similar to
//! [TailBench](https://people.csail.mit.edu/sanchez/papers/2016.tailbench.iiswc.pdf), but it also
//! approximates the partly open-loop designed outlined in [*Open Versus Closed: A Cautionary
//! Tale*](https://www.usenix.org/legacy/event/nsdi06/tech/full_papers/schroeder/schroeder.pdf) by
//! having clients potentially issue more than one query per request.
//!
//! The benchmarker main component is the "load generator". It generates requests according to
//! actual lobste.rs traffic patterns as reported in [here](https://lobste.rs/s/cqnzl5/), records
//! the request time, and sends the request description to an implementor of
//! `Service<TrawlerRequest>`. When the resulting future resolves, the generator logs how long the
//! request took to process, *and* how long the request took from when it was generated until it
//! was satisfied (this is called the *sojourn time*).
//!
//! Trawler is written so that it can *either* be run against an instance of
//! the [lobsters Rails app](https://github.com/lobsters/lobsters) *or*
//! directly against a backend by issuing queries. The former is done using the provided binary,
//! whereas the latter is done by linking against this crate as a library an implementing the
//! `Service` trait. The latter allows benchmarking a data storage backend without also incurring
//! the overhead of the Rails frontend. Note that if you want to benchmark against the Rails
//! application, you must apply the patches in `lobsters.diff` first.
#![deny(missing_docs)]

mod client;

pub use self::client::{CommentId, StoryId, UserId};
pub use self::client::{LobstersRequest, RequestProcessor, TrawlerRequest, Vote};
use execution::harness::run;

mod execution;
mod timing;

use anyhow::Result;
use std::time;
use tokio::time::Duration;

include!(concat!(env!("OUT_DIR"), "/statistics.rs"));

/// There were 2893183 relevant requests in the 63166 minutes between 2018-02-11 04:40:31 and
/// 2018-03-27 01:26:49 according to https://lobste.rs/s/cqnzl5/#c_jz5hqv.
pub const BASE_OPS_PER_MIN: usize = 46;

const DEFAULT_REPORT_INTERVAL_SECS: u64 = 10;

/// Set the parameters for a new Lobsters-like workload.
pub struct WorkloadBuilder {
    load: execution::Workload,
    histogram_file: Option<String>,
    max_in_flight: usize,
    report_interval: Duration,
}

impl Default for WorkloadBuilder {
    fn default() -> Self {
        WorkloadBuilder {
            load: execution::Workload {
                scale: 1.0,
                runtime: Duration::from_secs(30),
            },
            histogram_file: None,
            max_in_flight: 20,
            report_interval: Duration::from_secs(DEFAULT_REPORT_INTERVAL_SECS),
        }
    }
}

impl WorkloadBuilder {
    /// Set the scaling factor for the workload.
    ///
    /// A factor of 1 generates a workload commensurate with what the [real lobste.rs
    /// sees](https://lobste.rs/s/cqnzl5/). At scale 1, the site starts out with ~40k stories with
    /// a total of ~300k comments spread across 9k users. The generated load is on average 44
    /// requests/minute, with a request distribution set according to the one observed on lobste.rs
    /// (see `data/` for details).
    pub fn scale(&mut self, factor: f64) -> &mut Self {
        self.load.scale = factor;
        self
    }

    /// Set the runtime for the benchmark.
    pub fn time(&mut self, runtime: time::Duration) -> &mut Self {
        self.load.runtime = runtime;
        self
    }

    /// The maximum number of outstanding request any single issuer is allowed to have to the
    /// backend. Defaults to 20.
    pub fn in_flight(&mut self, max: usize) -> &mut Self {
        self.max_in_flight = max;
        self
    }

    /// The time interval, in seconds, betweeen printing stats to the console.
    pub fn report_interval(&mut self, report_interval_secs: u32) -> &mut Self {
        self.report_interval = Duration::from_secs(report_interval_secs as u64);
        self
    }

    /// Instruct the load generator to store raw histogram data of request latencies into the given
    /// file upon completion.
    pub fn with_histogram(&mut self, path: String) -> &mut Self {
        self.histogram_file = Some(path);
        self
    }
}

impl WorkloadBuilder {
    /// Run this workload with clients spawned from the given factory.
    ///
    /// If `prime` is true, the database will be seeded with stories and comments according to the
    /// scaling factory before the benchmark starts. If the site has already been primed, there is
    /// no need to prime again unless the backend is emptied or the scaling factor is changed. Note
    /// that priming does not delete the database, nor detect the current scaling factor, so always
    /// empty the backend before calling `run` with `prime` set.
    ///
    /// The provided client must be able to (asynchronously) create a `Service<TrawlerRequest>`. To
    /// do so, it must implement `Service<bool>`, where the boolean parameter indicates whether
    /// the database is also scheduled to be primed before the workload begins.
    pub async fn run<T>(&self, client: T, prime: bool) -> Result<()>
    where
        T: RequestProcessor + Clone + Send + 'static,
    {
        run(self.load.clone(), self.max_in_flight, client, prime, self.report_interval, self.histogram_file.clone()).await
    }
}
