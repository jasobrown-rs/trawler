use crate::{LobstersRequest, BASE_OPS_PER_MIN};
use hdrhistogram::serialization::interval_log;
use hdrhistogram::Histogram;
use std::collections::HashMap;
use std::fs;
use std::time::{Duration, SystemTime};

#[derive(Default, Clone)]
pub struct StatsReporter {
    stats: HashMap<String, crate::timing::Timeline>,
}

/// Convience struct for passing parameters to `StatsReporter::report()`.
#[derive(Clone, Debug)]
pub(crate) struct Stat {
    pub(crate) request_name: String,
    pub(crate) time_since_start: Duration,
    pub(crate) processing_time: Duration,
    pub(crate) sojourn_time: Duration,
}

/// Convience struct for passing parameters to `StatsReporter::finish()`.
#[derive(Clone, Debug)]
pub(crate) struct EndStats {
    pub(crate) start: SystemTime,
    pub(crate) scale: f64,
    pub(crate) generated_per_sec: f64,
    pub(crate) dropped: usize,
    pub(crate) total_duration: Duration,
    pub(crate) histo_file: Option<String>,
}

impl StatsReporter {
    pub fn report(&mut self, stat: Stat) {
        let hist = self
            .stats
            .entry(stat.request_name)
            .or_default()
            .histogram_for(stat.time_since_start);

        hist.processing(stat.processing_time.as_micros() as u64);
        hist.sojourn(stat.sojourn_time.as_micros() as u64);
    }

    pub fn dump_metrics(&self) {
        // TODO impl me!
        self.print_final();
    }

    pub fn finish(&mut self, end_stats: EndStats) {
        println!(
            "# target ops/s: {:.2}",
            BASE_OPS_PER_MIN as f64 * end_stats.scale / 60.0,
        );
        println!("# generated ops/s: {:.2}", end_stats.generated_per_sec);
        println!("# dropped requests: {}", end_stats.dropped);

        for timeline in self.stats.values_mut() {
            timeline.set_total_duration(end_stats.total_duration);
        }

        if let Some(ref h) = end_stats.histo_file {
            self.write_histo(end_stats.start, h)
        }

        self.print_final();
    }

    fn write_histo(&self, start: SystemTime, file_name: &str) {
        match fs::File::create(file_name) {
            Ok(mut f) => {
                use hdrhistogram::serialization::interval_log;
                use hdrhistogram::serialization::V2DeflateSerializer;
                let mut s = V2DeflateSerializer::new();
                let mut w = interval_log::IntervalLogWriterBuilder::new()
                    .with_base_time(start)
                    .begin_log_with(&mut f, &mut s)
                    .unwrap();
                for variant in LobstersRequest::all() {
                    if let Some(t) = self.stats.get(variant.name()) {
                        t.write(&mut w).unwrap();
                    } else {
                        Timeline::default().write(&mut w).unwrap();
                    }
                }
            }
            Err(e) => {
                eprintln!("failed to open histogram file for writing: {:?}", e);
            }
        }
    }

    fn print_final(&self) {
        println!("{:<12}\t{:<12}\tpct\tµs", "# op", "metric");
        for variant in LobstersRequest::all() {
            if let Some((proc_hist, sjrn_hist)) =
                self.stats.get(variant.name()).and_then(|h| h.last())
            {
                for (metric, h) in &[("processing", proc_hist), ("sojourn", sjrn_hist)] {
                    if h.max() == 0 {
                        continue;
                    }
                    for &pct in &[50, 95, 99] {
                        println!(
                            "{:<12}\t{:<12}\t{}\t{:.2}",
                            variant.name(),
                            metric,
                            pct,
                            h.value_at_quantile(pct as f64 / 100.0),
                        );
                    }
                    println!(
                        "{:<12}\t{:<12}\t100\t{:.2}",
                        variant.name(),
                        metric,
                        h.max()
                    );
                }
            }
        }
    }
}

#[derive(Default, Clone)]
pub struct Timeline {
    // these are logarithmically spaced
    // the first histogram is 0-1s after start, the second 1-2s after start, then 2-4s, etc.
    histograms: Vec<Histograms>,
    total_duration: Duration,
}

#[derive(Clone)]
pub struct Histograms {
    processing: Histogram<u64>,
    sojourn: Histogram<u64>,
}

impl Default for Histograms {
    fn default() -> Self {
        Self {
            processing: Histogram::new_with_bounds(1, 60_000_000, 3).unwrap(),
            sojourn: Histogram::new_with_bounds(1, 60_000_000, 3).unwrap(),
        }
    }
}

impl Histograms {
    pub fn processing(&mut self, time: u64) {
        self.processing.saturating_record(time);
    }

    pub fn sojourn(&mut self, time: u64) {
        self.sojourn.saturating_record(time);
    }
}

impl Timeline {
    pub fn set_total_duration(&mut self, total: Duration) {
        self.total_duration = total;
    }

    pub fn histogram_for(&mut self, issued_at: Duration) -> &mut Histograms {
        let hist = ((issued_at.as_secs_f64() + 0.000000000001).ceil() as usize)
            .next_power_of_two()
            .trailing_zeros() as usize;

        if hist >= self.histograms.len() {
            self.histograms.resize(hist + 1, Histograms::default());
        }
        self.histograms.get_mut(hist).unwrap()
    }

    pub fn write<W: std::io::Write, S: hdrhistogram::serialization::Serializer>(
        &self,
        w: &mut interval_log::IntervalLogWriter<W, S>,
    ) -> Result<(), interval_log::IntervalLogWriterError<S::SerializeError>> {
        let proc_tag = interval_log::Tag::new("processing").unwrap();
        let sjrn_tag = interval_log::Tag::new("sojourn").unwrap();
        for (i, hs) in self.histograms.iter().enumerate() {
            let start = Duration::from_secs((1 << i) >> 1);
            let mut dur = Duration::from_secs(1 << i) - start;
            if self.total_duration != Duration::new(0, 0) && start + dur > self.total_duration {
                dur = self.total_duration - start;
            }
            w.write_histogram(&hs.processing, start, dur, Some(proc_tag))?;
            w.write_histogram(&hs.sojourn, start, dur, Some(sjrn_tag))?;
        }
        Ok(())
    }

    pub fn last(&self) -> Option<(&Histogram<u64>, &Histogram<u64>)> {
        self.histograms.last().map(|h| (&h.processing, &h.sojourn))
    }
}
