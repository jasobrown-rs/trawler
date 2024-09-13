use crate::client::{LobstersRequest, RequestProcessor, TrawlerRequest, Vote};
use crate::execution::{self, id_to_slug, Sampler, MAX_SLUGGABLE_ID};
use crate::timing::{EndStats, Stat, StatsReporter};
use crate::BASE_OPS_PER_MIN;

use anyhow::Result;
use futures_util::stream::futures_unordered::FuturesUnordered;
use futures_util::stream::StreamExt;
use rand::distributions::Distribution;
use rand::rngs::ThreadRng;
use rand::{self, Rng};
use tokio::sync::mpsc::{channel, Receiver};

use std::sync::atomic;
use std::time;
use tokio::time::{Duration, Instant};

/// The number of concurrent operations to allow during data priming.
const PRIMING_CONCURRENT_OPS: usize = 64;

fn next_request<F>(
    pick: &mut F,
    sampler: &Sampler,
    rng: &mut ThreadRng,
    ncomments: u32,
    nstories: u32,
) -> LobstersRequest
where
    F: FnMut(isize) -> bool,
{
    if pick(55842) {
        // XXX: we're assuming here that stories with more votes are viewed more
        LobstersRequest::Story(id_to_slug(sampler.story_for_vote(rng)))
    } else if pick(30105) {
        LobstersRequest::Frontpage
    } else if pick(6702) {
        // XXX: we're assuming that users who vote a lot are also "popular"
        LobstersRequest::User(sampler.user(rng))
    } else if pick(4674) {
        LobstersRequest::Comments
    } else if pick(967) {
        LobstersRequest::Recent
    } else if pick(630) {
        LobstersRequest::CommentVote(id_to_slug(sampler.comment_for_vote(rng)), Vote::Up)
    } else if pick(475) {
        LobstersRequest::StoryVote(id_to_slug(sampler.story_for_vote(rng)), Vote::Up)
    } else if pick(316) {
        // comments without a parent
        LobstersRequest::Comment {
            id: id_to_slug(rng.gen_range(ncomments..MAX_SLUGGABLE_ID)),
            story: id_to_slug(sampler.story_for_comment(rng)),
            parent: None,
        }
    } else if pick(87) {
        LobstersRequest::Login
    } else if pick(71) {
        // comments with a parent
        let id = rng.gen_range(ncomments..MAX_SLUGGABLE_ID);
        let story = sampler.story_for_comment(rng);
        // we need to pick a comment that's on the chosen story
        // we know that every nth comment from prepopulation is to the same story
        let comments_per_story = ncomments / nstories;
        let parent = story + nstories * rng.gen_range(0..comments_per_story);
        LobstersRequest::Comment {
            id: id_to_slug(id),
            story: id_to_slug(story),
            parent: Some(id_to_slug(parent)),
        }
    } else if pick(54) {
        LobstersRequest::CommentVote(id_to_slug(sampler.comment_for_vote(rng)), Vote::Down)
    } else if pick(53) {
        let id = rng.gen_range(nstories..MAX_SLUGGABLE_ID);
        LobstersRequest::Submit {
            id: id_to_slug(id),
            title: format!("benchmark {}", id),
        }
    } else if pick(21) {
        LobstersRequest::StoryVote(id_to_slug(sampler.story_for_vote(rng)), Vote::Down)
    } else {
        // ~.003%
        LobstersRequest::Logout
    }
}

macro_rules! await_all {
    ($fu:ident) => {
        if !$fu.is_empty() {
            let mut futs = std::mem::replace(&mut $fu, FuturesUnordered::new());
            tokio::spawn(async move {
                while let Some(r) = futs.next().await {
                    r.unwrap().unwrap();
                }
            })
            .await?;
        }
    };
}

macro_rules! maybe_await_all {
    ($fu:ident, $cap:expr) => {
        if $fu.len() >= $cap {
            await_all!($fu);
        }
    };
}

async fn prime<T>(client: T, sampler: Sampler, concurrent_ops: usize) -> Result<()>
where
    T: RequestProcessor + Clone + Send + 'static,
{
    eprintln!("--> priming database");
    let start = time::Instant::now();

    client.clone().data_prime_init().await?;

    // log in all the users
    let mut futs = FuturesUnordered::new();
    eprintln!("inserting {} users ...", sampler.nusers());
    for uid in 0..sampler.nusers() {
        let mut c = client.clone();
        futs.push(tokio::spawn(async move {
            c.process(TrawlerRequest {
                user: Some(uid),
                page: LobstersRequest::Login,
                is_priming: true,
            })
            .await
        }));
        maybe_await_all!(futs, concurrent_ops);
    }
    await_all!(futs);

    let mut rng = rand::thread_rng();
    // prime the database stories
    let mut futs = FuturesUnordered::new();
    let nstories = sampler.nstories();
    eprintln!("inserting {} stories ...", nstories);
    for id in 0..nstories {
        // NOTE: we're assuming that users who vote much also submit many stories
        let uid = sampler.user(&mut rng);
        let mut c = client.clone();
        futs.push(tokio::spawn(async move {
            c.process(TrawlerRequest {
                user: Some(uid),
                page: LobstersRequest::Submit {
                    id: id_to_slug(id),
                    title: format!("Base article {}", id),
                },
                is_priming: true,
            })
            .await
        }));
        maybe_await_all!(futs, concurrent_ops);
    }
    await_all!(futs);

    // insert comments on stories
    eprintln!("inserting {} comments ...", sampler.ncomments());
    for id in 0..sampler.ncomments() {
        let story = id % nstories; // TODO: distribution

        // synchronize occasionally to ensure that we can safely generate parent comments
        if story == 0 {
            await_all!(futs);
        }

        let parent = if rng.gen_bool(0.5) {
            // we need to pick a parent in the same story
            let generated_comments = id - story;
            // how many stories to we know there are per story?
            let generated_comments_per_story = generated_comments / nstories;
            // pick the nth comment to chosen story
            if generated_comments_per_story != 0 {
                let story_comment = rng.gen_range(0..generated_comments_per_story);
                Some(story + nstories * story_comment)
            } else {
                None
            }
        } else {
            None
        };

        // NOTE: we're assuming that users who vote much also submit many stories
        let uid = sampler.user(&mut rng);
        let mut c = client.clone();
        futs.push(tokio::spawn(async move {
            c.process(TrawlerRequest {
                user: Some(uid),
                page: LobstersRequest::Comment {
                    id: id_to_slug(id),
                    story: id_to_slug(story),
                    parent: parent.map(id_to_slug),
                },
                is_priming: true,
            })
            .await
        }));
        maybe_await_all!(futs, concurrent_ops);
    }

    // wait for all priming comments
    await_all!(futs);
    eprintln!(
        "--> finished priming database in {:?} seconds",
        start.elapsed().as_secs()
    );
    Ok(())
}

pub(crate) async fn run<T>(
    load: execution::Workload,
    in_flight: usize,
    mut client: T,
    prime_database: bool,
    report_interval: Duration,
    histo_file: Option<String>,
) -> Result<()>
where
    T: RequestProcessor + Clone + Send + 'static,
{
    let target = BASE_OPS_PER_MIN as f64 * load.scale / 60.0;

    // generating a request takes a while because we have to generate random numbers (including
    // zipfs). so, depending on the target load, we may need more than one load generation
    // thread. we'll make them all share the pool of issuers though.
    let generator_capacity = 100_000.0; // req/s == 10 µs to generate a request
    assert!(
        target < generator_capacity,
        "one generator thread cannot generate that much load"
    );

    // compute how many of each thing there will be in the database after scaling by mem_scale
    let sampler = Sampler::new(load.scale);

    if prime_database {
        let c = client.clone();
        let s = sampler.clone();
        prime(c, s, PRIMING_CONCURRENT_OPS).await?;
    }

    let start_t = time::SystemTime::now();
    let start = time::Instant::now();
    let end = start + load.runtime;

    let nstories = sampler.nstories();
    let ncomments = sampler.ncomments();

    let mut ops = 0;
    let npending = &*Box::leak(Box::new(atomic::AtomicUsize::new(0)));
    let mut rng = rand::thread_rng();
    let interarrival_ns = rand_distr::Exp::new(target * 1e-9).unwrap();

    let stats_reporter = StatsReporter::default();
    let (stat_sender, stat_receiver) = channel(in_flight * 2);
    let (stats_stop_sender, stats_stop_receiver) = channel(1);

    let _ = tokio::spawn(async move {
        let _ = stats_report(
            report_interval,
            stats_reporter,
            stat_receiver,
            stats_stop_receiver,
        ).await;
    });

    let mut next = time::Instant::now();
    while next < end {
        let now = time::Instant::now();
        if next > now || npending.load(atomic::Ordering::Acquire) > in_flight {
            if now > end {
                // don't spin after we need to be done
                break;
            }
            std::hint::spin_loop();
            continue;
        }

        // randomly pick next request type based on relative frequency
        let mut seed: isize = rng.gen_range(0..100000);
        let seed = &mut seed;
        let mut pick = |f| {
            let applies = *seed <= f;
            *seed -= f;
            applies
        };

        // XXX: we're assuming that basically all page views happen as a user, and that the users
        // who are most active voters are also the ones that interact most with the site.
        // XXX: we're assuming that users who vote a lot also comment a lot
        // XXX: we're assuming that users who vote a lot also submit many stories
        let user = Some(sampler.user(&mut rng));
        let req = next_request(&mut pick, &sampler, &mut rng, ncomments, nstories);

        ops += 1;

        let issued = next;
        npending.fetch_add(1, atomic::Ordering::AcqRel);

        let mut c = client.clone();
        let stat_sender = stat_sender.clone();

        tokio::spawn(async move {
            let request_name = req.name().to_string();
            let fut = c.process(TrawlerRequest {
                user,
                page: req,
                is_priming: false,
            });
            let _ = issued.elapsed();
            let sent = time::Instant::now();
            let response = fut.await;
            npending.fetch_sub(1, atomic::Ordering::AcqRel);

            match response {
                Ok(_) => {
                    let stat = Stat {
                        request_name,
                        time_since_start: issued.duration_since(start),
                        processing_time: sent.elapsed(),
                        sojourn_time: issued.elapsed(),
                    };
                    let _ = stat_sender.send(stat).await;
                }
                Err(e) => {
                    eprintln!("Error recived: {:?}", e)
                }
            }
        });

        // schedule next delivery
        next += time::Duration::from_nanos(interarrival_ns.sample(&mut rng) as u64);
    }

    // wait a short amount of time for tasks to complete,
    let loop_end_time = Instant::now() + Duration::from_secs(60);
    while loop_end_time > Instant::now() {
        let cnt = npending.load(atomic::Ordering::Acquire);
        if cnt == 0 {
            break;
        }
        println!("waiting for {:?} tasks", cnt);
        std::thread::sleep(std::time::Duration::from_secs(2));
    }

    let total_duration = start.elapsed();
    ops -= npending.load(atomic::Ordering::Acquire);
    let per_second = ops as f64 / total_duration.as_secs_f64();

    let _ = stats_stop_sender.send(EndStats {
        start: start_t,
        scale: load.scale,
        generated_per_sec: per_second,
        dropped: 0,
        total_duration,
        histo_file,
    });

    client.shutdown().await?;

    Ok(())
}

async fn stats_report(
    report_interval_sec: Duration,
    mut stats_reporter: StatsReporter,
    mut stat_tx: Receiver<Stat>,
    mut stop_tx: Receiver<EndStats>,
) -> Result<()> {
    let mut interval = tokio::time::interval(report_interval_sec);

    tokio::select! {
        biased;
        // bias toward dumping stats on time, so observers do not get worried
        _ = interval.tick() => stats_reporter.dump_metrics(),

        stat = stat_tx.recv() => {
            if let Some(stat) = stat {
                stats_reporter.report(stat);
            }
        }

        end_stats = stop_tx.recv() => {
            if let Some(end_stats) = end_stats {
                stats_reporter.finish(end_stats);
            }
            return Ok(());
        }
    }

    Ok(())
}
