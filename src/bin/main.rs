use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use lazy_static::lazy_static;
use reqwest::{header, Client, StatusCode};
use trawler::{LobstersRequest, RequestProcessor, TrawlerRequest, Vote};

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;

lazy_static! {
    static ref SESSION_COOKIES: RwLock<HashMap<u32, cookie::CookieJar>> = RwLock::default();
}

#[derive(Clone)]
struct WebClient {
    prefix: url::Url,
    client: Client,
}

impl WebClient {
    fn new(prefix: &str) -> Self {
        let prefix = url::Url::parse(prefix).unwrap();
        let client = Client::new();
        WebClient { prefix, client }
    }

    async fn get_cookie_for(&self, uid: u32) -> Result<cookie::CookieJar> {
        {
            let cookies = SESSION_COOKIES.read().unwrap();
            if let Some(cookie) = cookies.get(&uid) {
                return Ok(cookie.clone());
            }
        }

        let mut params = HashMap::new();
        params.insert("utf8", "✓");
        let email = &format!("user{}", uid);
        params.insert("email", email);
        params.insert("password", "test");
        params.insert("commit", "Login");
        params.insert("referer", self.prefix.as_ref());

        let url = self.prefix.join("login").unwrap();
        let res = self.client.post(url).form(&params).send().await?;

        if res.status() != StatusCode::FOUND {
            let body = res.text().await?;
            panic!(
                "Failed to log in as user{}/test. Make sure to apply the patches!\n{}",
                uid, body,
            );
        }

        let mut cookies = cookie::CookieJar::new();
        for cookie in res.cookies() {
            let c = cookie::Cookie::parse(format!("{:?}", cookie)).unwrap();
            cookies.add(c);
        }

        SESSION_COOKIES.write().unwrap().insert(uid, cookies.clone());
        Ok(cookies)
    }
}

#[async_trait]
impl RequestProcessor for WebClient {
    async fn data_prime_init(&mut self) -> Result<()> {
        return Ok(())
    }

    async fn process(
        &mut self,
        TrawlerRequest {
            user: uid,
            page: req,
            ..
        }: TrawlerRequest,
    ) -> Result<()> {
        let mut expected = StatusCode::OK;
        let mut req = match req {
            LobstersRequest::Frontpage => self.client.get(self.prefix.as_ref()),
            LobstersRequest::Recent => {
                let url = self.prefix.join("recent").unwrap();
                self.client.get(url)
            }
            LobstersRequest::Comments => {
                let url = self.prefix.join("comments").unwrap();
                self.client.get(url)
            }
            LobstersRequest::User(uid) => {
                let url = self.prefix.join(&format!("u/user{}", uid)).unwrap();
                self.client.get(url)
            }
            LobstersRequest::Login => {
                self.get_cookie_for(uid.unwrap()).await?;
                return Ok(());
            }
            LobstersRequest::Logout => {
                /*
                let url = self.prefix.join("logout").unwrap();
                self.client.post(self.prefix)
                */
                return Ok(());
            }
            LobstersRequest::Story(id) => {
                let url = self
                    .prefix
                    .join("s/")
                    .unwrap()
                    .join(::std::str::from_utf8(&id[..]).unwrap())
                    .unwrap();
                self.client.get(url)
            }
            LobstersRequest::StoryVote(story, v) => {
                let url = self
                    .prefix
                    .join(&format!(
                        "stories/{}/{}",
                        ::std::str::from_utf8(&story[..]).unwrap(),
                        match v {
                            Vote::Up => "upvote",
                            Vote::Down => "unvote",
                        }
                    ))
                    .unwrap();
                self.client.post(url)
            }
            LobstersRequest::CommentVote(comment, v) => {
                let url = self
                    .prefix
                    .join(&format!(
                        "comments/{}/{}",
                        ::std::str::from_utf8(&comment[..]).unwrap(),
                        match v {
                            Vote::Up => "upvote",
                            Vote::Down => "unvote",
                        }
                    ))
                    .unwrap();
                self.client.post(url)
            }
            LobstersRequest::Submit { id, title } => {
                expected = StatusCode::FOUND;

                let mut params = HashMap::new();
                params.insert("commit", "Submit");
                params.insert("story[short_id]", ::std::str::from_utf8(&id[..]).unwrap());
                params.insert("story[tags_a][]", "test");
                params.insert("story[title]", &title);
                params.insert("story[description]", "to infinity");
                params.insert("utf8", "✓");

                let url = self.prefix.join("stories").unwrap();
                self.client.post(url).form(&params)
            }
            LobstersRequest::Comment { id, story, parent } => {
                let mut params = HashMap::new();
                params.insert("short_id", ::std::str::from_utf8(&id[..]).unwrap());
                params.insert("comment", "moar benchmarking");
                if let Some(_parent) = parent {
                    // params.insert(
                    //     "parent_comment_short_id",
                    //     ::std::str::from_utf8(&parent[..]).unwrap(),
                    // );
                }
                params.insert("story_id", ::std::str::from_utf8(&story[..]).unwrap());
                params.insert("utf8", "✓");

                let url = self.prefix.join("comments").unwrap();
                self.client.post(url).form(&params)
            }
        };

        if let Some(uid) = uid {
            let cookies = WebClient::get_cookie_for(self, uid).await?;
            for cookie in cookies.iter() {
                let c = format!("{}", cookie);
                req = req.header(header::COOKIE, c);
            }
        };

        let res = req.send().await?;
        if res.status() != expected {
            let status = res.status();
            let body = res.text().await?;
            panic!(
                "{:?} status response. You probably forgot to prime.\n{}",
                status, body,
            );
        }
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, Debug, Parser)]
#[command(
    name = "trawler",
    version = "0.1",
    about = "Benchmark a lobste.rs Rails installation"
)]
struct Options {
    /// Reuest load scale factor for workload
    #[arg(long, default_value = "1.0")]
    scale: f64,

    /// Number of allowed concurrent requests
    #[arg(long, default_value = "50")]
    in_flight: usize,

    /// Set if the backend must be primed with initial stories and comments.
    #[arg(long, default_value = "false")]
    prime: bool,

    /// Benchmark runtime in seconds
    #[arg(short = 'r', long, default_value = "30")]
    runtime: u64,

    /// Use file-based serialized HdrHistograms. There are multiple histograms,
    /// two for each lobsters request.
    #[arg(long)]
    histogram: Option<String>,

    #[arg(long, env = "URL-PREFIX", default_value = "http://localhost:3000")]
    prefix: Option<String>,
}

fn main() {
    let options = Options::parse();

    let mut wl = trawler::WorkloadBuilder::default();
    wl.scale(options.scale)
        .time(Duration::new(options.runtime, 0));

    if let Some(h) = options.histogram {
        wl.with_histogram(h.clone());
    }

    wl.run(WebClient::new(&options.prefix.unwrap()), options.prime);
}
