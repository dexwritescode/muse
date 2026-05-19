use std::collections::VecDeque;
use std::time::Duration;

use aktor::extensions::AsyncHttpClientExtension;
use aktor::{Actor, ActorContext, ActorError, ActorRef};
use robotstxt::matcher::{LongestMatchRobotsMatchStrategy, RobotsMatcher};
use tracing::{debug, info, warn};
use url::Url;

use super::crawler::CrawlerActor;
use crate::messages::{CrawlUrl, CrawlerMessage};

const USER_AGENT: &str = "muse-crawler";

enum RobotsState {
    Fetching,
    Done(Option<String>),
}

#[derive(Debug)]
pub struct DomainActor {
    domain: String,
    queue: VecDeque<CrawlUrl>,
    robots_state: RobotsState,
    crawl_delay: Duration,
    default_delay: Duration,
    in_flight: bool,
    frontier_ref: ActorRef<CrawlerMessage>,
}

impl std::fmt::Debug for RobotsState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RobotsState::Fetching => write!(f, "Fetching"),
            RobotsState::Done(Some(_)) => write!(f, "Done(Some(...))"),
            RobotsState::Done(None) => write!(f, "Done(None)"),
        }
    }
}

impl DomainActor {
    pub fn new(
        domain: String,
        default_delay: Duration,
        frontier_ref: ActorRef<CrawlerMessage>,
    ) -> Self {
        Self {
            domain,
            queue: VecDeque::new(),
            robots_state: RobotsState::Fetching,
            crawl_delay: default_delay,
            default_delay,
            in_flight: false,
            frontier_ref,
        }
    }

    fn is_allowed(&self, url: &str) -> bool {
        let RobotsState::Done(ref body_opt) = self.robots_state else {
            return true;
        };
        let Some(body) = body_opt else {
            return true;
        };
        let mut matcher = RobotsMatcher::<LongestMatchRobotsMatchStrategy>::default();
        matcher.one_agent_allowed_by_robots(body, USER_AGENT, url)
    }

    fn try_dispatch(&mut self, ctx: &ActorContext<CrawlerMessage>) {
        if self.in_flight {
            return;
        }
        let crawl_url = loop {
            let Some(candidate) = self.queue.pop_front() else {
                return;
            };
            if self.is_allowed(&candidate.url) {
                break candidate;
            }
            debug!("robots.txt disallows {}", candidate.url);
        };

        self.in_flight = true;
        let self_ref = ctx.actor_ref().clone();
        let crawler = CrawlerActor::new(self_ref);
        let name = format!("fetch-{}", uuid::Uuid::new_v4().simple());
        match ctx.spawn_child(&name, crawler, None) {
            Ok(crawler_ref) => {
                if let Err(e) = crawler_ref.tell(CrawlerMessage::CrawlUrl(crawl_url), None) {
                    tracing::error!("Failed to send URL to ephemeral crawler: {}", e);
                    self.in_flight = false;
                }
            }
            Err(e) => {
                tracing::error!("Failed to spawn crawler child: {}", e);
                self.in_flight = false;
            }
        }
    }
}

fn parse_crawl_delay(robots_body: &str) -> Option<Duration> {
    let mut in_matching_section = false;
    for line in robots_body.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        if let Some(val) = line.strip_prefix("User-agent:") {
            let agent = val.trim();
            in_matching_section = agent == "*" || agent.eq_ignore_ascii_case(USER_AGENT);
        } else if in_matching_section
            && let Some(val) = line.strip_prefix("Crawl-delay:")
            && let Ok(secs) = val.trim().parse::<f64>()
        {
            return Some(Duration::from_secs_f64(secs));
        }
    }
    None
}

impl Actor for DomainActor {
    type Msg = CrawlerMessage;

    fn pre_start(&mut self, ctx: &ActorContext<CrawlerMessage>) -> Result<(), ActorError> {
        info!("DomainActor started for {}", self.domain);
        let robots_url = format!("https://{}/robots.txt", self.domain);
        let client = ctx
            .system()
            .extension::<AsyncHttpClientExtension>()
            .client();
        ctx.pipe_to_self::<_, String>(async move {
            let body = match client.get(&robots_url).send().await {
                Ok(resp) if resp.status().is_success() => resp.text().await.ok(),
                _ => None,
            };
            Ok(CrawlerMessage::RobotsReady(body))
        });
        Ok(())
    }

    fn handle(&mut self, msg: CrawlerMessage, ctx: &ActorContext<CrawlerMessage>) {
        match msg {
            CrawlerMessage::CrawlUrl(crawl_url) => {
                self.queue.push_back(crawl_url);
                if matches!(self.robots_state, RobotsState::Done(_)) {
                    self.try_dispatch(ctx);
                }
            }

            CrawlerMessage::RobotsReady(body) => {
                let delay = body
                    .as_deref()
                    .and_then(parse_crawl_delay)
                    .unwrap_or(self.default_delay);
                self.crawl_delay = delay;
                if body.is_some() {
                    info!(
                        "robots.txt fetched for {} (crawl-delay: {:?})",
                        self.domain, delay
                    );
                } else {
                    debug!(
                        "No robots.txt for {} — using default delay {:?}",
                        self.domain, delay
                    );
                }
                self.robots_state = RobotsState::Done(body);
                self.try_dispatch(ctx);
            }

            CrawlerMessage::PageResult(result) => {
                self.in_flight = false;
                if let Err(e) = self
                    .frontier_ref
                    .tell(CrawlerMessage::PageResult(result), None)
                {
                    warn!("Failed to forward PageResult to frontier: {}", e);
                }
                if self.queue.is_empty() {
                    info!("DomainActor idle for {} — stopping", self.domain);
                    ctx.stop_self();
                } else {
                    ctx.schedule_to_self(self.crawl_delay, CrawlerMessage::Tick);
                }
            }

            CrawlerMessage::Tick => {
                self.try_dispatch(ctx);
            }

            _ => {}
        }
    }

    fn post_stop(&mut self, _ctx: &ActorContext<CrawlerMessage>) -> Result<(), ActorError> {
        let _ = self
            .frontier_ref
            .tell(CrawlerMessage::DomainStopped(self.domain.clone()), None);
        Ok(())
    }
}

pub fn extract_domain(url: &str) -> Option<String> {
    Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
}
