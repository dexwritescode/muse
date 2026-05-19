use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

use aktor::{Actor, ActorContext, ActorError, ActorRef};
use tracing::{debug, info};

use super::domain::{DomainActor, extract_domain};
use crate::messages::{CrawlUrl, CrawlerMessage, FrontierStatus, PageResult};

#[derive(Debug)]
pub struct URLFrontierActor {
    queue: VecDeque<CrawlUrl>,
    seen: HashSet<String>,
    crawled_count: usize,
    max_depth: u32,
    max_urls: usize,
    default_delay: Duration,
    domains: HashMap<String, ActorRef<CrawlerMessage>>,
}

impl URLFrontierActor {
    pub fn new(max_depth: u32, max_urls: usize, default_delay: Duration) -> Self {
        Self {
            queue: VecDeque::new(),
            seen: HashSet::new(),
            crawled_count: 0,
            max_depth,
            max_urls,
            default_delay,
            domains: HashMap::new(),
        }
    }

    fn domain_actor(
        &mut self,
        domain: &str,
        ctx: &ActorContext<CrawlerMessage>,
    ) -> Option<&ActorRef<CrawlerMessage>> {
        if !self.domains.contains_key(domain) {
            let actor = DomainActor::new(
                domain.to_string(),
                self.default_delay,
                ctx.actor_ref().clone(),
            );
            match ctx.spawn_child(domain, actor, None) {
                Ok(actor_ref) => {
                    info!("Spawned DomainActor for {}", domain);
                    self.domains.insert(domain.to_string(), actor_ref);
                }
                Err(e) => {
                    tracing::error!("Failed to spawn DomainActor for {}: {}", domain, e);
                    return None;
                }
            }
        }
        self.domains.get(domain)
    }

    fn route_url(&mut self, crawl_url: CrawlUrl, ctx: &ActorContext<CrawlerMessage>) {
        let Some(domain) = extract_domain(&crawl_url.url) else {
            debug!("Could not extract domain from {}", crawl_url.url);
            return;
        };
        if let Some(actor_ref) = self.domain_actor(&domain, ctx)
            && let Err(e) = actor_ref.tell(CrawlerMessage::CrawlUrl(crawl_url), None)
        {
            tracing::error!("Failed to route URL to DomainActor {}: {}", domain, e);
        }
    }

    fn handle_page_result(&mut self, result: PageResult, ctx: &ActorContext<CrawlerMessage>) {
        self.crawled_count += 1;
        info!(
            "Crawled: {} (depth: {}) — {} links, {} chars — {}/{}",
            result.url,
            result.depth,
            result.links.len(),
            result.text.len(),
            self.crawled_count,
            self.max_urls
        );

        if self.crawled_count >= self.max_urls {
            info!("Reached max URL limit ({}), stopping", self.max_urls);
            return;
        }

        if result.depth >= self.max_depth {
            return;
        }

        let new_depth = result.depth + 1;
        for link in result.links {
            let normalized = link.trim().to_string();
            if normalized.is_empty()
                || (!normalized.starts_with("http://") && !normalized.starts_with("https://"))
            {
                continue;
            }
            if self.seen.insert(normalized.clone()) {
                let crawl_url = CrawlUrl {
                    url: normalized,
                    depth: new_depth,
                };
                self.route_url(crawl_url, ctx);
            }
        }
    }

    fn handle_status_request(&self, ctx: &ActorContext<CrawlerMessage>) {
        if ctx.is_ask_request() {
            let status = FrontierStatus {
                queued: self.queue.len(),
                crawled: self.crawled_count,
            };
            let ctx_clone = ctx.clone();
            tokio::spawn(async move {
                let _ = ctx_clone
                    .respond(CrawlerMessage::FrontierStatus(status))
                    .await;
            });
        }
    }
}

impl Default for URLFrontierActor {
    fn default() -> Self {
        Self::new(3, 100, Duration::from_secs(1))
    }
}

impl Actor for URLFrontierActor {
    type Msg = CrawlerMessage;

    fn pre_start(&mut self, _ctx: &ActorContext<CrawlerMessage>) -> Result<(), ActorError> {
        info!(
            "URLFrontierActor started (max_depth: {}, max_urls: {}, default_delay: {:?})",
            self.max_depth, self.max_urls, self.default_delay
        );
        Ok(())
    }

    fn handle(&mut self, msg: CrawlerMessage, ctx: &ActorContext<CrawlerMessage>) {
        match msg {
            CrawlerMessage::CrawlUrl(crawl_url) if self.seen.insert(crawl_url.url.clone()) => {
                self.route_url(crawl_url, ctx);
            }
            CrawlerMessage::CrawlUrl(_) => {}
            CrawlerMessage::PageResult(result) => {
                self.handle_page_result(result, ctx);
            }
            CrawlerMessage::GetFrontierStatus(_) => {
                self.handle_status_request(ctx);
            }
            CrawlerMessage::DomainStopped(domain) => {
                debug!("DomainActor stopped for {}", domain);
                self.domains.remove(&domain);
            }
            _ => {}
        }
    }

    fn post_stop(&mut self, _ctx: &ActorContext<CrawlerMessage>) -> Result<(), ActorError> {
        info!(
            "URLFrontierActor stopped — crawled {} URLs",
            self.crawled_count
        );
        Ok(())
    }
}
