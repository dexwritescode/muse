use aktor::{Actor, ActorContext, ActorError, ActorRef};
use std::collections::{HashSet, VecDeque};
use tracing::{debug, info};

use crate::messages::{CrawlUrl, CrawlerMessage, FrontierStatus, PageResult};

/// URLFrontierActor manages the queue of URLs to crawl
/// It handles deduplication and maintains crawl statistics
#[derive(Debug)]
pub struct URLFrontierActor {
    /// Queue of URLs waiting to be crawled
    queue: VecDeque<CrawlUrl>,
    /// Set of URLs we've already seen (for deduplication)
    seen: HashSet<String>,
    /// Count of URLs we've crawled
    crawled_count: usize,
    /// Maximum crawl depth
    max_depth: u32,
    /// Maximum URLs to crawl (safety limit)
    max_urls: usize,
    /// Reference to crawler actor(s)
    crawler_refs: Vec<ActorRef<CrawlerMessage>>,
    /// Current crawler index for round-robin distribution
    current_crawler: usize,
}

impl URLFrontierActor {
    pub fn new(max_depth: u32, max_urls: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            seen: HashSet::new(),
            crawled_count: 0,
            max_depth,
            max_urls,
            crawler_refs: Vec::new(),
            current_crawler: 0,
        }
    }

    pub fn add_seed_url(&mut self, url: String) {
        if self.seen.insert(url.clone()) {
            self.queue.push_back(CrawlUrl { url, depth: 0 });
            info!("Added seed URL to frontier");
        }
    }

    pub fn register_crawler(&mut self, crawler: ActorRef<CrawlerMessage>) {
        self.crawler_refs.push(crawler);
        info!("Registered crawler actor");
    }

    fn handle_crawl_request(&mut self) {
        if self.crawled_count >= self.max_urls {
            info!("Reached max URL limit ({}), stopping", self.max_urls);
            return;
        }
        // Send next URL to a crawler if available
        if let Some(crawl_url) = self.queue.pop_front()
            && !self.crawler_refs.is_empty()
        {
            // Round-robin distribution to crawlers
            let crawler = &self.crawler_refs[self.current_crawler];
            self.current_crawler = (self.current_crawler + 1) % self.crawler_refs.len();

            debug!(
                "Sending URL to crawler: {} (depth: {})",
                crawl_url.url, crawl_url.depth
            );

            // TODO: How does the crawler knows where to send the response to?
            if let Err(e) = crawler.tell(CrawlerMessage::CrawlUrl(crawl_url), None) {
                tracing::error!("Failed to send URL to crawler: {}", e);
            }
        }
    }

    fn handle_page_result(&mut self, result: PageResult) {
        self.crawled_count += 1;

        info!(
            "Crawled: {} (depth: {}) - Found {} links, {} chars text - Progress: {}/{}",
            result.url,
            result.depth,
            result.links.len(),
            result.text.len(),
            self.crawled_count,
            self.max_urls
        );

        // Add new URLs to queue if we haven't exceeded depth limit
        if result.depth < self.max_depth {
            let new_depth = result.depth + 1;
            let mut added = 0;

            for link in result.links {
                // Basic URL normalization and deduplication
                let normalized = link.trim().to_string();

                if !normalized.is_empty()
                    && (normalized.starts_with("http://") || normalized.starts_with("https://"))
                    && self.seen.insert(normalized.clone())
                {
                    self.queue.push_back(CrawlUrl {
                        url: normalized,
                        depth: new_depth,
                    });
                    added += 1;
                }
            }

            if added > 0 {
                debug!("Added {} new URLs to frontier", added);
            }
        }

        // Keep crawling
        self.handle_crawl_request();
    }

    fn handle_status_request(&self, ctx: &ActorContext<CrawlerMessage>) {
        let status = FrontierStatus {
            queued: self.queue.len(),
            crawled: self.crawled_count,
        };

        if ctx.is_ask_request() {
            let ctx_clone = ctx.clone();
            tokio::spawn(async move {
                let _ = ctx_clone
                    .respond(CrawlerMessage::FrontierStatus(status))
                    .await;
            });
        }
    }
}

impl Actor<CrawlerMessage> for URLFrontierActor {
    fn handle(&mut self, msg: CrawlerMessage, ctx: &ActorContext<CrawlerMessage>) {
        match msg {
            CrawlerMessage::CrawlUrl(crawl_url) => {
                // Re-add URL to queue (from external source)
                if self.seen.insert(crawl_url.url.clone()) {
                    self.queue.push_back(crawl_url);
                }
                self.handle_crawl_request();
            }
            CrawlerMessage::PageResult(result) => {
                self.handle_page_result(result);
            }
            CrawlerMessage::GetFrontierStatus(_) => {
                self.handle_status_request(ctx);
            }
            CrawlerMessage::FrontierStatus(_) => {
                // Ignored by frontier
            }
        }
    }

    fn pre_start(&mut self, _ctx: &ActorContext<CrawlerMessage>) -> Result<(), ActorError> {
        info!(
            "URLFrontierActor started (max_depth: {}, max_urls: {})",
            self.max_depth, self.max_urls
        );
        Ok(())
    }

    fn post_stop(&mut self, _ctx: &ActorContext<CrawlerMessage>) -> Result<(), ActorError> {
        info!(
            "URLFrontierActor stopped - Crawled {} URLs",
            self.crawled_count
        );
        Ok(())
    }
}
