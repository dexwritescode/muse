//! CrawlerActor - Processes a single URL and extracts content
//!
//! Design principles:
//! - Stateless (uses HttpClientExtension for HTTP requests)
//! - Synchronous message handling
//! - Extracts links and text content for indexing

use aktor::{Actor, ActorContext};
use aktor::extensions::HttpClientExtension;
use scraper::{Html, Selector};
use tracing::{info, error, debug, warn};
use url::Url;

use crate::messages::{CrawlerMessage, CrawlUrl, PageResult};

/// CrawlerActor processes URLs and extracts content
///
/// This actor uses the HttpClientExtension to make HTTP requests without
/// holding the client in its state (which would prevent serialization).
#[derive(Debug, Default)]
pub struct CrawlerActor;

impl CrawlerActor {
    pub fn new() -> Self {
        Self
    }

    /// Extract links from HTML content
    fn extract_links(&self, html: &str, base_url: &str) -> Vec<String> {
        let document = Html::parse_document(html);

        // Select all anchor tags with href attributes
        let link_selector = match Selector::parse("a[href]") {
            Ok(selector) => selector,
            Err(e) => {
                error!("Failed to parse link selector: {}", e);
                return Vec::new();
            }
        };

        let base = match Url::parse(base_url) {
            Ok(url) => url,
            Err(e) => {
                error!("Failed to parse base URL {}: {}", base_url, e);
                return Vec::new();
            }
        };

        let mut links = Vec::new();

        for element in document.select(&link_selector) {
            if let Some(href) = element.value().attr("href") {
                // Resolve relative URLs
                match base.join(href) {
                    Ok(absolute_url) => {
                        // Only include http/https URLs
                        if absolute_url.scheme() == "http" || absolute_url.scheme() == "https" {
                            links.push(absolute_url.to_string());
                        }
                    }
                    Err(e) => {
                        debug!("Failed to join URL {} with base {}: {}", href, base_url, e);
                    }
                }
            }
        }

        links
    }

    /// Extract page title from HTML content
    fn extract_title(&self, html: &str) -> String {
        let document = Html::parse_document(html);

        let title_selector = match Selector::parse("title") {
            Ok(selector) => selector,
            Err(e) => {
                error!("Failed to parse title selector: {}", e);
                return String::from("(No title)");
            }
        };

        if let Some(title_element) = document.select(&title_selector).next() {
            title_element.text().collect::<String>()
        } else {
            String::from("(No title)")
        }
    }

    /// Extract visible text content from HTML for indexing
    fn extract_text(&self, html: &str) -> String {
        let document = Html::parse_document(html);

        // Select body content (ignore scripts, styles, etc.)
        let body_selector = match Selector::parse("body") {
            Ok(selector) => selector,
            Err(e) => {
                error!("Failed to parse body selector: {}", e);
                return String::new();
            }
        };

        // Remove script and style tags (done manually below)

        let mut text_parts = Vec::new();

        if let Some(body) = document.select(&body_selector).next() {
            // Get all text, filtering out script/style content
            for node in body.descendants() {
                if let Some(element) = node.value().as_element() {
                    if element.name() == "script" || element.name() == "style" {
                        continue;
                    }
                }

                if let Some(text) = node.value().as_text() {
                    let trimmed = text.trim();
                    if !trimmed.is_empty() {
                        text_parts.push(trimmed.to_string());
                    }
                }
            }
        }

        text_parts.join(" ")
    }

    /// Crawl a single URL (synchronous, blocking I/O)
    fn crawl_url(&self, crawl_url: CrawlUrl, ctx: &ActorContext<CrawlerMessage>) {
        let url = crawl_url.url.clone();
        let depth = crawl_url.depth;

        info!("Crawling: {} (depth: {})", url, depth);

        // Get HTTP client from extension (not held in actor state!)
        let http_client = ctx.system().extension::<HttpClientExtension>();

        // Make blocking HTTP request
        let response = match http_client.get(&url).send() {
            Ok(resp) => resp,
            Err(e) => {
                error!("Failed to fetch {}: {}", url, e);
                return;
            }
        };

        // Check if successful
        if !response.status().is_success() {
            warn!("HTTP {} for {}", response.status(), url);
            return;
        }

        // Extract HTML body
        let html = match response.text() {
            Ok(text) => text,
            Err(e) => {
                error!("Failed to read body from {}: {}", url, e);
                return;
            }
        };

        // Parse and extract data
        let title = self.extract_title(&html);
        let links = self.extract_links(&html, &url);
        let text = self.extract_text(&html);

        debug!("Extracted {} links and {} chars of text from {}",
               links.len(), text.len(), url);

        // Create result
        let result = PageResult {
            url: url.clone(),
            title,
            links,
            text,
            depth,
        };

        // Send result back to parent (frontier)
        // TODO: Use ctx.sender() when implemented (Task 1.19) for more flexibility
        if let Some(parent) = ctx.parent() {
            if let Err(e) = parent.tell(CrawlerMessage::PageResult(result), None) {
                error!("Failed to send result back to parent: {}", e);
            }
        } else {
            warn!("No parent - cannot send results back (orphan crawler)");
        }

        // TODO: Self-terminate after crawling (Task 1.20)
        // ctx.stop_self() or similar API needed
    }
}

impl Actor<CrawlerMessage> for CrawlerActor {
    fn handle(&mut self, msg: CrawlerMessage, ctx: &ActorContext<CrawlerMessage>) {
        match msg {
            CrawlerMessage::CrawlUrl(crawl_url) => {
                // Process the URL synchronously
                self.crawl_url(crawl_url, ctx);
            }
            _ => {
                // Ignore other messages
            }
        }
    }
}