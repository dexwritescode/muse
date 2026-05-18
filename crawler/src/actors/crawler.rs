use std::sync::Arc;

use aktor::{Actor, ActorContext, ActorRef};
use scraper::{Html, Selector};
use tracing::{debug, error, info};
use url::Url;

use crate::messages::{CrawlUrl, CrawlerMessage, PageResult};

/// Fetches and parses a single URL, then reports PageResult to the frontier.
/// Holds a direct ref to the frontier so results can be returned regardless
/// of message-type differences between parent and child (see ctx.parent_address()).
/// Shares the frontier's reqwest::Client for connection pooling.
#[derive(Debug)]
pub struct CrawlerActor {
    frontier: ActorRef<CrawlerMessage>,
    client: Arc<reqwest::Client>,
}

impl CrawlerActor {
    pub fn new(frontier: ActorRef<CrawlerMessage>, client: Arc<reqwest::Client>) -> Self {
        Self { frontier, client }
    }
}

fn extract_links(html: &str, base_url: &str) -> Vec<String> {
    let document = Html::parse_document(html);
    let link_selector = match Selector::parse("a[href]") {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to parse link selector: {}", e);
            return Vec::new();
        }
    };
    let base = match Url::parse(base_url) {
        Ok(u) => u,
        Err(e) => {
            error!("Failed to parse base URL {}: {}", base_url, e);
            return Vec::new();
        }
    };
    let mut links = Vec::new();
    for element in document.select(&link_selector) {
        if let Some(href) = element.value().attr("href")
            && let Ok(absolute) = base.join(href)
            && (absolute.scheme() == "http" || absolute.scheme() == "https")
        {
            links.push(absolute.to_string());
        }
    }
    links
}

fn extract_title(html: &str) -> String {
    let document = Html::parse_document(html);
    let selector = match Selector::parse("title") {
        Ok(s) => s,
        Err(_) => return String::from("(No title)"),
    };
    document
        .select(&selector)
        .next()
        .map(|el| el.text().collect::<String>())
        .unwrap_or_else(|| String::from("(No title)"))
}

fn extract_text(html: &str) -> String {
    let document = Html::parse_document(html);
    let body_selector = match Selector::parse("body") {
        Ok(s) => s,
        Err(_) => return String::new(),
    };
    let mut parts = Vec::new();
    if let Some(body) = document.select(&body_selector).next() {
        for node in body.descendants() {
            if let Some(element) = node.value().as_element()
                && (element.name() == "script" || element.name() == "style")
            {
                continue;
            }
            if let Some(text) = node.value().as_text() {
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    parts.push(trimmed.to_string());
                }
            }
        }
    }
    parts.join(" ")
}

impl Actor for CrawlerActor {
    type Msg = CrawlerMessage;

    fn handle(&mut self, msg: CrawlerMessage, ctx: &ActorContext<CrawlerMessage>) {
        match msg {
            CrawlerMessage::CrawlUrl(CrawlUrl { url, depth }) => {
                info!("Fetching: {} (depth: {})", url, depth);
                let client = self.client.clone();
                ctx.pipe_to_self(async move {
                    let response = client.get(&url).send().await.map_err(|e| e.to_string())?;
                    if !response.status().is_success() {
                        return Err(format!("HTTP {} for {}", response.status(), url));
                    }
                    let html = response.text().await.map_err(|e| e.to_string())?;
                    let title = extract_title(&html);
                    let links = extract_links(&html, &url);
                    let text = extract_text(&html);
                    debug!(
                        "Extracted {} links, {} chars from {}",
                        links.len(),
                        text.len(),
                        url
                    );
                    Ok(CrawlerMessage::PageResult(PageResult {
                        url,
                        title,
                        links,
                        text,
                        depth,
                    }))
                });
            }
            CrawlerMessage::PageResult(result) => {
                if let Err(e) = self.frontier.tell(CrawlerMessage::PageResult(result), None) {
                    error!("Failed to send PageResult to frontier: {}", e);
                }
                ctx.stop_self();
            }
            _ => {}
        }
    }
}
