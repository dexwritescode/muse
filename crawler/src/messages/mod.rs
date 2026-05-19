use aktor::Message;

/// Message to crawl a URL
#[derive(Debug, Clone)]
pub struct CrawlUrl {
    pub url: String,
    pub depth: u32,
}

impl Message for CrawlUrl {
    fn type_id(&self) -> &'static str {
        "CrawlUrl"
    }
}

/// Result from crawling a page
#[derive(Debug, Clone)]
pub struct PageResult {
    pub url: String,
    pub title: String,
    pub links: Vec<String>,
    pub text: String, // Full text content for indexing
    pub depth: u32,
}

impl Message for PageResult {
    fn type_id(&self) -> &'static str {
        "PageResult"
    }
}

/// Request to get frontier status
#[derive(Debug, Clone)]
pub struct GetFrontierStatus;

impl Message for GetFrontierStatus {
    fn type_id(&self) -> &'static str {
        "GetFrontierStatus"
    }
}

/// Response with frontier status
#[derive(Debug, Clone)]
pub struct FrontierStatus {
    pub queued: usize,
    pub crawled: usize,
}

impl Message for FrontierStatus {
    fn type_id(&self) -> &'static str {
        "FrontierStatus"
    }
}

/// Unified message type for all crawler actors
#[derive(Debug, Clone)]
pub enum CrawlerMessage {
    CrawlUrl(CrawlUrl),
    PageResult(PageResult),
    GetFrontierStatus(GetFrontierStatus),
    FrontierStatus(FrontierStatus),
    /// Carries fetched robots.txt body (None = unreachable/404, allow all)
    RobotsReady(Option<String>),
    /// Scheduled by DomainActor after crawl-delay elapses
    Tick,
    /// Sent by DomainActor to Frontier when it self-terminates (idle)
    DomainStopped(String),
}

impl Message for CrawlerMessage {
    fn type_id(&self) -> &'static str {
        "CrawlerMessage"
    }
}

impl From<CrawlUrl> for CrawlerMessage {
    fn from(msg: CrawlUrl) -> Self {
        CrawlerMessage::CrawlUrl(msg)
    }
}

impl From<PageResult> for CrawlerMessage {
    fn from(msg: PageResult) -> Self {
        CrawlerMessage::PageResult(msg)
    }
}

impl From<GetFrontierStatus> for CrawlerMessage {
    fn from(msg: GetFrontierStatus) -> Self {
        CrawlerMessage::GetFrontierStatus(msg)
    }
}

impl From<FrontierStatus> for CrawlerMessage {
    fn from(msg: FrontierStatus) -> Self {
        CrawlerMessage::FrontierStatus(msg)
    }
}
