use aktor::extensions::HttpClientExtension;
use aktor::{ActorSystem, ActorSystemConfig, Extension};
use crawler::{CrawlUrl, CrawlerActor, CrawlerMessage};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("=== Web Crawler Demo ===\n");

    // Create actor system
    let config = ActorSystemConfig::default();
    let system: Arc<ActorSystem<CrawlerMessage>> = ActorSystem::new(config).await?;

    // Register HTTP client extension
    system.register_extension(HttpClientExtension::new_extension());
    println!("✓ Registered HttpClientExtension");

    // Create crawler actors as children of the system
    let crawler1 = system.actor_of::<CrawlerActor>("crawler-1").await?;
    let _crawler2 = system.actor_of::<CrawlerActor>("crawler-2").await?;
    println!("✓ Created {} crawler actors", 2);

    // Send some URLs to crawl
    let test_url = CrawlUrl {
        url: "https://example.com".to_string(),
        depth: 0,
    };

    crawler1.tell(CrawlerMessage::CrawlUrl(test_url.clone()), None)?;
    println!("✓ Sent URL to crawler: {}", test_url.url);

    // Wait for crawling to complete
    println!("\nCrawling...");
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    println!("\nShutting down...");
    system.shutdown().await?;

    Ok(())
}
