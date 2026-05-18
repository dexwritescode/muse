use aktor::{ActorSystem, ActorSystemConfig};
use crawler::{CrawlUrl, CrawlerMessage, URLFrontierActor};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Web Crawler Demo ===\n");

    let config = ActorSystemConfig::default();
    let system: Arc<ActorSystem> = ActorSystem::new(config).await?;

    let frontier = system.actor_of::<URLFrontierActor>("frontier").await?;
    println!("✓ Spawned URLFrontierActor (crawlers initialised in pre_start)");

    let seed = CrawlUrl {
        url: "https://example.com".to_string(),
        depth: 0,
    };
    frontier.tell(CrawlerMessage::CrawlUrl(seed.clone()), None)?;
    println!("✓ Sent seed URL to frontier: {}", seed.url);

    println!("\nCrawling...");
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    println!("\nShutting down...");
    system.shutdown().await?;

    Ok(())
}
