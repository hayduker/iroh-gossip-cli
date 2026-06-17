use anyhow::Result;
use iroh::protocol::Router;
use iroh::{Endpoint, endpoint::presets};
use iroh_gossip::net::Gossip;

#[tokio::main]
async fn main() -> Result<()> {
    let endpoint = Endpoint::builder(presets::N0)
        .bind()
        .await?;

    println!("> our endpoint id: {}", endpoint.id());

    let gossip = Gossip::builder().spawn(endpoint.clone());

    let router = Router::builder(endpoint.clone())
        .accept(iroh_gossip::ALPN, gossip.clone())
        .spawn();

    router.shutdown().await?;

    Ok(())
}