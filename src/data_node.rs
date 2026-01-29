mod daemon;

use crate::daemon::shared::error::RustDFSError;

#[tokio::main]
async fn main() -> Result<(), RustDFSError> {
    use crate::daemon::data_node::service::DataNodeService;
    use crate::daemon::shared::config::RustDFSConfig;

    let config = RustDFSConfig::new()?;

    for (k, v) in &config.data_nodes {
        println!("Data Node ID: {}", k);
        println!("Host: {}, Port: {}", v.host, v.port);
    }

    DataNodeService::new(config)?
        .serve()
        .await?;

    Ok(())
}