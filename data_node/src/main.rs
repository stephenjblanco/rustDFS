mod data_node;

use data_node::service::DataNodeService;

use rustdfs_shared::args::RustDFSArgs;
use rustdfs_shared::result::Result;
use rustdfs_shared::config::RustDFSConfig;

#[tokio::main]
async fn main() -> Result<()> {
    let config = RustDFSConfig::new()?;
    let args = RustDFSArgs::new();

    for (k, v) in &config.data_nodes {
        println!("Data Node ID: {}", k);
        println!("Host: {}, Port: {}", v.host, v.port);
    }

    DataNodeService::new(args, config)?
        .serve()
        .await?;

    Ok(())
}
