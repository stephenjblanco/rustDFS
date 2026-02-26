mod proto;
mod service;
mod name_mgr;

use rustdfs_shared::base::args::RustDFSArgs;
use rustdfs_shared::base::result::Result;
use rustdfs_shared::base::config::RustDFSConfig;

use service::NameNodeService;

#[tokio::main]
async fn main() -> Result<()> {
    let config = RustDFSConfig::new()?;
    let args = RustDFSArgs::new();

    NameNodeService::new(args, config)?
        .serve()
        .await?;

    Ok(())
}
