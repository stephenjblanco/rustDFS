mod data_mgr;
mod service;

use rustdfs_shared::args::RustDFSArgs;
use rustdfs_shared::config::RustDFSConfig;
use service::DataNodeService;

#[tokio::main]
async fn main() {
    let config = RustDFSConfig::new().unwrap();
    let args = RustDFSArgs::new();

    DataNodeService::new(args, config)
        .unwrap()
        .serve()
        .await
        .unwrap();
}
