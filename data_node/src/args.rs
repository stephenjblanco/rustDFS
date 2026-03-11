use clap::Parser;

use rustdfs_shared::logging::LogLevel;

/**
 * Command line arguments for RustDFS data node.
 *
 *  @field port - Port number for the data node service.
 *  @field silent - If true, suppresses console output.
 *  @field log_level - Logging level for the data node.
 *
 * CLI Usage:
 *  rustdfs-datanode --port <PORT> [--silent] [--log-level <LOG_LEVEL>]
 */
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct RustDFSArgs {
    #[arg(short, long, default_value_t = 5001)]
    pub port: u16,

    #[arg(short, long, default_value_t = false)]
    pub silent: bool,

    #[arg(short, long, value_enum, default_value_t = LogLevel::Info)]
    pub log_level: LogLevel,
}

impl Default for RustDFSArgs {
    fn default() -> Self {
        Self::new()
    }
}

impl RustDFSArgs {
    pub fn new() -> Self {
        Self::parse()
    }
}
