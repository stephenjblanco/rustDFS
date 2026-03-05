use clap::Parser;

use super::logging::LogLevel;

/**
 * Command line arguments for RustDFS name node.
 *
 *  @field id - Unique identifier for the name node.
 *  @field silent - If true, suppresses console output.
 *  @field log_level - Logging level for the name node.
 *
 * CLI Usage:
 *  rustdfs-<BIN_TYPE> --id <NODE_ID> [--silent] [--log-level <LOG_LEVEL>]
 */
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct RustDFSArgs {
    #[arg(long)]
    pub id: String,

    #[arg(short, long, default_value_t = false)]
    pub silent: bool,

    #[arg(short, long, value_enum, default_value_t = LogLevel::Info)]
    pub log_level: LogLevel,
}

impl RustDFSArgs {
    pub fn new() -> Self {
        Self::parse()
    }
}
