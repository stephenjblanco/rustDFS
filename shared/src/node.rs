use std::net::{SocketAddr, ToSocketAddrs};

use super::error::RustDFSError;
use super::result::Result;

/**
 * Represents a generic network node with host and port.
 */
#[derive(Debug)]
pub struct GenericNode {
    pub host: String,
    pub port: u16,
}

 // Convert GenericNode to [SocketAddr]
impl From<&GenericNode> for Result<SocketAddr> {

    fn from(node: &GenericNode) -> Self {
        let addr_str = format!("{}:{}", node.host, node.port);
        addr_str.to_socket_addrs()
            .map_err(|e| RustDFSError::IoError(e))?
            .next()
            .ok_or_else(|| err_invalid_addr(&node.host, node.port))
    }
}

fn err_invalid_addr(
    host: &str,
    port: u16,
) -> RustDFSError {
    let str = format!("Invalid address: {}:{}", host, port);
    RustDFSError::CustomError(str)
}
