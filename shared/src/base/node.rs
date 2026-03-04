use std::net::{SocketAddr, ToSocketAddrs};

use super::error::RustDFSError;
use super::result::Result;

#[derive(Debug)]
pub struct GenericNode {
    pub host: String,
    pub port: u16,
}

pub trait Node {
    fn to_socket_addr(&self) -> Result<SocketAddr>;
}

impl Node for GenericNode {

    fn to_socket_addr(
        &self
    ) -> Result<SocketAddr> {
        let addr_str = format!("{}:{}", self.host, self.port);
        addr_str.to_socket_addrs()
            .map_err(|e| RustDFSError::IoError(e))?
            .next()
            .ok_or_else(|| err_invalid_addr(&self.host, self.port))
    }
}

fn err_invalid_addr(
    host: &str,
    port: u16,
) -> RustDFSError {
    let str = format!("Invalid address: {}:{}", host, port);
    RustDFSError::CustomError(str)
}
