use futures::StreamExt;
use futures::stream;
use std::path::Path;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::transport::Channel;

use crate::args::RustDFSArgs;
use crate::proto::NameReadRequest;
use crate::proto::NameWriteRequest;
use crate::proto::name_node_client::NameNodeClient;

const BLOCK_SIZE: usize = 1024 * 1024 * 2; // 2 MB

/**
 * Runs supported operations (read / write) on RustDFS name node.
 */
pub trait RustDFSClient {
    async fn client_write(&mut self, args: RustDFSArgs);
    async fn client_read(&mut self, args: RustDFSArgs);
}

impl RustDFSClient for NameNodeClient<Channel> {
    /**
     * Writes a file to the RustDFS cluster.
     * Breaks file into 2 MB blocks and streams to name node.
     *
     *  @param args - Program args including source / destination file path.
     *
     * Source file from args corresponsds to local file path.
     * Destination file from args corresponds to file name in RustDFS cluster.
     */
    async fn client_write(&mut self, args: RustDFSArgs) {
        let rs = stream::unfold(
            (args.dest, File::open(&args.source).await.unwrap()),
            |(name, mut file)| async move {
                let mut buf = vec![0u8; BLOCK_SIZE];
                let n = file.read(&mut buf).await.unwrap();

                if n == 0 {
                    return None;
                }

                Some((
                    NameWriteRequest { file_name: name.clone(), data: buf[..n].to_vec() },
                    (name, file),
                ))
            },
        );

        self.write(rs).await.unwrap();
    }

    /**
     * Reads a file from the RustDFS cluster.
     * Streams file blocks from name node and writes to local file.
     * Creates parent directories as needed.
     *
     *  @param args - Program args including source / destination file path.
     *
     * Source file from args corresponsds to file name in RustDFS cluster.
     * Destination file from args corresponds to local file path.
     */
    async fn client_read(&mut self, args: RustDFSArgs) {
        let mut stream =
            self.read(NameReadRequest { file_name: args.source }).await.unwrap().into_inner();

        if let Some(parent) = Path::new(&args.dest).parent() {
            fs::create_dir_all(parent).await.unwrap();
        }

        let mut file = File::create(&args.dest).await.unwrap();

        while let Some(res) = stream.next().await {
            file.write_all(&res.unwrap().data).await.unwrap();
        }
    }
}
