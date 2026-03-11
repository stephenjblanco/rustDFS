use std::collections::HashMap;
use tokio::sync::RwLock;

use rustdfs_shared::{host::HostAddr, result::ServiceResult};

/**
 * Manages the namespace for the distributed file system.
 *  => Keeps track of files and their associated blocks and data nodes.
 *  => RwLock is used to ensure thread-safe access to the namespace data.
 */
#[derive(Debug)]
pub struct FileManager {
    files: RwLock<HashMap<String, Vec<BlockDescriptor>>>,
}

#[derive(Debug, Clone)]
pub struct BlockDescriptor {
    pub id: String,
    pub nodes: Vec<HostAddr>,
}

impl FileManager {
    /**
     * Creates a new FileManager instance.
     */
    pub fn new() -> Self {
        FileManager {
            files: RwLock::new(HashMap::new()),
        }
    }

    /**
     * Adds a new file and its block descriptors to the namespace.
     *
     *  @param file_name - Name of the file.
     *  @param blocks - Vector of BlockDescriptor for the file.
     */
    pub async fn add_file(&self, file_name: String, blocks: Vec<BlockDescriptor>) {
        let mut files = self.files.write().await;
        files.insert(file_name.to_string(), blocks);
    }

    /**
     * Retrieves the block descriptors for a given file.
     *
     *  @param file_name - Name of the file.
     *  @return ServiceResult<Vec<BlockDescriptor>> - Vector of BlockDescriptor or error.
     */
    pub async fn get_blocks(&self, file_name: &str) -> ServiceResult<Vec<BlockDescriptor>> {
        let files = self.files.read().await;
        Ok(files[file_name].clone())
    }
}
