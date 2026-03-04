use std::fs::{self, OpenOptions};
use std::io::{Write, Error as IoError};
use std::path::Path;

use rustdfs_shared::base::error::RustDFSError;
use rustdfs_shared::base::result::{Result, ServiceResult};
use rustdfs_shared::base::logging::LogManager;
use tonic::Status;

#[derive(Debug)]
pub struct DataDirManager {
    path: String,
    log_mgr: LogManager,
}

impl DataDirManager {

    pub fn new(
        path_str: &str,
        log_mgr: LogManager,
    ) -> Result<Self> {
        let path = Path::new(path_str);

        if path.exists() && !path.is_dir() {
            let err = err_invalid_dir(path_str);
            log_mgr.write_err(&err);
            return Err(err);
        } else {
            fs::create_dir_all(path)
                .map_err(|e| {
                    let err = RustDFSError::IoError(e);
                    log_mgr.write_err(&err);
                    err
                })?;
        }

        Ok(
            DataDirManager {
                path: path_str.to_string(),
                log_mgr: log_mgr,
            }
        )
    }

    pub fn write_block(
        &self, 
        block_id: &str, 
        data: &[u8]
    ) -> ServiceResult<()> {
        let block_path = format!("{}/{}", self.path, block_id);

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&block_path)
            .map_err(|e| {
                let err = status_err_writing(block_id, e);
                self.log_mgr.write_status(&err);
                err
            })?;

        file.write_all(data)
            .map_err(|e| {
                let err = status_err_writing(block_id, e);
                self.log_mgr.write_status(&err);
                err
            })?;

        Ok(())
    }

    pub fn read_block(
        &self, 
        block_id: &str
    ) -> ServiceResult<Vec<u8>> {
        let block_path = format!("{}/{}", self.path, block_id);

        fs::read(block_path)
            .map_err(|e| {
                let err = status_err_reading(block_id, e);
                self.log_mgr.write_status(&err);
                err
            })   
    }
}

fn err_invalid_dir(
    path: &str,
) -> RustDFSError {
    let str = format!("Invalid data directory path: {}", path);
    RustDFSError::CustomError(str)
}

fn status_err_writing(
    block: &str,
    err: IoError,
) -> Status {
    let str = format!("Encountered IoError writing block {}: {}", block, err);
    Status::internal(str)
}

fn status_err_reading(
    block: &str,
    err: IoError,
) -> Status {
    let str = format!("Encountered IoError reading block {}: {}", block, err);
    Status::internal(str)
}
