use crate::rustdfs::shared::error::RustDFSError;
use tonic::Status;

pub type Result<T> = std::result::Result<T, RustDFSError>;
pub type ServiceResult<T> = std::result::Result<T, Status>;
