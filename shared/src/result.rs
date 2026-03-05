use tonic::Status;

use super::error::RustDFSError;

/**
 * Result type alias for operations that return [RustDFSError]
 * or [tonic::Status].
 */
pub type Result<T> = std::result::Result<T, RustDFSError>;
pub type ServiceResult<T> = std::result::Result<T, Status>;
