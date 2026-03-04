use std::collections::HashMap;

use tonic::Status;

use crate::base::result::ServiceResult;
use crate::base::logging::LogManager;
use super::conn::DataNodeConn;

#[derive(Debug)]
pub struct DataNodeManager {
    connections: HashMap<String, DataNodeConn>,
    log_mgr: LogManager,
}

impl DataNodeManager {

    pub fn new(
        connections: HashMap<String, DataNodeConn>,
        log_mgr: LogManager,
    ) -> Self {
        DataNodeManager {
            connections: connections,
            log_mgr: log_mgr,
        }
    }

    pub fn get_node_ids(
        &self,
    ) -> Vec<&String> {
        self.connections
            .keys()
            .collect()
    }

    pub fn get_conn(
        &self,
        id: &str,
    ) -> ServiceResult<&DataNodeConn> {
        self.connections
            .get(id)
            .ok_or_else(|| {
                let err = status_err_unknown_node(id);
                self.log_mgr.write_status(&err);
                err
            })
    }
}

fn status_err_unknown_node(
    node_id: &str,
) -> Status {
    let log = format!("Unknown data node: {}", node_id).to_string();
    Status::invalid_argument(log)
}
