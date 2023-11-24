use std::collections::HashMap;

use crate::consts::{
    ADDRESS, ADMIN, BURST_SIZE, CLIENTS, DESCRIPTION, HEADER_TIMEOUT, HOST, HTTP_MAX_LENGTH,
    HTTP_MAX_REDIRECTS, LOCATION, MAX_SOURCES, METAINT, PORT, QUEUE_SIZE, SERVER_ID, SOURCES,
    SOURCE_TIMEOUT,
};
use crate::structs::{Credential, MasterServer, ServerLimits, SourceLimits};

// Serde default deserialization values
pub fn default_property_address() -> String {
    ADDRESS.to_string()
}

pub fn default_property_port() -> u16 {
    PORT
}

pub fn default_property_metaint() -> usize {
    METAINT
}

pub fn default_property_server_id() -> String {
    SERVER_ID.to_string()
}

pub fn default_property_admin() -> String {
    ADMIN.to_string()
}

pub fn default_property_host() -> String {
    HOST.to_string()
}

pub fn default_property_location() -> String {
    LOCATION.to_string()
}

pub fn default_property_description() -> String {
    DESCRIPTION.to_string()
}

pub fn default_property_users() -> Vec<Credential> {
    vec![
        Credential {
            username: "admin".to_string(),
            password: "hackme".to_string(),
        },
        Credential {
            username: "source".to_string(),
            password: "hackme".to_string(),
        },
    ]
}

pub fn default_property_limits() -> ServerLimits {
    ServerLimits {
        clients: default_property_limits_clients(),
        sources: default_property_limits_sources(),
        total_sources: default_property_limits_total_sources(),
        queue_size: default_property_limits_queue_size(),
        burst_size: default_property_limits_burst_size(),
        header_timeout: default_property_limits_header_timeout(),
        source_timeout: default_property_limits_source_timeout(),
        source_limits: default_property_limits_source_limits(),
        http_max_length: default_property_limits_http_max_length(),
        http_max_redirects: default_property_limits_http_max_redirects(),
    }
}

pub fn default_property_limits_clients() -> usize {
    CLIENTS
}

pub fn default_property_limits_sources() -> usize {
    SOURCES
}

pub fn default_property_limits_total_sources() -> usize {
    MAX_SOURCES
}

pub fn default_property_limits_queue_size() -> usize {
    QUEUE_SIZE
}

pub fn default_property_limits_burst_size() -> usize {
    BURST_SIZE
}

pub fn default_property_limits_header_timeout() -> u64 {
    HEADER_TIMEOUT
}

pub fn default_property_limits_source_timeout() -> u64 {
    SOURCE_TIMEOUT
}

pub fn default_property_limits_http_max_length() -> usize {
    HTTP_MAX_LENGTH
}

pub fn default_property_limits_http_max_redirects() -> usize {
    HTTP_MAX_REDIRECTS
}

pub fn default_property_limits_source_mountpoint() -> String {
    "/radio".to_string()
}

pub fn default_property_limits_source_limits() -> HashMap<String, SourceLimits> {
    let mut map = HashMap::new();
    map.insert(
        default_property_limits_source_mountpoint(),
        SourceLimits {
            clients: default_property_limits_clients(),
            burst_size: default_property_limits_burst_size(),
            source_timeout: default_property_limits_source_timeout(),
        },
    );
    map
}

pub fn default_property_master_server() -> MasterServer {
    MasterServer {
        enabled: default_property_master_server_enabled(),
        url: default_property_master_server_url(),
        update_interval: default_property_master_server_update_interval(),
        relay_limit: default_property_master_server_relay_limit(),
    }
}

pub fn default_property_master_server_enabled() -> bool {
    false
}

pub fn default_property_master_server_url() -> String {
    format!("http://localhost:{}", default_property_port() + 1)
}

pub fn default_property_master_server_update_interval() -> u64 {
    120
}

pub fn default_property_master_server_relay_limit() -> usize {
    SOURCES
}
