use std::collections::HashMap;
use std::error::Error;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use httparse::{Error as HttpError, Status};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::RwLock;
use tokio_native_tls::TlsStream;
use uuid::Uuid;

use crate::validators::{
    default_property_address, default_property_admin, default_property_description,
    default_property_host, default_property_limits, default_property_limits_burst_size,
    default_property_limits_clients, default_property_limits_header_timeout,
    default_property_limits_http_max_length, default_property_limits_http_max_redirects,
    default_property_limits_queue_size, default_property_limits_source_limits,
    default_property_limits_source_timeout, default_property_limits_sources,
    default_property_limits_total_sources, default_property_location,
    default_property_master_server, default_property_master_server_enabled,
    default_property_master_server_relay_limit, default_property_master_server_update_interval,
    default_property_master_server_url, default_property_metaint, default_property_port,
    default_property_server_id, default_property_users,
};

#[derive(Clone)]
pub struct Query {
    pub field: String,
    pub value: String,
}

pub struct Client {
    pub source: RwLock<String>,
    pub sender: RwLock<UnboundedSender<Arc<Vec<u8>>>>,
    pub receiver: RwLock<UnboundedReceiver<Arc<Vec<u8>>>>,
    pub buffer_size: RwLock<usize>,
    pub properties: ClientProperties,
    pub stats: RwLock<ClientStats>,
}

#[derive(Serialize, Clone)]
pub struct ClientProperties {
    pub id: Uuid,
    pub uagent: Option<String>,
    pub metadata: bool,
}

#[derive(Serialize, Clone)]
pub struct ClientStats {
    pub start_time: u64,
    pub bytes_sent: usize,
}

#[derive(Serialize, Clone)]
pub struct IcyMetadata {
    pub title: Option<String>,
    pub url: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SourceLimits {
    #[serde(default = "default_property_limits_clients")]
    pub clients: usize,
    #[serde(default = "default_property_limits_burst_size")]
    pub burst_size: usize,
    #[serde(default = "default_property_limits_source_timeout")]
    pub source_timeout: u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SourceStats {
    pub start_time: u64,
    pub bytes_read: usize,
    pub peak_listeners: usize,
}

// TODO Add a list of "relay" structs
// Relay structs should have an auth of their own, if provided
// Having a master server is not very good
// It would be much better to add relays through an api or something
#[derive(Serialize, Deserialize, Clone)]
pub struct MasterServer {
    #[serde(default = "default_property_master_server_enabled")]
    pub enabled: bool,
    #[serde(default = "default_property_master_server_url")]
    pub url: String,
    #[serde(default = "default_property_master_server_update_interval")]
    pub update_interval: u64,
    #[serde(default = "default_property_master_server_relay_limit")]
    pub relay_limit: usize,
}

// TODO Add permissions, specifically source, admin, bypass client count, etc
#[derive(Serialize, Deserialize, Clone)]
pub struct Credential {
    pub username: String,
    pub password: String,
}

// Add a total source limit
#[derive(Serialize, Deserialize, Clone)]
pub struct ServerLimits {
    #[serde(default = "default_property_limits_clients")]
    pub clients: usize,
    #[serde(default = "default_property_limits_sources")]
    pub sources: usize,
    #[serde(default = "default_property_limits_total_sources")]
    pub total_sources: usize,
    #[serde(default = "default_property_limits_queue_size")]
    pub queue_size: usize,
    #[serde(default = "default_property_limits_burst_size")]
    pub burst_size: usize,
    #[serde(default = "default_property_limits_header_timeout")]
    pub header_timeout: u64,
    #[serde(default = "default_property_limits_source_timeout")]
    pub source_timeout: u64,
    #[serde(default = "default_property_limits_http_max_length")]
    pub http_max_length: usize,
    #[serde(default = "default_property_limits_http_max_redirects")]
    pub http_max_redirects: usize,
    #[serde(default = "default_property_limits_source_limits")]
    pub source_limits: HashMap<String, SourceLimits>,
}

#[derive(PartialEq)]
pub enum TransferEncoding {
    Identity,
    Chunked,
    Length(usize),
}

#[derive(Serialize, Clone)]
pub struct IcyProperties {
    pub uagent: Option<String>,
    pub public: bool,
    pub name: Option<String>,
    pub description: Option<String>,
    pub url: Option<String>,
    pub genre: Option<String>,
    pub bitrate: Option<String>,
    pub content_type: String,
}

impl IcyProperties {
    pub fn new(content_type: String) -> IcyProperties {
        IcyProperties {
            uagent: None,
            public: false,
            name: None,
            description: None,
            url: None,
            genre: None,
            bitrate: None,
            content_type,
        }
    }
}

// TODO Add something determining if a source is a relay, or any other kind of source, for that matter
// TODO Implement hidden sources
pub struct Source {
    // Is setting the mountpoint in the source really useful, since it's not like the source has any use for it
    pub mountpoint: String,
    pub properties: IcyProperties,
    pub metadata: Option<IcyMetadata>,
    pub metadata_vec: Vec<u8>,
    pub clients: HashMap<Uuid, Arc<RwLock<Client>>>,
    pub burst_buffer: Vec<u8>,
    pub stats: RwLock<SourceStats>,
    pub fallback: Option<String>,
    // Not really sure how else to signal when to disconnect the source
    pub disconnect_flag: bool,
}

impl Source {
    pub fn new(mountpoint: String, properties: IcyProperties) -> Source {
        Source {
            mountpoint,
            properties,
            metadata: None,
            metadata_vec: vec![0],
            clients: HashMap::new(),
            burst_buffer: Vec::new(),
            stats: RwLock::new(SourceStats {
                start_time: {
                    if let Ok(time) = SystemTime::now().duration_since(UNIX_EPOCH) {
                        time.as_secs()
                    } else {
                        0
                    }
                },
                bytes_read: 0,
                peak_listeners: 0,
            }),
            fallback: None,
            disconnect_flag: false,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ServerProperties {
    // Ideally, there would be multiple addresses and ports and TLS support
    #[serde(default = "default_property_address")]
    pub address: String,
    #[serde(default = "default_property_port")]
    pub port: u16,
    #[serde(default = "default_property_metaint")]
    pub metaint: usize,
    #[serde(default = "default_property_server_id")]
    pub server_id: String,
    #[serde(default = "default_property_admin")]
    pub admin: String,
    #[serde(default = "default_property_host")]
    pub host: String,
    #[serde(default = "default_property_location")]
    pub location: String,
    #[serde(default = "default_property_description")]
    pub description: String,
    #[serde(default = "default_property_limits")]
    pub limits: ServerLimits,
    #[serde(default = "default_property_users")]
    pub users: Vec<Credential>,
    #[serde(default = "default_property_master_server")]
    pub master_server: MasterServer,
}

impl ServerProperties {
    pub fn new() -> ServerProperties {
        ServerProperties {
            address: default_property_address(),
            port: default_property_port(),
            metaint: default_property_metaint(),
            server_id: default_property_server_id(),
            admin: default_property_admin(),
            host: default_property_host(),
            location: default_property_location(),
            description: default_property_description(),
            limits: default_property_limits(),
            users: default_property_users(),
            master_server: default_property_master_server(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ServerStats {
    pub start_time: u64,
    pub peak_listeners: usize,
    pub session_bytes_sent: usize,
    pub session_bytes_read: usize,
}

impl ServerStats {
    pub fn new() -> ServerStats {
        ServerStats {
            start_time: 0,
            peak_listeners: 0,
            session_bytes_sent: 0,
            session_bytes_read: 0,
        }
    }
}

pub struct Server {
    pub sources: HashMap<String, Arc<RwLock<Source>>>,
    pub clients: HashMap<Uuid, ClientProperties>,
    // TODO Find a better place to put these, for constant time fetching
    pub source_count: usize,
    pub relay_count: usize,
    pub properties: ServerProperties,
    pub stats: ServerStats,
}

impl Server {
    pub fn new(properties: ServerProperties) -> Server {
        Server {
            sources: HashMap::new(),
            clients: HashMap::new(),
            source_count: 0,
            relay_count: 0,
            properties,
            stats: ServerStats::new(),
        }
    }
}

pub enum RorR {
    Request,
    Response,
}

pub enum ReqOrRes<'a, 'b> {
    Request(httparse::Request<'a, 'b>),
    Response(httparse::Response<'a, 'b>),
}

impl<'a, 'b> ReqOrRes<'a, 'b> {
    pub fn parse<'c: 'b>(&mut self, buffer: &'c [u8]) -> Result<Status<usize>, HttpError> {
        match self {
            ReqOrRes::Response(response) => response.parse(buffer),
            ReqOrRes::Request(request) => request.parse(buffer),
        }
    }
}

pub enum Stream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl Stream {
    pub async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Stream::Plain(stream) => stream.read(buf).await,
            Stream::Tls(stream) => stream.read(buf).await,
        }
    }

    pub async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Stream::Plain(stream) => stream.write_all(buf).await,
            Stream::Tls(stream) => stream.write_all(buf).await,
        }
    }
}

pub struct StreamDecoder {
    encoding: TransferEncoding,
    remainder: usize,
    chunk: Vec<u8>,
}

impl StreamDecoder {
    pub fn new(encoding: TransferEncoding) -> StreamDecoder {
        let remainder = match &encoding {
            TransferEncoding::Length(v) => *v,
            _ => 1,
        };
        StreamDecoder {
            encoding,
            remainder,
            chunk: Vec::new(),
        }
    }

    pub fn decode(
        &mut self,
        out: &mut Vec<u8>,
        buf: &[u8],
        length: usize,
    ) -> Result<usize, Box<dyn Error + Send>> {
        if length == 0 || self.is_finished() {
            return Ok(0);
        }

        match &self.encoding {
            TransferEncoding::Identity => {
                out.extend_from_slice(&buf[..length]);
                Ok(length)
            }
            TransferEncoding::Chunked => self.extract_chunked(out, &buf, length),
            TransferEncoding::Length(_) => {
                let allowed = std::cmp::min(length, self.remainder);
                if allowed != 0 {
                    out.extend_from_slice(&buf[..allowed]);
                    self.remainder -= allowed;
                }
                Ok(allowed)
            }
        }
    }

    fn extract_chunked(
        &mut self,
        out: &mut Vec<u8>,
        buf: &&[u8],
        length: usize,
    ) -> Result<usize, Box<dyn Error + Send>> {
        let mut read = 0;
        let mut index = 0;

        while index < length && self.remainder != 0 {
            match self.remainder {
                1 => {
                    // Get the chunk size
                    self.chunk.push(buf[index]);
                    index += 1;
                    if self.chunk.windows(2).nth_back(0) == Some(b"\r\n") {
                        // Ignore chunk extensions
                        let cutoff = match self.chunk.iter().position(|&x| x == b';' || x == b'\r')
                        {
                            Some(cutoff) => cutoff,
                            None => {
                                return Err(Box::new(std::io::Error::new(
                                    ErrorKind::InvalidData,
                                    "Missing CRLF",
                                )));
                            }
                        };
                        self.remainder = match std::str::from_utf8(&self.chunk[..cutoff]) {
                            Ok(res) => match usize::from_str_radix(res, 16) {
                                Ok(hex) => hex,
                                Err(e) => {
                                    return Err(Box::new(std::io::Error::new(
                                        ErrorKind::InvalidData,
                                        format!("Invalid value provided for chunk size: {}", e),
                                    )))
                                }
                            },
                            Err(e) => {
                                return Err(Box::new(std::io::Error::new(
                                    ErrorKind::InvalidData,
                                    format!("Could not parse chunk size: {}", e),
                                )))
                            }
                        };
                        // Check if it's the last chunk
                        // Ignore trailers
                        if self.remainder != 0 {
                            // +2 for remainder
                            // +2 for extra CRLF
                            self.remainder += 4;
                            self.chunk.clear();
                        }
                    }
                }
                2 => {
                    // No more chunk data should be read
                    if self.chunk.windows(2).nth_back(0) == Some(b"\r\n") {
                        // Append current data
                        read += self.chunk.len() - 2;
                        out.extend_from_slice(&self.chunk[..self.chunk.len() - 2]);
                        // Prepare for reading the next chunk size
                        self.remainder = 1;
                        self.chunk.clear();
                    } else {
                        return Err(Box::new(std::io::Error::new(
                            ErrorKind::InvalidData,
                            "Missing CRLF from chunk",
                        )));
                    }
                }
                v => {
                    // Get the chunk data
                    let max_read = std::cmp::min(length - index, v - 2);
                    self.chunk.extend_from_slice(&buf[index..index + max_read]);
                    index += max_read;
                    self.remainder -= max_read;
                }
            }
        }

        Ok(read)
    }

    pub fn is_finished(&self) -> bool {
        self.encoding != TransferEncoding::Identity && self.remainder == 0
    }
}
