// Default constants
pub const ADDRESS: &str = "0.0.0.0";
pub const PORT: u16 = 8000;
// The default interval in bytes between icy metadata chunks
// The metaint cannot be changed per client once the response has been sent
// https://thecodeartist.blogspot.com/2013/02/shoutcast-internet-radio-protocol.html
pub const METAINT: usize = 16_000;
// Server that gets sent in the header
pub const SERVER_ID: &str = "Rusty Zenith 0.1.0";
// Contact information
pub const ADMIN: &str = "admin@localhost";
// Public facing domain/address
pub const HOST: &str = "localhost";
// Geographic location. Icecast included it in their settings, so why not
pub const LOCATION: &str = "1.048596";
// Description of the internet radio
pub const DESCRIPTION: &str = "Yet Another Internet Radio";

// How many regular sources, not including relays
pub const SOURCES: usize = 4;
// How many sources can be connected, in total
pub const MAX_SOURCES: usize = 4;
// How many clients can be connected, in total
pub const CLIENTS: usize = 400;
// How many bytes a client can have queued until they get disconnected
pub const QUEUE_SIZE: usize = 102400;
// How many bytes to send to the client all at once when they first connect
// Useful for filling up the client buffer quickly, but also introduces some delay
pub const BURST_SIZE: usize = 65536;
// How long in milliseconds a client has to send a complete request
pub const HEADER_TIMEOUT: u64 = 15_000;
// How long in milliseconds a source has to send something before being disconnected
pub const SOURCE_TIMEOUT: u64 = 10_000;
// The maximum size in bytes of an acceptable http message not including the body
pub const HTTP_MAX_LENGTH: usize = 8192;
// The maximum number of redirects allowed, when fetching relays from another server/stream
pub const HTTP_MAX_REDIRECTS: usize = 5;

pub const DEFAULT_CHARSET: &str = "utf-8";
pub const DEFAULT_CONTENT_TYPE: &str = "text/plain";
pub const JSON_CONTENT_TYPE: &str = "application/json";
pub const HTML_CONTENT_TYPE: &str = "text/html";
