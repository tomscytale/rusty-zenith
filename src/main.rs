use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, ErrorKind, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use httparse::Status;
use httpdate::fmt_http_date;
use serde::Deserialize;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio::time::timeout;
use tokio_native_tls::native_tls::TlsConnector;
use url::Url;

use crate::structs::{MasterServer, Query, Server, ServerProperties, Stream};

mod consts;
mod server;
mod structs;
mod validators;

async fn read_http_response(
    stream: &mut Stream,
    buffer: &mut Vec<u8>,
    max_len: usize,
) -> Result<usize, Box<dyn Error>> {
    let mut buf = [0; 1024];
    loop {
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut res = httparse::Response::new(&mut headers);
        let read = stream.read(&mut buf).await?;
        buffer.extend_from_slice(&buf[..read]);
        match res.parse(&buffer) {
            Ok(Status::Complete(offset)) => return Ok(offset),
            Ok(Status::Partial) if buffer.len() > max_len => {
                return Err(Box::new(std::io::Error::new(
                    ErrorKind::Other,
                    "Request exceeded the maximum allowed length",
                )));
            }
            Ok(Status::Partial) => (),
            Err(e) => {
                return Err(Box::new(std::io::Error::new(
                    ErrorKind::InvalidData,
                    format!("Received an invalid request: {}", e),
                )));
            }
        }
    }
}

async fn connect_and_redirect(
    url: String,
    headers: Vec<String>,
    max_len: usize,
    max_redirects: usize,
) -> Result<(Stream, Vec<u8>), Box<dyn Error>> {
    let mut str_url = url;
    let mut remaining_redirects = max_redirects;
    loop {
        let mut url = Url::parse(str_url.as_str())?;
        if let Some(host) = url.host_str() {
            let addr = {
                if let Some(port) = url.port_or_known_default() {
                    format!("{}:{}", host, port)
                } else {
                    host.to_string()
                }
            };

            let mut stream = match url.scheme() {
                "https" => {
                    // Use tls
                    let stream = TcpStream::connect(addr.clone()).await?;
                    let cx = tokio_native_tls::TlsConnector::from(TlsConnector::builder().build()?);
                    Stream::Tls(Box::new(cx.connect(host, stream).await?))
                }
                _ => Stream::Plain(TcpStream::connect(addr.clone()).await?),
            };

            // Build the path
            let mut path = url.path().to_string();
            if let Some(query) = url.query() {
                path = format!("{}?{}", path, query);
            }
            if let Some(fragment) = url.fragment() {
                path = format!("{}#{}", path, fragment);
            }

            // Write the message
            let mut req_buf = Vec::new();
            req_buf.extend_from_slice(format!("GET {} HTTP/1.1\r\n", path).as_bytes());
            let mut auth_included = false;
            for header in &headers {
                req_buf.extend_from_slice(header.as_bytes());
                req_buf.extend_from_slice(b"\r\n");
                auth_included |= header.to_lowercase().starts_with("authorization:");
            }
            req_buf.extend_from_slice(format!("Host: {}\r\n", addr).as_bytes());
            if !auth_included {
                if let Some(passwd) = url.password() {
                    let encoded = base64::encode(format!("{}:{}", url.username(), passwd));
                    req_buf.extend_from_slice(
                        format!("Authorization: Basic {}\r\n", encoded).as_bytes(),
                    );
                }
            }
            req_buf.extend_from_slice(b"\r\n");
            stream.write_all(&req_buf).await?;

            let mut buf = Vec::new();
            // First time parsing the response
            read_http_response(&mut stream, &mut buf, max_len).await?;

            let mut _headers = [httparse::EMPTY_HEADER; 32];
            let mut res = httparse::Response::new(&mut _headers);

            // Second time parsing the response
            if res.parse(&buf)? == Status::Partial {
                return Err(Box::new(std::io::Error::new(
                    ErrorKind::Other,
                    "Received an incomplete response",
                )));
            }

            match res.code {
                Some(code) => {
                    if code / 100 == 3 || code == 201 {
                        if remaining_redirects == 0 {
                            // Reached maximum number of redirects!
                            return Err(Box::new(std::io::Error::new(
                                ErrorKind::Other,
                                "Maximum redirects reached",
                            )));
                        } else if let Some(location) = get_header("Location", res.headers) {
                            // Try parsing it into a URL first
                            let loc_str = std::str::from_utf8(location)?;
                            if let Ok(mut redirect) = Url::parse(loc_str) {
                                redirect.set_query(url.query());
                                str_url = redirect.as_str().to_string();
                            } else {
                                if location[0] == b'/' {
                                    url.set_path(loc_str);
                                } else {
                                    url.join(loc_str)?;
                                }
                                str_url = url.as_str().to_string();
                            }

                            remaining_redirects -= 1;
                        } else {
                            return Err(Box::new(std::io::Error::new(
                                ErrorKind::Other,
                                "Invalid Location",
                            )));
                        }
                    } else {
                        return Ok((stream, buf));
                    }
                }
                None => {
                    return Err(Box::new(std::io::Error::new(
                        ErrorKind::Other,
                        "Missing response code",
                    )));
                }
            }
        } else {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::AddrNotAvailable,
                format!("Invalid URL provided: {}", str_url),
            )));
        }
    }
}

async fn master_server_mountpoints(
    server: &Arc<RwLock<Server>>,
    master_info: &MasterServer,
) -> Result<Vec<String>, Box<dyn Error>> {
    // Get all master mountpoints
    let (server_id, header_timeout, http_max_len, http_max_redirects) = {
        let properties = &server.read().await.properties;
        (
            properties.server_id.clone(),
            properties.limits.header_timeout,
            properties.limits.http_max_length,
            properties.limits.http_max_redirects,
        )
    };

    // read headers from client
    let headers = vec![
        format!("User-Agent: {}", server_id),
        "Connection: Closed".to_string(),
    ];
    let (mut sock, message) = timeout(
        Duration::from_millis(header_timeout),
        connect_and_redirect(
            format!("{}/api/serverinfo", master_info.url),
            headers,
            http_max_len,
            http_max_redirects,
        ),
    )
    .await??;

    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut res = httparse::Response::new(&mut headers);

    let body_offset = match res.parse(&message)? {
        Status::Complete(offset) => offset,
        Status::Partial => {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                "Received an incomplete response",
            )));
        }
    };

    let mut len = match get_header("Content-Length", res.headers) {
        Some(val) => {
            let parsed = std::str::from_utf8(val)?;
            parsed.parse::<usize>()?
        }
        None => {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                "No Content-Length specified",
            )));
        }
    };

    match res.code {
        Some(200) => (),
        Some(code) => {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!("Invalid response: {} {}", code, res.reason.unwrap()),
            )));
        }
        None => {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                "Missing response code",
            )));
        }
    }

    let source_timeout = server.read().await.properties.limits.source_timeout;

    let mut json_slice = Vec::new();
    if message.len() > body_offset {
        len -= message.len() - body_offset;
        json_slice.extend_from_slice(&message[body_offset..]);
    }
    let mut buf = [0; 512];
    while len != 0 {
        // Read the incoming stream data until it closes
        let read = timeout(Duration::from_millis(source_timeout), sock.read(&mut buf)).await??;

        // Not guaranteed but most likely EOF or some premature closure
        if read == 0 {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::UnexpectedEof,
                "Response body is less than specified",
            )));
        } else if read > len {
            // Read too much?
            return Err(Box::new(std::io::Error::new(
                ErrorKind::InvalidData,
                "Response body is larger than specified",
            )));
        } else {
            len -= read;
            json_slice.extend_from_slice(&buf[..read]);
        }
    }

    #[derive(Deserialize)]
    struct MasterMounts {
        mounts: Vec<String>,
    }

    // we either will found mounts or client is not an icecast node?
    Ok(serde_json::from_slice::<MasterMounts>(&json_slice)?.mounts)
}

async fn slave_node(server: Arc<RwLock<Server>>, master_server: MasterServer) {
    /*
        Master-slave polling
        We will retrieve mountpoints from master node every update_interval and mount them in slave node.
        If mountpoint already exists in slave node (ie. a source uses same mountpoint that also exists in master node),
        then we will ignore that mountpoint from master
    */
    loop {
        // first we retrieve mountpoints from master
        let mut mounts = Vec::new();
        match master_server_mountpoints(&server, &master_server).await {
            Ok(v) => mounts.extend(v),
            Err(e) => println!(
                "Error while fetching mountpoints from {}: {}",
                master_server.url, e
            ),
        }

        for mount in mounts {
            let path = {
                // Remove the trailing '/'
                if mount.ends_with('/') {
                    let mut chars = mount.chars();
                    chars.next_back();
                    chars.collect()
                } else {
                    mount.to_string()
                }
            };

            // Check if the path contains 'admin' or 'api'
            // TODO Allow for custom stream directory, such as http://example.com/stream/radio
            if path == "/admin"
                || path.starts_with("/admin/")
                || path == "/api"
                || path.starts_with("/api/")
            {
                println!(
                    "Attempted to mount a relay at an invalid mountpoint: {}",
                    path
                );
                continue;
            }

            {
                let serv = server.read().await;
                // Check relay limit and if the source already exists
                if serv.relay_count >= master_server.relay_limit
                    || serv.sources.len() >= serv.properties.limits.total_sources
                    || server.read().await.sources.contains_key(&path)
                {
                    continue;
                }
            }

            // trying to mount all mounts from master
            let server_clone = server.clone();
            let master_clone = master_server.clone();
            tokio::spawn(async move {
                let url = master_clone.url.clone();
                if let Err(e) =
                    server::relay_mountpoint(server_clone, master_clone, path.clone()).await
                {
                    println!(
                        "An error occured while relaying {} from {}: {}",
                        path, url, e
                    );
                }
            });
        }

        // update interval
        tokio::time::sleep(tokio::time::Duration::from_secs(
            master_server.update_interval,
        ))
        .await;
    }
}

fn extract_queries(url: &str) -> (&str, Option<Vec<Query>>) {
    if let Some((path, last)) = url.split_once("?") {
        let mut queries: Vec<Query> = Vec::new();
        for field in last.split('&') {
            // decode doesn't treat + as a space
            if let Some((name, value)) = field.replace("+", " ").split_once('=') {
                let name = urlencoding::decode(name);
                let value = urlencoding::decode(value);

                if let Ok(field) = name {
                    if let Ok(value) = value {
                        queries.push(Query { field, value });
                    }
                }
            }
        }

        (path, Some(queries))
    } else {
        (url, None)
    }
}

fn get_header<'a>(key: &str, headers: &[httparse::Header<'a>]) -> Option<&'a [u8]> {
    let key = key.to_lowercase();
    for header in headers {
        if header.name.to_lowercase() == key {
            return Some(header.value);
        }
    }
    None
}

#[tokio::main]
async fn main() {
    // TODO Log everything somehow or something
    let mut properties = ServerProperties::new();

    let args: Vec<String> = std::env::args().collect();
    let config_location = {
        if args.len() > 1 {
            args[1].clone()
        } else {
            match std::env::current_dir() {
                Ok(mut buf) => {
                    buf.push("config");
                    buf.set_extension("json");
                    if let Some(string) = buf.as_path().to_str() {
                        string.to_string()
                    } else {
                        "config.json".to_string()
                    }
                }
                Err(_) => "config.json".to_string(),
            }
        }
    };
    println!("Using config path {}", config_location);

    match std::fs::read_to_string(&config_location) {
        Ok(contents) => {
            println!("Attempting to parse the config");
            match serde_json::from_str(&contents.as_str()) {
                Ok(prop) => properties = prop,
                Err(e) => println!("An error occured while parsing the config: {}", e),
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            println!("The config file was not found! Attempting to save to file");
        }
        Err(e) => println!("An error occured while trying to read the config: {}", e),
    }

    // Create or update the current config
    match File::create(&config_location) {
        Ok(file) => match serde_json::to_string_pretty(&properties) {
            Ok(config) => {
                let mut writer = BufWriter::new(file);
                if let Err(e) = writer.write_all(config.as_bytes()) {
                    println!("An error occured while writing to the config file: {}", e);
                }
            }
            Err(e) => println!(
                "An error occured while trying to serialize the server properties: {}",
                e
            ),
        },
        Err(e) => println!("An error occured while to create the config file: {}", e),
    }

    println!("Using ADDRESS            : {}", properties.address);
    println!("Using PORT               : {}", properties.port);
    println!("Using METAINT            : {}", properties.metaint);
    println!("Using SERVER ID          : {}", properties.server_id);
    println!("Using ADMIN              : {}", properties.admin);
    println!("Using HOST               : {}", properties.host);
    println!("Using LOCATION           : {}", properties.location);
    println!("Using DESCRIPTION        : {}", properties.description);
    println!("Using CLIENT LIMIT       : {}", properties.limits.clients);
    println!("Using SOURCE LIMIT       : {}", properties.limits.sources);
    println!(
        "Using QUEUE SIZE         : {}",
        properties.limits.queue_size
    );
    println!(
        "Using BURST SIZE         : {}",
        properties.limits.burst_size
    );
    println!(
        "Using HEADER TIMEOUT     : {}",
        properties.limits.header_timeout
    );
    println!(
        "Using SOURCE TIMEOUT     : {}",
        properties.limits.source_timeout
    );
    println!(
        "Using HTTP MAX LENGTH    : {}",
        properties.limits.http_max_length
    );
    println!(
        "Using HTTP MAX REDIRECTS : {}",
        properties.limits.http_max_length
    );
    if properties.master_server.enabled {
        println!("Using a master server:");
        println!(
            "      URL                : {}",
            properties.master_server.url
        );
        println!(
            "      UPDATE INTERVAL    : {} seconds",
            properties.master_server.update_interval
        );
        println!(
            "      RELAY LIMIT        : {}",
            properties.master_server.relay_limit
        );
    }
    for (mount, limit) in &properties.limits.source_limits {
        println!("Using limits for {}:", mount);
        println!("      CLIENT LIMIT       : {}", limit.clients);
        println!("      SOURCE TIMEOUT     : {}", limit.source_timeout);
        println!("      BURST SIZE         : {}", limit.burst_size);
    }

    if properties.users.is_empty() {
        println!("At least one user must be configured in the config!");
    } else {
        println!("{} users registered", properties.users.len());
        match format!("{}:{}", properties.address, properties.port).parse::<SocketAddr>() {
            Ok(address) => {
                println!(
                    "Attempting to bind to {}:{}",
                    properties.address, properties.port
                );
                match TcpListener::bind(address).await {
                    Ok(listener) => {
                        let server = Arc::new(RwLock::new(Server::new(properties)));

                        if let Ok(time) = SystemTime::now().duration_since(UNIX_EPOCH) {
                            println!(
                                "The server has started on {}",
                                fmt_http_date(SystemTime::now())
                            );
                            server.write().await.stats.start_time = time.as_secs();
                        } else {
                            println!("Unable to capture when the server started!");
                        }

                        let master_server = server.read().await.properties.master_server.clone();
                        if master_server.enabled {
                            // Start our slave node
                            let server_clone = server.clone();
                            tokio::spawn(async move {
                                slave_node(server_clone, master_server).await;
                            });
                        }

                        println!("Listening...");
                        loop {
                            match listener.accept().await {
                                Ok((socket, addr)) => {
                                    let server_clone = server.clone();

                                    tokio::spawn(async move {
                                        if let Err(e) =
                                            server::handle_connection(server_clone, socket).await
                                        {
                                            println!("An error occured while handling a connection from {}: {}", addr, e);
                                        }
                                    });
                                }
                                Err(e) => {
                                    println!("An error occured while accepting a connection: {}", e)
                                }
                            }
                        }
                    }
                    Err(e) => println!("Unable to bind to port: {}", e),
                }
            }
            Err(e) => println!("Could not parse the address: {}", e),
        }
    }
}
