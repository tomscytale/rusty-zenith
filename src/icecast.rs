use std::error::Error;
use std::io::ErrorKind;
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

use crate::server::get_offset;
use crate::structs::{MasterServer, RorR, Server, ServerProperties, Stream};
use crate::{http, server};

pub async fn run_server(properties: ServerProperties, listener: TcpListener) {
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
                    if let Err(e) = server::handle_connection(server_clone, socket).await {
                        println!(
                            "An error occurred while handling a connection from {}: {}",
                            addr, e
                        );
                    }
                });
            }
            Err(e) => {
                println!("An error occurred while accepting a connection: {}", e)
            }
        }
    }
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
                        "An error occurred while relaying {} from {}: {}",
                        path, url, e
                    );
                }
            });
        }

        // update interval
        tokio::time::sleep(Duration::from_secs(master_server.update_interval)).await;
    }
}

async fn master_server_mountpoints(
    server: &Arc<RwLock<Server>>,
    master_server: &MasterServer,
) -> Result<Vec<String>, Box<dyn Error>> {
    let url = format!("{}/api/serverinfo", master_server.url);
    // Get all master mountpoints
    let (_, mut sock, message) = server::read_headers(server, url, None).await?;

    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut res = httparse::Response::new(&mut headers);
    let body_offset = get_offset(&message, &mut res)?;

    let mut len = match http::get_header("Content-Length", res.headers) {
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

pub async fn connect_and_redirect<'a>(
    url: String,
    headers: Vec<String>,
    max_len: usize,
    max_redirects: usize,
) -> Result<(Stream, Vec<u8>), Box<dyn Error>> {
    let mut str_url = url;
    let mut remaining_redirects = max_redirects;

    loop {
        let mut url = Url::parse(str_url.as_str())?;

        let host = match url.host_str() {
            Some(host) => host,
            None => {
                return Err(Box::new(std::io::Error::new(
                    ErrorKind::AddrNotAvailable,
                    format!("Invalid URL provided: {}", str_url),
                )));
            }
        };

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
                req_buf
                    .extend_from_slice(format!("Authorization: Basic {}\r\n", encoded).as_bytes());
            }
        }
        req_buf.extend_from_slice(b"\r\n");
        stream.write_all(&req_buf).await?;
        let mut buf = Vec::new();

        // First time parsing the response
        server::read_http_response(&mut stream, &mut buf, max_len, RorR::Response).await?;

        let mut _headers = [httparse::EMPTY_HEADER; 32];
        let mut res = httparse::Response::new(&mut _headers);

        // Second time parsing the response
        if res.parse(&buf)? == Status::Partial {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                "Received an incomplete response",
            )));
        }

        let code = match res.code {
            None => {
                return Err(Box::new(std::io::Error::new(
                    ErrorKind::Other,
                    "Missing response code",
                )));
            }
            Some(code) => code,
        };

        if code / 100 != 3 && code != 201 {
            return Ok((stream, buf));
        }

        if remaining_redirects == 0 {
            // Reached maximum number of redirects!
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                "Maximum redirects reached",
            )));
        }

        let location = match http::get_header("Location", res.headers) {
            Some(location) => location,
            None => {
                return Err(Box::new(std::io::Error::new(
                    ErrorKind::Other,
                    "Invalid Location",
                )));
            }
        };

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
    }
}
