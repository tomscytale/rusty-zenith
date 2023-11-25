use std::error::Error;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use httparse::{Header, Status};
use httpdate::fmt_http_date;
use regex::Regex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::{RwLock, RwLockWriteGuard};
use tokio::time::timeout;
use uuid::Uuid;

use crate::structs::{
    Client, ClientProperties, ClientStats, IcyMetadata, IcyProperties, MasterServer, Query, Server,
    Source, StreamDecoder, TransferEncoding,
};
use crate::{admin, http};

fn remove_trailing_slash(path: &String) -> String {
    let this_str: String;

    // Remove the trailing '/'
    if path.ends_with('/') {
        let mut chars = path.chars();
        chars.next_back();
        this_str = chars.collect();
        this_str
    } else {
        path.to_string()
    }
}

pub async fn handle_source_put(
    server: &Arc<RwLock<Server>>,
    stream: &mut TcpStream,
    server_id: &str,
    message: &Vec<u8>,
    body_offset: &usize,
    method: &str,
    path: &String,
    headers: &mut [Header<'_>],
) -> Result<(), Box<dyn Error>> {
    // Check for authorization
    let serv = &server.read().await;
    if let Some(value) = http::do_auth(headers, serv).await {
        return http::send_unauthorized(
            stream,
            server_id,
            Some(("text/plain; charset=utf-8", value)),
        )
        .await;
    }

    // http://example.com/radio == http://example.com/radio/
    // Not sure if this is done client-side prior to sending the request though
    let path = &remove_trailing_slash(path);

    // Check if the path contains 'admin' or 'api'
    // TODO Allow for custom stream directory, such as http://example.com/stream/radio
    if path == "/admin"
        || path.starts_with("/admin/")
        || path == "/api"
        || path.starts_with("/api/")
    {
        return http::send_forbidden(
            stream,
            server_id,
            Some(("text/plain; charset=utf-8", "Invalid mountpoint")),
        )
        .await;
    }

    // Check if it is valid
    // For now this assumes the stream directory is /
    let dir = Path::new(&path);
    if let Some(parent) = dir.parent() {
        if let Some(parent_str) = parent.to_str() {
            if parent_str != "/" {
                return http::send_forbidden(
                    stream,
                    server_id,
                    Some(("text/plain; charset=utf-8", "Invalid mountpoint")),
                )
                .await;
            }
        } else {
            return http::send_forbidden(
                stream,
                server_id,
                Some(("text/plain; charset=utf-8", "Invalid mountpoint")),
            )
            .await;
        }
    } else {
        return http::send_forbidden(
            stream,
            server_id,
            Some(("text/plain; charset=utf-8", "Invalid mountpoint")),
        )
        .await;
    }

    // Sources must have a content type
    // Maybe the type that is served should be checked?
    let mut properties = match http::get_header("Content-Type", headers) {
        Some(content_type) => IcyProperties::new(std::str::from_utf8(content_type)?.to_string()),
        None => {
            return http::send_forbidden(
                stream,
                server_id,
                Some(("text/plain; charset=utf-8", "No Content-type provided")),
            )
            .await;
        }
    };

    let mut serv = server.write().await;
    // Check if the mountpoint is already in use
    if serv.sources.contains_key(path) {
        return http::send_forbidden(
            stream,
            server_id,
            Some(("text/plain; charset=utf-8", "Invalid mountpoint")),
        )
        .await;
    }

    // Check if the max number of sources has been reached
    if serv.source_count >= serv.properties.limits.sources
        || serv.sources.len() >= serv.properties.limits.total_sources
    {
        return http::send_forbidden(
            stream,
            server_id,
            Some(("text/plain; charset=utf-8", "Too many sources connected")),
        )
        .await;
    }

    let mut decoder: StreamDecoder;

    if method == "SOURCE" {
        // Give an 200 OK response
        http::send_ok(stream, server_id, None).await?;

        decoder = StreamDecoder::new(TransferEncoding::Identity);
    } else {
        // Verify that the transfer encoding is identity or not included
        // No support for chunked or encoding ATM
        // TODO Add support for transfer encoding options as specified here: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Transfer-Encoding
        match (
            http::get_header("Transfer-Encoding", headers),
            http::get_header("Content-Length", headers),
        ) {
            (Some(b"identity"), Some(value)) | (None, Some(value)) => {
                // Use content length decoder
                match std::str::from_utf8(value) {
                    Ok(string) => match string.parse::<usize>() {
                        Ok(length) => {
                            decoder = StreamDecoder::new(TransferEncoding::Length(length))
                        }
                        Err(_) => {
                            return http::send_bad_request(
                                stream,
                                server_id,
                                Some(("text/plain; charset=utf-8", "Invalid Content-Length")),
                            )
                            .await;
                        }
                    },
                    Err(_) => {
                        return http::send_bad_request(
                            stream,
                            server_id,
                            Some((
                                "text/plain; charset=utf-8",
                                "Unknown unicode found in Content-Length",
                            )),
                        )
                        .await;
                    }
                }
            }
            (Some(b"chunked"), None) => {
                // Use chunked decoder
                decoder = StreamDecoder::new(TransferEncoding::Chunked);
            }
            (Some(b"identity"), None) | (None, None) => {
                // Use identity
                decoder = StreamDecoder::new(TransferEncoding::Identity);
            }
            _ => {
                return http::send_bad_request(
                    stream,
                    server_id,
                    Some(("text/plain; charset=utf-8", "Unsupported transfer encoding")),
                )
                .await;
            }
        }

        // Check if client sent Expect: 100-continue in header, if that's the case we will need to return 100 in status code
        // Without it, it means that client has no body to send, we will stop if that's the case
        match http::get_header("Expect", headers) {
            Some(b"100-continue") => http::send_continue(stream, server_id).await?,
            Some(_) => {
                return http::send_bad_request(
                    stream,
                    server_id,
                    Some((
                        "text/plain; charset=utf-8",
                        "Expected 100-continue in Expect header",
                    )),
                )
                .await;
            }
            None => {
                return http::send_bad_request(
                    stream,
                    server_id,
                    Some((
                        "text/plain; charset=utf-8",
                        "PUT request must come with Expect header",
                    )),
                )
                .await;
            }
        }
    }

    // Parse the headers for the source properties
    populate_properties(&mut properties, headers);

    let source = Source::new(path.clone(), properties);

    let queue_size = serv.properties.limits.queue_size;
    let (burst_size, source_timeout) = {
        if let Some(limit) = serv.properties.limits.source_limits.get(path) {
            (limit.burst_size, limit.source_timeout)
        } else {
            (
                serv.properties.limits.burst_size,
                serv.properties.limits.header_timeout,
            )
        }
    };

    // Add to the server
    let arc = Arc::new(RwLock::new(source));
    serv.sources.insert(path.to_string(), arc.clone());
    serv.source_count += 1;
    drop(serv);

    println!(
        "Mounted source on {} via {}",
        arc.read().await.mountpoint,
        method
    );

    if message.len() > *body_offset {
        let slice = &message[*body_offset..];
        let mut data = Vec::new();
        match decoder.decode(&mut data, slice, message.len() - body_offset) {
            Ok(read) => {
                if read != 0 {
                    broadcast_to_clients(&arc, data, queue_size, burst_size).await;
                    arc.read().await.stats.write().await.bytes_read += read;
                }
            }
            Err(e) => {
                println!(
                    "An error occurred while decoding stream data from source {}: {}",
                    arc.read().await.mountpoint,
                    e
                );
                arc.write().await.disconnect_flag = true;
            }
        }
    }

    // Listen for bytes
    if !decoder.is_finished() && !arc.read().await.disconnect_flag {
        while {
            // Read the incoming stream data until it closes
            let mut buf = [0; 1024];
            let read =
                match timeout(Duration::from_millis(source_timeout), stream.read(&mut buf)).await {
                    Ok(Ok(n)) => n,
                    Ok(Err(e)) => {
                        println!(
                            "An error occurred while reading stream data from source {}: {}",
                            arc.read().await.mountpoint,
                            e
                        );
                        0
                    }
                    Err(_) => {
                        println!("A source timed out: {}", arc.read().await.mountpoint);
                        0
                    }
                };

            let mut data = Vec::new();
            match decoder.decode(&mut data, &buf, read) {
                Ok(decode_read) => {
                    if decode_read != 0 {
                        broadcast_to_clients(&arc, data, queue_size, burst_size).await;
                        arc.read().await.stats.write().await.bytes_read += decode_read;
                    }

                    // Check if the source needs to be disconnected
                    read != 0 && !decoder.is_finished() && !arc.read().await.disconnect_flag
                }
                Err(e) => {
                    println!(
                        "An error occurred while decoding stream data from source {}: {}",
                        arc.read().await.mountpoint,
                        e
                    );
                    false
                }
            }
        } {}
    }

    let mut source = arc.write().await;
    let fallback = source.fallback.clone();
    disconnect_or_fallback(server, &mut source, fallback).await;

    // Clean up and remove the source
    let mut serv = server.write().await;
    serv.sources.remove(&source.mountpoint);
    serv.source_count -= 1;
    serv.stats.session_bytes_read += source.stats.read().await.bytes_read;

    if method == "PUT" {
        // request must end with server 200 OK response
        http::send_ok(stream, server_id, None).await.ok();
    }

    println!("Unmounted source {}", source.mountpoint);
    Ok(())
}

async fn disconnect_or_fallback(
    server: &Arc<RwLock<Server>>,
    source: &mut RwLockWriteGuard<'_, Source>,
    fallback: Option<String>,
) {
    if let Some(fallback_id) = fallback {
        if let Some(fallback_source) = server.read().await.sources.get(&fallback_id) {
            println!(
                "Moving listeners from {} to {}",
                source.mountpoint, fallback_id
            );
            let mut fallback = fallback_source.write().await;
            for (uuid, client) in source.clients.drain() {
                *client.read().await.source.write().await = fallback_id.clone();
                fallback.clients.insert(uuid, client);
            }
        } else {
            println!(
                "No fallback source {} found! Disconnecting listeners on {}",
                fallback_id, source.mountpoint
            );
            drop_all(source).await;
        }
    } else {
        // Disconnect each client by sending an empty buffer
        println!("Disconnecting listeners on {}", source.mountpoint);
        drop_all(source).await;
    }
}

async fn drop_all(source: &mut RwLockWriteGuard<'_, Source>) {
    for cli in source.clients.values() {
        // Send an empty vec to signify the channel is closed
        drop(
            cli.read()
                .await
                .sender
                .write()
                .await
                .send(Arc::new(Vec::new())),
        );
    }
}

pub async fn handle_get(
    server: Arc<RwLock<Server>>,
    stream: &mut TcpStream,
    server_id: &str,
    queries: Option<Vec<Query>>,
    path: String,
    headers: &mut [Header<'_>],
) -> Result<(), Box<dyn Error>> {
    let source_id = remove_trailing_slash(&path);
    let mut serv = server.write().await;
    let source_option = serv.sources.get(&source_id);

    // Check if the source is valid
    if let Some(source_lock) = source_option {
        let mut source = source_lock.write().await;

        // Check if the max number of listeners has been reached
        let too_many_clients = {
            if let Some(limit) = serv.properties.limits.source_limits.get(&source_id) {
                source.clients.len() >= limit.clients
            } else {
                false
            }
        };
        if serv.clients.len() >= serv.properties.limits.clients || too_many_clients {
            http::send_forbidden(
                stream,
                server_id,
                Some(("text/plain; charset=utf-8", "Too many listeners connected")),
            )
            .await?;
            return Ok(());
        }

        // Check if metadata is enabled
        let meta_enabled = http::get_header("Icy-MetaData", headers).unwrap_or(b"0") == b"1";

        // Reply with a 200 OK
        send_listener_ok(
            stream,
            server_id,
            &source.properties,
            meta_enabled,
            serv.properties.metaint,
        )
        .await?;

        // Create a client
        // Get a valid UUID
        let client_id = {
            let mut unique = Uuid::new_v4();
            // Hopefully this doesn't take until the end of time
            while serv.clients.contains_key(&unique) {
                unique = Uuid::new_v4();
            }
            unique
        };

        let (sender, receiver) = unbounded_channel::<Arc<Vec<u8>>>();
        let properties = ClientProperties {
            id: client_id,
            uagent: {
                if let Some(arr) = http::get_header("User-Agent", headers) {
                    if let Ok(parsed) = std::str::from_utf8(arr) {
                        Some(parsed.to_string())
                    } else {
                        None
                    }
                } else {
                    None
                }
            },
            metadata: meta_enabled,
        };
        let stats = ClientStats {
            start_time: {
                if let Ok(time) = SystemTime::now().duration_since(UNIX_EPOCH) {
                    time.as_secs()
                } else {
                    0
                }
            },
            bytes_sent: 0,
        };
        let client = Client {
            source: RwLock::new(source_id),
            sender: RwLock::new(sender),
            receiver: RwLock::new(receiver),
            buffer_size: RwLock::new(0),
            properties: properties.clone(),
            stats: RwLock::new(stats),
        };

        if let Some(agent) = &client.properties.uagent {
            println!(
                "User {} started listening on {} with user-agent {}",
                client_id,
                client.source.read().await,
                agent
            );
        } else {
            println!(
                "User {} started listening on {}",
                client_id,
                client.source.read().await
            );
        }
        if meta_enabled {
            println!("User {} has icy metadata enabled", client_id);
        }

        // Get the metaint
        let metalen = serv.properties.metaint;

        // Keep track of how many bytes have been sent
        let mut sent_count = 0;

        // Get a copy of the burst buffer and metadata
        let burst_buf = source.burst_buffer.clone();
        let metadata_copy = source.metadata_vec.clone();

        let arc_client = Arc::new(RwLock::new(client));
        // Add the client id to the list of clients attached to the source
        source.clients.insert(client_id, arc_client.clone());

        {
            let mut source_stats = source.stats.write().await;
            source_stats.peak_listeners =
                std::cmp::max(source_stats.peak_listeners, source.clients.len());
        }

        // No more need for source
        drop(source);

        // Add our client
        serv.clients.insert(client_id, properties);

        // Set the max amount of listeners
        serv.stats.peak_listeners = std::cmp::max(serv.stats.peak_listeners, serv.clients.len());

        drop(serv);

        // Send the burst on connect buffer
        let burst_success = {
            if !burst_buf.is_empty() {
                match {
                    if meta_enabled {
                        write_to_client(
                            stream,
                            &mut sent_count,
                            metalen,
                            &burst_buf,
                            &metadata_copy,
                        )
                        .await
                    } else {
                        stream.write_all(&burst_buf).await
                    }
                } {
                    Ok(_) => {
                        arc_client.read().await.stats.write().await.bytes_sent += burst_buf.len();
                        true
                    }
                    Err(_) => false,
                }
            } else {
                true
            }
        };
        drop(metadata_copy);
        drop(burst_buf);

        if burst_success {
            loop {
                // Receive whatever bytes, then send to the client
                let client = arc_client.read().await;
                let res = client.receiver.write().await.recv().await;
                // Check if the channel is still alive
                if let Some(read) = res {
                    // If an empty buffer has been sent, then disconnect the client
                    if read.len() > 0 {
                        // Decrease the internal buffer
                        *client.buffer_size.write().await -= read.len();
                        match {
                            if meta_enabled {
                                let meta_vec = {
                                    let serv = server.read().await;
                                    if let Some(source_lock) =
                                        serv.sources.get(&*client.source.read().await)
                                    {
                                        let source = source_lock.read().await;

                                        source.metadata_vec.clone()
                                    } else {
                                        vec![0]
                                    }
                                };

                                write_to_client(
                                    stream,
                                    &mut sent_count,
                                    metalen,
                                    &read.to_vec(),
                                    &meta_vec,
                                )
                                .await
                            } else {
                                stream.write_all(&read.to_vec()).await
                            }
                        } {
                            Ok(_) => {
                                arc_client.read().await.stats.write().await.bytes_sent += read.len()
                            }
                            Err(_) => break,
                        }
                    } else {
                        break;
                    }
                } else {
                    // The sender has been dropped
                    // The listener has been kicked
                    break;
                }
            }
        }

        // Close the message queue
        arc_client.read().await.receiver.write().await.close();

        println!("User {} has disconnected", client_id);

        let mut serv = server.write().await;
        // Remove the client information from the list of clients
        serv.clients.remove(&client_id);
        serv.stats.session_bytes_sent += arc_client.read().await.stats.read().await.bytes_sent;
        drop(serv);
    } else {
        // Figure out what the request wants
        // /admin/metadata for updating the metadata
        // /admin/listclients for viewing the clients for a particular source
        // /admin/fallbacks for setting the fallback of a particular source
        // /admin/moveclients for moving listeners from one source to another
        // /admin/killclient for disconnecting a client
        // /admin/killsource for disconnecting a source
        // /admin/listmounts for listing all mounts available
        // Anything else is not vanilla or unimplemented
        // Return a 404 otherwise

        // Drop the write lock
        drop(serv);

        // Paths
        return admin::do_admin(server, stream, server_id, queries, path, headers).await;
    }

    Ok(())
}

pub fn populate_properties(properties: &mut IcyProperties, headers: &[Header<'_>]) {
    for header in headers {
        let name = header.name.to_lowercase();
        let name = name.as_str();
        let val = std::str::from_utf8(header.value).unwrap_or("");

        // There's a nice list here: https://github.com/ben221199/MediaCast
        // Although, these were taken directly from Icecast's source: https://github.com/xiph/Icecast-Server/blob/master/src/source.c
        match name {
            "user-agent" => properties.uagent = Some(val.to_string()),
            "ice-public" | "icy-pub" | "x-audiocast-public" | "icy-public" => {
                properties.public = val.parse::<usize>().unwrap_or(0) == 1
            }
            "ice-name" | "icy-name" | "x-audiocast-name" => properties.name = Some(val.to_string()),
            "ice-description" | "icy-description" | "x-audiocast-description" => {
                properties.description = Some(val.to_string())
            }
            "ice-url" | "icy-url" | "x-audiocast-url" => properties.url = Some(val.to_string()),
            "ice-genre" | "icy-genre" | "x-audiocast-genre" => {
                properties.genre = Some(val.to_string())
            }
            "ice-bitrate" | "icy-br" | "x-audiocast-bitrate" => {
                properties.bitrate = Some(val.to_string())
            }
            _ => (),
        }
    }
}

pub async fn broadcast_to_clients(
    source: &Arc<RwLock<Source>>,
    data: Vec<u8>,
    queue_size: usize,
    burst_size: usize,
) {
    // Remove these later
    let mut dropped: Vec<Uuid> = Vec::new();

    let read = data.len();
    let arc_slice = Arc::new(data);

    // Keep the write lock for the duration of the function, since a race condition with the burst on connect buffer is not wanted
    let mut locked = source.write().await;

    // Broadcast to all listeners
    for (uuid, cli) in &locked.clients {
        let client = cli.read().await;
        let mut buf_size = client.buffer_size.write().await;
        let queue = client.sender.write().await;
        if read + (*buf_size) > queue_size || queue.send(arc_slice.clone()).is_err() {
            dropped.push(*uuid);
        } else {
            (*buf_size) += read;
        }
    }

    // Fill the burst on connect buffer
    if burst_size > 0 {
        let burst_buf = &mut locked.burst_buffer;
        burst_buf.extend_from_slice(&arc_slice);
        // Trim it if it's larger than the allowed size
        if burst_buf.len() > burst_size {
            burst_buf.drain(..burst_buf.len() - burst_size);
        }
    }

    // Remove clients who have been kicked or disconnected
    for uuid in dropped {
        if let Some(client) = locked.clients.remove(&uuid) {
            drop(
                client
                    .read()
                    .await
                    .sender
                    .write()
                    .await
                    .send(Arc::new(Vec::new())),
            );
        }
    }
}

async fn send_listener_ok(
    stream: &mut TcpStream,
    id: &str,
    properties: &IcyProperties,
    meta_enabled: bool,
    metaint: usize,
) -> Result<(), Box<dyn Error>> {
    stream.write_all(b"HTTP/1.0 200 OK\r\n").await?;
    stream
        .write_all((format!("Server: {}\r\n", id)).as_bytes())
        .await?;
    stream.write_all(b"Connection: Close\r\n").await?;
    stream
        .write_all((format!("Date: {}\r\n", fmt_http_date(SystemTime::now()))).as_bytes())
        .await?;
    stream
        .write_all((format!("Content-Type: {}\r\n", properties.content_type)).as_bytes())
        .await?;
    stream
        .write_all(b"Cache-Control: no-cache, no-store\r\n")
        .await?;
    stream
        .write_all(b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n")
        .await?;
    stream.write_all(b"Pragma: no-cache\r\n").await?;
    stream
        .write_all(b"Access-Control-Allow-Origin: *\r\n")
        .await?;

    // If metaint is enabled
    if meta_enabled {
        stream
            .write_all((format!("icy-metaint:{}\r\n", metaint)).as_bytes())
            .await?;
    }

    // Properties or default
    if let Some(br) = properties.bitrate.as_ref() {
        stream
            .write_all((format!("icy-br:{}\r\n", br)).as_bytes())
            .await?;
    }
    stream
        .write_all(
            (format!(
                "icy-description:{}\r\n",
                properties
                    .description
                    .as_ref()
                    .unwrap_or(&"Unknown".to_string())
            ))
            .as_bytes(),
        )
        .await?;
    stream
        .write_all(
            (format!(
                "icy-genre:{}\r\n",
                properties
                    .genre
                    .as_ref()
                    .unwrap_or(&"Undefined".to_string())
            ))
            .as_bytes(),
        )
        .await?;
    stream
        .write_all(
            (format!(
                "icy-name:{}\r\n",
                properties
                    .name
                    .as_ref()
                    .unwrap_or(&"Unnamed Station".to_string())
            ))
            .as_bytes(),
        )
        .await?;
    stream
        .write_all((format!("icy-pub:{}\r\n", properties.public as usize)).as_bytes())
        .await?;
    stream
        .write_all(
            (format!(
                "icy-url:{}\r\n\r\n",
                properties.url.as_ref().unwrap_or(&"Unknown".to_string())
            ))
            .as_bytes(),
        )
        .await?;

    Ok(())
}

async fn write_to_client(
    stream: &mut TcpStream,
    sent_count: &mut usize,
    metalen: usize,
    data: &[u8],
    metadata: &[u8],
) -> Result<(), std::io::Error> {
    let new_sent = *sent_count + data.len();
    // Consume it here or something
    let send: &[u8];
    // Create a new vector to hold our data, if we need one
    let mut inserted: Vec<u8> = Vec::new();
    // Check if we need to send the metadata
    if new_sent > metalen {
        // Insert the current range
        let mut index = metalen - *sent_count;
        if index > 0 {
            inserted.extend_from_slice(&data[..index]);
        }
        while index < data.len() {
            inserted.extend_from_slice(metadata);

            // Add the data
            let end = std::cmp::min(data.len(), index + metalen);
            if index != end {
                inserted.extend_from_slice(&data[index..end]);
                index = end;
            }
        }

        // Update the total sent amount and send
        *sent_count = new_sent % metalen;
        send = &inserted;
    } else {
        // Copy over the new amount
        *sent_count = new_sent;
        send = data;
    }

    stream.write_all(send).await
}

pub async fn handle_connection(
    server: Arc<RwLock<Server>>,
    mut stream: TcpStream,
) -> Result<(), Box<dyn Error>> {
    let (server_id, header_timeout, http_max_len) = {
        let properties = &server.read().await.properties;
        (
            properties.server_id.clone(),
            properties.limits.header_timeout,
            properties.limits.http_max_length,
        )
    };

    let mut message = Vec::new();
    let mut buf = [0; 1024];

    // Add a timeout
    timeout(Duration::from_millis(header_timeout), async {
        loop {
            let mut headers = [httparse::EMPTY_HEADER; 32];
            let mut req = httparse::Request::new(&mut headers);
            let read = stream.read(&mut buf).await?;
            message.extend_from_slice(&buf[..read]);
            match req.parse(&message) {
                Ok(Status::Complete(offset)) => return Ok(offset),
                Ok(Status::Partial) if message.len() > http_max_len => {
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
    })
    .await??;

    let mut _headers = [httparse::EMPTY_HEADER; 32];
    let mut req = httparse::Request::new(&mut _headers);
    let body_offset = req.parse(&message)?.unwrap();
    let method = req.method.unwrap();

    let (base_path, queries) = crate::extract_queries(req.path.unwrap());
    let path = path_clean::clean(base_path);
    let headers = req.headers;

    match method {
        // Some info about the protocol is provided here: https://gist.github.com/ePirat/adc3b8ba00d85b7e3870
        "SOURCE" | "PUT" => {
            return handle_source_put(
                &server,
                &mut stream,
                &server_id,
                &message,
                &body_offset,
                method,
                &path,
                headers,
            )
            .await;
        }
        "GET" => {
            return handle_get(server, &mut stream, &server_id, queries, path, headers).await;
        }
        _ => {
            // Unknown
            stream
                .write_all(b"HTTP/1.0 405 Method Not Allowed\r\n")
                .await?;
            stream
                .write_all((format!("Server: {}\r\n", server_id)).as_bytes())
                .await?;
            stream.write_all(b"Connection: Close\r\n").await?;
            stream.write_all(b"Allow: GET, SOURCE\r\n").await?;
            stream
                .write_all((format!("Date: {}\r\n", fmt_http_date(SystemTime::now()))).as_bytes())
                .await?;
            stream
                .write_all(b"Cache-Control: no-cache, no-store\r\n")
                .await?;
            stream
                .write_all(b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n")
                .await?;
            stream.write_all(b"Pragma: no-cache\r\n").await?;
            stream
                .write_all(b"Access-Control-Allow-Origin: *\r\n\r\n")
                .await?;
        }
    }

    Ok(())
}

#[allow(clippy::map_entry)]
#[allow(clippy::blocks_in_if_conditions)]
pub async fn relay_mountpoint(
    server: Arc<RwLock<Server>>,
    master_server: MasterServer,
    mount: String,
) -> Result<(), Box<dyn Error>> {
    let (server_id, header_timeout, http_max_len, http_max_redirects) = {
        let properties = &server.read().await.properties;
        (
            properties.server_id.clone(),
            properties.limits.header_timeout,
            properties.limits.http_max_length,
            properties.limits.http_max_redirects,
        )
    };

    // read headers from server
    let headers = vec![
        format!("User-Agent: {}", server_id),
        "Connection: Closed".to_string(),
        "Icy-Metadata:1".to_string(),
    ];
    let (mut sock, buf) = timeout(
        Duration::from_millis(header_timeout),
        crate::icecast::connect_and_redirect(
            format!("{}{}", master_server.url, mount),
            headers,
            http_max_len,
            http_max_redirects,
        ),
    )
    .await??;

    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut res = httparse::Response::new(&mut headers);

    let body_offset = match res.parse(&buf)? {
        Status::Complete(offset) => offset,
        Status::Partial => {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                "Received an incomplete response",
            )));
        }
    };

    match res.code {
        Some(200) => (),
        Some(code) => {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                format!("Invalid response: {}", code),
            )));
        }
        None => {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                "Missing response code",
            )));
        }
    }

    // checking if our peer is really an icecast server
    if http::get_header("icy-name", res.headers).is_none() {
        return Err(Box::new(std::io::Error::new(
            ErrorKind::Other,
            "Is this a valid icecast stream?",
        )));
    }

    let mut decoder = match (
        http::get_header("Transfer-Encoding", res.headers),
        http::get_header("Content-Length", res.headers),
    ) {
        (Some(b"identity"), Some(value)) | (None, Some(value)) => {
            // Use content length decoder
            match std::str::from_utf8(value) {
                Ok(string) => match string.parse::<usize>() {
                    Ok(length) => StreamDecoder::new(TransferEncoding::Length(length)),
                    Err(_) => {
                        return Err(Box::new(std::io::Error::new(
                            ErrorKind::InvalidData,
                            "Invalid Content-Length",
                        )));
                    }
                },
                Err(_) => {
                    return Err(Box::new(std::io::Error::new(
                        ErrorKind::InvalidData,
                        "Unknown unicode found in Content-Length",
                    )));
                }
            }
        }
        (Some(b"chunked"), None) => StreamDecoder::new(TransferEncoding::Chunked),
        (Some(b"identity"), None) | (None, None) => StreamDecoder::new(TransferEncoding::Identity),
        _ => {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::InvalidData,
                "Unsupported Transfer-Encoding",
            )));
        }
    };

    // Sources must have a content type
    let mut properties = match http::get_header("Content-Type", res.headers) {
        Some(content_type) => IcyProperties::new(std::str::from_utf8(content_type)?.to_string()),
        None => {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                "No Content-Type provided",
            )));
        }
    };

    // Parse the headers for the source properties
    populate_properties(&mut properties, res.headers);
    properties.uagent = Some(server_id);

    let source = Source::new(mount.to_string(), properties);

    // TODO This code is almost an exact replica of the one used for regular source handling, although with a few differences
    let mut serv = server.write().await;
    // Check if the mountpoint is already in use
    let path = source.mountpoint.clone();
    // Not sure what clippy wants, https://rust-lang.github.io/rust-clippy/master/#map_entry
    // The source is not needed if the map already has one. TODO Try using try_insert if it's stable in the future
    if serv.sources.contains_key(&path) {
        // The error handling in this program is absolutely awful
        Err(Box::new(std::io::Error::new(
            ErrorKind::Other,
            "A source with the same mountpoint already exists",
        )))
    } else {
        if serv.relay_count >= master_server.relay_limit {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                "The server relay limit has been reached",
            )));
        } else if serv.sources.len() >= serv.properties.limits.total_sources {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                "The server total source limit has been reached",
            )));
        }

        let queue_size = serv.properties.limits.queue_size;
        let (burst_size, source_timeout) = {
            if let Some(limit) = serv.properties.limits.source_limits.get(&path) {
                (limit.burst_size, limit.source_timeout)
            } else {
                (
                    serv.properties.limits.burst_size,
                    serv.properties.limits.header_timeout,
                )
            }
        };

        // Add to the server
        let arc = Arc::new(RwLock::new(source));
        serv.sources.insert(path, arc.clone());
        serv.relay_count += 1;
        drop(serv);

        struct MetaParser {
            metaint: usize,
            vec: Vec<u8>,
            remaining: usize,
        }

        let metaint = match http::get_header("Icy-Metaint", res.headers) {
            Some(val) => std::str::from_utf8(val)?.parse::<usize>()?,
            None => 0,
        };
        let mut meta_info = MetaParser {
            metaint,
            vec: Vec::new(),
            remaining: metaint,
        };

        println!("Mounted relay on {}", arc.read().await.mountpoint);

        if buf.len() > body_offset {
            let slice = &buf[body_offset..];
            let mut data = Vec::new();
            // This bit of code is really ugly, but oh well
            match decoder.decode(&mut data, slice, buf.len() - body_offset) {
                Ok(read) => {
                    if read != 0 {
                        // Process any metadata, if it exists
                        let data = {
                            if meta_info.metaint != 0 {
                                let mut trimmed = Vec::new();
                                let mut position = 0;
                                let mut last_full: Option<Vec<u8>> = None;
                                while position < read {
                                    // Either reading in regular stream data
                                    // Reading in the length of the metadata
                                    // Or reading the metadata directly
                                    if meta_info.remaining != 0 {
                                        let frame_length =
                                            std::cmp::min(read - position, meta_info.remaining);
                                        if frame_length != 0 {
                                            trimmed.extend_from_slice(
                                                &data[position..position + frame_length],
                                            );
                                            meta_info.remaining -= frame_length;
                                            position += frame_length;
                                        }
                                    } else if meta_info.vec.is_empty() {
                                        // Reading the length of the metadata segment
                                        meta_info.vec.push(data[position]);
                                        position += 1;
                                    } else {
                                        // Reading in metadata
                                        let size = 1 + ((meta_info.vec[0] as usize) << 4);
                                        let remaining_metadata = std::cmp::min(
                                            read - position,
                                            size - meta_info.vec.len(),
                                        );
                                        meta_info.vec.extend_from_slice(
                                            &data[position..position + remaining_metadata],
                                        );
                                        position += remaining_metadata;

                                        // If it's reached the max size, then copy it over to last_full
                                        if meta_info.vec.len() == size {
                                            meta_info.remaining = meta_info.metaint;
                                            last_full = Some(meta_info.vec.clone());
                                            meta_info.vec.clear();
                                        }
                                    }
                                }

                                // Update the source's metadata
                                if let Some(metadata_vec) = last_full {
                                    if !{
                                        let serv_vec = &arc.read().await.metadata_vec;
                                        serv_vec.len() == metadata_vec.len()
                                            && serv_vec.iter().eq(metadata_vec.iter())
                                    } {
                                        if metadata_vec[..] == [1; 0] {
                                            let mut serv = arc.write().await;
                                            println!(
                                                "Updated relay {} metadata with no title and url",
                                                serv.mountpoint
                                            );
                                            serv.metadata_vec = vec![0];
                                            serv.metadata = None;
                                        } else {
                                            let cut = {
                                                let mut last = metadata_vec.len();
                                                while metadata_vec[last - 1] == 0 {
                                                    last -= 1;
                                                }
                                                last
                                            };
                                            if let Ok(meta_str) =
                                                std::str::from_utf8(&metadata_vec[1..cut])
                                            {
                                                let reg = Regex::new(
                                                    r"^StreamTitle='(.+?)';StreamUrl='(.+?)';$",
                                                )
                                                .unwrap();
                                                if let Some(captures) = reg.captures(meta_str) {
                                                    let metadata = IcyMetadata {
                                                        title: {
                                                            let m_str =
                                                                captures.get(1).unwrap().as_str();
                                                            if m_str.is_empty() {
                                                                None
                                                            } else {
                                                                Some(m_str.to_string())
                                                            }
                                                        },
                                                        url: {
                                                            let m_str =
                                                                captures.get(2).unwrap().as_str();
                                                            if m_str.is_empty() {
                                                                None
                                                            } else {
                                                                Some(m_str.to_string())
                                                            }
                                                        },
                                                    };

                                                    let mut serv = arc.write().await;
                                                    println!("Updated relay {} metadata with title '{}' and url '{}'", serv.mountpoint, metadata.title.as_ref().unwrap_or(&"".to_string()), metadata.url.as_ref().unwrap_or(&"".to_string()));
                                                    serv.metadata_vec = metadata_vec;
                                                    serv.metadata = Some(metadata);
                                                } else {
                                                    println!("Unknown metadata format received from relay {}: `{}`", arc.read().await.mountpoint, meta_str);
                                                    arc.write().await.disconnect_flag = true;
                                                }
                                            } else {
                                                println!(
                                                    "Invalid metadata parsed from relay {}",
                                                    arc.read().await.mountpoint
                                                );
                                                arc.write().await.disconnect_flag = true;
                                            }
                                        }
                                    }
                                }

                                trimmed
                            } else {
                                data
                            }
                        };

                        if !data.is_empty() {
                            arc.read().await.stats.write().await.bytes_read += data.len();
                            broadcast_to_clients(&arc, data, queue_size, burst_size).await;
                        }
                    }
                }
                Err(e) => {
                    println!(
                        "An error occurred while decoding stream data from relay {}: {}",
                        arc.read().await.mountpoint,
                        e
                    );
                    arc.write().await.disconnect_flag = true;
                }
            }
        }

        // Listen for bytes
        if !decoder.is_finished() && !arc.read().await.disconnect_flag {
            while {
                // Read the incoming stream data until it closes
                let mut buf = [0; 1024];
                let read = match timeout(Duration::from_millis(source_timeout), sock.read(&mut buf))
                    .await
                {
                    Ok(Ok(n)) => n,
                    Ok(Err(e)) => {
                        println!(
                            "An error occurred while reading stream data from relay {}: {}",
                            arc.read().await.mountpoint,
                            e
                        );
                        0
                    }
                    Err(_) => {
                        println!("A relay timed out: {}", arc.read().await.mountpoint);
                        0
                    }
                };

                let mut data = Vec::new();
                match decoder.decode(&mut data, &buf, read) {
                    Ok(decode_read) => {
                        if decode_read != 0 {
                            // Process any metadata, if it exists
                            let data = {
                                if meta_info.metaint != 0 {
                                    let mut trimmed = Vec::new();
                                    let mut position = 0;
                                    let mut last_full: Option<Vec<u8>> = None;
                                    while position < decode_read {
                                        // Either reading in regular stream data
                                        // Reading in the length of the metadata
                                        // Or reading the metadata directly
                                        if meta_info.remaining != 0 {
                                            let frame_length = std::cmp::min(
                                                decode_read - position,
                                                meta_info.remaining,
                                            );
                                            if frame_length != 0 {
                                                trimmed.extend_from_slice(
                                                    &data[position..position + frame_length],
                                                );
                                                meta_info.remaining -= frame_length;
                                                position += frame_length;
                                            }
                                        } else if meta_info.vec.is_empty() {
                                            // Reading the length of the metadata segment
                                            meta_info.vec.push(data[position]);
                                            position += 1;
                                        } else {
                                            // Reading in metadata
                                            let size = 1 + ((meta_info.vec[0] as usize) << 4);
                                            let remaining_metadata = std::cmp::min(
                                                decode_read - position,
                                                size - meta_info.vec.len(),
                                            );
                                            meta_info.vec.extend_from_slice(
                                                &data[position..position + remaining_metadata],
                                            );
                                            position += remaining_metadata;

                                            // If it's reached the max size, then copy it over to last_full
                                            if meta_info.vec.len() == size {
                                                meta_info.remaining = meta_info.metaint;
                                                last_full = Some(meta_info.vec.clone());
                                                meta_info.vec.clear();
                                            }
                                        }
                                    }

                                    // Update the source's metadata
                                    if let Some(metadata_vec) = last_full {
                                        if !{
                                            let serv_vec = &arc.read().await.metadata_vec;
                                            serv_vec.len() == metadata_vec.len()
                                                && serv_vec.iter().eq(metadata_vec.iter())
                                        } {
                                            if metadata_vec[..] == [1; 0] {
                                                let mut serv = arc.write().await;
                                                println!("Updated relay {} metadata with no title and url", serv.mountpoint);
                                                serv.metadata_vec = vec![0];
                                                serv.metadata = None;
                                            } else {
                                                let cut = {
                                                    let mut last = metadata_vec.len();
                                                    while metadata_vec[last - 1] == 0 {
                                                        last -= 1;
                                                    }
                                                    last
                                                };
                                                if let Ok(meta_str) =
                                                    std::str::from_utf8(&metadata_vec[1..cut])
                                                {
                                                    let reg = Regex::new(
                                                        r"^StreamTitle='(.*?)';StreamUrl='(.*?)';$",
                                                    )
                                                    .unwrap();
                                                    if let Some(captures) = reg.captures(meta_str) {
                                                        let metadata = IcyMetadata {
                                                            title: {
                                                                let m_str = captures
                                                                    .get(1)
                                                                    .unwrap()
                                                                    .as_str();
                                                                if m_str.is_empty() {
                                                                    None
                                                                } else {
                                                                    Some(m_str.to_string())
                                                                }
                                                            },
                                                            url: {
                                                                let m_str = captures
                                                                    .get(2)
                                                                    .unwrap()
                                                                    .as_str();
                                                                if m_str.is_empty() {
                                                                    None
                                                                } else {
                                                                    Some(m_str.to_string())
                                                                }
                                                            },
                                                        };

                                                        let mut serv = arc.write().await;
                                                        println!("Updated relay {} metadata with title '{}' and url '{}'", serv.mountpoint, metadata.title.as_ref().unwrap_or(&"".to_string()), metadata.url.as_ref().unwrap_or(&"".to_string()));
                                                        serv.metadata_vec = metadata_vec;
                                                        serv.metadata = Some(metadata);
                                                    } else {
                                                        println!("Unknown metadata format received from relay {}: `{}`", arc.read().await.mountpoint, meta_str);
                                                        arc.write().await.disconnect_flag = true;
                                                    }
                                                } else {
                                                    println!(
                                                        "Invalid metadata parsed from relay {}",
                                                        arc.read().await.mountpoint
                                                    );
                                                    arc.write().await.disconnect_flag = true;
                                                }
                                            }
                                        }
                                    }

                                    trimmed
                                } else {
                                    data
                                }
                            };

                            if !data.is_empty() {
                                arc.read().await.stats.write().await.bytes_read += data.len();
                                broadcast_to_clients(&arc, data, queue_size, burst_size).await;
                            }
                        }

                        // Check if the source needs to be disconnected
                        read != 0 && !decoder.is_finished() && !arc.read().await.disconnect_flag
                    }
                    Err(e) => {
                        println!(
                            "An error occurred while decoding stream data from relay {}: {}",
                            arc.read().await.mountpoint,
                            e
                        );
                        false
                    }
                }
            } {}
        }

        let mut source = arc.write().await;
        let fallback = source.fallback.clone();
        disconnect_or_fallback(&server, &mut source, fallback).await;

        // Clean up and remove the source
        let mut serv = server.write().await;
        serv.sources.remove(&source.mountpoint);
        serv.relay_count -= 1;
        serv.stats.session_bytes_read += source.stats.read().await.bytes_read;

        println!("Unmounted relay {}", source.mountpoint);

        Ok(())
    }
}
