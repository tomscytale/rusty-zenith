use std::error::Error;
use std::string::ToString;
use std::time::SystemTime;

use base64::{engine::general_purpose, Engine as _};
use httparse::Header;
use httpdate::fmt_http_date;
use regex::Regex;
use tokio::sync::RwLockReadGuard;

use crate::consts::{DEFAULT_CHARSET, DEFAULT_CONTENT_TYPE};
use crate::structs::{IcyMetadata, Message, Query, Server, ServerProperties, Stream};

pub fn has_failed_auth<'a>(
    headers: &'a mut [Header<'_>],
    serv: &'a RwLockReadGuard<'_, Server>,
) -> Option<&'a str> {
    if let Some((name, pass)) = get_basic_auth(headers) {
        // For testing purposes right now
        // TODO Add proper configuration
        if !validate_user(&serv.properties, name, pass) {
            return Some("Invalid credentials");
        }
    } else {
        // No auth, return and close
        return Some("You need to authenticate");
    }
    None
}

pub fn get_basic_auth(headers: &[Header]) -> Option<(String, String)> {
    if let Some(auth) = get_header("Authorization", headers) {
        let reg =
            Regex::new(r"^Basic ((?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?)$")
                .unwrap();
        if let Some(capture) = reg.captures(std::str::from_utf8(auth).unwrap()) {
            if let Some((name, pass)) =
                std::str::from_utf8(&general_purpose::STANDARD.decode(&capture[1]).unwrap())
                    .unwrap()
                    .split_once(':')
            {
                return Some((String::from(name), String::from(pass)));
            }
        }
    }
    None
}

// TODO Add some sort of permission system
pub fn validate_user(properties: &ServerProperties, username: String, password: String) -> bool {
    for cred in &properties.users {
        if cred.username == username && cred.password == password {
            return true;
        }
    }
    false
}

async fn send(
    stream: &mut Stream,
    id: &str,
    status: &[u8],
    message: Option<Message>,
    extra_headers: Option<&[u8]>,
) -> Result<(), Box<dyn Error>> {
    stream.write_all(status).await?;
    stream
        .write_all(format!("Server: {}\r\n", id).as_bytes())
        .await?;
    stream.write_all(b"Connection: Close\r\n").await?;
    let text = match message {
        Some(msg) => {
            stream
                .write_all(
                    format!(
                        "Content-Type: {}; charset={}\r\n",
                        msg.content_type.unwrap_or(DEFAULT_CONTENT_TYPE.to_string()),
                        msg.charset.unwrap_or(DEFAULT_CHARSET.to_string())
                    )
                    .as_bytes(),
                )
                .await?;
            stream
                .write_all(format!("Content-Length: {}\r\n", msg.message.len()).as_bytes())
                .await?;
            Some(msg.message)
        }
        None => None,
    };
    if let Some(extra_header) = extra_headers {
        stream.write_all(extra_header).await?;
    }
    server_info(stream).await?;
    if let Some(m) = text {
        stream.write_all(m.as_bytes()).await?
    }

    Ok(())
}

pub async fn send_unauthorized(
    stream: &mut Stream,
    id: &str,
    message: Option<Message>,
) -> Result<(), Box<dyn Error>> {
    send(
        stream,
        id,
        b"HTTP/1.0 401 Authorization Required\r\n",
        message,
        Some(b"WWW-Authenticate: Basic realm=\"Icy Server\"\r\n"),
    )
    .await
}

pub async fn send_forbidden(
    stream: &mut Stream,
    id: &str,
    message: Option<Message>,
) -> Result<(), Box<dyn Error>> {
    send(stream, id, b"HTTP/1.0 403 Forbidden\r\n", message, None).await
}

pub async fn send_ok(
    stream: &mut Stream,
    id: &str,
    message: Option<Message>,
) -> Result<(), Box<dyn Error>> {
    send(stream, id, b"HTTP/1.0 200 OK\r\n", message, None).await
}

pub async fn send_bad_request(
    stream: &mut Stream,
    id: &str,
    message: Option<Message>,
) -> Result<(), Box<dyn Error>> {
    send(stream, id, b"HTTP/1.0 400 Bad Request\r\n", message, None).await
}

pub async fn send_continue(stream: &mut Stream, id: &str) -> Result<(), Box<dyn Error>> {
    send(stream, id, b"HTTP/1.0 200 OK\r\n", None, None).await
}

pub async fn send_not_found(
    stream: &mut Stream,
    id: &str,
    message: Option<Message>,
) -> Result<(), Box<dyn Error>> {
    send(
        stream,
        id,
        b"HTTP/1.0 404 File Not Found\r\n",
        message,
        None,
    )
    .await
}

pub async fn server_info(stream: &mut Stream) -> Result<(), Box<dyn Error>> {
    stream
        .write_all(format!("Date: {}\r\n", fmt_http_date(SystemTime::now())).as_bytes())
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

    Ok(())
}

pub async fn send_internal_error(
    stream: &mut Stream,
    id: &str,
    message: Option<Message>,
) -> Result<(), Box<dyn Error>> {
    send(
        stream,
        id,
        b"HTTP/1.0 500 Internal Server Error\r\n",
        message,
        None,
    )
    .await
}

pub fn get_header<'a>(key: &str, headers: &[Header<'a>]) -> Option<&'a [u8]> {
    let key = key.to_lowercase();
    for header in headers {
        if header.name.to_lowercase() == key {
            return Some(header.value);
        }
    }
    None
}

pub fn get_queries_for(keys: Vec<&str>, queries: &[Query]) -> Vec<Option<String>> {
    let mut results = vec![None; keys.len()];

    for query in queries {
        let field = query.field.as_str();
        for (i, key) in keys.iter().enumerate() {
            if &field == key {
                results[i] = Some(query.value.to_string());
            }
        }
    }

    results
}

/**
 * Get a vector containing n and the padded data
 */
pub fn get_metadata_vec(metadata: &Option<IcyMetadata>) -> Vec<u8> {
    let mut subvec = vec![0];
    if let Some(icy_metadata) = metadata {
        subvec.extend_from_slice(b"StreamTitle='");
        if let Some(title) = &icy_metadata.title {
            subvec.extend_from_slice(title.as_bytes());
        }
        subvec.extend_from_slice(b"';StreamUrl='");
        if let Some(url) = &icy_metadata.url {
            subvec.extend_from_slice(url.as_bytes());
        }
        subvec.extend_from_slice(b"';");

        // Calculate n
        let len = subvec.len() - 1;
        subvec[0] = {
            let down = len >> 4;
            let remainder = len & 0b1111;
            if remainder > 0 {
                // Pad with zeroes
                subvec.append(&mut vec![0; 16 - remainder]);
                down + 1
            } else {
                down
            }
        } as u8;
    }

    subvec
}
