use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use httparse::Header;
use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::http;
use crate::structs::{IcyMetadata, Query, Server, Stream};

pub async fn do_admin(
    server: Arc<RwLock<Server>>,
    stream: &mut Stream,
    server_id: &str,
    queries: Option<Vec<Query>>,
    path: String,
    headers: &mut [Header<'_>],
) -> Result<(), Box<dyn Error>> {
    match path.as_str() {
        "/admin/metadata" => {
            let serv = server.read().await;
            // Check for authorization
            if let Some(value) = http::has_failed_auth(headers, &serv) {
                return send_unauthorized(stream, server_id, value).await;
            }

            // Authentication passed
            // Now check the query fields
            // Takes in mode, mount, song and url
            if let Some(queries) = queries {
                match http::get_queries_for(vec!["mode", "mount", "song", "url"], &queries)[..].as_ref() {
                    [ Some(mode), Some(mount), song, url ] if mode == "updinfo" => {
                        match serv.sources.get(mount) {
                            Some(source) => {
                                println!("Updated source {} metadata with title '{}' and url '{}'", mount, song.as_ref().unwrap_or(&"".to_string()), url.as_ref().unwrap_or(&"".to_string()));
                                let mut source = source.write().await;
                                source.metadata = match (song, url) {
                                    (None, None) => None,
                                    _ => Some(IcyMetadata {
                                        title: song.clone(),
                                        url: url.clone(),
                                    }),
                                };
                                source.metadata_vec = http::get_metadata_vec(&source.metadata);
                                send_ok(stream, server_id, Some("text/plain")).await?;
                            }
                            None => send_forbidden(stream, server_id, "Invalid mount").await?,
                        }
                    }
                    _ => send_bad_request(stream, server_id).await?,
                }
            } else {
                // Bad request
                send_bad_request(stream, server_id).await?;
            }
        }
        "/admin/listclients" => {
            let serv = server.read().await;
            // Check for authorization
            if let Some(value) = http::has_failed_auth(headers, &serv) {
                return send_unauthorized(stream, server_id, value).await;
            }

            if let Some(queries) = queries {
                match http::get_queries_for(vec!["mount"], &queries)[..].as_ref() {
                    [ Some(mount) ] => {
                        if let Some(source) = serv.sources.get(mount) {
                            let mut clients: HashMap<Uuid, Value> = HashMap::new();

                            for client in source.read().await.clients.values() {
                                let client = client.read().await;
                                let properties = client.properties.clone();

                                let value = json!( {
												"user_agent": properties.uagent,
												"metadata_enabled": properties.metadata,
												"stats": &*client.stats.read().await
											} );

                                clients.insert(properties.id, value);
                            }

                            send_ok_if_valid(stream, server_id, &clients).await?;
                        } else {
                            send_forbidden(stream, server_id, "Invalid mount").await?;
                        }
                    }
                    _ => send_bad_request(stream, server_id).await?,
                }
            } else {
                // Bad request
                send_bad_request(stream, server_id).await?;
            }
        }
        "/admin/fallbacks" => {
            let serv = server.read().await;
            if let Some(value) = http::has_failed_auth(headers, &serv) {
                return send_unauthorized(stream, server_id, value).await;
            }

            if let Some(queries) = queries {
                match http::get_queries_for(vec!["mount", "fallback"], &queries)[..].as_ref() {
                    [ Some(mount), fallback ] => {
                        if let Some(source) = serv.sources.get(mount) {
                            source.write().await.fallback = fallback.clone();

                            if let Some(fallback) = fallback {
                                println!("Set the fallback for {} to {}", mount, fallback);
                            } else {
                                println!("Unset the fallback for {}", mount);
                            }
                            send_ok(stream, server_id, None).await?;
                        } else {
                            send_forbidden(stream, server_id, "Invalid mount").await?;
                        }
                    }
                    _ => send_bad_request(stream, server_id).await?,
                }
            } else {
                // Bad request
                send_bad_request(stream, server_id).await?;
            }
        }
        "/admin/moveclients" => {
            let serv = server.read().await;
            // Check for authorization
            if let Some(value) = http::has_failed_auth(headers, &serv) {
                return send_unauthorized(stream, server_id, value).await;
            }

            if let Some(queries) = queries {
                match http::get_queries_for(vec!["mount", "destination"], &queries)[..].as_ref() {
                    [ Some(mount), Some(dest) ] => {
                        match (serv.sources.get(mount), serv.sources.get(dest)) {
                            (Some(source), Some(destination)) => {
                                let mut from = source.write().await;
                                let mut to = destination.write().await;

                                for (uuid, client) in from.clients.drain() {
                                    *client.read().await.source.write().await = to.mountpoint.clone();
                                    to.clients.insert(uuid, client);
                                }

                                println!("Moved clients from {} to {}", mount, dest);
                                send_ok(stream, server_id, None).await?;
                            }
                            _ => send_forbidden(stream, server_id, "Invalid mount").await?,
                        }
                    }
                    _ => send_bad_request(stream, server_id).await?,
                }
            } else {
                // Bad request
                send_bad_request(stream, server_id).await?;
            }
        }
        "/admin/killclient" => {
            let serv = server.read().await;
            // Check for authorization
            if let Some(value) = http::has_failed_auth(headers, &serv) {
                return send_unauthorized(stream, server_id, value).await;
            }

            if let Some(queries) = queries {
                match http::get_queries_for(vec!["mount", "id"], &queries)[..].as_ref() {
                    [ Some(mount), Some(uuid_str) ] => {
                        match (serv.sources.get(mount), Uuid::parse_str(uuid_str)) {
                            (Some(source), Ok(uuid)) => {
                                if let Some(client) = source.read().await.clients.get(&uuid) {
                                    drop(client.read().await.sender.write().await.send(Arc::new(Vec::new())));
                                    println!("Killing client {}", uuid);
                                    send_ok(stream, server_id, None).await?;
                                } else {
                                    send_forbidden(stream, server_id, "Invalid id").await?
                                }
                            }
                            (None, _) => send_forbidden(stream, server_id, "Invalid mount").await?,
                            (Some(_), Err(_)) => send_forbidden(stream, server_id, "Invalid id").await?,
                        }
                    }
                    _ => send_bad_request(stream, server_id).await?,
                }
            } else {
                // Bad request
                send_bad_request(stream, server_id).await?;
            }
        }
        "/admin/killsource" => {
            let serv = server.read().await;
            // Check for authorization
            if let Some(value) = http::has_failed_auth(headers, &serv) {
                return send_unauthorized(stream, server_id, value).await;
            }

            if let Some(queries) = queries {
                match http::get_queries_for(vec!["mount"], &queries)[..].as_ref() {
                    [ Some(mount) ] => {
                        if let Some(source) = serv.sources.get(mount) {
                            source.write().await.disconnect_flag = true;

                            println!("Killing source {}", mount);
                            send_ok(stream, server_id, None).await?;
                        } else {
                            send_forbidden(stream, server_id, "Invalid mount").await?;
                        }
                    }
                    _ => send_bad_request(stream, server_id).await?,
                }
            } else {
                // Bad request
                send_bad_request(stream, server_id).await?;
            }
        }
        "/admin/listmounts" => {
            let serv = server.read().await;
            // Check for authorization
            if let Some(value) = http::has_failed_auth(headers, &serv) {
                return send_unauthorized(stream, server_id, value).await;
            }

            let mut sources: HashMap<String, Value> = HashMap::new();

            for source in serv.sources.values() {
                let source = source.read().await;

                let value = json!( {
								"fallback": source.fallback,
								"metadata": source.metadata,
								"properties": source.properties,
								"stats": &*source.stats.read().await,
								"clients": source.clients.keys().cloned().collect::< Vec< Uuid > >()
							} );

                sources.insert(source.mountpoint.clone(), value);
            }

            send_ok_if_valid(stream, server_id, &sources).await?;
        }
        "/api/serverinfo" => {
            let serv = server.read().await;

            let info = json!( {
							"mounts": serv.sources.keys().cloned().collect::< Vec< String > >(),
							"properties": {
								"server_id": serv.properties.server_id,
								"admin": serv.properties.admin,
								"host": serv.properties.host,
								"location": serv.properties.location,
								"description": serv.properties.description
							},
							"stats": {
								"start_time": serv.stats.start_time,
								"peak_listeners": serv.stats.peak_listeners
							},
							"current_listeners": serv.clients.len()
						} );

            send_ok_if_valid(stream, server_id, &info).await?;
        }
        "/api/mountinfo" => {
            if let Some(queries) = queries {
                match http::get_queries_for(vec!["mount"], &queries)[..].as_ref() {
                    [ Some(mount) ] => {
                        let serv = server.read().await;
                        if let Some(source) = serv.sources.get(mount) {
                            let source = source.read().await;
                            let properties = &source.properties;
                            let stats = &source.stats.read().await;

                            let info = json!( {
											"metadata": source.metadata,
											"properties": {
												"name": properties.name,
												"description": properties.description,
												"url": properties.url,
												"genre": properties.genre,
												"bitrate": properties.bitrate,
												"content_type": properties.content_type
											},
											"stats": {
												"start_time": stats.start_time,
												"peak_listeners": stats.peak_listeners
											},
											"current_listeners": source.clients.len()
										} );

                            send_ok_if_valid(stream, server_id, &info).await?;
                        } else {
                            send_forbidden(stream, server_id, "Invalid mount").await?;
                        }
                    }
                    _ => send_bad_request(stream, server_id).await?,
                }
            } else {
                // Bad request
                send_bad_request(stream, server_id).await?;
            }
        }
        "/api/stats" => {
            let server = server.read().await;
            let stats = &server.stats;
            let mut total_bytes_sent = stats.session_bytes_sent;
            let mut total_bytes_read = stats.session_bytes_read;
            for source in server.sources.values() {
                let source = source.read().await;
                total_bytes_read += source.stats.read().await.bytes_read;
                for clients in source.clients.values() {
                    total_bytes_sent += clients.read().await.stats.read().await.bytes_sent;
                }
            }

            let epoch = {
                if let Ok(time) = SystemTime::now().duration_since(UNIX_EPOCH) {
                    time.as_secs()
                } else {
                    0
                }
            };

            let response = json!( {
							"uptime": epoch - stats.start_time,
							"peak_listeners": stats.peak_listeners,
							"session_bytes_read": total_bytes_read,
							"session_bytes_sent": total_bytes_sent
						} );

            send_ok_if_valid(stream, server_id, &response).await?;
        }
        // Return 404
        _ => http::send_not_found(stream, server_id, Some(("text/html; charset=utf-8", "<html><head><title>Error 404</title></head><body><b>404 - The file you requested could not be found</b></body></html>"))).await?
    }
    Ok(())
}

async fn send_ok(
    stream: &mut Stream,
    server_id: &str,
    content_type: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    let c_type = content_type.unwrap_or("application/json");
    http::send_ok(
        stream,
        server_id,
        Some((&(format!("{}; charset=utf-6", c_type)), "Success")),
    )
    .await?;
    Ok(())
}

async fn send_ok_if_valid<T: Sized + Serialize>(
    stream: &mut Stream,
    server_id: &str,
    data: &T,
) -> Result<(), Box<dyn Error>> {
    if let Ok(serialized) = serde_json::to_string(data) {
        http::send_ok(
            stream,
            server_id,
            Some(("application/json; charset=utf-8", &serialized)),
        )
        .await?;
    } else {
        http::send_internal_error(stream, server_id, None).await?;
    }
    Ok(())
}

async fn send_unauthorized(
    stream: &mut Stream,
    server_id: &str,
    value: &str,
) -> Result<(), Box<dyn Error>> {
    http::send_unauthorized(
        stream,
        server_id,
        Some(("text/plain; charset=utf-8", value)),
    )
    .await
}

async fn send_bad_request(stream: &mut Stream, server_id: &str) -> Result<(), Box<dyn Error>> {
    http::send_bad_request(
        stream,
        server_id,
        Some(("text/plain; charset=utf-8", "Invalid query")),
    )
    .await
}

async fn send_forbidden(
    stream: &mut Stream,
    server_id: &str,
    msg: &str,
) -> Result<(), Box<dyn Error>> {
    http::send_forbidden(stream, server_id, Some(("text/plain; charset=utf-8", &msg))).await
}
