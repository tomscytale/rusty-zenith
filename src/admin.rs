use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use httparse::Header;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::http;
use crate::structs::{IcyMetadata, Query, Server};

pub async fn do_admin(
    server: Arc<RwLock<Server>>,
    stream: &mut TcpStream,
    server_id: &str,
    queries: Option<Vec<Query>>,
    path: String,
    headers: &mut [Header<'_>],
) -> Result<(), Box<dyn Error>> {
    match path.as_str() {
        "/admin/metadata" => {
            let serv = server.read().await;
            // Check for authorization
            if let Some(value) = http::do_auth(headers, &serv).await {
                return http::send_unauthorized(stream, server_id, Some(("text/plain; charset=utf-8", value))).await;
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
                                http::send_ok(stream, server_id, Some(("text/plain; charset=utf-8", "Success"))).await?;
                            }
                            None => http::send_forbidden(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid mount"))).await?,
                        }
                    }
                    _ => http::send_bad_request(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid query"))).await?,
                }
            } else {
                // Bad request
                http::send_bad_request(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid query"))).await?;
            }
        }
        "/admin/listclients" => {
            let serv = server.read().await;
            // Check for authorization
            if let Some(value) = http::do_auth(headers, &serv).await {
                return http::send_unauthorized(stream, server_id, Some(("text/plain; charset=utf-8", value))).await;
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

                            if let Ok(serialized) = serde_json::to_string(&clients) {
                                http::send_ok(stream, server_id, Some(("application/json; charset=utf-8", &serialized))).await?;
                            } else {
                                http::send_internal_error(stream, server_id, None).await?;
                            }
                        } else {
                            http::send_forbidden(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid mount"))).await?;
                        }
                    }
                    _ => http::send_bad_request(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid query"))).await?,
                }
            } else {
                // Bad request
                http::send_bad_request(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid query"))).await?;
            }
        }
        "/admin/fallbacks" => {
            let serv = server.read().await;
            if let Some(value) = http::do_auth(headers, &serv).await {
                return http::send_unauthorized(stream, server_id, Some(("text/plain; charset=utf-8", value))).await;
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
                            http::send_ok(stream, server_id, Some(("application/json; charset=utf-8", "Success"))).await?;
                        } else {
                            http::send_forbidden(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid mount"))).await?;
                        }
                    }
                    _ => http::send_bad_request(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid query"))).await?,
                }
            } else {
                // Bad request
                http::send_bad_request(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid query"))).await?;
            }
        }
        "/admin/moveclients" => {
            let serv = server.read().await;
            // Check for authorization
            if let Some(value) = http::do_auth(headers, &serv).await {
                return http::send_unauthorized(stream, server_id, Some(("text/plain; charset=utf-8", value))).await;
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
                                http::send_ok(stream, server_id, Some(("application/json; charset=utf-8", "Success"))).await?;
                            }
                            _ => http::send_forbidden(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid mount"))).await?,
                        }
                    }
                    _ => http::send_bad_request(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid query"))).await?,
                }
            } else {
                // Bad request
                http::send_bad_request(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid query"))).await?;
            }
        }
        "/admin/killclient" => {
            let serv = server.read().await;
            // Check for authorization
            if let Some(value) = http::do_auth(headers, &serv).await {
                return http::send_unauthorized(stream, server_id, Some(("text/plain; charset=utf-8", value))).await;
            }

            if let Some(queries) = queries {
                match http::get_queries_for(vec!["mount", "id"], &queries)[..].as_ref() {
                    [ Some(mount), Some(uuid_str) ] => {
                        match (serv.sources.get(mount), Uuid::parse_str(uuid_str)) {
                            (Some(source), Ok(uuid)) => {
                                if let Some(client) = source.read().await.clients.get(&uuid) {
                                    drop(client.read().await.sender.write().await.send(Arc::new(Vec::new())));
                                    println!("Killing client {}", uuid);
                                    http::send_ok(stream, server_id, Some(("application/json; charset=utf-8", "Success"))).await?;
                                } else {
                                    http::send_forbidden(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid id"))).await?;
                                }
                            }
                            (None, _) => http::send_forbidden(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid mount"))).await?,
                            (Some(_), Err(_)) => http::send_forbidden(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid id"))).await?,
                        }
                    }
                    _ => http::send_bad_request(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid query"))).await?,
                }
            } else {
                // Bad request
                http::send_bad_request(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid query"))).await?;
            }
        }
        "/admin/killsource" => {
            let serv = server.read().await;
            // Check for authorization
            if let Some(value) = http::do_auth(headers, &serv).await {
                return http::send_unauthorized(stream, server_id, Some(("text/plain; charset=utf-8", value))).await;
            }

            if let Some(queries) = queries {
                match http::get_queries_for(vec!["mount"], &queries)[..].as_ref() {
                    [ Some(mount) ] => {
                        if let Some(source) = serv.sources.get(mount) {
                            source.write().await.disconnect_flag = true;

                            println!("Killing source {}", mount);
                            http::send_ok(stream, server_id, Some(("application/json; charset=utf-8", "Success"))).await?;
                        } else {
                            http::send_forbidden(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid mount"))).await?;
                        }
                    }
                    _ => http::send_bad_request(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid query"))).await?,
                }
            } else {
                // Bad request
                http::send_bad_request(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid query"))).await?;
            }
        }
        "/admin/listmounts" => {
            let serv = server.read().await;
            // Check for authorization
            if let Some(value) = http::do_auth(headers, &serv).await {
                return http::send_unauthorized(stream, server_id, Some(("text/plain; charset=utf-8", value))).await;
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

            if let Ok(serialized) = serde_json::to_string(&sources) {
                http::send_ok(stream, server_id, Some(("application/json; charset=utf-8", &serialized))).await?;
            } else {
                http::send_internal_error(stream, server_id, None).await?;
            }
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

            if let Ok(serialized) = serde_json::to_string(&info) {
                http::send_ok(stream, server_id, Some(("application/json; charset=utf-8", &serialized))).await?;
            } else {
                http::send_internal_error(stream, server_id, None).await?;
            }
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

                            if let Ok(serialized) = serde_json::to_string(&info) {
                                http::send_ok(stream, server_id, Some(("application/json; charset=utf-8", &serialized))).await?;
                            } else {
                                http::send_internal_error(stream, server_id, None).await?;
                            }
                        } else {
                            http::send_forbidden(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid mount"))).await?;
                        }
                    }
                    _ => http::send_bad_request(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid query"))).await?,
                }
            } else {
                // Bad request
                http::send_bad_request(stream, server_id, Some(("text/plain; charset=utf-8", "Invalid query"))).await?;
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

            http::send_ok(stream, server_id, Some(("application/json; charset=utf-8", &response.to_string()))).await?;
        }
        // Return 404
        _ => http::send_not_found(stream, server_id, Some(("text/html; charset=utf-8", "<html><head><title>Error 404</title></head><body><b>404 - The file you requested could not be found</b></body></html>"))).await?
    }
    Ok(())
}
