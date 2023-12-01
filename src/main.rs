use std::fs::File;
use std::io::{BufWriter, Write};
use std::net::SocketAddr;

use tokio::net::TcpListener;

use crate::structs::ServerProperties;

mod admin;
mod consts;
mod http;
mod icecast;
mod server;
mod structs;
mod validators;

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
            match serde_json::from_str(contents.as_str()) {
                Ok(prop) => properties = prop,
                Err(e) => println!("An error occurred while parsing the config: {}", e),
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            println!("The config file was not found! Attempting to save to file");
        }
        Err(e) => println!("An error occurred while trying to read the config: {}", e),
    }

    // Create or update the current config
    match File::create(&config_location) {
        Ok(file) => match serde_json::to_string_pretty(&properties) {
            Ok(config) => {
                let mut writer = BufWriter::new(file);
                if let Err(e) = writer.write_all(config.as_bytes()) {
                    println!("An error occurred while writing to the config file: {}", e);
                }
            }
            Err(e) => println!(
                "An error occurred while trying to serialize the server properties: {}",
                e
            ),
        },
        Err(e) => println!("An error occurred while to create the config file: {}", e),
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
                        icecast::run_server(properties, listener).await;
                    }
                    Err(e) => println!("Unable to bind to port: {}", e),
                }
            }
            Err(e) => println!("Could not parse the address: {}", e),
        }
    }
}
