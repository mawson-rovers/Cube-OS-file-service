//
// Copyright (C) 2019 Kubos Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#![allow(clippy::block_in_if_condition_stmt)]

use file_protocol::{FileProtocol, FileProtocolConfig, ProtocolError, State};
use cubeos_service::Config;
use log::{error, info, warn};
use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use udp_rs::{UdpStream};
use hal_stream::Stream;
use cbor_protocol::Protocol;
use failure::bail;
use std::net::UdpSocket;

#[derive(Debug,Clone)]
pub struct FileService {
    config: FileProtocolConfig,
}
impl FileService {
    pub fn new(config: &Config) -> Self {
        // Get the storage directory prefix that we'll be using for our
        // temporary/intermediate storage location
        let prefix = match config.get("storage_dir") {
            Some(val) => val.as_str().map(|str| str.to_owned()),
            None => None,
        };

        // Get the chunk size to be used for transfers
        let transfer_chunk_size = match config.get("transfer_chunk_size") {
            Some(val) => val.as_integer().unwrap_or(1024),
            None => 1024,
        };

        // Get the chunk size to be used for hashing
        let hash_chunk_size = match config.get("hash_chunk_size") {
            Some(val) => val.as_integer().unwrap_or(transfer_chunk_size * 2),
            None => transfer_chunk_size * 2,
        } as usize;

        let transfer_chunk_size = transfer_chunk_size as usize;

        let hold_count = match config.get("hold_count") {
            Some(val) => val.as_integer().unwrap_or(5),
            None => 5,
        } as u16;

        // Get the inter chunk delay value
        let inter_chunk_delay = config
            .get("inter_chunk_delay")
            .and_then(|i| i.as_integer())
            .unwrap_or(1) as u64;

        // Get the max chunk transmission value
        let max_chunks_transmit = config
            .get("max_chunks_transmit")
            .and_then(|chunks| chunks.as_integer())
            .map(|chunks| chunks as u32);

        FileService {
            config: FileProtocolConfig::new(
                prefix,
                transfer_chunk_size,
                hold_count,
                inter_chunk_delay,
                max_chunks_transmit,
                hash_chunk_size,
            ),
        }
    }
    pub fn download(&self, source_path: String, target_ip: String, target_port: u16, target_path: String) -> Result<(), failure::Error> {
        let host = "127.0.0.1".to_string();
        
        let f_protocol: FileProtocol<UdpStream> = FileProtocol::new(
            UdpStream::new(host,format!("{}:{}",target_ip,target_port)),
            self.config.clone(),
        );
        download(f_protocol, &source_path, &target_path)
    }
    pub fn upload(&self, source_ip: String, source_port: u16, source_path: String, target_path: String) -> Result<(), failure::Error> {
        let host = "127.0.0.1".to_string();
        
        let f_protocol: FileProtocol<UdpStream> = FileProtocol::new(
            UdpStream::new(host,format!("{}:{}",source_ip,source_port)),
            self.config.clone(),
        );
        upload(f_protocol, &source_path, &target_path)
    }
    pub fn cleanup(&self, target_ip: String, target_port: u16, hash: Option<String>) -> Result<(), failure::Error> {
        let host = "127.0.0.1".to_string();
        
        let f_protocol: FileProtocol<UdpStream> = FileProtocol::new(
            UdpStream::new(host,format!("{}:{}",target_ip,target_port)),
            self.config.clone(),
        );
        cleanup(f_protocol, hash)
    }
}

pub fn upload<T: Stream + std::fmt::Debug>(f_protocol: FileProtocol<T>, source_path: &str, target_path: &str) -> Result<(), failure::Error> 
where std::io::Error: From<<T as Stream>::StreamError>
{

    info!("Uploading local: {} to remote: {}", source_path, target_path);

    // Copy file to upload to temp storage. Calculate the hash and chunk info
    let (hash, num_chunks, mode) = f_protocol.initialize_file(&source_path)?;

        // Generate channel id for transaction
    let channel = f_protocol.generate_channel()?;

    // Tell our destination the hash and number of chunks to expect
    f_protocol.send_metadata(channel, &hash, num_chunks)?;

    // Send export command for file
    f_protocol.send_export(channel, &hash, &target_path, mode)?;

    // Start the engine to send the file data chunks
    f_protocol.message_engine(
        |d| f_protocol.recv(Some(d)),
        Duration::from_secs(2),
        &State::Transmitting,
    )?;
    Ok(())
}

pub fn download<T: Stream + std::fmt::Debug>(f_protocol: FileProtocol<T>, source_path: &str, target_path: &str) -> Result<(), failure::Error> 
where std::io::Error: From<<T as Stream>::StreamError>
{
    info!(
        "Downloading remote: {} to local: {}",
        source_path, target_path
    );

    // Generate channel id for transaction
    let channel = f_protocol.generate_channel()?;
    
    // set the default chunk number as 9999, WIP
    // let num_chunks = 9999;

    // Send our file request to the remote addr and verify that it's
    // going to be able to send it
    f_protocol.send_import(channel, source_path)?;

    // Wait for the request reply.
    // Note/TODO: We don't use a timeout here because we don't know how long it will
    // take the server to prepare the file we've requested.
    // Larger files (> 100MB) can take over a minute to process.
    let reply = match f_protocol.recv(None) {
        Ok(message) => message,
        Err(error) => bail!("Failed to import file: {}", error),
    };


    let (num_chunks, recv_message) = f_protocol.get_import_size(reply).unwrap();

    let state = f_protocol.process_message(
        recv_message,
        &State::StartReceive {
            path: target_path.to_string(),
        },
    )?;

    f_protocol.message_engine(
        |d| f_protocol.recv(Some(d)),
        Duration::from_secs(2),
        &state,
    )?;
    Ok(())
}

pub fn cleanup<T: Stream + std::fmt::Debug>(f_protocol: FileProtocol<T>, hash: Option<String>) -> Result<(), failure::Error> 
where std::io::Error: From<<T as Stream>::StreamError>
{
    match &hash {
        Some(s) => info!("Requesting remote cleanup of temp storage for hash {}", s),
        None => info!("Requesting remote cleanup of all temp storage"),
    }

    // Generate channel ID for transaction
    let channel = f_protocol.generate_channel()?;

    // Send our cleanup request to the remote addr and verify that it's
    // going to be able to send it
    f_protocol.send_cleanup(channel, hash)?;

    Ok(())
}

// We need this in this lib.rs file so we can build integration tests
pub fn recv_loop(config: &Config) -> Result<(), failure::Error> {
    // Get and bind our UDP listening socket
    let host = config
        .hosturl()
        .ok_or_else(|| failure::format_err!("Unable to fetch addr for service"))?;

    // Extract our local IP address so we can spawn child sockets later
    let mut host_parts = host.split(':').map(|val| val.to_owned());
    let host_ip = host_parts
        .next()
        .ok_or_else(|| failure::format_err!("Failed to parse service IP address"))?;

    // Get the storage directory prefix that we'll be using for our
    // temporary/intermediate storage location
    let prefix = match config.get("storage_dir") {
        Some(val) => val.as_str().map(|str| str.to_owned()),
        None => None,
    };

    // Get the chunk size to be used for transfers
    let transfer_chunk_size = match config.get("transfer_chunk_size") {
        Some(val) => val.as_integer().unwrap_or(1024),
        None => 1024,
    };

    // Get the chunk size to be used for hashing
    let hash_chunk_size = match config.get("hash_chunk_size") {
        Some(val) => val.as_integer().unwrap_or(transfer_chunk_size * 2),
        None => transfer_chunk_size * 2,
    } as usize;

    let transfer_chunk_size = transfer_chunk_size as usize;

    let hold_count = match config.get("hold_count") {
        Some(val) => val.as_integer().unwrap_or(5),
        None => 5,
    } as u16;

    // Get the downlink port we'll be using when sending responses
    let downlink_port = config
        .get("downlink_port")
        .and_then(|i| i.as_integer())
        .unwrap_or(8080) as u16;

    // Get the downlink ip we'll be using when sending responses
    let downlink_ip = match config.get("downlink_ip") {
        Some(ip) => match ip.as_str().map(|ip| ip.to_owned()) {
            Some(ip) => ip,
            None => "127.0.0.1".to_owned(),
        },
        None => "127.0.0.1".to_owned(),
    };

    // Get the inter chunk delay value
    let inter_chunk_delay = config
        .get("inter_chunk_delay")
        .and_then(|i| i.as_integer())
        .unwrap_or(1) as u64;

    // Get the max chunk transmission value
    let max_chunks_transmit = config
        .get("max_chunks_transmit")
        .and_then(|chunks| chunks.as_integer())
        .map(|chunks| chunks as u32);

    info!("Starting file transfer service");
    info!("Listening on {}", host);
    info!("Downlinking to {}:{}", downlink_ip, downlink_port);
    info!("Transfer Chunk {}", transfer_chunk_size);
    info!("Hash Chunk Size {}", hash_chunk_size);

    let f_config = FileProtocolConfig::new(
        prefix,
        transfer_chunk_size,
        hold_count,
        inter_chunk_delay,
        max_chunks_transmit,
        hash_chunk_size,
    );

    let c_protocol = cbor_protocol::Protocol::new(UdpStream::new(host,format!("{}:{}",downlink_ip,downlink_port)), transfer_chunk_size);

    let timeout = config
        .get("timeout")
        .and_then(|val| val.as_integer().map(|num| Duration::from_secs(num as u64)))
        .unwrap_or(Duration::from_secs(2));

    // Setup map of channel IDs to thread channels
    let raw_threads: HashMap<u32, Sender<serde_cbor::Value>> = HashMap::new();
    // Create thread sharable wrapper
    let threads = Arc::new(Mutex::new(raw_threads));

    loop {
        // Listen on UDP port
        // let (_source, first_message) = match c_protocol.recv_message_peer() {
        //     Ok((source, first_message)) => (source, first_message),
        //     Err(e) => {
        //         warn!("Error receiving message: {:?}", e);
        //         continue;
        //     }
        // };
        let first_message = match c_protocol.recv_message() {
            Ok(first_message) => first_message,
            Err(e) => {
                    warn!("Error receiving message: {:?}", e);
                    continue;
                }
        };

        let config_ref = f_config.clone();
        let host_ref = host_ip.clone();
        let timeout_ref = timeout;

        let channel_id = match file_protocol::parse_channel_id(&first_message) {
            Ok(channel_id) => channel_id,
            Err(e) => {
                warn!("Error parsing channel ID: {:?}", e);
                continue;
            }
        };

        if !threads
            .lock()
            .map_err(|err| {
                error!("Failed to get threads mutex: {:?}", err);
                err
            })
            .unwrap()
            .contains_key(&channel_id)
        {
            let (sender, receiver): (Sender<serde_cbor::Value>, Receiver<serde_cbor::Value>) =
                mpsc::channel();

            threads
                .lock()
                .map_err(|err| {
                    error!("Failed to get threads mutex: {:?}", err);
                    err
                })
                .unwrap()
                .insert(channel_id, sender.clone());

            // Break the processing work off into its own thread so we can
            // listen for requests from other clients
            let shared_threads = threads.clone();
            let downlink_ip_ref = downlink_ip.to_owned();
            thread::spawn(move || {
                let state = State::Holding {
                    count: 0,
                    prev_state: Box::new(State::Done),
                };

                // Set up the file system processor with the reply socket information
                let f_protocol = FileProtocol::new(
                    UdpStream::new(host_ref,format!("{}:{}",downlink_ip_ref,downlink_port)),
                    config_ref,
                );

                // Listen, process, and react to the remaining messages in the
                // requested operation
                if let Err(e) = f_protocol.message_engine(
                    |d| match receiver.recv_timeout(d) {
                        Ok(v) => Ok(v),
                        Err(RecvTimeoutError::Timeout) => Err(ProtocolError::ReceiveTimeout),
                        Err(e) => Err(ProtocolError::ReceiveError {
                            err: format!("Error {:?}", e),
                        }),
                    },
                    timeout_ref,
                    &state,
                ) {
                    warn!("Encountered errors while processing transaction: {}", e);
                }

                // Remove ourselves from threads list if we are finished
                shared_threads
                    .lock()
                    .map_err(|err| {
                        error!("Failed to get threads mutex: {:?}", err);
                        err
                    })
                    .unwrap()
                    .remove(&channel_id);
            });
        }

        if let Some(sender) = threads
            .lock()
            .map_err(|err| {
                error!("Failed to get threads mutex: {:?}", err);
                err
            })
            .unwrap()
            .get(&channel_id)
        {
            if let Err(e) = sender.send(first_message) {
                warn!("Error when sending to channel {}: {:?}", channel_id, e);
            }
        }

        if !threads
            .lock()
            .map_err(|err| {
                error!("Failed to get threads mutex: {:?}", err);
                err
            })
            .unwrap()
            .contains_key(&channel_id)
        {
            warn!("No sender found for {}", channel_id);
            threads
                .lock()
                .map_err(|err| {
                    error!("Failed to get threads mutex: {:?}", err);
                    err
                })
                .unwrap()
                .remove(&channel_id);
        }
    }
}