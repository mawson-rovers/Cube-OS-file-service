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

// #![deny(warnings)]
mod service;
mod lib;

use crate::lib::*;
// use kubos_system::logger as ServiceLogger;
// use kubos_system::Config as ServiceConfig;
use cubeos_service::{Service, Config, Logger};
use log::{error, warn};
use std::sync::Arc;
use crate::service::*;
use failure::format_err;

fn main() -> Result<(), failure::Error> {
    Logger::init().unwrap();

    let config = Config::new("file-transfer-service")
        .map_err(|err| {
            error!("Failed to load service config: {:?}", err);
            err
        })
        .unwrap();

    let service = Box::new(FileService::new(&config))
    ;

    #[cfg(feature = "ground")]
    let socket = config
        .get("udp_socket")
        .ok_or_else(|| {
            error!("Failed to load 'udp-socket' config value");
            format_err!("Failed to load 'udp-socket' config value");
        })
        .unwrap();

    #[cfg(feature = "ground")]
    let target = config
        .get("target")
        .ok_or_else(|| {
            error!("Failed to load 'target' config value");
            format_err!("Failed to load 'target' config value");
        })
        .unwrap();

    #[cfg(feature = "ground")]
    // Start ground service
    Service::new(
        config.clone(),
        socket.as_str().unwrap().to_string(),
        target.as_str().unwrap().to_string(),
        Some(Arc::new(json_handler)),
    )
    .start();

    #[cfg(not(feature = "ground"))]
    //Start up UDP server
    Service::new(config.clone(), service, Some(Arc::new(udp_handler))).start();

    recv_loop(&config)
    // Ok(())
    // match recv_loop(&config) {
    //     Ok(()) => warn!("Service listener loop exited successfully?"),
    //     Err(err) => error!("Service listener exited early: {}", err),
    // }
}
