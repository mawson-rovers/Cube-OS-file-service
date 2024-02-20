//
// Copyright (C) 2024 Mawson Rovers
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

mod common;

use std::fs;
use std::thread;
use tempfile::TempDir;
use crate::common::*;
use file_service::recv_loop;
use std::time::Duration;
use kubos_system::Config as ServiceConfig;

#[test]
fn upload_large_file() {
    let test_dir = TempDir::new().expect("Failed to create test dir");
    let test_dir_str = test_dir.path().to_str().unwrap();
    let source = format!("{}/source", test_dir_str);
    let dest = format!("{}/dest", test_dir_str);
    let service_port = 7000;
    let downlink_port = 6000;
    let chunk_size = 896;

    // let mut contents: [u8; 20000] = core::array::from_fn(|_| rand::random::<u8>());
    let mut contents: Vec<u8> = (0..2000000).map(|_| rand::random::<u8>()).collect();
    contents[0..12].copy_from_slice("upload_large".as_bytes());

    let hash = create_test_file(&source, &contents);

    let storage_dir = format!("{}/service", test_dir_str);
    service_new!(service_port, downlink_port, chunk_size, storage_dir);

    let result = upload(
        "127.0.0.1",
        downlink_port,
        &format!("127.0.0.1:{}", service_port),
        &source,
        &dest,
        Some(format!("{}/client", test_dir_str)),
        chunk_size,
    );

    if let Err(err) = &result {
        println!("Error: {}", err);
    }

    result.unwrap();

    // Verify the final file's contents
    let dest_contents = fs::read(dest).unwrap();

    assert_eq!(&contents[..], dest_contents.as_slice());

    // Cleanup the temporary files so that the test can be repeatable
    // The client folder is cleaned up by the protocol as a result
    // of the hash mismatch
    let _ = fs::remove_dir_all(format!("service/storage/{}", hash));
}