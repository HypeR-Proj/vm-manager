// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Fleet manager process entry.

#[cfg(target_os = "hyper")]
extern crate hyper_vm_policy_shared as hyper_vm_policy;

mod manager;

use hyper_os::startup::Startup;
use manager::FleetManager;
use std::process::ExitCode;

fn application_main(mut startup: Startup<'_>) -> ExitCode {
    if hyper_os::require_core_abi().is_err() {
        return ExitCode::FAILURE;
    }
    let mut manager = match FleetManager::from_startup(&mut startup) {
        Ok(manager) => manager,
        Err(error) => {
            eprintln!("vm-manager: startup failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    // Fleet admission already acknowledges startup to the finite coordinator.
    // Ongoing status belongs to the resident manager, not that one-shot channel.
    if let Ok(connector) = startup.borrow(hyper_component::CONNECTOR) {
        let _ = hyper_component::connection::status(connector, "ready");
    }
    match manager.run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("vm-manager: supervisor failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn main() -> ExitCode {
    match hyper_rt::process::startup() {
        Ok(startup) => application_main(startup),
        Err(_) => ExitCode::FAILURE,
    }
}
