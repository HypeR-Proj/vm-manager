// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! One-shot fleet admission, completed before any guest starts.

use super::FleetManager;
use hyper_os::capability_channel::{CapabilityChannel, CapabilityReceiveSlot};
use hyper_os::fs::File;
use hyper_os::handle::{ByteChannelObject, FileObject};
use hyper_service::vm as vm_contract;
use hyper_vm_manager::FleetConfiguration;
use hyper_vm_policy::fleet;
use std::io::Read;
use std::mem::MaybeUninit;

impl FleetManager {
    pub(super) fn configure_fleet(
        &mut self,
        provisioning: CapabilityChannel,
        io_broker: Option<CapabilityChannel>,
    ) -> hyper_os::Result<()> {
        let mut bytes = [MaybeUninit::<u8>::uninit(); vm_contract::MESSAGE_BYTES];
        let mut slots = [
            CapabilityReceiveSlot::new::<FileObject>(vm_contract::PROVISIONED_CONFIG_RIGHTS),
            CapabilityReceiveSlot::new::<ByteChannelObject>(vm_contract::PROVISIONED_RESULT_RIGHTS),
        ];
        let message = provisioning.receive(hyper_os::DEADLINE_INFINITE, &mut bytes, &mut slots)?;
        let request = vm_contract::ProvisionRequest::decode(message.bytes())
            .ok_or(hyper_os::Error::InvalidResponse)?;
        if message.capability_count() != request.capability_count() {
            return Err(hyper_os::Error::InvalidResponse);
        }
        if request == vm_contract::ProvisionRequest::ConfigurationUnavailable {
            // This explicit terminal request carries no file or result writer.
            // Dropping an unavailable broker cannot invent an empty namespace:
            // the restricted state rejects all later VM admission requests.
            self.configuration = FleetConfiguration::Unavailable;
            eprintln!("HypeR vm-manager: initial fleet configuration unavailable");
            return Ok(());
        }
        let config = slots[0]
            .take::<FileObject>()?
            .ok_or(hyper_os::Error::MissingHandle)?;
        let result = slots[1]
            .take::<ByteChannelObject>()?
            .ok_or(hyper_os::Error::MissingHandle)?;
        let admitted = (|| {
            let config = read_config(File::from_handle(config))?;
            // Init sends this request only after the I/O service reports ready.
            // Discover its actual VM identity before admitting any fleet names.
            self.io_service = io_broker
                .map(super::inventory::ObservedVm::connect)
                .transpose()
                .map_err(|error| format!("cannot discover observed VMs: {error}"))?;
            self.install_definitions(config.machines)
        })();
        let response = match &admitted {
            Ok(()) => {
                self.configuration = FleetConfiguration::Configured;
                vm_contract::ProvisionResult::Configured
            }
            Err(error) => {
                self.configuration = FleetConfiguration::Unavailable;
                eprintln!("HypeR vm-manager: fleet configuration rejected: {error}");
                vm_contract::ProvisionResult::Rejected
            }
        };
        // A fresh endpoint has room for this single result. Neither endpoint
        // survives configuration, and init acquires no guest-lifecycle authority.
        result.as_byte_channel().try_send(&response.encode())?;
        // A rejected fleet owns no published definitions. Keep the service
        // alive for list/diagnostic requests instead of failing Native init.
        Ok(())
    }
}

fn read_config(file: File) -> Result<fleet::Config, String> {
    // Bound allocation even if the file grows after provisioning.
    let mut bytes = Vec::new();
    file.into_std()
        .take(fleet::MAX_CONFIG_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("cannot read configuration: {error}"))?;
    if bytes.len() as u64 > fleet::MAX_CONFIG_BYTES {
        return Err("configuration exceeds size limit".into());
    }
    fleet::Config::parse(&bytes)
}
