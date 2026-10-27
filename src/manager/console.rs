// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Console attachment and bounded capability transfer.

use super::FleetManager;
use hyper_os::capability_channel::{CapabilityChannel, CapabilityDisposition};
use hyper_os::channel;
use hyper_os::handle::{ByteChannelObject, CapabilityChannelObject, OwnedHandle, RightsOffer};
use hyper_os::wait::{ObjectSignals, WaitItem, wait_many};
use hyper_service::vm as vm_contract;
use hyper_vm_policy::fleet::Response;

impl FleetManager {
    pub(super) fn attach_console(&mut self, vm: usize, client: usize) -> hyper_os::Result<()> {
        let Some(instance) = self.machines[vm].instance.as_ref() else {
            return self.reply_error(
                client,
                "VM must be running and its console must be unattached",
            );
        };
        if !instance.policy.can_attach_console() {
            return self.reply_error(
                client,
                "VM must be running and its console must be unattached",
            );
        }
        let (runtime_end, client_end) = channel::create_pair()?;
        let deadline =
            hyper_os::time::deadline_after(vm_contract::CONSOLE_HANDOFF_TIMEOUT)?.as_raw();
        instance
            .runtime_control
            .as_ref()
            .ok_or(hyper_os::Error::MissingHandle)?
            .as_byte_channel()
            .try_send(
                &vm_contract::ConsoleAttachRequest {
                    deadline_ns: deadline,
                }
                .encode(),
            )?;
        let mut runtime_end = Some(runtime_end);
        if let Err(error) =
            send_console_endpoint(&instance.console_connection, &mut runtime_end, deadline)
        {
            return self.reply_error(client, &format!("console connection failed: {error:?}"));
        }
        let mut console_channel = Some(client_end);
        if let Some(instance) = self.machines[vm].instance.as_mut() {
            instance.policy.attach_console(client);
        }
        self.reply(client, Response::Accepted)?;
        if self.clients[client].is_none() {
            return Ok(());
        }
        let deadline =
            hyper_os::time::deadline_after(vm_contract::CONSOLE_HANDOFF_TIMEOUT)?.as_raw();
        let endpoint = self.clients[client]
            .as_ref()
            .map(|client| &client.capabilities)
            .ok_or(hyper_os::Error::InvalidResponse)?;
        loop {
            if hyper_os::time::monotonic_now()?.as_nanoseconds() >= deadline {
                self.disconnect_client(client);
                return Ok(());
            }
            let waits = [WaitItem::new(
                endpoint.as_handle_ref(),
                ObjectSignals::<CapabilityChannelObject>::PEER_RECEIVING
                    .union(ObjectSignals::<CapabilityChannelObject>::PEER_CLOSED),
            )];
            let observation = match wait_many(&waits, deadline) {
                Ok(observation) => observation,
                Err(_) => {
                    self.disconnect_client(client);
                    return Ok(());
                }
            };
            if !ObjectSignals::<CapabilityChannelObject>::PEER_RECEIVING
                .is_present_in(observation.observed)
            {
                self.disconnect_client(client);
                return Ok(());
            }
            let disposition = CapabilityDisposition::move_handle(
                &mut console_channel,
                RightsOffer::Exact(vm_contract::CONSOLE_SESSION_RIGHTS),
            )?;
            match endpoint.try_send(&vm_contract::ConsoleCapability.encode(), &mut [disposition]) {
                Ok(()) => break,
                Err(hyper_os::Error::Status(hyper_os::Status::WOULD_BLOCK)) => {}
                Err(_) => {
                    self.disconnect_client(client);
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}
fn send_console_endpoint(
    endpoint: &CapabilityChannel,
    handle: &mut Option<OwnedHandle<ByteChannelObject>>,
    deadline: u64,
) -> hyper_os::Result<()> {
    loop {
        if hyper_os::time::monotonic_now()?.as_nanoseconds() >= deadline {
            return Err(hyper_os::Error::Status(hyper_os::Status::TIMED_OUT));
        }
        let waits = [WaitItem::new(
            endpoint.as_handle_ref(),
            ObjectSignals::<CapabilityChannelObject>::PEER_RECEIVING
                .union(ObjectSignals::<CapabilityChannelObject>::PEER_CLOSED),
        )];
        let observed = wait_many(&waits, deadline)?;
        if !ObjectSignals::<CapabilityChannelObject>::PEER_RECEIVING
            .is_present_in(observed.observed)
        {
            let status = if ObjectSignals::<CapabilityChannelObject>::PEER_CLOSED
                .is_present_in(observed.observed)
            {
                hyper_os::Status::PEER_CLOSED
            } else {
                hyper_os::Status::TIMED_OUT
            };
            return Err(hyper_os::Error::Status(status));
        }
        let disposition = CapabilityDisposition::move_handle(
            handle,
            RightsOffer::Exact(vm_contract::CONSOLE_SESSION_RIGHTS),
        )?;
        match endpoint.try_send(&vm_contract::ConsoleCapability.encode(), &mut [disposition]) {
            Err(hyper_os::Error::Status(hyper_os::Status::WOULD_BLOCK)) => {}
            result => return result,
        }
    }
}
