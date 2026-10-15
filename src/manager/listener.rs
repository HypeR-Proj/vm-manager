// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Blocking capability rendezvous bridged into the manager's object wait set.

use hyper_os::capability_channel::{CapabilityChannel, CapabilityReceiveSlot};
use hyper_os::handle::{ByteChannelObject, CapabilityChannelObject, OwnedHandle, Rights};
use hyper_os::wait::{ObjectSignals, WaitItem};
use hyper_service::vm as vm_contract;
use std::mem::MaybeUninit;
use std::sync::mpsc::{self, Receiver};

pub(super) struct Connection {
    pub(super) control: OwnedHandle<ByteChannelObject>,
    pub(super) capabilities: CapabilityChannel,
}

pub(super) struct Listener {
    connections: Receiver<hyper_os::Result<Connection>>,
    notification: OwnedHandle<ByteChannelObject>,
}

impl Listener {
    pub(super) fn start(endpoint: CapabilityChannel) -> hyper_os::Result<Self> {
        let (sender, connections) = mpsc::sync_channel(1);
        let (notification, bell) = hyper_os::channel::create_pair()?;
        // This worker lives for the manager process. It only handles connections and
        // blocks in rendezvous or bounded handoff; VM state stays on the main
        // Thread. These Threads share a Process, not an isolation boundary.
        std::thread::Builder::new()
            .name("vm-accept".into())
            .spawn(move || {
                loop {
                    let connection = receive(&endpoint);
                    let terminal = connection.is_err();
                    // Publish ownership before the wakeup. One bell corresponds to
                    // one queued result, including terminal listener failure.
                    if sender.send(connection).is_err()
                        || bell.as_byte_channel().send(&[1]).is_err()
                    {
                        return;
                    }
                    if terminal {
                        return;
                    }
                }
            })
            .map_err(|_| hyper_os::Error::Status(hyper_os::Status::NO_MEMORY))?;
        Ok(Self {
            connections,
            notification,
        })
    }

    pub(super) fn wait_item(&self) -> WaitItem<'_> {
        WaitItem::new(
            self.notification.as_handle_ref(),
            ObjectSignals::<ByteChannelObject>::READABLE
                .union(ObjectSignals::<ByteChannelObject>::PEER_CLOSED),
        )
    }

    pub(super) fn accept(&self) -> hyper_os::Result<Option<Connection>> {
        let mut byte = [0];
        let length = match self.notification.as_byte_channel().try_receive(&mut byte) {
            Ok(length) => length,
            // The wait set observes readiness without reserving a message.
            // Rejoin the full wait set instead of blocking the supervisor here.
            Err(hyper_os::Error::Status(hyper_os::Status::WOULD_BLOCK)) => return Ok(None),
            Err(error) => return Err(error),
        };
        if length != 1 || byte != [1] {
            return Err(hyper_os::Error::InvalidResponse);
        }
        self.connections
            .try_recv()
            .map_err(|_| hyper_os::Error::InvalidResponse)?
            .map(Some)
    }
}

fn receive(endpoint: &CapabilityChannel) -> hyper_os::Result<Connection> {
    loop {
        let mut bytes = [MaybeUninit::<u8>::uninit(); vm_contract::MESSAGE_BYTES];
        let mut slots = [
            CapabilityReceiveSlot::new::<ByteChannelObject>(
                Rights::WAIT.union(Rights::READ).union(Rights::WRITE),
            ),
            CapabilityReceiveSlot::new::<CapabilityChannelObject>(
                Rights::WAIT.union(Rights::WRITE),
            ),
        ];
        let message = endpoint.receive(hyper_os::DEADLINE_INFINITE, &mut bytes, &mut slots)?;
        if vm_contract::ManagerConnectionRequest::decode(message.bytes()).is_none()
            || message.capability_count() != vm_contract::ManagerConnectionRequest::CAPABILITY_COUNT
        {
            continue;
        }
        return Ok(Connection {
            control: slots[0]
                .take::<ByteChannelObject>()?
                .ok_or(hyper_os::Error::MissingHandle)?,
            capabilities: CapabilityChannel::from_handle(
                slots[1]
                    .take::<CapabilityChannelObject>()?
                    .ok_or(hyper_os::Error::MissingHandle)?,
            ),
        });
    }
}
