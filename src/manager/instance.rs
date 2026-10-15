// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Runtime control transport, stop escalation, and completed instance retirement.

use super::FleetManager;
use hyper_os::capability_channel::{CapabilityChannel, CapabilityDisposition};
use hyper_os::handle::{
    ByteChannelObject, CapabilityChannelObject, OwnedHandle, ProcessObject, ResourceDomainObject,
    RightsOffer, TaskGroupObject,
};
use hyper_os::task::ProcessTermination;
use hyper_os::wait::{ObjectSignals, WaitItem, wait_many};
use hyper_service::vm as vm_contract;
use hyper_vm_manager::{InstancePolicy, RuntimeControlState, complete_admission};
use hyper_vm_policy::fleet;
use std::time::Instant;

pub(super) struct IoAdmission {
    pub(super) endpoint: Option<OwnedHandle<CapabilityChannelObject>>,
    pub(super) record: [u8; hyper_service::io::CONNECT_BYTES],
}

pub(super) struct VmInstance {
    pub(super) _resource_domain: OwnedHandle<ResourceDomainObject>,
    pub(super) _task_group: OwnedHandle<TaskGroupObject>,
    pub(super) runtime: OwnedHandle<ProcessObject>,
    pub(super) runtime_control: Option<OwnedHandle<ByteChannelObject>>,
    pub(super) console_connection: CapabilityChannel,
    pub(super) policy: InstancePolicy,
    pub(super) io_admission: Option<IoAdmission>,
    pub(super) observation_sequence: u64,
}

impl VmInstance {
    fn next_request_sequence(&mut self) -> hyper_os::Result<u64> {
        self.observation_sequence = self
            .observation_sequence
            .checked_add(1)
            .ok_or(hyper_os::Error::InvalidResponse)?;
        Ok(self.observation_sequence)
    }

    pub(super) fn control_vcpu(
        &mut self,
        vcpu: u32,
        affinity: Option<[u64; vm_contract::VCPU_AFFINITY_WORDS]>,
        deadline: u64,
    ) -> hyper_os::Result<vm_contract::VcpuControlReply> {
        let request = vm_contract::VcpuControlRequest {
            sequence: self.next_request_sequence()?,
            vcpu,
            affinity,
        };
        self.exchange(&request.encode(), deadline, |bytes| {
            vm_contract::VcpuControlReply::decode(bytes)
                .filter(|reply| reply.sequence == request.sequence && reply.vcpu == request.vcpu)
        })
    }

    pub(super) fn observe_memory(&mut self, deadline: u64) -> Option<vm_contract::Observation> {
        if self.policy.state() != fleet::State::Running {
            return None;
        }
        let request = vm_contract::ObservationRequest(self.next_request_sequence().ok()?);
        self.exchange(&request.encode(), deadline, |bytes| {
            vm_contract::Observation::decode(bytes).filter(|reply| reply.request == request)
        })
        .ok()
    }

    /// One bounded transport for observations and control acknowledgements.
    /// Lifecycle messages keep advancing, and late replies remain harmless.
    fn exchange<T>(
        &mut self,
        request: &[u8],
        deadline: u64,
        decode: impl Fn(&[u8]) -> Option<T>,
    ) -> hyper_os::Result<T> {
        if hyper_os::time::monotonic_now()?.as_nanoseconds() >= deadline {
            return Err(hyper_os::Error::Status(hyper_os::Status::TIMED_OUT));
        }
        let control = self
            .runtime_control
            .as_ref()
            .ok_or(hyper_os::Error::MissingHandle)?
            .as_byte_channel();
        control.try_send(request)?;
        loop {
            if hyper_os::time::monotonic_now()?.as_nanoseconds() >= deadline {
                return Err(hyper_os::Error::Status(hyper_os::Status::TIMED_OUT));
            }
            let mut bytes = [0u8; vm_contract::OBSERVATION_BYTES];
            match control.try_receive(&mut bytes) {
                Ok(length) => {
                    if let Some(reply) = decode(&bytes[..length]) {
                        return Ok(reply);
                    }
                    if self.policy.observe_message(&bytes[..length]).is_err() {
                        self.policy.reject_protocol();
                        self.force_stop();
                        return Err(hyper_os::Error::InvalidResponse);
                    }
                    if self.policy.is_terminal() {
                        let _ = self.arm_exit_deadline();
                        return Err(hyper_os::Error::Status(hyper_os::Status::BAD_STATE));
                    }
                }
                Err(hyper_os::Error::Status(hyper_os::Status::WOULD_BLOCK)) => {
                    let waits = [WaitItem::new(
                        control.as_handle_ref(),
                        ObjectSignals::<ByteChannelObject>::READABLE
                            .union(ObjectSignals::<ByteChannelObject>::PEER_CLOSED),
                    )];
                    let ready = wait_many(&waits, deadline)?;
                    if !ObjectSignals::<ByteChannelObject>::READABLE.is_present_in(ready.observed) {
                        return Err(hyper_os::Error::Status(hyper_os::Status::PEER_CLOSED));
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(super) fn wants_io_admission(&self) -> bool {
        self.policy.wants_io_admission(self.io_admission.is_some())
    }

    fn receive_runtime_status(&mut self) -> hyper_os::Result<RuntimeControlState> {
        let mut message = [0u8; vm_contract::OBSERVATION_BYTES];
        let received = self
            .runtime_control
            .as_ref()
            .ok_or(hyper_os::Error::MissingHandle)?
            .as_byte_channel()
            .try_receive(&mut message);
        self.policy
            .observe_receive(received.map(|length| &message[..length]))
    }

    fn drain_runtime_statuses(&mut self) -> hyper_os::Result<()> {
        let Some(control) = self.runtime_control.as_ref() else {
            return Ok(());
        };
        loop {
            let mut message = [0u8; vm_contract::OBSERVATION_BYTES];
            match control.as_byte_channel().try_receive(&mut message) {
                Ok(length) => {
                    if self.policy.observe_message(&message[..length]).is_err() {
                        self.policy.reject_protocol();
                    }
                }
                Err(hyper_os::Error::Status(hyper_os::Status::WOULD_BLOCK))
                | Err(hyper_os::Error::Status(hyper_os::Status::PEER_CLOSED)) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
    }

    fn request_cooperative_stop(&mut self) -> hyper_os::Result<()> {
        if self.policy.request_cooperative_stop() != vm_contract::StopAction::SendCooperative {
            return Ok(());
        }
        let sent = self
            .runtime_control
            .as_ref()
            .ok_or(hyper_os::Error::MissingHandle)
            .and_then(|control| {
                control
                    .as_byte_channel()
                    .try_send(&vm_contract::InstanceCommand::Stop.encode())
            });
        if self.policy.cooperative_stop_sent(sent, Instant::now())
            == vm_contract::StopAction::ForceProcess
        {
            let _ = self.runtime.as_process_supervisor().request_stop();
        }
        Ok(())
    }

    fn force_stop(&mut self) {
        if self.policy.force_stop() == vm_contract::StopAction::ForceProcess {
            let _ = self.runtime.as_process_supervisor().request_stop();
        }
    }

    fn arm_exit_deadline(&mut self) -> hyper_os::Result<()> {
        if self.policy.arm_exit_deadline(Instant::now()).is_none() {
            self.force_stop();
        }
        Ok(())
    }
}

impl FleetManager {
    pub(super) fn admit_io(&mut self) -> hyper_os::Result<()> {
        let Some(instance) = self.machines.iter_mut().find_map(|machine| {
            machine
                .instance
                .as_mut()
                .filter(|instance| instance.wants_io_admission())
        }) else {
            return Ok(());
        };
        let admission = instance
            .io_admission
            .as_mut()
            .ok_or(hyper_os::Error::InvalidResponse)?;
        let broker = self
            .io_service
            .as_ref()
            .ok_or(hyper_os::Error::MissingHandle)?;
        let disposition = CapabilityDisposition::move_handle(
            &mut admission.endpoint,
            RightsOffer::Exact(hyper_service::io::SESSION_RIGHTS),
        )?;
        let result = broker
            .broker()
            .try_send(&admission.record, &mut [disposition]);
        complete_admission(&mut instance.io_admission, result);
        if instance.io_admission.is_some() {
            return Ok(());
        }
        #[cfg(feature = "broker-test")]
        if self
            .machines
            .iter()
            .filter_map(|machine| machine.instance.as_ref())
            .filter(|instance| instance.io_admission.is_none())
            .count()
            == 2
        {
            self.io_service = None;
            println!("BROKER-TEST MANAGER-ENDPOINT-CLOSED");
        }
        Ok(())
    }

    pub(super) fn handle_runtime_control(&mut self, vm: usize) -> hyper_os::Result<()> {
        let Some(instance) = self.machines[vm].instance.as_mut() else {
            return Ok(());
        };
        // Signal publication can lag the channel queue. Always read first:
        // READABLE may already be drained, and EOF must not discard records.
        match instance.receive_runtime_status() {
            Ok(RuntimeControlState::Closed) => {
                drop(instance.runtime_control.take());
            }
            Ok(RuntimeControlState::Open) => {}
            Err(error) => {
                eprintln!("HypeR vm-manager: VM {vm} runtime control failed: {error}");
                instance.policy.reject_protocol();
                instance.force_stop();
            }
        }
        if instance.policy.is_terminal() {
            instance.arm_exit_deadline()?;
        }
        Ok(())
    }

    pub(super) fn request_stop(&mut self, vm: usize) -> hyper_os::Result<()> {
        if let Some(instance) = self.machines[vm].instance.as_mut() {
            instance.request_cooperative_stop()?;
            drop(instance.io_admission.take());
        }
        Ok(())
    }

    pub(super) fn finish_instance(&mut self, vm: usize) -> hyper_os::Result<()> {
        let Some(mut instance) = self.machines[vm].instance.take() else {
            return Ok(());
        };
        instance
            .runtime
            .as_process_supervisor()
            .wait_terminated(hyper_os::DEADLINE_INFINITE)?;
        instance.drain_runtime_statuses()?;
        let succeeded = matches!(
            instance.runtime.as_process_supervisor().info()?.terminal,
            Some(ProcessTermination::ProcessExited { status: 0 })
        );
        let outcome =
            std::mem::take(&mut instance.policy).finish(succeeded, &mut instance.io_admission);
        self.machines[vm].policy.finished(&outcome);
        if let vm_contract::InstanceEvent::Failed(reason) = outcome.event {
            eprintln!(
                "HypeR vm-manager: VM '{}' failed: {reason:?}",
                self.machines[vm].definition.name
            );
        }
        drop(instance);
        Ok(())
    }

    pub(super) fn complete_restarts(&mut self) {
        let now = Instant::now();
        for vm in 0..self.machines.len() {
            if let Some(instance) = self.machines[vm].instance.as_mut()
                && instance.policy.expire_deadline(now) == vm_contract::StopAction::ForceProcess
            {
                let _ = instance.runtime.as_process_supervisor().request_stop();
            }
            let machine = &mut self.machines[vm];
            if machine.policy.take_restart(machine.instance.is_some()) {
                let _ = self.start_instance(vm);
            }
        }
    }
}
