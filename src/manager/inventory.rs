// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Named VM lookup and observations through the capabilities held by this manager.

use super::FleetManager;
use hyper_os::capability_channel::{CapabilityChannel, CapabilityDisposition};
use hyper_os::handle::RightsOffer;
use hyper_os::vm::VirtualMachinePhase as Phase;
use hyper_service::io;
use hyper_vm_policy::fleet;
use std::mem::MaybeUninit;
use std::time::Duration;

// Identity discovery and start admission need the same broker response budget.
// Unlike best-effort status snapshots, they may wait behind mapping retirement.
const IDENTITY_QUERY_TIMEOUT: Duration = Duration::from_secs(io::OBSERVATION_TIMEOUT_SECONDS);

/// A definition owned by this manager grants lifecycle management; a broker
/// observation grants only a snapshot. Names identify entries, never authority.
pub(super) enum VmTarget<'a> {
    Managed(usize),
    Observed(&'a ObservedVm),
}

/// Owns the broker endpoint and the identity discovered from its VM owner.
/// It has no VM handle or runtime control channel.
pub(super) struct ObservedVm {
    name: String,
    image: String,
    broker: CapabilityChannel,
}

impl FleetManager {
    pub(super) fn observed_vm(&self) -> Option<&ObservedVm> {
        self.io_service.as_ref()
    }

    pub(super) fn check_observed_names<'a>(
        &self,
        names: impl Iterator<Item = &'a str>,
    ) -> Result<(), String> {
        match self.io_service.as_ref() {
            Some(service) => service.check_names(names),
            None => Ok(()),
        }
    }

    pub(super) fn find_vm(&self, name: &str) -> Result<VmTarget<'_>, String> {
        if let Some(vm) = self
            .machines
            .iter()
            .position(|vm| vm.definition.name == name)
        {
            return Ok(VmTarget::Managed(vm));
        }
        if let Some(observed) = self.observed_vm().filter(|vm| vm.name == name) {
            return Ok(VmTarget::Observed(observed));
        }
        Err(format!("VM '{name}' does not exist; use 'vmm list'"))
    }

    pub(super) fn summary(&mut self, vm: usize, deadline: u64) -> fleet::Summary {
        let observation = self.machines[vm]
            .instance
            .as_mut()
            .and_then(|instance| instance.observe_memory(deadline));
        let mut placement = Vec::new();
        if let Some(observation) = observation {
            for vcpu in 0..observation.vcpus {
                let Some(instance) = self.machines[vm].instance.as_mut() else {
                    break;
                };
                let Ok(reply) = instance.control_vcpu(vcpu, None, deadline) else {
                    break;
                };
                if reply.status == hyper_os::Status::OK {
                    placement.push(fleet::VcpuPlacement {
                        vcpu,
                        host_cpu: reply.host_cpu,
                        pending_host_cpu: reply.pending_host_cpu,
                    });
                }
            }
        }
        let definition = &self.machines[vm].definition;
        let state = self.fleet_state(vm);
        fleet::Summary {
            guest_metrics: None,
            placement,
            read_only: false,
            vcpus: observation.map(|value| value.vcpus),
            memory_bytes: observation.map(|value| value.capacity_bytes),
            resident_memory_bytes: observation.and_then(|value| value.resident_bytes),
            name: definition.name.clone(),
            image: definition.image.clone(),
            autostart: definition.autostart,
            disk: definition.disk.clone(),
            network: definition.network.clone(),
            state,
        }
    }

    pub(super) fn trace_threads(&mut self, deadline: u64) -> Vec<hyper_service::vm::trace::Thread> {
        let mut threads = Vec::new();
        for machine in &mut self.machines {
            let Some(instance) = machine.instance.as_mut() else {
                continue;
            };
            for vcpu in 0..machine.definition.configuration.vcpus {
                if threads.len() == hyper_service::vm::trace::MAX_THREADS {
                    return threads;
                }
                let Ok(reply) = instance.control_vcpu(vcpu, None, deadline) else {
                    break;
                };
                if reply.status == hyper_os::Status::OK
                    && let Some(thread) = reply.scheduler_thread_id
                {
                    threads.push(hyper_service::vm::trace::Thread {
                        thread,
                        vcpu,
                        name: machine.definition.name.clone(),
                    });
                }
            }
        }
        if threads.len() < hyper_service::vm::trace::MAX_THREADS
            && let Some(vm) = self.observed_vm()
        {
            let _ = read_observation(&vm.broker, deadline, |info| {
                if info.name == vm.name
                    && info.image == vm.image
                    && let Some(thread) = info.boot_thread_id
                {
                    threads.push(hyper_service::vm::trace::Thread {
                        thread,
                        vcpu: 0,
                        name: info.name.into(),
                    });
                }
            });
        }
        threads
    }

    fn fleet_state(&self, vm: usize) -> fleet::State {
        let machine = &self.machines[vm];
        machine
            .policy
            .state(machine.instance.as_ref().map(|instance| &instance.policy))
    }
}

impl ObservedVm {
    pub(super) fn connect(broker: CapabilityChannel) -> hyper_os::Result<Self> {
        let deadline = hyper_os::time::deadline_after(IDENTITY_QUERY_TIMEOUT)?.as_raw();
        let (name, image) = read_observation(&broker, deadline, |info| {
            (info.name.to_owned(), info.image.to_owned())
        })?;
        Ok(Self {
            name,
            image,
            broker,
        })
    }

    pub(super) fn broker(&self) -> &CapabilityChannel {
        &self.broker
    }

    fn check_names<'a>(&self, mut names: impl Iterator<Item = &'a str>) -> Result<(), String> {
        let check = (|| {
            let deadline = hyper_os::time::deadline_after(IDENTITY_QUERY_TIMEOUT)?.as_raw();
            read_observation(&self.broker, deadline, |info| {
                if info.name != self.name || info.image != self.image {
                    return Err("observed VM identity changed on the broker endpoint".into());
                }
                match names.find(|name| *name == info.name) {
                    Some(name) => Err(format!(
                        "VM '{name}' already exists (observed through broker)"
                    )),
                    None => Ok(()),
                }
            })
        })();
        check.map_err(|error| format!("cannot check observed VM names: {error}"))?
    }

    pub(super) fn summary(&self, deadline: u64) -> fleet::Summary {
        let mut summary = fleet::Summary {
            guest_metrics: Some(io::metrics::GuestMetrics::default()),
            name: self.name.clone(),
            image: self.image.clone(),
            autostart: true,
            disk: None,
            network: None,
            read_only: true,
            placement: Vec::new(),
            vcpus: None,
            memory_bytes: None,
            resident_memory_bytes: None,
            state: fleet::State::Unavailable,
        };
        // Keep the discovered identity visible if observations fail. A failed
        // query must not make its name available for a managed definition.
        let _ = read_observation(&self.broker, deadline, |info| {
            if info.name != self.name || info.image != self.image {
                return;
            }
            summary.vcpus = Some(info.vcpus);
            summary.memory_bytes = Some(info.ram_bytes);
            summary.resident_memory_bytes = info.resident_bytes;
            summary.guest_metrics = Some(info.metrics);
            if info.vcpus != 0 {
                summary.placement.push(fleet::VcpuPlacement {
                    vcpu: 0,
                    host_cpu: info.boot_host_cpu,
                    pending_host_cpu: None,
                });
            }
            summary.state = match info.phase {
                Phase::Installed => fleet::State::Starting,
                Phase::Running => fleet::State::Running,
                Phase::Stopping => fleet::State::Stopping,
                Phase::Stopped => fleet::State::Stopped,
            };
        });
        summary
    }
}

// The decoder borrows the receive buffer; consume the snapshot before it expires.
fn read_observation<T>(
    broker: &CapabilityChannel,
    deadline: u64,
    read: impl FnOnce(io::Observation<'_>) -> T,
) -> hyper_os::Result<T> {
    let (local, remote) = CapabilityChannel::create()?;
    let mut remote = Some(remote.into_handle());
    if hyper_os::time::monotonic_now()?.as_nanoseconds() >= deadline {
        return Err(hyper_os::Error::Status(hyper_os::Status::TIMED_OUT));
    }
    // WRITE authorizes broker requests, not VM control. Replies carry data only.
    io::send_capabilities(
        broker,
        io::OBSERVE_MESSAGE,
        &mut [CapabilityDisposition::move_handle(
            &mut remote,
            RightsOffer::Exact(io::SESSION_RIGHTS),
        )?],
        deadline,
    )?;
    let mut bytes = [MaybeUninit::uninit(); io::OBSERVATION_BYTES];
    let reply = local.receive(deadline, &mut bytes, &mut [])?;
    if reply.capability_count() != 0 {
        return Err(hyper_os::Error::InvalidResponse);
    }
    let observation =
        io::decode_observation(reply.bytes()).ok_or(hyper_os::Error::InvalidResponse)?;
    Ok(read(observation))
}
