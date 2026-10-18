// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Fleet ownership, startup provisioning, and fair event dispatch.

mod commands;
mod console;
mod instance;
mod inventory;
mod launch;
mod listener;
mod provision;

use self::instance::VmInstance;
use hyper_os::capability_channel::CapabilityChannel;
use hyper_os::fs::{Directory, File};
use hyper_os::handle::{
    ByteChannelObject, CapabilityChannelObject, OwnedHandle, ProcessObject, ResourceDomainObject,
};
use hyper_os::startup::{self, Startup};
use hyper_os::wait::{ObjectSignals, WaitItem, wait_many};
use hyper_service::process as process_contract;
use hyper_service::vm as vm_contract;
use hyper_vm_manager::{FleetConfiguration, MachinePolicy};
use hyper_vm_policy::fleet;
use std::time::Instant;

const MAX_CLIENTS: usize = 8;

/// Long-lived fleet authority separated from disposable VM instances.
pub(super) struct FleetManager {
    runtime_image: File,
    loader: File,
    libraries: Directory,
    factory: OwnedHandle<hyper_os::handle::TaskFactoryObject>,
    fleet_domain: OwnedHandle<ResourceDomainObject>,
    authority: OwnedHandle<hyper_os::handle::VirtualMachineCreationAuthorityObject>,
    io_service: Option<inventory::ObservedVm>,
    configuration: FleetConfiguration,
    connections: listener::Listener,
    component_connector: Option<OwnedHandle<CapabilityChannelObject>>,
    root: Directory,
    machines: Vec<Machine>,
    clients: [Option<Client>; MAX_CLIENTS],
    next_wait: usize,
}

impl FleetManager {
    pub(super) fn from_startup(startup: &mut Startup<'_>) -> hyper_os::Result<Self> {
        let provisioning = CapabilityChannel::from_handle(startup.take(vm_contract::PROVISIONING)?);
        let io_broker = startup
            .take_optional(hyper_service::io::BROKER_CLIENT)?
            .map(CapabilityChannel::from_handle);
        let mut manager = Self {
            runtime_image: File::from_handle(startup.take(vm_contract::RUNTIME_IMAGE)?),
            loader: File::from_handle(startup.take(process_contract::CHILD_LOADER)?),
            libraries: Directory::from_handle(
                startup.take(process_contract::CHILD_LIBRARY_DIRECTORY)?,
            ),
            factory: startup.take(startup::TASK_FACTORY)?,
            fleet_domain: startup.take(startup::RESOURCE_DOMAIN)?,
            authority: startup.take(startup::VIRTUAL_MACHINE_CREATION_AUTHORITY)?,
            io_service: None,
            configuration: FleetConfiguration::Unavailable,
            connections: listener::Listener::start(CapabilityChannel::from_handle(
                startup.take(vm_contract::MANAGER_CONNECTION)?,
            ))?,
            // Retain the component route for failures after startup. Keep the
            // original grant available for the entry point's readiness report.
            component_connector: startup
                .borrow(hyper_component::CONNECTOR)
                .ok()
                .map(|connector| connector.duplicate(hyper_service_ipc::CLIENT_RIGHTS))
                .transpose()?,
            root: Directory::from_handle(startup.take(startup::ROOT_DIRECTORY)?),
            machines: Vec::new(),
            clients: std::array::from_fn(|_| None),
            next_wait: 0,
        };
        manager.configure_fleet(provisioning, io_broker)?;
        Ok(manager)
    }

    pub(super) fn run(&mut self) -> hyper_os::Result<()> {
        eprintln!("HypeR vm-manager: ready");
        for vm in 0..self.machines.len() {
            if self.machines[vm].definition.autostart {
                // Failure is recorded on this definition; other guests still start.
                let _ = self.start_instance(vm);
            }
        }
        loop {
            self.observe_one_event()?;
            self.complete_restarts();
        }
    }

    fn observe_one_event(&mut self) -> hyper_os::Result<()> {
        const WAIT_CAPACITY: usize = 2 + MAX_CLIENTS + 2 * fleet::MAX_DEFINITIONS;
        let mut waits = Vec::with_capacity(WAIT_CAPACITY);
        let mut sources = Vec::with_capacity(WAIT_CAPACITY);
        waits.push(self.connections.wait_item());
        sources.push(WaitSource::Connection);
        if self.machines.iter().any(|machine| {
            machine
                .instance
                .as_ref()
                .is_some_and(VmInstance::wants_io_admission)
        }) && let Some(broker) = self.io_service.as_ref()
        {
            waits.push(WaitItem::new(
                broker.broker().as_handle_ref(),
                ObjectSignals::<CapabilityChannelObject>::PEER_RECEIVING
                    .union(ObjectSignals::<CapabilityChannelObject>::PEER_CLOSED),
            ));
            sources.push(WaitSource::IoAdmission);
        }
        for (vm, machine) in self.machines.iter().enumerate() {
            if let Some(instance) = machine.instance.as_ref() {
                waits.push(WaitItem::new(
                    instance.runtime.as_handle_ref(),
                    ObjectSignals::<ProcessObject>::TERMINATED,
                ));
                sources.push(WaitSource::RuntimeProcess(vm));
                if let Some(control) = instance.runtime_control.as_ref() {
                    waits.push(WaitItem::new(
                        control.as_handle_ref(),
                        ObjectSignals::<ByteChannelObject>::READABLE
                            .union(ObjectSignals::<ByteChannelObject>::PEER_CLOSED),
                    ));
                    sources.push(WaitSource::RuntimeControl(vm));
                }
            }
        }
        for (index, client) in self.clients.iter().enumerate() {
            let Some(client) = client else {
                continue;
            };
            waits.push(WaitItem::new(
                client.control.as_handle_ref(),
                ObjectSignals::<ByteChannelObject>::READABLE
                    .union(ObjectSignals::<ByteChannelObject>::PEER_CLOSED),
            ));
            sources.push(WaitSource::Client(index));
        }
        let deadline = match self
            .machines
            .iter()
            .filter_map(|machine| {
                machine
                    .instance
                    .as_ref()
                    .and_then(|instance| instance.policy.exit_deadline())
            })
            .min()
        {
            Some(deadline) => {
                hyper_os::time::deadline_after(deadline.saturating_duration_since(Instant::now()))?
                    .as_raw()
            }
            None => hyper_os::DEADLINE_INFINITE,
        };
        // Ready connection traffic must not starve lifecycle/control events.
        let first = self.next_wait % waits.len();
        waits.rotate_left(first);
        sources.rotate_left(first);
        let observation = match wait_many(&waits, deadline) {
            Ok(observation) => observation,
            Err(hyper_os::Error::Status(hyper_os::Status::TIMED_OUT)) => {
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        self.next_wait = (first + observation.index + 1) % sources.len();
        match sources[observation.index] {
            WaitSource::Connection => self.accept_client(),
            WaitSource::IoAdmission => self.admit_io(),
            WaitSource::RuntimeProcess(vm) => self.finish_instance(vm),
            WaitSource::RuntimeControl(vm) => self.handle_runtime_control(vm),
            WaitSource::Client(index) => self.handle_client(index, observation.observed),
        }
    }

    fn install_definitions(&mut self, definitions: Vec<fleet::Definition>) -> Result<(), String> {
        fleet::validate_definitions(&definitions)?;
        if self.machines.len() + definitions.len() > fleet::MAX_DEFINITIONS {
            return Err(format!(
                "at most {} VM definitions are supported",
                fleet::MAX_DEFINITIONS
            ));
        }
        self.check_observed_names(
            definitions
                .iter()
                .map(|definition| definition.name.as_str()),
        )?;
        let mut prepared = Vec::new();
        for definition in definitions {
            if self
                .machines
                .iter()
                .any(|machine| machine.definition.name == definition.name)
            {
                return Err(format!("VM '{}' already exists", definition.name));
            }
            for machine in &self.machines {
                definition.check_conflicts(&machine.definition)?;
            }
            let rights = hyper_os::fs::FileRights::from_rights(vm_contract::MANAGED_IMAGE_RIGHTS)
                .ok_or("invalid image rights")?;
            let image = self
                .root
                .open(&definition.image, rights)
                .map_err(|error| format!("cannot open image '{}': {error}", definition.image))?;
            hyper_vm_policy::image::validate_file(&image, &definition.configuration)
                .map_err(|error| format!("cannot load image '{}': {error}", definition.image))?;
            prepared.push(Machine {
                definition,
                image: image.into_handle(),
                instance: None,
                policy: MachinePolicy::default(),
            });
        }
        // No definition becomes visible until the whole batch is validated.
        self.machines.extend(prepared);
        Ok(())
    }
}
struct Machine {
    definition: fleet::Definition,
    image: OwnedHandle<hyper_os::handle::FileObject>,
    instance: Option<VmInstance>,
    policy: MachinePolicy,
}

struct Client {
    control: OwnedHandle<ByteChannelObject>,
    capabilities: CapabilityChannel,
}

#[derive(Clone, Copy)]
enum WaitSource {
    Connection,
    IoAdmission,
    RuntimeProcess(usize),
    RuntimeControl(usize),
    Client(usize),
}
