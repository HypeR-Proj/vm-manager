// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Prepare child authorities before publishing a started VM runtime.

use super::{
    FleetManager,
    instance::{IoAdmission, VmInstance},
};
use hyper_os::capability_channel::CapabilityChannel;
use hyper_os::channel;
use hyper_os::handle::{Rights, RightsOffer};
use hyper_os::startup;
use hyper_os::task::{ProcessBuilder, create_resource_domain, create_task_group};
use hyper_service::stdio as stdio_contract;
use hyper_service::vm as vm_contract;
use hyper_vm_manager::InstancePolicy;

const RUNTIME_ARGUMENT: &str = "/svc/vm-runtime";

impl FleetManager {
    pub(super) fn start_instance(&mut self, vm: usize) -> Result<(), String> {
        self.configuration
            .check_admission()
            .map_err(str::to_owned)?;
        if self.machines[vm].instance.is_some() {
            return Err("VM is already active".into());
        }
        let result = self
            .check_observed_names(std::iter::once(self.machines[vm].definition.name.as_str()))
            .and_then(|()| self.launch_instance(vm).map_err(|error| error.to_string()));
        if let Err(error) = &result {
            let machine = &mut self.machines[vm];
            machine.policy.start_failed();
            eprintln!(
                "HypeR vm-manager: cannot start VM '{}': {error}",
                machine.definition.name
            );
        }
        result
    }

    /// Starts a per-VM runtime with its resource domain and delegated capabilities.
    ///
    /// Publish the instance only after the process builder starts successfully.
    /// Keep the manager's I/O session endpoint until the runtime reports
    /// installation; process creation alone is too early for broker admission.
    fn launch_instance(&mut self, vm: usize) -> hyper_os::Result<()> {
        let definition = &self.machines[vm];
        let connection = definition
            .definition
            .io_connection()
            .map_err(|_| hyper_os::Error::InvalidResponse)?;
        let domain = create_resource_domain(
            self.fleet_domain.as_handle_ref(),
            hyper_vm_policy::VM_INSTANCE_LIMITS,
        )?;
        let group = create_task_group(self.factory.as_handle_ref(), domain.as_handle_ref())?;
        let lease = hyper_os::vm::derive_creation_lease(
            self.authority.as_handle_ref(),
            domain.as_handle_ref(),
        )?;
        let (console_connection, runtime_connection) = CapabilityChannel::create()?;
        let (manager_runtime, runtime_control) = channel::create_pair()?;
        let builder = ProcessBuilder::create(
            self.factory.as_handle_ref(),
            group.as_handle_ref(),
            domain.as_handle_ref(),
            self.runtime_image.as_handle_ref(),
            self.loader.as_handle_ref(),
        )?;
        builder.set_name("vm-runtime")?;
        builder.add_argument(RUNTIME_ARGUMENT)?;
        if let Some(connection) = connection {
            builder.add_argument(&format!("--io-devices={}", connection.devices()))?;
        }
        for argument in definition
            .definition
            .configuration
            .runtime_arguments()
            .map_err(|_| hyper_os::Error::InvalidResponse)?
        {
            builder.add_argument(&argument)?;
        }
        for (output, contract) in [
            (
                hyper_rt::process::stdout()?,
                stdio_contract::STANDARD_OUTPUT_CONTRACT,
            ),
            (
                hyper_rt::process::stderr()?,
                stdio_contract::STANDARD_ERROR_CONTRACT,
            ),
        ] {
            builder.add_handle_duplicate(
                output.as_handle_ref(),
                contract.purpose(),
                RightsOffer::Exact(contract.required_rights()),
            )?;
        }
        builder.add_handle_duplicate(
            self.libraries.as_handle_ref(),
            startup::DYNAMIC_LIBRARY_DIRECTORY.as_raw(),
            RightsOffer::Exact(Rights::READ.union(Rights::EXECUTE).union(Rights::PROTECT)),
        )?;
        builder.add_handle_duplicate(
            definition.image.as_handle_ref(),
            vm_contract::RUNTIME_IMAGE_CONTRACT.purpose(),
            RightsOffer::Exact(vm_contract::RUNTIME_IMAGE_CONTRACT.required_rights()),
        )?;
        builder
            .add_handle_move(
                lease,
                vm_contract::RUNTIME_CREATION_LEASE_CONTRACT.purpose(),
                RightsOffer::Exact(vm_contract::RUNTIME_CREATION_LEASE_CONTRACT.required_rights()),
            )
            .map_err(|failure| failure.error())?;
        builder
            .add_handle_move(
                runtime_control,
                vm_contract::RUNTIME_INSTANCE_CONTROL_CONTRACT.purpose(),
                RightsOffer::Exact(
                    vm_contract::RUNTIME_INSTANCE_CONTROL_CONTRACT.required_rights(),
                ),
            )
            .map_err(|failure| failure.error())?;
        builder
            .add_handle_move(
                runtime_connection.into_handle(),
                vm_contract::RUNTIME_CONSOLE_CONNECTION_CONTRACT.purpose(),
                RightsOffer::Exact(
                    vm_contract::RUNTIME_CONSOLE_CONNECTION_CONTRACT.required_rights(),
                ),
            )
            .map_err(|failure| failure.error())?;
        let io_admission = if let Some(connection) = connection {
            self.io_service
                .as_ref()
                .ok_or(hyper_os::Error::MissingHandle)?;
            let (owner, runtime) = CapabilityChannel::create()?;
            builder
                .add_handle_move(
                    runtime.into_handle(),
                    hyper_service::io::SESSION.as_raw(),
                    RightsOffer::Exact(hyper_service::io::SESSION_RIGHTS),
                )
                .map_err(|failure| failure.error())?;
            let record = connection
                .encode()
                .ok_or(hyper_os::Error::InvalidResponse)?;
            Some(IoAdmission {
                endpoint: Some(owner.into_handle()),
                record,
            })
        } else {
            None
        };
        let retirement = builder.retain_on_exit(8 * 1024 * 1024, 10_000_000_000)?;
        builder.seal()?;
        let runtime = builder.start().map_err(|failure| failure.error())?;
        self.machines[vm].instance = Some(VmInstance {
            _resource_domain: domain,
            _task_group: group,
            runtime,
            retirement,
            runtime_control: Some(manager_runtime),
            console_connection,
            policy: InstancePolicy::default(),
            observation_sequence: 0,
            io_admission,
        });
        self.machines[vm].policy.started();
        Ok(())
    }
}
