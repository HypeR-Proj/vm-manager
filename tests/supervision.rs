// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::cell::Cell;

#[test]
fn unavailable_initial_fleet_rejects_admission_but_keeps_queries_responsive() {
    let unavailable = FleetConfiguration::Unavailable;
    assert!(unavailable.check_request(&fleet::Request::List).is_ok());
    for action in [fleet::Action::Start, fleet::Action::Restart] {
        let request = fleet::Request::Control {
            name: "io".into(),
            action,
        };
        assert!(unavailable.check_request(&request).is_err());
        assert!(
            FleetConfiguration::Configured
                .check_request(&request)
                .is_ok()
        );
    }
    let create = fleet::Request::Create {
        definitions: Vec::new(),
    };
    assert!(unavailable.check_request(&create).is_err());
    assert!(
        FleetConfiguration::Configured
            .check_request(&create)
            .is_ok()
    );
    assert!(
        unavailable
            .check_request(&fleet::Request::Control {
                name: "io".into(),
                action: fleet::Action::Status,
            })
            .is_ok()
    );
}

fn running() -> InstancePolicy {
    let mut policy = InstancePolicy::default();
    policy.observe(InstanceStatus::ImageValidated).unwrap();
    policy.observe(InstanceStatus::MemoryPrepared).unwrap();
    policy.observe(InstanceStatus::Installed).unwrap();
    policy.observe(InstanceStatus::Running).unwrap();
    policy
}

fn finish(policy: InstancePolicy, succeeded: bool) -> InstanceOutcome {
    policy.finish(succeeded, &mut None::<()>)
}

#[test]
fn explicit_stop_cancels_guest_reboot_and_pending_admin_restart() {
    let mut machine = MachinePolicy::default();
    machine.request_stop(true);
    let mut instance = running();
    instance.observe(InstanceStatus::RebootRequested).unwrap();
    machine.request_stop(false);
    assert_eq!(
        instance.request_cooperative_stop(),
        StopAction::SendCooperative
    );
    assert_eq!(instance.request_cooperative_stop(), StopAction::None);
    let outcome = finish(instance, true);
    assert!(!outcome.reboot);
    machine.finished(&outcome);
    assert!(!machine.take_restart(false));
}

#[test]
fn restart_waits_for_process_exit_not_terminal_message() {
    let mut machine = MachinePolicy::default();
    machine.request_stop(true);
    let mut instance = running();
    instance.observe(InstanceStatus::Stopped).unwrap();
    assert_eq!(machine.state(Some(&instance)), fleet::State::Stopping);
    assert!(!machine.take_restart(true));
    machine.finished(&finish(instance, true));
    assert!(machine.take_restart(false));
    assert!(!machine.take_restart(false));
}

#[test]
fn cleanup_timeout_preserves_successful_guest_reboot() {
    let now = Instant::now();
    let mut instance = running();
    instance.observe(InstanceStatus::RebootRequested).unwrap();
    instance.arm_exit_deadline(now).unwrap();
    assert_eq!(
        instance.expire_deadline(now + INSTANCE_EXIT_GRACE),
        StopAction::ForceProcess
    );
    assert!(finish(instance, true).reboot);
    let mut instance = running();
    instance.observe(InstanceStatus::RebootRequested).unwrap();
    assert!(!finish(instance, false).reboot);
}

#[test]
fn deadlines_do_not_extend_and_escalate_once() {
    let now = Instant::now();
    let mut instance = running();
    assert_eq!(
        instance.request_cooperative_stop(),
        StopAction::SendCooperative
    );
    instance.arm_exit_deadline(now).unwrap();
    instance
        .arm_exit_deadline(now + Duration::from_secs(1))
        .unwrap();
    assert_eq!(instance.exit_deadline(), Some(now + INSTANCE_EXIT_GRACE));
    assert_eq!(instance.expire_deadline(now), StopAction::None);
    assert_eq!(
        instance.expire_deadline(now + INSTANCE_EXIT_GRACE),
        StopAction::ForceProcess
    );
    assert_eq!(
        instance.expire_deadline(now + INSTANCE_EXIT_GRACE),
        StopAction::None
    );
    assert_eq!(instance.force_stop(), StopAction::None);
}

#[test]
fn protocol_failure_cannot_be_hidden_by_terminal_status() {
    let mut instance = running();
    instance.reject_protocol();
    assert!(instance.observe(InstanceStatus::Stopped).is_err());
    assert_eq!(instance.force_stop(), StopAction::ForceProcess);
    assert!(matches!(
        finish(instance, true).event,
        InstanceEvent::Failed(vm::InstanceFailure::InvalidControlProtocol)
    ));
}

#[test]
fn console_disconnect_only_releases_matching_owner() {
    let mut instance = InstancePolicy::default();
    assert!(!instance.can_attach_console());
    instance.observe(InstanceStatus::ImageValidated).unwrap();
    instance.observe(InstanceStatus::MemoryPrepared).unwrap();
    instance.observe(InstanceStatus::Installed).unwrap();
    instance.observe(InstanceStatus::Running).unwrap();
    assert!(instance.can_attach_console());
    instance.attach_console(3);
    instance.disconnect_client(4);
    assert!(!instance.can_attach_console());
    instance.disconnect_client(3);
    assert!(instance.can_attach_console());
    instance.request_cooperative_stop();
    assert!(!instance.can_attach_console());
}

struct Endpoint<'a>(&'a Cell<usize>);
impl Drop for Endpoint<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

#[test]
fn broker_backpressure_retains_endpoint_but_exit_releases_it() {
    let closed = Cell::new(0);
    let mut pending = Some(Endpoint(&closed));
    let mut instance = InstancePolicy::default();
    assert!(!instance.wants_io_admission(pending.is_some()));
    instance.observe(InstanceStatus::ImageValidated).unwrap();
    instance.observe(InstanceStatus::MemoryPrepared).unwrap();
    instance.observe(InstanceStatus::Installed).unwrap();
    assert!(instance.wants_io_admission(pending.is_some()));
    complete_admission(
        &mut pending,
        Err(hyper_os::Error::Status(hyper_os::Status::WOULD_BLOCK)),
    );
    assert_eq!(closed.get(), 0);
    assert!(instance.wants_io_admission(pending.is_some()));
    let outcome = instance.finish(false, &mut pending);
    assert_eq!(closed.get(), 1);
    assert!(pending.is_none());
    assert!(!outcome.reboot);
    assert!(matches!(outcome.event, InstanceEvent::Failed(_)));
}

#[test]
fn broker_completion_and_failure_release_pending_record() {
    for result in [
        Ok(()),
        Err(hyper_os::Error::Status(hyper_os::Status::PEER_CLOSED)),
    ] {
        let closed = Cell::new(0);
        let mut pending = Some(Endpoint(&closed));
        complete_admission(&mut pending, result);
        assert!(pending.is_none());
        assert_eq!(closed.get(), 1);
    }
}

#[test]
fn stopped_instance_cannot_be_admitted_to_io_broker() {
    let mut instance = InstancePolicy::default();
    instance.observe(InstanceStatus::ImageValidated).unwrap();
    instance.observe(InstanceStatus::MemoryPrepared).unwrap();
    instance.observe(InstanceStatus::Installed).unwrap();
    instance.request_cooperative_stop();
    assert!(!instance.wants_io_admission(true));
}

#[test]
fn failed_restart_is_consumed_and_visible_until_next_success() {
    let mut machine = MachinePolicy::default();
    machine.request_stop(true);
    assert!(machine.take_restart(false));
    machine.start_failed();
    assert!(!machine.take_restart(false));
    assert_eq!(machine.state(None), fleet::State::Failed);
    machine.started();
    assert_eq!(machine.state(None), fleet::State::Stopped);
}

#[test]
fn stop_send_failure_forces_cleanup_without_stranding_deadline() {
    for error in [
        hyper_os::Error::MissingHandle,
        hyper_os::Error::Status(hyper_os::Status::WOULD_BLOCK),
        hyper_os::Error::Status(hyper_os::Status::PEER_CLOSED),
        hyper_os::Error::InvalidResponse,
    ] {
        let mut instance = running();
        assert_eq!(
            instance.request_cooperative_stop(),
            StopAction::SendCooperative
        );
        assert_eq!(
            instance.cooperative_stop_sent(Err(error), Instant::now()),
            StopAction::ForceProcess
        );
        assert_eq!(instance.exit_deadline(), None);
        assert_eq!(instance.force_stop(), StopAction::None);
    }
    let now = Instant::now();
    let mut instance = running();
    instance.request_cooperative_stop();
    assert_eq!(
        instance.cooperative_stop_sent(Ok(()), now),
        StopAction::None
    );
    assert_eq!(instance.exit_deadline(), Some(now + INSTANCE_EXIT_GRACE));
}

#[test]
fn memory_observations_neither_advance_nor_poison_lifecycle() {
    let mut instance = InstancePolicy::default();
    let observation = hyper_service::vm::Observation {
        request: hyper_service::vm::ObservationRequest(91),
        vcpus: 4,
        capacity_bytes: 128 * 1024 * 1024,
        resident_bytes: Some(2 * 1024 * 1024),
    };
    assert_eq!(
        instance.observe_message(&observation.encode()).unwrap(),
        Some(observation)
    );
    instance.observe(InstanceStatus::ImageValidated).unwrap();
    instance.observe(InstanceStatus::MemoryPrepared).unwrap();
    instance.observe(InstanceStatus::Installed).unwrap();
    instance.observe(InstanceStatus::Running).unwrap();
    instance.observe(InstanceStatus::Stopped).unwrap();
    // A timed-out request may finish after a terminal status; it is harmless.
    assert_eq!(
        instance.observe_message(&observation.encode()).unwrap(),
        Some(observation)
    );
    assert!(instance.is_terminal());
}

#[test]
fn affinity_requires_a_running_managed_instance() {
    assert!(affinity_allowed(Some(fleet::State::Running)).is_ok());
    for state in [
        None,
        Some(fleet::State::Starting),
        Some(fleet::State::Stopping),
        Some(fleet::State::Stopped),
        Some(fleet::State::Failed),
        Some(fleet::State::Unavailable),
    ] {
        assert!(affinity_allowed(state).is_err());
    }
}

#[test]
fn late_affinity_reply_does_not_corrupt_lifecycle() {
    let mut policy = running();
    let reply = vm::VcpuControlReply {
        sequence: 9,
        vcpu: 0,
        host_cpu: Some(0),
        pending_host_cpu: Some(1),
        status: hyper_os::Status::OK,
    };
    assert!(policy.observe_message(&reply.encode()).unwrap().is_none());
    assert_eq!(policy.state(), fleet::State::Running);
    policy.observe(InstanceStatus::Stopped).unwrap();
    assert!(policy.observe_message(&reply.encode()).unwrap().is_none());
    assert!(policy.is_terminal());
}

#[test]
fn stale_readiness_at_reboot_does_not_poison_terminal_status() {
    let mut instance = running();
    assert_eq!(
        instance.observe_receive(Err(hyper_os::Error::Status(hyper_os::Status::WOULD_BLOCK))),
        Ok(RuntimeControlState::Open)
    );
    // READABLE can be observed again while its previous publisher catches up.
    // The actual queue has already delivered the reboot record and closed.
    assert_eq!(
        instance.observe_receive(Ok(&InstanceStatus::RebootRequested.encode())),
        Ok(RuntimeControlState::Open)
    );
    assert_eq!(
        instance.observe_receive(Err(hyper_os::Error::Status(hyper_os::Status::PEER_CLOSED))),
        Ok(RuntimeControlState::Closed)
    );
    assert!(finish(instance, true).reboot);
}

#[test]
fn control_eof_without_terminal_record_is_still_an_error() {
    let mut instance = running();
    assert!(
        instance
            .observe_receive(Err(hyper_os::Error::Status(hyper_os::Status::PEER_CLOSED)))
            .is_err()
    );
    assert!(instance.observe_receive(Ok(b"malformed")).is_err());
}
