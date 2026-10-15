// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Fleet decisions shared by the supervisor and deterministic host tests.
//!
//! Native handles and their effects remain in the executable. Protocol order and
//! stop escalation remain owned by `hyper_service::vm`; this layer composes
//! them with fleet restart intent, console ownership and monotonic deadlines.

#[cfg(target_os = "hyper")]
extern crate hyper_vm_policy_shared as hyper_vm_policy;

use hyper_service::vm::{self, InstanceEvent, InstanceStatus, StopAction};
use hyper_vm_policy::fleet;
use std::time::{Duration, Instant};

const INSTANCE_EXIT_GRACE: Duration = Duration::from_secs(2);

/// Initial configuration failure does not terminate the Native management service.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum FleetConfiguration {
    Configured,
    #[default]
    Unavailable,
}

impl FleetConfiguration {
    pub fn check_admission(self) -> Result<(), &'static str> {
        match self {
            Self::Configured => Ok(()),
            Self::Unavailable => Err("VM fleet unavailable: initial configuration is unavailable"),
        }
    }

    pub fn check_request(self, request: &fleet::Request) -> Result<(), &'static str> {
        match request {
            fleet::Request::Create { .. }
            | fleet::Request::Control {
                action: fleet::Action::Start | fleet::Action::Restart,
                ..
            } => self.check_admission(),
            _ => Ok(()),
        }
    }
}

#[derive(Default)]
pub struct MachinePolicy {
    restart_pending: bool,
    failed: bool,
}

impl MachinePolicy {
    pub fn request_stop(&mut self, restart: bool) {
        self.restart_pending = restart;
    }

    /// Call only after the old instance's process termination was observed.
    pub fn finished(&mut self, outcome: &InstanceOutcome) {
        self.restart_pending |= outcome.reboot;
        self.failed = matches!(outcome.event, InstanceEvent::Failed(_));
    }

    pub fn started(&mut self) {
        self.failed = false;
    }

    pub fn start_failed(&mut self) {
        self.failed = true;
    }

    /// Consumes restart intent only after runtime ownership has been retired.
    pub fn take_restart(&mut self, instance_present: bool) -> bool {
        if instance_present {
            return false;
        }
        std::mem::take(&mut self.restart_pending)
    }

    pub fn state(&self, instance: Option<&InstancePolicy>) -> fleet::State {
        instance.map_or(
            if self.failed {
                fleet::State::Failed
            } else {
                fleet::State::Stopped
            },
            InstancePolicy::state,
        )
    }
}

/// A managed instance must be running before its placement can change.
pub fn affinity_allowed(state: Option<fleet::State>) -> Result<(), &'static str> {
    if state != Some(fleet::State::Running) {
        return Err("VM must be running");
    }
    Ok(())
}

pub struct InstanceOutcome {
    pub event: InstanceEvent,
    pub reboot: bool,
}

/// Actual receive progress; readiness signals are only advisory snapshots.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeControlState {
    Open,
    Closed,
}

#[derive(Default)]
pub struct InstancePolicy {
    tracker: vm::InstanceTracker,
    stop: vm::InstanceStopState,
    exit_deadline: Option<Instant>,
    console_client: Option<usize>,
}

impl InstancePolicy {
    pub fn observe(&mut self, status: InstanceStatus) -> Result<(), vm::InvalidStatusTransition> {
        self.tracker.observe(status)
    }

    /// Observation responses do not advance the monotonic lifecycle protocol.
    /// Late responses remain valid after an inspection timeout.
    pub fn observe_message(&mut self, message: &[u8]) -> hyper_os::Result<Option<vm::Observation>> {
        if let Some(observation) = vm::Observation::decode(message) {
            return Ok(Some(observation));
        }
        if vm::VcpuControlReply::decode(message).is_some() {
            return Ok(None);
        }
        let status = vm::InstanceStatus::decode(message).ok_or(hyper_os::Error::InvalidResponse)?;
        self.observe(status)
            .map_err(|_| hyper_os::Error::InvalidResponse)?;
        Ok(None)
    }

    /// Drain queued records before accepting EOF. A stale `READABLE` snapshot
    /// may lead to `WOULD_BLOCK` or `PEER_CLOSED` after another receive completed.
    pub fn observe_receive(
        &mut self,
        received: hyper_os::Result<&[u8]>,
    ) -> hyper_os::Result<RuntimeControlState> {
        match received {
            Ok(message) => {
                self.observe_message(message)?;
                Ok(RuntimeControlState::Open)
            }
            Err(hyper_os::Error::Status(hyper_os::Status::WOULD_BLOCK)) => {
                Ok(RuntimeControlState::Open)
            }
            Err(hyper_os::Error::Status(hyper_os::Status::PEER_CLOSED)) if self.is_terminal() => {
                Ok(RuntimeControlState::Closed)
            }
            Err(error) => Err(error),
        }
    }

    pub fn reject_protocol(&mut self) {
        self.tracker.reject_protocol();
    }

    pub fn is_terminal(&self) -> bool {
        self.tracker.is_terminal()
    }

    pub fn state(&self) -> fleet::State {
        if self.stop != vm::InstanceStopState::new() || self.tracker.is_terminal() {
            fleet::State::Stopping
        } else if self.tracker.last_status() == Some(InstanceStatus::Running) {
            fleet::State::Running
        } else {
            fleet::State::Starting
        }
    }

    pub fn wants_io_admission(&self, pending: bool) -> bool {
        pending
            && self.stop == vm::InstanceStopState::new()
            && self.tracker.last_status() == Some(InstanceStatus::Installed)
            && !self.tracker.is_terminal()
    }

    pub fn can_attach_console(&self) -> bool {
        self.console_client.is_none() && self.state() == fleet::State::Running
    }

    /// Record ownership after the runtime accepted its console endpoint.
    pub fn attach_console(&mut self, client: usize) {
        self.console_client = Some(client);
    }

    pub fn disconnect_client(&mut self, client: usize) {
        if self.console_client == Some(client) {
            self.console_client = None;
        }
    }

    pub fn request_cooperative_stop(&mut self) -> StopAction {
        self.stop.request_cooperative()
    }

    /// A stop is already committed before the send. Any failed send must
    /// escalate instead of leaving an instance with no progress deadline.
    pub fn cooperative_stop_sent(
        &mut self,
        result: hyper_os::Result<()>,
        now: Instant,
    ) -> StopAction {
        if result.is_err() || self.arm_exit_deadline(now).is_none() {
            self.force_stop()
        } else {
            StopAction::None
        }
    }

    pub fn force_stop(&mut self) -> StopAction {
        self.exit_deadline = None;
        self.stop.request_forced()
    }

    pub fn arm_exit_deadline(&mut self, now: Instant) -> Option<()> {
        if self.exit_deadline.is_none() {
            self.exit_deadline = Some(now.checked_add(INSTANCE_EXIT_GRACE)?);
        }
        Some(())
    }

    pub fn exit_deadline(&self) -> Option<Instant> {
        self.exit_deadline
    }

    pub fn expire_deadline(&mut self, now: Instant) -> StopAction {
        if self.exit_deadline.is_some_and(|deadline| deadline <= now) {
            self.exit_deadline = None;
            self.stop.grace_period_expired()
        } else {
            StopAction::None
        }
    }

    /// Terminal protocol status alone is insufficient: the caller must first
    /// observe process termination and drain queued status messages.
    pub fn finish<T>(
        self,
        process_succeeded: bool,
        pending_admission: &mut Option<T>,
    ) -> InstanceOutcome {
        drop(pending_admission.take());
        InstanceOutcome {
            event: self.tracker.finish(process_succeeded),
            reboot: self.tracker.should_restart(process_succeeded, self.stop),
        }
    }
}

/// Failed admission releases the local endpoint to wake the runtime handshake;
/// backpressure retains it for the next broker readiness event. A successful
/// transfer has already consumed the handle before this drops its record.
pub fn complete_admission<T>(pending: &mut Option<T>, result: hyper_os::Result<()>) {
    if !matches!(
        result,
        Err(hyper_os::Error::Status(hyper_os::Status::WOULD_BLOCK))
    ) {
        drop(pending.take());
    }
}

#[cfg(test)]
#[path = "../tests/supervision.rs"]
mod tests;
