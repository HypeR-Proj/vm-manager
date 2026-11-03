<!--
SPDX-FileCopyrightText: 2026 roolrz
SPDX-License-Identifier: Apache-2.0
-->

# vm-manager

Native VM supervision and control service. Its main thread owns VM lifecycle
state; a bounded admission worker delivers independent control connections.

## Workspace tests

Use the [HypeR workspace](https://github.com/HypeR-Proj/HypeR) to resolve dependencies:

```sh
python3 project.py test vm-manager
```

Paths in the documentation are relative to the repo-managed workspace root.
Edit this repository and prepare the workspace again before building.

## Listener failure policy

Malformed connection payloads and matched transfer rejections discard only that
request. Subsequent clients use the same listener. Receiver-local failures reach
the manager as terminal admission errors; they are not retried indefinitely.

## Command acceptance and lost replies

A closed control peer is discarded even when unread command bytes remain. The
fleet owner checks closure again immediately before accepting policy. After
acceptance, definition installation and lifecycle intent survive a lost reply;
VM instances and retirement retain their existing owners. Inspect names and
status before retrying. Creation rejects an existing name, and a definition
which was installed but failed to autostart remains inspectable. Restart is not
safe to retry automatically after a timeout.
