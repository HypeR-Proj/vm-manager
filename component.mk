# SPDX-FileCopyrightText: 2026 roolrz
# SPDX-License-Identifier: Apache-2.0

$(eval $(call native-rust-app,svc/vm-manager,hyper-vm-manager,vm-manager,svc/vm-manager,system development,std,vm-runtime))
