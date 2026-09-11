# acrcssc Network Rule Bypass Implementation Plan

## Goal

Make CSSC continuous-patching tasks work with network-restricted Azure
Container Registries that explicitly enable ACR Tasks network-rule bypass.
Support both new and existing workflows without silently broadening registry
network access.

## Approved Product Decisions

- All three newly created CSSC tasks are configured for system-assigned
  identity authentication by default.
- The extension changes `networkRuleBypassAllowedForTasks` only after explicit
  user intent through `workflow create --enable-network-bypass` or
  `workflow configure-network-bypass`.
- Existing workflows use a dedicated idempotent configure command. Normal
  `workflow update` is unsuitable because it runs a task dry-run before update
  logic and can fail before repairing a blocked task.
- Both classic RBAC and RBAC Registry + ABAC Repository Permissions are
  supported.
- `update_acr_tasks.sh` remains an example customer workaround and is not
  modified.

## Current State and Root Cause

- `templates/arm/CSSC-AutoImagePatching-encodedtasks.json` creates three tasks
  without task `properties.credentials`.
- `cssc-trigger-workflow` and `cssc-scan-image` have system identities and the
  Container Registry Tasks Contributor role. That role permits task
  orchestration but is not the required registry data-plane role.
- `cssc-patch-image` has no identity or role.
- All three task definitions access the workflow registry:
  - trigger enumerates registry content and schedules scans;
  - scan pulls images and schedules patch tasks;
  - patch pulls and pushes images.
- The desired task state is `identity.type = SystemAssigned`,
  `credentials.sourceRegistry.loginMode = None`, supported source identity set
  to `[system]`, and a custom registry credential for the real login server
  using system identity.
- Registry bypass is an explicit opt-in and is denied by default when not set.

## Task Role Matrix

| Task | Classic RBAC | RBAC+ABAC | Task control |
|---|---|---|---|
| `cssc-trigger-workflow` | AcrPull | Repository Reader + Catalog Lister | Tasks Contributor |
| `cssc-scan-image` | AcrPull | Repository Reader | Tasks Contributor |
| `cssc-patch-image` | AcrPush | Repository Writer | None |

Role IDs:

- AcrPull: `7f951dda-4ed3-4680-a7ca-43fe172d538d`
- AcrPush: `8311e382-0749-4cb8-b61a-304f252e45ec`
- Container Registry Tasks Contributor:
  `fb382eab-e894-4461-af04-94435c366c3f`
- Container Registry Repository Reader:
  `b93aa761-3e63-49ed-ac28-beffa264f7ac`
- Container Registry Repository Writer:
  `41e95607-eb55-4a7f-8412-1b7d4b4e6ed6`
- Container Registry Repository Catalog Lister:
  `bfdb9389-c9a5-478a-bb2f-ba9ca092c3c7`

Existing ABAC tasks are migrated in place:

1. Read `registry.role_assignment_mode`.
2. Verify all three fixed task names are CSSC-owned.
3. Add a system identity when absent and poll for `principalId`.
4. Merge the identity-backed registry credential into existing credentials.
5. Create missing registry-scoped ABAC role assignments with deterministic
   GUID names.
6. Preserve existing and unrelated assignments, including ineffective legacy
   AcrPull/AcrPush assignments.
7. Re-read state and report readiness. Repeated execution performs no writes.

## Command Design

### New workflows

```text
az acr supply-chain workflow create ... [--enable-network-bypass]
```

- Always deploy identity-ready CSSC tasks.
- Without the flag, never change the registry policy.
- If public access is disabled and bypass is false, fail before deployment
  with the exact opt-in command.
- With the flag, set the policy through registry API `2025-06-01-preview`,
  re-read it, and proceed only after it is true.
- If a later create step fails, report that the explicitly enabled registry
  policy remains enabled; do not silently roll back a security setting.

### Existing workflows

```text
az acr supply-chain workflow configure-network-bypass \
  --registry <name> \
  --resource-group <group> \
  --type continuouspatchv1
```

Invoking this command is explicit consent to enable the registry bypass
policy. It validates ownership/completeness, enables the policy, reconciles
identity/credentials/roles, and returns structured per-task readiness. It
does not require config or schedule and does not call the normal update path.

## Pre-create Quick-run Handling

The existing create flow runs an identity-less `FileTaskRunRequest` before
deploying persistent tasks. This may not qualify for SAMI network bypass.

1. Inspect public network and bypass policy state before the quick run.
2. Restricted + bypass false: fail with actionable opt-in guidance.
3. Restricted + bypass true: attempt the existing quick run.
4. Convert only the known network/authentication 403 into a targeted error
   that includes policy state and the service correlation ID and confirms no
   CSSC tasks were created.
5. Preserve unrelated authorization failures unchanged.

An early gated live test must establish the real service behavior. If the
quick run is unsupported and successful first-time creation on this topology
is required, stop for approval of a revised design such as a temporary
identity-bearing validation task with guaranteed cleanup. Do not silently
skip image-count validation.

## Approved Review Remediations

The following corrections were approved after implementation review.

### Fail closed on asynchronous pre-create validation

Scheduling a quick run is not validation success. Creation may continue only
when the run reaches terminal status `Succeeded`, its logs are retrievable,
and the image-count result is present and parseable.

- Treat `Failed`, `Canceled`, `Error`, `Timeout`, polling timeout, missing
  logs, and malformed image-count output as fatal.
- Do not publish the workflow OCI artifact or deploy persistent tasks after an
  unsuccessful or inconclusive validation.
- Translate both scheduling-time and asynchronous known firewall failures,
  preserving the run ID, service diagnostic, and available correlation IDs.
- Preserve unrelated authorization and service failures without
  reclassifying them as network-bypass failures.
- Reuse one internal terminal-run outcome path for quick-run validation and
  immediate-run handling.

If a gated live test proves that an identity-less `FileTaskRunRequest` cannot
succeed on a restricted registry with bypass enabled, stop for approval of a
temporary identity-bearing validation task. Do not skip image-count
validation.

### Separate ARM convergence from data-plane readiness

ARM role-assignment visibility proves configuration convergence, not that ACR
data-plane authorization has propagated.

- Report `roleAssignmentsVisibleInArm` and `configurationReady`.
- Report `dataPlaneAuthorization` as `notVerified`.
- Remove ambiguous `ready` and `rolesReady` fields before release.
- Warn that task-identity data-plane authorization may still be propagating.
- Remove the fixed 30-second delay before `--run-immediately`.
- Attempt an immediate run without delay and observe its terminal outcome.
- Retry only a narrowly identified managed-identity propagation failure that
  a gated live test proves occurs before task business effects.
- Use deterministic bounded backoff: 5-second initial delay, multiplier 2,
  30-second per-delay cap, and 180-second total deadline.
- If retry safety cannot be proven, do not automatically reschedule. Preserve
  the original failure and provide manual retry guidance.

### Disclose retained bypass policy after create failure

If explicit create opt-in successfully enables the bypass policy and a later
stage fails:

- emit exactly one warning that the policy remains enabled;
- distinguish policy enabled by this invocation from policy already enabled;
- provide the explicit disable command when this invocation changed it;
- do not automatically roll back;
- do not perform Azure calls from the warning path; and
- preserve the original exception, response, cause, and telemetry
  classification.

Registry preparation must return internal transition metadata including
explicit intent, confirmed state, whether the policy was already enabled, and
whether this invocation changed it.

### Gated live decisions

1. Verify whether the identity-less pre-create quick run can succeed on a
   restricted Premium registry with bypass enabled.
2. In classic and RBAC+ABAC modes, prove any proposed automatic-retry
   diagnostic occurs before catalog, pull, scan, patch, or push effects.
3. Verify end-to-end trigger read/catalog, scan pull, and patch pull/push
   behavior.
4. Force post-enable failures and confirm the persistent-policy warning does
   not mask the original error.
5. Re-run public-network creation without opt-in and confirm no policy
   mutation.

## Tests-first Work Plan

1. Add desired-state, SDK serialization, registry-state, RBAC-mode, and
   restricted quick-run contract tests.
2. Update new task deployment so all tasks have system identity,
   identity-backed registry credentials, and task-specific classic/ABAC roles.
3. Add explicit create policy opt-in and early registry-state validation.
4. Add the idempotent `configure-network-bypass` command in a focused
   `_network_bypass.py` helper using SDK clients rather than subprocesses.
5. Test partial states, credential preservation, deterministic roles,
   propagation delay, authorization failures, idempotence, and live scenarios.
6. Document security implications, commands, validation, and manual customer
   steps without modifying `update_acr_tasks.sh`.
7. Update `setup.py` and `HISTORY.rst` to the release-owner-approved version.

## Expected Files

- `src/acrcssc/azext_acrcssc/commands.py`
- `src/acrcssc/azext_acrcssc/_params.py`
- `src/acrcssc/azext_acrcssc/_help.py`
- `src/acrcssc/azext_acrcssc/cssc.py`
- `src/acrcssc/azext_acrcssc/helper/_constants.py`
- `src/acrcssc/azext_acrcssc/helper/_taskoperations.py`
- New `src/acrcssc/azext_acrcssc/helper/_network_bypass.py`
- `src/acrcssc/azext_acrcssc/templates/arm/CSSC-AutoImagePatching-encodedtasks.json`
- Tests under `src/acrcssc/azext_acrcssc/tests/latest/`
- `src/acrcssc/README.rst`
- `src/acrcssc/setup.py`
- `src/acrcssc/HISTORY.rst`

Explicitly unchanged: `update_acr_tasks.sh`, workflow config schema, CSSC scan
and patch behavior, and unrelated dependency pins.

## Acceptance Criteria

1. Every new CSSC task has SAMI and identity-backed registry credentials.
2. Default source-registry authentication is disabled on all three tasks.
3. Registry bypass is enabled only through explicit user intent.
4. Restricted registries without opt-in fail before partial deployment.
5. Known identity-less quick-run firewall failures produce a targeted,
   correlation-preserving error and no task resources.
6. Existing tasks are repaired in place without config or schedule input.
7. ABAC roles are added after task principal IDs exist; tasks are not
   recreated.
8. Repeated configure execution is a successful no-op.
9. Unrelated credentials and role assignments are preserved.
10. Static output shows policy, role mode, and per-task readiness without
    exposing secrets.
11. Network-restricted workflow dry-run succeeds after configuration.
12. Public-network workflows remain functional.
13. Unit/scenario tests, ARM validation, style, linter, index checks, and wheel
    build pass.
14. Creation continues only after a terminally successful quick run with a
    valid image-count result.
15. Failed or inconclusive quick runs leave no workflow artifact or persistent
    CSSC tasks.
16. Configure output distinguishes ARM assignment visibility from unverified
    ACR data-plane authorization.
17. `--run-immediately` uses no unconditional propagation sleep or unsafe
    retry.
18. A failed create after confirmed explicit bypass enablement warns that the
    policy remains enabled without masking the original failure.

## References

- https://learn.microsoft.com/en-us/azure/container-registry/manage-network-bypass-policy-for-tasks
- https://learn.microsoft.com/en-us/azure/container-registry/container-registry-tasks-authentication-managed-identity
- https://learn.microsoft.com/en-us/cli/azure/acr/task/credential
- https://learn.microsoft.com/en-us/azure/templates/microsoft.containerregistry/2025-03-01-preview/registries/tasks
- https://learn.microsoft.com/en-us/azure/container-registry/container-registry-rbac-abac-repository-permissions
- https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles/containers
