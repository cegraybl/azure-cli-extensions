# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------
"""Configure CSSC tasks for ACR network-rule bypass."""

from copy import deepcopy
import re
import time
import uuid

from azure.cli.core.azclierror import AzCLIError, ResourceNotFoundError
from azure.cli.core.commands import LongRunningOperation
from azure.core.exceptions import HttpResponseError
from azure.mgmt.authorization.models import RoleAssignmentCreateParameters
from azure.mgmt.core.tools import parse_resource_id
from azure.mgmt.resource.resources.models import GenericResource

from azext_acrcssc._client_factory import (
    cf_acr_tasks,
    cf_authorization,
    cf_resources,
    get_acr_tasks_models)

from ._constants import (
    ACR_NETWORK_BYPASS_API_VERSION,
    CSSC_TASK_ROLE_IDS,
    CONTINUOUSPATCH_ALL_TASK_NAMES,
    CONTINUOUSPATCH_TASK_DEFINITION,
    RESOURCE_GROUP)
from ._utility import create_encoded_task


SYSTEM_IDENTITY = "[system]"
NETWORK_BYPASS_ERROR_CODES = frozenset({
    "TaskRunRequestNetworkRuleBypassNotAllowed",
    "NetworkRuleBypassNotAllowed",
})
ABAC_ROLE_ASSIGNMENT_MODES = frozenset({
    "abacrepositorypermissions",
    "rbacregistryabacrepositorypermissions",
    "rbacabac",
})
IDENTITY_PROPAGATION_TIMEOUT_SECONDS = 60
IDENTITY_PROPAGATION_POLL_SECONDS = 2
ROLE_PROPAGATION_TIMEOUT_SECONDS = 60
ROLE_PROPAGATION_POLL_SECONDS = 2


def build_task_credentials(task_models, login_server):
    """Build profile-compatible credentials for system-identity registry access."""
    source_registry = {"loginMode": "None"}
    custom_registry = {"identity": SYSTEM_IDENTITY}

    source_model = getattr(task_models, "SourceRegistryCredentials", None)
    custom_model = getattr(task_models, "CustomRegistryCredentials", None)
    if source_model and custom_model:
        source_registry = source_model(login_mode="None")
        custom_registry = custom_model(identity=SYSTEM_IDENTITY)

    return task_models.Credentials(
        source_registry=source_registry,
        custom_registries={login_server: custom_registry})


def evaluate_registry_preflight(public_network_access, bypass_enabled, explicit_opt_in):
    """Return the action required before a CSSC validation run."""
    if explicit_opt_in and not bypass_enabled:
        return "enable"
    if public_network_access == "Disabled" and not bypass_enabled:
        return "deny"
    return "proceed"


def is_abac_role_assignment_mode(role_assignment_mode):
    """Return whether the registry uses RBAC+ABAC repository permissions."""
    return _get_role_mode(role_assignment_mode) == "abac"


def raise_for_quick_run_failure(error, bypass_enabled, tasks_created):
    """Translate only known network-bypass failures into an actionable CLI error."""
    status_code = getattr(error, "status_code", None)
    response = getattr(error, "response", None)
    status_code = status_code or getattr(response, "status_code", None)
    error_code = getattr(getattr(error, "error", None), "code", None)
    error_message = str(error).lower()
    is_network_denial = (
        error_code in NETWORK_BYPASS_ERROR_CODES
        or ("not allowed access" in error_message and "firewall" in error_message)
    )
    if status_code != 403 or not is_network_denial:
        raise error

    headers = getattr(response, "headers", {}) or {}
    correlation_id = (
        headers.get("x-ms-correlation-request-id")
        or headers.get("x-ms-request-id")
        or "unavailable")
    task_state = (
        "CSSC tasks may already exist."
        if tasks_created
        else "no CSSC tasks were created.")
    raise AzCLIError(
        "The ACR validation quick run was denied by registry network rules even "
        "though network-rule bypass is {}. The pre-create quick run does not "
        "have a persistent task system identity. {} Correlation ID: {}"
        .format("enabled" if bypass_enabled else "disabled", task_state, correlation_id))


def build_reconciliation_plan(tasks, login_server, role_assignment_mode, existing_role_ids):
    """Build an immutable-input reconciliation plan for task and role updates."""
    role_mode = _get_role_mode(role_assignment_mode)
    task_updates = {}
    role_assignments = []

    for task_name, required_roles in CSSC_TASK_ROLE_IDS.items():
        task = tasks[task_name]
        identity = task.get("identity")
        credentials = task.get("credentials") or {}
        source_registry = credentials.get("sourceRegistry") or {}
        custom_registries = credentials.get("customRegistries") or {}
        principal_id = identity.get("principalId") if identity else None

        needs_identity = not identity or identity.get("type") != "SystemAssigned"
        desired_custom_registries = deepcopy(custom_registries)
        desired_custom_registries[login_server] = {"identity": SYSTEM_IDENTITY}
        desired_credentials = {
            "sourceRegistry": {"loginMode": "None"},
            "customRegistries": desired_custom_registries,
        }

        if (needs_identity
                or source_registry.get("loginMode") != "None"
                or custom_registries.get(login_server) != {"identity": SYSTEM_IDENTITY}):
            update = {"credentials": desired_credentials}
            if needs_identity:
                update["identity"] = {"type": "SystemAssigned"}
            task_updates[task_name] = update

        if principal_id:
            for role_id in required_roles[role_mode]:
                assignment = (principal_id, role_id)
                if assignment not in existing_role_ids:
                    role_assignments.append(assignment)

    return {
        "task_updates": task_updates,
        "role_assignments": role_assignments,
    }


def get_registry_security_state(cmd, registry):
    """Read network and authorization mode properties using the current registry API."""
    resources = cf_resources(cmd.cli_ctx).resources
    resource = resources.get_by_id(registry.id, ACR_NETWORK_BYPASS_API_VERSION)
    properties = resource.properties or {}
    return {
        "public_network_access": _get_value(properties, "publicNetworkAccess", "public_network_access"),
        "network_bypass_enabled": bool(_get_value(
            properties,
            "networkRuleBypassAllowedForTasks",
            "network_rule_bypass_allowed_for_tasks")),
        "role_assignment_mode": _get_value(
            properties,
            "roleAssignmentMode",
            "role_assignment_mode") or "rbac",
    }


def prepare_registry_for_workflow(cmd, registry, explicit_opt_in):
    """Validate registry network state and apply an explicitly requested bypass policy."""
    state = get_registry_security_state(cmd, registry)
    decision = evaluate_registry_preflight(
        state["public_network_access"],
        state["network_bypass_enabled"],
        explicit_opt_in)
    if decision == "deny":
        raise AzCLIError(
            "Registry '{}' has public network access disabled and ACR Tasks "
            "network-rule bypass is not enabled. Re-run with "
            "--enable-network-bypass or run: az resource update --ids {} "
            "--api-version {} --set "
            "properties.networkRuleBypassAllowedForTasks=true"
            .format(
                registry.name,
                registry.id,
                ACR_NETWORK_BYPASS_API_VERSION))
    if decision == "enable":
        state = enable_registry_network_bypass(cmd, registry)
    return state


def enable_registry_network_bypass(cmd, registry):
    """Enable and verify the explicitly requested registry bypass policy."""
    state = get_registry_security_state(cmd, registry)
    if state["network_bypass_enabled"]:
        return state

    resources = cf_resources(cmd.cli_ctx).resources
    parameters = GenericResource(properties={
        "networkRuleBypassAllowedForTasks": True,
    })
    poller = resources.begin_update_by_id(
        registry.id,
        ACR_NETWORK_BYPASS_API_VERSION,
        parameters)
    LongRunningOperation(cmd.cli_ctx)(poller)
    state = get_registry_security_state(cmd, registry)
    if not state["network_bypass_enabled"]:
        raise AzCLIError(
            "The registry update completed, but ACR Tasks network-rule bypass "
            "is not enabled.")
    return state


def configure_existing_workflow_network_bypass(cmd, registry):
    """Converge existing CSSC tasks and registry policy to bypass-ready state."""
    tasks_client = cf_acr_tasks(cmd.cli_ctx)
    resource_group = parse_resource_id(registry.id)[RESOURCE_GROUP]
    tasks = _get_owned_tasks(tasks_client, resource_group, registry.name)

    state = enable_registry_network_bypass(cmd, registry)
    task_models = get_acr_tasks_models(cmd.cli_ctx)
    changed_tasks = []
    for task_name, task in tasks.items():
        if not _has_system_identity(task):
            update = task_models.TaskUpdateParameters(
                identity=_build_system_identity(
                    task_models,
                    getattr(task, "identity", None)))
            task = LongRunningOperation(cmd.cli_ctx)(tasks_client.begin_update(
                resource_group,
                registry.name,
                task_name,
                update))
            changed_tasks.append(task_name)

        desired_credentials = _merge_task_credentials(
            task_models,
            getattr(task, "credentials", None),
            registry.login_server)
        if not _has_desired_credentials(task, registry.login_server):
            update = task_models.TaskUpdateParameters(credentials=desired_credentials)
            task = LongRunningOperation(cmd.cli_ctx)(tasks_client.begin_update(
                resource_group,
                registry.name,
                task_name,
                update))
            if task_name not in changed_tasks:
                changed_tasks.append(task_name)
        tasks[task_name] = task

    for task_name, task in tasks.items():
        tasks[task_name] = _wait_for_principal_id(
            tasks_client,
            resource_group,
            registry.name,
            task_name,
            task)

    role_mode = _get_role_mode(state["role_assignment_mode"])
    added_roles = _ensure_role_assignments(
        cmd,
        registry,
        tasks,
        role_mode)
    refreshed_tasks = _get_owned_tasks(tasks_client, resource_group, registry.name)
    role_assignments = _wait_for_role_assignments(
        cmd,
        registry,
        refreshed_tasks,
        role_mode)
    result = _build_readiness_result(
        registry,
        state,
        refreshed_tasks,
        role_mode,
        changed_tasks,
        added_roles,
        role_assignments)
    unready_tasks = [task["name"] for task in result["tasks"] if not task["ready"]]
    if unready_tasks:
        raise AzCLIError(
            "Network-bypass configuration did not converge for CSSC task(s): {}."
            .format(", ".join(unready_tasks)))
    return result


def _get_owned_tasks(tasks_client, resource_group, registry_name):
    tasks = {}
    for task_name in CONTINUOUSPATCH_ALL_TASK_NAMES:
        try:
            task = tasks_client.get(resource_group, registry_name, task_name)
        except ResourceNotFoundError as error:
            raise AzCLIError(
                "CSSC task '{}' was not found. Create the workflow before "
                "configuring network bypass.".format(task_name)) from error
        tags = getattr(task, "tags", None) or {}
        if str(tags.get("cssc", "")).lower() != "true":
            raise AzCLIError(
                "Task '{}' exists but is not marked as CSSC-owned; no changes "
                "were made.".format(task_name))
        expected_task = create_encoded_task(
            CONTINUOUSPATCH_TASK_DEFINITION[task_name]["template_file"])
        deployed_task = getattr(
            getattr(task, "step", None),
            "encoded_task_content",
            None)
        if deployed_task != expected_task:
            raise AzCLIError(
                "Task '{}' does not match the packaged CSSC task definition; "
                "no changes were made.".format(task_name))
        tasks[task_name] = task
    return tasks


def _merge_task_credentials(task_models, existing_credentials, login_server):
    custom_registries = dict(_get_value(
        existing_credentials or {},
        "customRegistries",
        "custom_registries") or {})
    source_model = getattr(task_models, "SourceRegistryCredentials", None)
    custom_model = getattr(task_models, "CustomRegistryCredentials", None)
    if source_model and custom_model:
        custom_registries[login_server] = custom_model(
            identity=SYSTEM_IDENTITY)
        try:
            source_registry = source_model(
                identity=SYSTEM_IDENTITY,
                login_mode="None")
        except TypeError:
            source_registry = source_model(login_mode="None")
    else:
        source_registry = {"loginMode": "None"}
        custom_registries[login_server] = {"identity": SYSTEM_IDENTITY}
    return task_models.Credentials(
        source_registry=source_registry,
        custom_registries=custom_registries)


def _build_system_identity(task_models, existing_identity):
    user_assigned_identities = getattr(
        existing_identity,
        "user_assigned_identities",
        None)
    if user_assigned_identities:
        return task_models.IdentityProperties(
            type="SystemAssigned, UserAssigned",
            user_assigned_identities=user_assigned_identities)
    return task_models.IdentityProperties(type="SystemAssigned")


def _has_system_identity(task):
    identity = getattr(task, "identity", None)
    identity_type = re.sub(
        r"[^a-z]",
        "",
        str(getattr(identity, "type", "") or "").lower())
    return "systemassigned" in identity_type


def _has_desired_credentials(task, login_server):
    credentials = getattr(task, "credentials", None)
    source_registry = _get_value(
        credentials or {},
        "sourceRegistry",
        "source_registry") or {}
    custom_registries = _get_value(
        credentials or {},
        "customRegistries",
        "custom_registries") or {}
    custom_credential = custom_registries.get(login_server)
    return (
        _get_value(source_registry, "loginMode", "login_mode") == "None"
        and _get_value(
            custom_credential or {},
            "identity",
            "identity") in {SYSTEM_IDENTITY, "system"})


def _ensure_role_assignments(cmd, registry, tasks, role_mode):
    role_client = cf_authorization(cmd.cli_ctx).role_assignments
    existing_assignments = _list_role_assignment_pairs(cmd, registry.id)
    added_roles = []
    subscription_id = parse_resource_id(registry.id)["subscription"]
    for task_name, roles in CSSC_TASK_ROLE_IDS.items():
        principal_id = getattr(tasks[task_name].identity, "principal_id", None)
        if not principal_id:
            raise AzCLIError(
                "System identity for task '{}' has no principal ID after the "
                "task update completed.".format(task_name))
        for role_id in roles[role_mode]:
            if (principal_id, role_id.lower()) in existing_assignments:
                continue
            assignment_name = str(uuid.uuid5(
                uuid.NAMESPACE_URL,
                "{}|{}|{}|{}".format(
                    registry.id.lower(),
                    task_name,
                    principal_id,
                    role_id)))
            role_definition_id = (
                "/subscriptions/{}/providers/Microsoft.Authorization/"
                "roleDefinitions/{}".format(subscription_id, role_id))
            parameters = RoleAssignmentCreateParameters(
                role_definition_id=role_definition_id,
                principal_id=principal_id,
                principal_type="ServicePrincipal")
            try:
                role_client.create(registry.id, assignment_name, parameters)
            except HttpResponseError as error:
                if getattr(error, "status_code", None) != 409:
                    raise
                continue
            existing_assignments.add((principal_id, role_id.lower()))
            added_roles.append({
                "task": task_name,
                "roleId": role_id,
            })
    return added_roles


def _build_readiness_result(
        registry,
        state,
        tasks,
        role_mode,
        changed_tasks,
        added_roles,
        role_assignments):
    return {
        "registry": registry.name,
        "loginServer": registry.login_server,
        "publicNetworkAccess": state["public_network_access"],
        "networkRuleBypassAllowedForTasks": state["network_bypass_enabled"],
        "roleAssignmentMode": state["role_assignment_mode"],
        "tasks": [
            _build_task_readiness(
                name,
                task,
                registry.login_server,
                role_mode,
                role_assignments,
                name in changed_tasks)
            for name, task in tasks.items()
        ],
        "rolesAdded": added_roles,
    }


def _wait_for_principal_id(
        tasks_client,
        resource_group,
        registry_name,
        task_name,
        task,
        timeout=IDENTITY_PROPAGATION_TIMEOUT_SECONDS,
        poll_interval=IDENTITY_PROPAGATION_POLL_SECONDS):
    current_task = task
    deadline = time.monotonic() + timeout
    while True:
        principal_id = getattr(
            getattr(current_task, "identity", None),
            "principal_id",
            None)
        if principal_id:
            return current_task
        if time.monotonic() >= deadline:
            break
        time.sleep(poll_interval)
        current_task = tasks_client.get(
            resource_group,
            registry_name,
            task_name)
    raise AzCLIError(
        "Timed out waiting for the system identity principal ID for task '{}'."
        .format(task_name))


def _list_role_assignment_pairs(cmd, scope):
    assignments = cf_authorization(cmd.cli_ctx).role_assignments.list_for_scope(scope)
    return {
        (
            getattr(assignment, "principal_id", None),
            str(getattr(assignment, "role_definition_id", "")).rsplit("/", 1)[-1].lower())
        for assignment in assignments
    }


def _wait_for_role_assignments(
        cmd,
        registry,
        tasks,
        role_mode,
        timeout=ROLE_PROPAGATION_TIMEOUT_SECONDS,
        poll_interval=ROLE_PROPAGATION_POLL_SECONDS):
    deadline = time.monotonic() + timeout
    while True:
        assignments = _list_role_assignment_pairs(cmd, registry.id)
        missing = []
        for task_name, roles_by_mode in CSSC_TASK_ROLE_IDS.items():
            principal_id = getattr(
                getattr(tasks[task_name], "identity", None),
                "principal_id",
                None)
            missing.extend(
                (task_name, role_id)
                for role_id in roles_by_mode[role_mode]
                if (principal_id, role_id.lower()) not in assignments)
        if not missing:
            return assignments
        if time.monotonic() >= deadline:
            missing_text = ", ".join(
                "{}:{}".format(task_name, role_id)
                for task_name, role_id in missing)
            raise AzCLIError(
                "Timed out waiting for CSSC role assignments: {}."
                .format(missing_text))
        time.sleep(poll_interval)


def _build_task_readiness(
        name,
        task,
        login_server,
        role_mode,
        role_assignments,
        changed):
    principal_id = getattr(getattr(task, "identity", None), "principal_id", None)
    required_role_ids = list(CSSC_TASK_ROLE_IDS[name][role_mode])
    assigned_role_ids = [
        role_id
        for role_id in required_role_ids
        if (principal_id, role_id.lower()) in role_assignments
    ]
    identity_ready = _has_system_identity(task) and bool(principal_id)
    credentials_ready = _has_desired_credentials(task, login_server)
    roles_ready = len(assigned_role_ids) == len(required_role_ids)
    return {
        "name": name,
        "principalId": principal_id,
        "identityReady": identity_ready,
        "credentialsReady": credentials_ready,
        "requiredRoleIds": required_role_ids,
        "assignedRoleIds": assigned_role_ids,
        "rolesReady": roles_ready,
        "ready": identity_ready and credentials_ready and roles_ready,
        "changed": changed,
    }


def _get_role_mode(role_assignment_mode):
    normalized = re.sub(
        r"[^a-z]",
        "",
        str(role_assignment_mode or "").lower())
    return (
        "abac"
        if any(normalized.endswith(mode) for mode in ABAC_ROLE_ASSIGNMENT_MODES)
        else "classic")


def _get_value(properties, camel_name, snake_name):
    if isinstance(properties, dict):
        return properties.get(camel_name, properties.get(snake_name))
    return getattr(properties, snake_name, getattr(properties, camel_name, None))
