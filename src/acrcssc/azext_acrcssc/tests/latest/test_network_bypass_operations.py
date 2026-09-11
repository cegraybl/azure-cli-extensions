# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

import json
from pathlib import Path
import unittest
from types import SimpleNamespace
from unittest import mock

from azure.cli.core.azclierror import AzCLIError

from azext_acrcssc.helper import _network_bypass
from azext_acrcssc.helper._constants import (
    ACR_NETWORK_BYPASS_API_VERSION,
    CSSC_TASK_ROLE_IDS,
)


class NetworkBypassOperationsTests(unittest.TestCase):

    def setUp(self):
        self.cmd = SimpleNamespace(cli_ctx=mock.MagicMock())
        self.registry = SimpleNamespace(
            id=(
                "/subscriptions/sub/resourceGroups/rg/providers/"
                "Microsoft.ContainerRegistry/registries/registry"
            ),
            name="registry",
            login_server="registry.azurecr.io",
        )

    @mock.patch.object(_network_bypass, "cf_resources")
    def test_registry_security_state_uses_network_bypass_api(self, cf_resources):
        resources = cf_resources.return_value.resources
        resources.get_by_id.return_value = SimpleNamespace(properties={
            "publicNetworkAccess": "Disabled",
            "networkRuleBypassAllowedForTasks": True,
            "roleAssignmentMode": "AbacRepositoryPermissions",
        })

        state = _network_bypass.get_registry_security_state(self.cmd, self.registry)

        resources.get_by_id.assert_called_once_with(
            self.registry.id,
            ACR_NETWORK_BYPASS_API_VERSION)
        self.assertEqual("Disabled", state["public_network_access"])
        self.assertTrue(state["network_bypass_enabled"])
        self.assertEqual("AbacRepositoryPermissions", state["role_assignment_mode"])

    @mock.patch.object(_network_bypass, "enable_registry_network_bypass")
    @mock.patch.object(_network_bypass, "get_registry_security_state")
    def test_prepare_registry_enables_only_with_explicit_opt_in(
            self,
            get_state,
            enable_bypass):
        get_state.return_value = {
            "public_network_access": "Disabled",
            "network_bypass_enabled": False,
            "role_assignment_mode": "rbac",
        }
        enable_bypass.return_value = {
            "public_network_access": "Disabled",
            "network_bypass_enabled": True,
            "role_assignment_mode": "rbac",
        }

        with self.assertRaises(AzCLIError):
            _network_bypass.prepare_registry_for_workflow(
                self.cmd,
                self.registry,
                explicit_opt_in=False)
        enable_bypass.assert_not_called()

        state = _network_bypass.prepare_registry_for_workflow(
            self.cmd,
            self.registry,
            explicit_opt_in=True)
        enable_bypass.assert_called_once_with(self.cmd, self.registry)
        self.assertTrue(state["network_bypass_enabled"])

    @mock.patch.object(_network_bypass, "enable_registry_network_bypass")
    @mock.patch.object(_network_bypass, "create_encoded_task")
    @mock.patch.object(_network_bypass, "cf_acr_tasks")
    def test_configure_rejects_replaced_task_before_registry_mutation(
            self,
            cf_acr_tasks,
            create_encoded_task,
            enable_bypass):
        create_encoded_task.return_value = "trusted-definition"
        cf_acr_tasks.return_value.get.return_value = SimpleNamespace(
            tags={"cssc": "true"},
            step=SimpleNamespace(encoded_task_content="attacker-definition"))

        with self.assertRaisesRegex(AzCLIError, "does not match"):
            _network_bypass.configure_existing_workflow_network_bypass(
                self.cmd,
                self.registry)

        enable_bypass.assert_not_called()

    @mock.patch.object(_network_bypass, "cf_resources")
    @mock.patch.object(_network_bypass, "get_registry_security_state")
    def test_enable_registry_bypass_is_no_op_when_already_enabled(
            self,
            get_state,
            cf_resources):
        expected = {
            "public_network_access": "Disabled",
            "network_bypass_enabled": True,
            "role_assignment_mode": "rbac",
        }
        get_state.return_value = expected

        result = _network_bypass.enable_registry_network_bypass(
            self.cmd,
            self.registry)

        self.assertIs(expected, result)
        cf_resources.assert_not_called()

    @mock.patch.object(_network_bypass.time, "sleep")
    @mock.patch.object(_network_bypass.time, "monotonic", side_effect=[0, 0, 1])
    def test_wait_for_principal_id_polls_until_identity_propagates(
            self,
            _monotonic,
            sleep):
        tasks_client = mock.MagicMock()
        pending = SimpleNamespace(
            identity=SimpleNamespace(principal_id=None))
        ready = SimpleNamespace(
            identity=SimpleNamespace(principal_id="principal"))
        tasks_client.get.return_value = ready

        result = _network_bypass._wait_for_principal_id(
            tasks_client,
            "rg",
            "registry",
            "cssc-patch-image",
            pending,
            timeout=10,
            poll_interval=0)

        self.assertIs(ready, result)
        sleep.assert_called_once_with(0)
        tasks_client.get.assert_called_once_with(
            "rg",
            "registry",
            "cssc-patch-image")

    def test_wait_for_principal_id_times_out_with_task_name(self):
        pending = SimpleNamespace(
            identity=SimpleNamespace(principal_id=None))

        with self.assertRaisesRegex(AzCLIError, "cssc-patch-image"):
            _network_bypass._wait_for_principal_id(
                mock.MagicMock(),
                "rg",
                "registry",
                "cssc-patch-image",
                pending,
                timeout=0,
                poll_interval=0)

    def test_identity_and_abac_enum_values_are_normalized(self):
        task = SimpleNamespace(
            identity=SimpleNamespace(type="IdentityType.system_assigned"))

        self.assertTrue(_network_bypass._has_system_identity(task))
        self.assertEqual(
            "abac",
            _network_bypass._get_role_mode(
                "RoleAssignmentMode.rbac_registry_abac_repository_permissions"))

    def test_system_identity_update_preserves_user_assigned_identities(self):
        class IdentityProperties:
            def __init__(self, type, user_assigned_identities=None):
                self.type = type
                self.user_assigned_identities = user_assigned_identities

        user_assigned = {"/identities/existing": {}}
        identity = _network_bypass._build_system_identity(
            SimpleNamespace(IdentityProperties=IdentityProperties),
            SimpleNamespace(user_assigned_identities=user_assigned))

        self.assertEqual("SystemAssigned, UserAssigned", identity.type)
        self.assertIs(user_assigned, identity.user_assigned_identities)

    def test_merge_credentials_preserves_unrelated_registry(self):
        class SourceRegistryCredentials:
            def __init__(self, login_mode, identity=None):
                self.login_mode = login_mode
                self.identity = identity

        class CustomRegistryCredentials:
            def __init__(self, identity):
                self.identity = identity

        class Credentials:
            def __init__(self, source_registry, custom_registries):
                self.source_registry = source_registry
                self.custom_registries = custom_registries

        models = SimpleNamespace(
            SourceRegistryCredentials=SourceRegistryCredentials,
            CustomRegistryCredentials=CustomRegistryCredentials,
            Credentials=Credentials)
        partner_credential = CustomRegistryCredentials(identity="partner-id")
        existing = Credentials(
            source_registry=SourceRegistryCredentials(login_mode="Default"),
            custom_registries={"partner.example": partner_credential})

        result = _network_bypass._merge_task_credentials(
            models,
            existing,
            self.registry.login_server)

        self.assertIs(
            partner_credential,
            result.custom_registries["partner.example"])
        self.assertEqual(
            "[system]",
            result.custom_registries[self.registry.login_server].identity)
        self.assertEqual("None", result.source_registry.login_mode)

    def test_readiness_requires_identity_credentials_and_every_role(self):
        task_name = "cssc-patch-image"
        task = SimpleNamespace(
            identity=SimpleNamespace(
                type="SystemAssigned",
                principal_id="patch-principal"),
            credentials=SimpleNamespace(
                source_registry=SimpleNamespace(login_mode="None"),
                custom_registries={
                    self.registry.login_server:
                        SimpleNamespace(identity="[system]")
                }))
        role_id = CSSC_TASK_ROLE_IDS[task_name]["classic"][0]

        ready = _network_bypass._build_task_readiness(
            task_name,
            task,
            self.registry.login_server,
            "classic",
            {("patch-principal", role_id)},
            changed=False)
        missing_role = _network_bypass._build_task_readiness(
            task_name,
            task,
            self.registry.login_server,
            "classic",
            set(),
            changed=False)

        self.assertTrue(ready["ready"])
        self.assertFalse(missing_role["ready"])
        self.assertFalse(missing_role["rolesReady"])

    @mock.patch.object(_network_bypass, "cf_authorization")
    def test_role_reconciliation_adds_only_missing_assignment(
            self,
            cf_authorization):
        tasks = {}
        existing_assignments = []
        for task_name, roles_by_mode in CSSC_TASK_ROLE_IDS.items():
            principal_id = "{}-principal".format(task_name)
            tasks[task_name] = SimpleNamespace(
                identity=SimpleNamespace(principal_id=principal_id))
            for role_id in roles_by_mode["classic"]:
                if task_name != "cssc-patch-image":
                    existing_assignments.append(SimpleNamespace(
                        principal_id=principal_id,
                        role_definition_id="/roleDefinitions/{}".format(role_id)))
        role_client = cf_authorization.return_value.role_assignments
        role_client.list_for_scope.return_value = existing_assignments

        added = _network_bypass._ensure_role_assignments(
            self.cmd,
            self.registry,
            tasks,
            "classic")

        self.assertEqual(
            [{
                "task": "cssc-patch-image",
                "roleId": CSSC_TASK_ROLE_IDS[
                    "cssc-patch-image"]["classic"][0],
            }],
            added)
        role_client.create.assert_called_once()

    @mock.patch.object(_network_bypass.time, "sleep")
    @mock.patch.object(_network_bypass.time, "monotonic", side_effect=[0, 0])
    @mock.patch.object(_network_bypass, "_list_role_assignment_pairs")
    def test_role_verification_polls_through_propagation(
            self,
            list_assignments,
            _monotonic,
            sleep):
        tasks = {}
        expected = set()
        for task_name, roles_by_mode in CSSC_TASK_ROLE_IDS.items():
            principal_id = "{}-principal".format(task_name)
            tasks[task_name] = SimpleNamespace(
                identity=SimpleNamespace(principal_id=principal_id))
            expected.update(
                (principal_id, role_id)
                for role_id in roles_by_mode["classic"])
        list_assignments.side_effect = [set(), expected]

        result = _network_bypass._wait_for_role_assignments(
            self.cmd,
            self.registry,
            tasks,
            "classic",
            timeout=10,
            poll_interval=0)

        self.assertEqual(expected, result)
        sleep.assert_called_once_with(0)


class NetworkBypassTemplateTests(unittest.TestCase):

    def test_template_creates_identity_credentials_and_mode_specific_roles(self):
        template_path = (
            Path(__file__).parents[2]
            / "templates"
            / "arm"
            / "CSSC-AutoImagePatching-encodedtasks.json"
        )
        template = json.loads(template_path.read_text(encoding="utf-8"))
        tasks = [
            resource
            for resource in template["resources"]
            if resource["type"] == "Microsoft.ContainerRegistry/registries/tasks"
        ]
        assignments = [
            resource
            for resource in template["resources"]
            if resource["type"] == "Microsoft.Authorization/roleAssignments"
        ]

        self.assertEqual(3, len(tasks))
        self.assertTrue(all(
            task["identity"]["type"] == "SystemAssigned"
            and task["properties"]["credentials"] == "[parameters('taskCredentials')]"
            and task["apiVersion"] == "2025-03-01-preview"
            for task in tasks))
        self.assertEqual(9, len(assignments))
        self.assertEqual(
            3,
            sum(
                resource.get("condition") == "[not(parameters('UseAbacRoles'))]"
                for resource in assignments))
        self.assertEqual(
            4,
            sum(
                resource.get("condition") == "[parameters('UseAbacRoles')]"
                for resource in assignments))


if __name__ == "__main__":
    unittest.main()
