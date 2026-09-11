# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------
"""Contract tests for CSSC task access to network-restricted registries."""

import importlib
import importlib.util
import unittest
from types import MappingProxyType, SimpleNamespace

from azext_acrcssc.helper import _constants


ACR_PULL = "7f951dda-4ed3-4680-a7ca-43fe172d538d"
ACR_PUSH = "8311e382-0749-4cb8-b61a-304f252e45ec"
TASKS_CONTRIBUTOR = "fb382eab-e894-4461-af04-94435c366c3f"
REPOSITORY_READER = "b93aa761-3e63-49ed-ac28-beffa264f7ac"
REPOSITORY_WRITER = "41e95607-eb55-4a7f-8412-1b7d4b4e6ed6"
CATALOG_LISTER = "bfdb9389-c9a5-478a-bb2f-ba9ca092c3c7"

EXPECTED_ROLE_MATRIX = {
    "cssc-trigger-workflow": {
        "classic": (ACR_PULL, TASKS_CONTRIBUTOR),
        "abac": (REPOSITORY_READER, CATALOG_LISTER, TASKS_CONTRIBUTOR),
    },
    "cssc-scan-image": {
        "classic": (ACR_PULL, TASKS_CONTRIBUTOR),
        "abac": (REPOSITORY_READER, TASKS_CONTRIBUTOR),
    },
    "cssc-patch-image": {
        "classic": (ACR_PUSH,),
        "abac": (REPOSITORY_WRITER,),
    },
}


def _network_bypass_module(test_case):
    module_name = "azext_acrcssc.helper._network_bypass"
    test_case.assertIsNotNone(
        importlib.util.find_spec(module_name),
        "the network-bypass helper implementing the approved contracts is missing",
    )
    return importlib.import_module(module_name)


def _require(test_case, owner, name):
    value = getattr(owner, name, None)
    test_case.assertIsNotNone(value, "{} is required by the network-bypass contract".format(name))
    return value


class _Serializable:
    _attribute_map = {}

    def serialize(self):
        result = {}
        for attribute, wire_name in self._attribute_map.items():
            value = getattr(self, attribute)
            if isinstance(value, _Serializable):
                value = value.serialize()
            elif isinstance(value, dict):
                value = {
                    key: item.serialize() if isinstance(item, _Serializable) else item
                    for key, item in value.items()
                }
            result[wire_name] = value
        return result


class _CurrentSourceRegistryCredentials(_Serializable):
    _attribute_map = {"login_mode": "loginMode"}

    def __init__(self, login_mode):
        self.login_mode = login_mode


class _CurrentCustomRegistryCredentials(_Serializable):
    _attribute_map = {"identity": "identity"}

    def __init__(self, identity):
        self.identity = identity


class _CurrentCredentials(_Serializable):
    _attribute_map = {
        "source_registry": "sourceRegistry",
        "custom_registries": "customRegistries",
    }

    def __init__(self, source_registry, custom_registries):
        self.source_registry = source_registry
        self.custom_registries = custom_registries


class _LegacyCredentials(_Serializable):
    """Simulates a profile where nested task credential models are unavailable."""

    _attribute_map = {
        "source_registry": "sourceRegistry",
        "custom_registries": "customRegistries",
    }

    def __init__(self, source_registry, custom_registries):
        if not isinstance(source_registry, dict):
            raise TypeError("legacy source_registry must be a dictionary")
        if not all(isinstance(value, dict) for value in custom_registries.values()):
            raise TypeError("legacy custom registry credentials must be dictionaries")
        self.source_registry = source_registry
        self.custom_registries = custom_registries


class NetworkBypassDesiredStateTests(unittest.TestCase):

    def test_cssc_task_role_matrix_is_complete_exact_and_immutable(self):
        role_matrix = _require(self, _constants, "CSSC_TASK_ROLE_IDS")

        self.assertIsInstance(role_matrix, MappingProxyType)
        self.assertEqual(EXPECTED_ROLE_MATRIX, {name: dict(roles) for name, roles in role_matrix.items()})
        with self.assertRaises(TypeError):
            role_matrix["cssc-patch-image"] = {}  # type: ignore[index]
        with self.assertRaises(TypeError):
            role_matrix["cssc-patch-image"]["classic"] = (ACR_PULL,)  # type: ignore[index]


class TaskCredentialContractTests(unittest.TestCase):

    def test_credentials_serialize_identically_for_split_and_legacy_task_sdks(self):
        helper = _network_bypass_module(self)
        build_task_credentials = _require(self, helper, "build_task_credentials")
        login_server = "contoso.azurecr.io"
        expected = {
            "sourceRegistry": {"loginMode": "None"},
            "customRegistries": {login_server: {"identity": "[system]"}},
        }
        current_models = SimpleNamespace(
            Credentials=_CurrentCredentials,
            SourceRegistryCredentials=_CurrentSourceRegistryCredentials,
            CustomRegistryCredentials=_CurrentCustomRegistryCredentials,
        )
        legacy_models = SimpleNamespace(Credentials=_LegacyCredentials)

        current = build_task_credentials(current_models, login_server)
        legacy = build_task_credentials(legacy_models, login_server)

        self.assertIsInstance(current, _CurrentCredentials)
        self.assertIsInstance(legacy, _LegacyCredentials)
        self.assertEqual(expected, current.serialize())
        self.assertEqual(expected, legacy.serialize())


class RegistryPreflightContractTests(unittest.TestCase):

    def test_preflight_decisions_never_enable_bypass_without_explicit_intent(self):
        helper = _network_bypass_module(self)
        evaluate = _require(self, helper, "evaluate_registry_preflight")

        self.assertEqual("proceed", evaluate("Enabled", False, False))
        self.assertEqual("proceed", evaluate("Disabled", True, False))
        self.assertEqual("deny", evaluate("Disabled", False, False))
        self.assertEqual("enable", evaluate("Disabled", False, True))


class _QuickRunError(Exception):

    def __init__(self, code, correlation_id):
        super().__init__(code)
        self.status_code = 403
        self.error = SimpleNamespace(code=code)
        self.response = SimpleNamespace(
            status_code=403,
            headers={"x-ms-correlation-request-id": correlation_id},
        )


class QuickRunFailureContractTests(unittest.TestCase):

    def test_known_quick_run_403_is_actionable_and_preserves_correlation_id(self):
        helper = _network_bypass_module(self)
        raise_for_failure = _require(self, helper, "raise_for_quick_run_failure")
        error = _QuickRunError("TaskRunRequestNetworkRuleBypassNotAllowed", "corr-123")

        with self.assertRaises(Exception) as raised:
            raise_for_failure(error, bypass_enabled=True, tasks_created=False)

        message = str(raised.exception)
        self.assertIn("network", message.lower())
        self.assertIn("bypass", message.lower())
        self.assertIn("corr-123", message)
        self.assertIn("no CSSC tasks were created", message)

    def test_unrelated_quick_run_authorization_403_is_not_rewritten(self):
        helper = _network_bypass_module(self)
        raise_for_failure = _require(self, helper, "raise_for_quick_run_failure")
        error = _QuickRunError("AuthorizationFailed", "corr-unrelated")

        with self.assertRaises(_QuickRunError) as raised:
            raise_for_failure(error, bypass_enabled=True, tasks_created=False)

        self.assertIs(error, raised.exception)


class ReconciliationContractTests(unittest.TestCase):

    def test_reconciliation_repairs_patch_identity_selects_abac_and_preserves_credentials(self):
        helper = _network_bypass_module(self)
        build_plan = _require(self, helper, "build_reconciliation_plan")
        login_server = "contoso.azurecr.io"
        tasks = {
            "cssc-trigger-workflow": self._ready_task(
                "trigger-principal", login_server, {"mirror.example": {"identity": "client-id"}}
            ),
            "cssc-scan-image": self._ready_task("scan-principal", login_server),
            "cssc-patch-image": {
                "identity": None,
                "credentials": {
                    "sourceRegistry": {"loginMode": "Default"},
                    "customRegistries": {"partner.example": {"identity": "partner-client-id"}},
                },
            },
        }

        plan = build_plan(
            tasks,
            login_server,
            role_assignment_mode="AbacRepositoryPermissions",
            existing_role_ids=frozenset(),
        )

        patch_update = plan["task_updates"]["cssc-patch-image"]
        self.assertEqual({"type": "SystemAssigned"}, patch_update["identity"])
        self.assertEqual({"loginMode": "None"}, patch_update["credentials"]["sourceRegistry"])
        self.assertEqual(
            {"identity": "partner-client-id"},
            patch_update["credentials"]["customRegistries"]["partner.example"],
        )
        self.assertEqual(
            {"identity": "[system]"},
            patch_update["credentials"]["customRegistries"][login_server],
        )
        expected_roles = {
            ("trigger-principal", REPOSITORY_READER),
            ("trigger-principal", CATALOG_LISTER),
            ("trigger-principal", TASKS_CONTRIBUTOR),
            ("scan-principal", REPOSITORY_READER),
            ("scan-principal", TASKS_CONTRIBUTOR),
        }
        self.assertEqual(expected_roles, set(plan["role_assignments"]))
        self.assertNotIn(("trigger-principal", ACR_PULL), plan["role_assignments"])
        self.assertNotIn(("scan-principal", ACR_PULL), plan["role_assignments"])

    def test_reconciliation_of_desired_state_is_an_idempotent_no_op(self):
        helper = _network_bypass_module(self)
        build_plan = _require(self, helper, "build_reconciliation_plan")
        login_server = "contoso.azurecr.io"
        tasks = {
            "cssc-trigger-workflow": self._ready_task("trigger-principal", login_server),
            "cssc-scan-image": self._ready_task("scan-principal", login_server),
            "cssc-patch-image": self._ready_task("patch-principal", login_server),
        }
        existing_roles = frozenset({
            ("trigger-principal", REPOSITORY_READER),
            ("trigger-principal", CATALOG_LISTER),
            ("trigger-principal", TASKS_CONTRIBUTOR),
            ("scan-principal", REPOSITORY_READER),
            ("scan-principal", TASKS_CONTRIBUTOR),
            ("patch-principal", REPOSITORY_WRITER),
        })

        plan = build_plan(
            tasks,
            login_server,
            role_assignment_mode="AbacRepositoryPermissions",
            existing_role_ids=existing_roles,
        )

        self.assertEqual({}, plan["task_updates"])
        self.assertEqual([], plan["role_assignments"])

    @staticmethod
    def _ready_task(principal_id, login_server, extra_custom_credentials=None):
        custom_credentials = dict(extra_custom_credentials or {})
        custom_credentials[login_server] = {"identity": "[system]"}
        return {
            "identity": {"type": "SystemAssigned", "principalId": principal_id},
            "credentials": {
                "sourceRegistry": {"loginMode": "None"},
                "customRegistries": custom_credentials,
            },
        }
