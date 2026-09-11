# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------
"""Focused tests for the approved network-bypass review remediations."""

import unittest
from contextlib import ExitStack
from types import SimpleNamespace
from unittest import mock

from azure.cli.core.azclierror import AzCLIError, InvalidArgumentValueError
from azure.core.exceptions import HttpResponseError

from azext_acrcssc import cssc
from azext_acrcssc import _validators
from azext_acrcssc.helper import _network_bypass
from azext_acrcssc.helper import _taskoperations


class QuickRunTerminalOutcomeTests(unittest.TestCase):

    def setUp(self):
        self.cmd = SimpleNamespace(cli_ctx=mock.MagicMock())
        self.registry = SimpleNamespace(
            id=(
                "/subscriptions/sub/resourceGroups/rg/providers/"
                "Microsoft.ContainerRegistry/registries/registry"
            ),
            name="registry",
        )

    def _run_quick_validation(self, terminal_run, logs="Matches found: 1"):
        run_client = mock.MagicMock()
        run_client.get.return_value = terminal_run
        task_client = mock.MagicMock()
        task_models = SimpleNamespace(
            OS=SimpleNamespace(linux=SimpleNamespace(value="linux")),
            Architecture=SimpleNamespace(amd64=SimpleNamespace(value="amd64")),
            FileTaskRunRequest=mock.Mock(),
            PlatformProperties=mock.Mock(),
            Credentials=mock.Mock(),
        )

        generate_logs = mock.patch.object(
            _taskoperations.WorkflowTaskStatus,
            "generate_logs",
            side_effect=logs if isinstance(logs, BaseException) else None,
            return_value=None if isinstance(logs, BaseException) else logs)
        patches = [
            mock.patch.object(
                _taskoperations,
                "check_continuous_task_exists",
                return_value=(False, [])),
            mock.patch.object(
                _taskoperations.tempfile,
                "mkdtemp",
                return_value="deterministic-temp"),
            mock.patch.object(_taskoperations, "create_temporary_dry_run_file"),
            mock.patch.object(_taskoperations, "delete_temporary_dry_run_file"),
            mock.patch.object(
                _taskoperations,
                "prepare_source_location",
                return_value="source-location"),
            mock.patch.object(
                _taskoperations,
                "cf_acr_registries_tasks",
                return_value=task_client),
            mock.patch.object(
                _taskoperations,
                "cf_acr_runs",
                return_value=run_client),
            mock.patch.object(
                _taskoperations,
                "get_acr_tasks_models",
                return_value=task_models),
            generate_logs,
            mock.patch.object(
                _taskoperations.WorkflowTaskStatus,
                "remove_internal_acr_statements",
                side_effect=lambda value: value),
            mock.patch.object(
                _taskoperations,
                "_wait_for_terminal_run",
                side_effect=(
                    AzCLIError("Timed out waiting for run 'run-123'")
                    if terminal_run.status == "Running"
                    else None),
                return_value=terminal_run),
            mock.patch.object(_taskoperations, "LongRunningOperation"),
        ]
        with ExitStack() as stack:
            started = [stack.enter_context(patcher) for patcher in patches]
            started[-1].return_value.return_value = SimpleNamespace(run_id="run-123")

            return _taskoperations.acr_cssc_dry_run(
                self.cmd,
                self.registry,
                "config.json",
                is_create=True,
                network_bypass_enabled=True)

    def test_accepted_quick_run_must_reject_every_non_succeeded_terminal_status(self):
        for status in ("Failed", "Canceled", "Error", "Timeout"):
            with self.subTest(status=status):
                terminal_run = SimpleNamespace(
                    run_id="run-123",
                    status=status,
                    run_error_message="validation did not succeed",
                )

                with self.assertRaisesRegex(AzCLIError, "run-123"):
                    self._run_quick_validation(terminal_run)

    def test_succeeded_quick_run_returns_retrievable_logs(self):
        terminal_run = SimpleNamespace(run_id="run-123", status="Succeeded")

        result = self._run_quick_validation(
            terminal_run,
            logs="DRY RUN mode enabled\nMatches found: 1")

        self.assertEqual(
            "DRY RUN mode enabled\nMatches found: 1",
            result)

    def test_succeeded_quick_run_with_missing_logs_fails_closed(self):
        terminal_run = SimpleNamespace(run_id="run-123", status="Succeeded")

        with self.assertRaisesRegex(AzCLIError, "run-123"):
            self._run_quick_validation(terminal_run, logs="")

    def test_asynchronous_known_firewall_failure_has_targeted_diagnostics(self):
        terminal_run = SimpleNamespace(
            run_id="run-123",
            status="Failed",
            error_code="TaskRunRequestNetworkRuleBypassNotAllowed",
            run_error_message=(
                "Registry firewall denied the run; "
                "service correlation ID corr-async-456"
            ),
        )

        with self.assertRaises(AzCLIError) as raised:
            self._run_quick_validation(terminal_run, logs="")

        message = str(raised.exception)
        self.assertIn("run-123", message)
        self.assertIn("firewall", message.lower())
        self.assertIn("bypass", message.lower())
        self.assertIn("corr-async-456", message)
        self.assertIn("no CSSC tasks were created", message)

    def test_unrelated_asynchronous_failure_is_not_reclassified_as_firewall(self):
        terminal_run = SimpleNamespace(
            run_id="run-123",
            status="Failed",
            error_code="AuthorizationFailed",
            run_error_message="Caller lacks action Microsoft.ContainerRegistry/tasks/read",
        )

        with self.assertRaises(AzCLIError) as raised:
            self._run_quick_validation(terminal_run, logs="")

        message = str(raised.exception)
        self.assertIn("run-123", message)
        self.assertIn("Microsoft.ContainerRegistry/tasks/read", message)
        self.assertNotIn("firewall", message.lower())
        self.assertNotIn("network-rule bypass", message.lower())

    def test_quick_run_polling_timeout_is_fatal(self):
        pending_run = SimpleNamespace(run_id="run-123", status="Running")

        with self.assertRaisesRegex(AzCLIError, "run-123"):
            self._run_quick_validation(
                pending_run,
                logs=TimeoutError("polling deadline expired"))

    def test_log_sas_failure_preserves_service_error_and_cause(self):
        service_error = HttpResponseError(
            message="log service unavailable",
            response=SimpleNamespace(
                status_code=503,
                reason="Service Unavailable",
                headers={}))
        client = mock.MagicMock()
        client.get_log_sas_url.side_effect = service_error

        with self.assertRaises(AzCLIError) as raised:
            _taskoperations.WorkflowTaskStatus.generate_logs(
                self.cmd,
                client,
                "run-123",
                self.registry.name,
                "rg")

        self.assertIn("log service unavailable", str(raised.exception))
        self.assertIs(service_error, raised.exception.__cause__)


class QuickRunOutputFailClosedTests(unittest.TestCase):

    def test_successful_run_without_parseable_image_count_fails_closed(self):
        for output in ("", "validation completed", "Matches found: many"):
            with self.subTest(output=output):
                with self.assertRaises(InvalidArgumentValueError):
                    _validators.validate_continuous_patch_v1_image_limit(output)

    @mock.patch.object(cssc, "create_update_continuous_patch_v1")
    @mock.patch.object(cssc, "acr_cssc_dry_run")
    @mock.patch.object(cssc, "prepare_registry_for_workflow")
    @mock.patch.object(cssc, "validate_inputs")
    @mock.patch.object(cssc, "cf_acr_registries")
    def test_malformed_image_count_prevents_artifact_and_task_deployment(
            self,
            cf_registries,
            _validate_inputs,
            prepare_registry,
            dry_run,
            deploy):
        registry = SimpleNamespace(name="registry")
        cf_registries.return_value.get.return_value = registry
        prepare_registry.return_value = {
            "network_bypass_enabled": True,
            "policy_changed": False,
        }
        dry_run.return_value = "terminal run succeeded, but count is malformed"

        with self.assertRaises(InvalidArgumentValueError):
            cssc._perform_continuous_patch_operation(
                SimpleNamespace(cli_ctx=mock.MagicMock()),
                "rg",
                "registry",
                "config.json",
                "1d",
                is_create=True,
                enable_network_bypass=True)

        deploy.assert_not_called()


class ImmediateRunSafetyTests(unittest.TestCase):

    @mock.patch.object(_taskoperations.time, "sleep")
    @mock.patch.object(_taskoperations, "_trigger_task_run")
    def test_immediate_run_attempts_scheduling_before_any_delay(
            self,
            trigger_task_run,
            sleep):
        calls = mock.MagicMock()
        calls.attach_mock(trigger_task_run, "trigger")
        calls.attach_mock(sleep, "sleep")

        _taskoperations._eval_trigger_run(
            SimpleNamespace(),
            SimpleNamespace(name="registry"),
            "rg",
            run_immediately=True)

        self.assertEqual("trigger", calls.mock_calls[0][0])
        sleep.assert_not_called()

    @mock.patch.object(_taskoperations, "cf_acr_runs")
    @mock.patch.object(_taskoperations, "get_acr_tasks_models")
    @mock.patch.object(_taskoperations, "cf_acr_registries_tasks")
    @mock.patch.object(_taskoperations, "LongRunningOperation")
    def test_generic_terminal_failure_is_observed_without_automatic_retry(
            self,
            long_running_operation,
            cf_registry_tasks,
            get_models,
            cf_runs):
        registry = SimpleNamespace(
            id="/subscriptions/sub/resourceGroups/rg/providers/"
               "Microsoft.ContainerRegistry/registries/registry",
            name="registry")
        task_runs = cf_registry_tasks.return_value
        models = get_models.return_value
        models.TaskRunRequest.return_value = mock.sentinel.request
        long_running_operation.return_value.return_value = SimpleNamespace(
            run_id="immediate-run-123")
        cf_runs.return_value.get.return_value = SimpleNamespace(
            run_id="immediate-run-123",
            status="Failed",
            run_error_message="generic task business failure")

        with self.assertRaises(AzCLIError) as raised:
            _taskoperations._trigger_task_run(
                SimpleNamespace(cli_ctx=mock.MagicMock()),
                registry,
                "rg",
                "cssc-trigger-workflow")

        self.assertIn("generic task business failure", str(raised.exception))
        self.assertIn("manual", str(raised.exception).lower())
        task_runs.begin_schedule_run.assert_called_once()

    @mock.patch.object(_taskoperations, "cf_acr_runs")
    @mock.patch.object(_taskoperations, "get_acr_tasks_models")
    @mock.patch.object(_taskoperations, "cf_acr_registries_tasks")
    @mock.patch.object(_taskoperations, "LongRunningOperation")
    def test_unproven_managed_identity_failure_is_not_automatically_retried(
            self,
            long_running_operation,
            cf_registry_tasks,
            get_models,
            cf_runs):
        registry = SimpleNamespace(
            id="/subscriptions/sub/resourceGroups/rg/providers/"
               "Microsoft.ContainerRegistry/registries/registry",
            name="registry")
        task_runs = cf_registry_tasks.return_value
        get_models.return_value.TaskRunRequest.return_value = mock.sentinel.request
        long_running_operation.return_value.return_value = SimpleNamespace(
            run_id="immediate-run-mi")
        cf_runs.return_value.get.return_value = SimpleNamespace(
            run_id="immediate-run-mi",
            status="Failed",
            run_error_message=(
                "ManagedIdentityCredential authentication unavailable"))

        with self.assertRaises(AzCLIError) as raised:
            _taskoperations._trigger_task_run(
                SimpleNamespace(cli_ctx=mock.MagicMock()),
                registry,
                "rg",
                "cssc-trigger-workflow")

        self.assertIn("manual", str(raised.exception).lower())
        task_runs.begin_schedule_run.assert_called_once()


class ConfigureOutputTests(unittest.TestCase):

    @mock.patch.object(_network_bypass, "logger", create=True)
    @mock.patch.object(_network_bypass, "_ensure_role_assignments", return_value=[])
    @mock.patch.object(_network_bypass, "_wait_for_role_assignment_visibility")
    @mock.patch.object(_network_bypass, "_wait_for_principal_id")
    @mock.patch.object(_network_bypass, "get_acr_tasks_models")
    @mock.patch.object(_network_bypass, "enable_registry_network_bypass")
    @mock.patch.object(_network_bypass, "_get_owned_tasks")
    @mock.patch.object(_network_bypass, "cf_acr_tasks")
    def test_configure_reports_arm_convergence_and_warns_data_plane_not_verified(
            self,
            cf_tasks,
            get_owned_tasks,
            enable_bypass,
            _get_models,
            wait_for_principal,
            wait_for_roles,
            _ensure_roles,
            logger):
        registry = SimpleNamespace(
            id="/subscriptions/sub/resourceGroups/rg/providers/"
               "Microsoft.ContainerRegistry/registries/registry",
            name="registry",
            login_server="registry.azurecr.io")
        tasks = {}
        role_assignments = set()
        for task_name, role_modes in _network_bypass.CSSC_TASK_ROLE_IDS.items():
            principal_id = "{}-principal".format(task_name)
            task = SimpleNamespace(
                identity=SimpleNamespace(
                    type="SystemAssigned",
                    principal_id=principal_id),
                credentials=SimpleNamespace(
                    source_registry=SimpleNamespace(login_mode="None"),
                    custom_registries={
                        registry.login_server:
                            SimpleNamespace(identity="[system]")
                    }))
            tasks[task_name] = task
            role_assignments.update(
                (principal_id, role_id.lower())
                for role_id in role_modes["classic"])
        get_owned_tasks.side_effect = [tasks, tasks]
        enable_bypass.return_value = {
            "public_network_access": "Disabled",
            "network_bypass_enabled": True,
            "role_assignment_mode": "rbac",
        }
        wait_for_principal.side_effect = lambda *args: args[-1]
        wait_for_roles.return_value = role_assignments

        result = _network_bypass.configure_existing_workflow_network_bypass(
            SimpleNamespace(cli_ctx=mock.MagicMock()),
            registry)

        self.assertTrue(result["configurationReady"])
        self.assertTrue(result["roleAssignmentsVisibleInArm"])
        self.assertEqual("notVerified", result["dataPlaneAuthorization"])
        self.assertEqual(
            set(_network_bypass.CSSC_TASK_ROLE_IDS),
            {task["name"] for task in result["tasks"]})
        for task in result["tasks"]:
            self.assertTrue(task["configurationReady"])
            self.assertTrue(task["roleAssignmentsVisibleInArm"])
            self.assertEqual("notVerified", task["dataPlaneAuthorization"])
            self.assertNotIn("ready", task)
            self.assertNotIn("rolesReady", task)
        logger.warning.assert_called_once()
        self.assertIn(
            "propagat",
            str(logger.warning.call_args).lower())


class RegistryPolicyTransitionTests(unittest.TestCase):

    def setUp(self):
        self.cmd = SimpleNamespace(cli_ctx=mock.MagicMock())
        self.registry = SimpleNamespace(
            id=(
                "/subscriptions/sub/resourceGroups/rg/providers/"
                "Microsoft.ContainerRegistry/registries/registry"
            ),
            name="registry",
        )

    @mock.patch.object(_network_bypass, "enable_registry_network_bypass")
    @mock.patch.object(_network_bypass, "get_registry_security_state")
    def test_prepare_metadata_reports_policy_changed_by_this_invocation(
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

        result = _network_bypass.prepare_registry_for_workflow(
            self.cmd,
            self.registry,
            explicit_opt_in=True)

        self.assertTrue(result["explicit_opt_in"])
        self.assertTrue(result["confirmed_enabled"])
        self.assertFalse(result["already_enabled"])
        self.assertTrue(result["policy_changed"])

    @mock.patch.object(_network_bypass, "enable_registry_network_bypass")
    @mock.patch.object(_network_bypass, "get_registry_security_state")
    def test_prepare_metadata_reports_policy_was_already_enabled(
            self,
            get_state,
            enable_bypass):
        get_state.return_value = {
            "public_network_access": "Disabled",
            "network_bypass_enabled": True,
            "role_assignment_mode": "rbac",
        }

        result = _network_bypass.prepare_registry_for_workflow(
            self.cmd,
            self.registry,
            explicit_opt_in=True)

        self.assertTrue(result["explicit_opt_in"])
        self.assertTrue(result["confirmed_enabled"])
        self.assertTrue(result["already_enabled"])
        self.assertFalse(result["policy_changed"])
        enable_bypass.assert_not_called()

    @mock.patch.object(_network_bypass, "enable_registry_network_bypass")
    @mock.patch.object(_network_bypass, "get_registry_security_state")
    def test_prepare_does_not_claim_concurrent_policy_change(
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
            "policy_changed": False,
        }

        result = _network_bypass.prepare_registry_for_workflow(
            self.cmd,
            self.registry,
            explicit_opt_in=True)

        self.assertFalse(result["policy_changed"])

    @mock.patch.object(cssc.logger, "warning")
    @mock.patch.object(cssc, "prepare_registry_for_workflow")
    @mock.patch.object(cssc, "validate_inputs")
    @mock.patch.object(cssc, "cf_acr_registries")
    def test_prepare_verification_failure_warns_policy_state_may_be_enabled(
            self,
            cf_registries,
            _validate_inputs,
            prepare_registry,
            warning):
        cf_registries.return_value.get.return_value = self.registry
        original = RuntimeError("registry verification failed")
        original.network_bypass_update_started = True
        prepare_registry.side_effect = original

        with self.assertRaises(RuntimeError) as raised:
            cssc._perform_continuous_patch_operation(
                self.cmd,
                "rg",
                "registry",
                "config.json",
                "1d",
                is_create=True,
                enable_network_bypass=True)

        self.assertIs(original, raised.exception)
        warning.assert_called_once()
        warning_text = " ".join(str(arg) for arg in warning.call_args.args)
        self.assertIn("may remain enabled", warning_text)
        self.assertIn("could not be verified", warning_text)

    @mock.patch.object(_network_bypass, "cf_resources")
    @mock.patch.object(_network_bypass, "get_registry_security_state")
    def test_enable_marks_begin_update_failure_as_uncertain_policy_state(
            self,
            get_state,
            cf_resources):
        get_state.return_value = {
            "public_network_access": "Disabled",
            "network_bypass_enabled": False,
            "role_assignment_mode": "rbac",
        }
        original = RuntimeError("connection lost after request send")
        cf_resources.return_value.resources.begin_update_by_id.side_effect = original

        with self.assertRaises(RuntimeError) as raised:
            _network_bypass.enable_registry_network_bypass(
                self.cmd,
                self.registry)

        self.assertIs(original, raised.exception)
        self.assertTrue(raised.exception.network_bypass_update_started)

    @mock.patch.object(cssc.logger, "warning")
    @mock.patch.object(cssc, "create_update_continuous_patch_v1")
    @mock.patch.object(cssc, "acr_cssc_dry_run")
    @mock.patch.object(cssc, "prepare_registry_for_workflow")
    @mock.patch.object(cssc, "validate_inputs")
    @mock.patch.object(cssc, "cf_acr_registries")
    def test_post_enable_failure_warns_once_and_preserves_original_exception(
            self,
            cf_registries,
            _validate_inputs,
            prepare_registry,
            dry_run,
            deploy,
            warning):
        cf_registries.return_value.get.return_value = self.registry
        prepare_registry.return_value = {
            "network_bypass_enabled": True,
            "explicit_opt_in": True,
            "confirmed_enabled": True,
            "already_enabled": False,
            "policy_changed": True,
        }
        response = mock.sentinel.response
        cause = ValueError("service cause")
        original = RuntimeError("downstream validation failed")
        original.response = response
        original.__cause__ = cause
        dry_run.side_effect = original

        with self.assertRaises(RuntimeError) as raised:
            cssc._perform_continuous_patch_operation(
                self.cmd,
                "rg",
                "registry",
                "config.json",
                "1d",
                is_create=True,
                enable_network_bypass=True)

        self.assertIs(original, raised.exception)
        self.assertIs(response, raised.exception.response)
        self.assertIs(cause, raised.exception.__cause__)
        cf_registries.assert_called_once_with(self.cmd.cli_ctx, None)
        prepare_registry.assert_called_once_with(
            self.cmd,
            self.registry,
            True)
        warning.assert_called_once()
        warning_text = " ".join(str(arg) for arg in warning.call_args.args)
        self.assertIn("remains enabled", warning_text)
        self.assertIn("az resource update --ids", warning_text)
        self.assertIn(self.registry.id, warning_text)
        self.assertIn("--api-version", warning_text)
        self.assertIn("networkRuleBypassAllowedForTasks=false", warning_text)
        deploy.assert_not_called()

    @mock.patch.object(cssc.logger, "warning")
    @mock.patch.object(cssc, "create_update_continuous_patch_v1")
    @mock.patch.object(cssc, "acr_cssc_dry_run")
    @mock.patch.object(cssc, "prepare_registry_for_workflow")
    @mock.patch.object(cssc, "validate_inputs")
    @mock.patch.object(cssc, "cf_acr_registries")
    def test_post_deployment_failure_also_warns_once_without_masking_exception(
            self,
            cf_registries,
            _validate_inputs,
            prepare_registry,
            dry_run,
            deploy,
            warning):
        cf_registries.return_value.get.return_value = self.registry
        prepare_registry.return_value = {
            "network_bypass_enabled": True,
            "explicit_opt_in": True,
            "confirmed_enabled": True,
            "already_enabled": False,
            "policy_changed": True,
        }
        dry_run.return_value = "Matches found: 1"
        original = RuntimeError("artifact publication or deployment failed")
        deploy.side_effect = original

        with self.assertRaises(RuntimeError) as raised:
            cssc._perform_continuous_patch_operation(
                self.cmd,
                "rg",
                "registry",
                "config.json",
                "1d",
                is_create=True,
                enable_network_bypass=True)

        self.assertIs(original, raised.exception)
        cf_registries.assert_called_once_with(self.cmd.cli_ctx, None)
        prepare_registry.assert_called_once_with(
            self.cmd,
            self.registry,
            True)
        warning.assert_called_once()

    @mock.patch.object(cssc.logger, "warning")
    @mock.patch.object(cssc, "create_update_continuous_patch_v1")
    @mock.patch.object(cssc, "acr_cssc_dry_run")
    @mock.patch.object(cssc, "prepare_registry_for_workflow")
    @mock.patch.object(cssc, "validate_inputs")
    @mock.patch.object(cssc, "cf_acr_registries")
    def test_post_failure_warns_policy_was_already_enabled_without_disable_command(
            self,
            cf_registries,
            _validate_inputs,
            prepare_registry,
            dry_run,
            deploy,
            warning):
        cf_registries.return_value.get.return_value = self.registry
        prepare_registry.return_value = {
            "network_bypass_enabled": True,
            "explicit_opt_in": True,
            "confirmed_enabled": True,
            "already_enabled": True,
            "policy_changed": False,
        }
        dry_run.side_effect = RuntimeError("downstream validation failed")

        with self.assertRaises(RuntimeError):
            cssc._perform_continuous_patch_operation(
                self.cmd,
                "rg",
                "registry",
                "config.json",
                "1d",
                is_create=True,
                enable_network_bypass=True)

        warning.assert_called_once()
        warning_text = " ".join(str(arg) for arg in warning.call_args.args)
        self.assertIn("already enabled", warning_text)
        self.assertNotIn("networkRuleBypassAllowedForTasks=false", warning_text)
        deploy.assert_not_called()


if __name__ == "__main__":
    unittest.main()
