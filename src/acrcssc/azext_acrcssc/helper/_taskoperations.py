# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------
# pylint: disable=line-too-long
# pylint: disable=broad-exception-caught
import base64
import os
import tempfile
import time
from knack.log import get_logger
from ._constants import (
    CONTINUOSPATCH_DEPLOYMENT_NAME,
    CONTINUOSPATCH_DEPLOYMENT_TEMPLATE,
    CONTINUOSPATCH_ALL_TASK_NAMES,
    CONTINUOSPATCH_TASK_DEFINITION,
    CONTINUOSPATCH_TASK_SCANREGISTRY_NAME,
    RESOURCE_GROUP,
    CONTINUOSPATCH_OCI_ARTIFACT_CONFIG,
    CONTINUOSPATCH_OCI_ARTIFACT_CONFIG_TAG_V1,
    TMP_DRY_RUN_FILE_NAME,
    CONTINUOUS_PATCHING_WORKFLOW_NAME,
    CSSC_WORKFLOW_POLICY_REPOSITORY,
    CONTINUOUS_PATCH_WORKFLOW,
    CONTINUOSPATCH_TASK_PATCHIMAGE_NAME,
    CONTINUOSPATCH_TASK_SCANIMAGE_NAME,
    DESCRIPTION)
from azure.cli.core.azclierror import AzCLIError
from azure.cli.core.commands import LongRunningOperation
from azure.cli.command_modules.acr._utils import prepare_source_location
from azure.mgmt.core.tools import parse_resource_id
from azext_acrcssc._client_factory import cf_acr_tasks, cf_authorization, cf_acr_registries_tasks, cf_acr_runs
from azext_acrcssc.helper._deployment import validate_and_deploy_template
from azext_acrcssc.helper._ociartifactoperations import create_oci_artifact_continuous_patch, get_oci_artifact_continuous_patch, delete_oci_artifact_continuous_patch
from azext_acrcssc._validators import check_continuous_task_exists
from ._utility import convert_timespan_to_cron, transform_cron_to_cadence, create_temporary_dry_run_file, delete_temporary_dry_run_file
from ._workflow_status import WorkflowTaskStatus
import datetime
import time

logger = get_logger(__name__)
DEFAULT_CHUNK_SIZE = 1024 * 4


def create_update_continuous_patch_v1(cmd, registry, cssc_config_file, cadence, dryrun, defer_immediate_run, is_create_workflow=True):
    logger.debug("Entering continuousPatchV1_creation %s %s %s", cssc_config_file, dryrun, defer_immediate_run)
    resource_group = parse_resource_id(registry.id)[RESOURCE_GROUP]
    schedule_cron_expression = None
    if cadence is not None:
        schedule_cron_expression = convert_timespan_to_cron(cadence)
    logger.debug("converted cadence to cron expression: %s", schedule_cron_expression)
    cssc_tasks_exists = check_continuous_task_exists(cmd, registry)
    if is_create_workflow:
        if cssc_tasks_exists:
            raise AzCLIError(f"{CONTINUOUS_PATCHING_WORKFLOW_NAME} workflow task already exists. Use 'az acr supply-chain workflow update' command to perform updates.")
        _create_cssc_workflow(cmd, registry, schedule_cron_expression, resource_group, dryrun)
    else:
        if not cssc_tasks_exists:
            raise AzCLIError(f"{CONTINUOUS_PATCHING_WORKFLOW_NAME} workflow task does not exist. Use 'az acr supply-chain workflow create' command to create {CONTINUOUS_PATCHING_WORKFLOW_NAME} workflow.")
        _update_cssc_workflow(cmd, registry, schedule_cron_expression, resource_group, dryrun)

    if cssc_config_file is not None:
        create_oci_artifact_continuous_patch(registry, cssc_config_file, dryrun)
        logger.debug("Uploading of %s completed successfully.", cssc_config_file)

    _eval_trigger_run(cmd, registry, resource_group, defer_immediate_run)


def _create_cssc_workflow(cmd, registry, schedule_cron_expression, resource_group, dry_run):
    parameters = {
        "AcrName": {"value": registry.name},
        "AcrLocation": {"value": registry.location},
        "taskSchedule": {"value": schedule_cron_expression}
    }

    for task in CONTINUOSPATCH_TASK_DEFINITION:
        encoded_task = {"value": _create_encoded_task(CONTINUOSPATCH_TASK_DEFINITION[task]["template_file"])}
        param_name = CONTINUOSPATCH_TASK_DEFINITION[task]["parameter_name"]
        parameters[param_name] = encoded_task

    validate_and_deploy_template(
        cmd.cli_ctx,
        registry,
        resource_group,
        CONTINUOSPATCH_DEPLOYMENT_NAME,
        CONTINUOSPATCH_DEPLOYMENT_TEMPLATE,
        parameters,
        dry_run
    )

    logger.warning("Deployment of %s tasks completed successfully.", CONTINUOUS_PATCHING_WORKFLOW_NAME)


def _update_cssc_workflow(cmd, registry, schedule_cron_expression, resource_group, dry_run):
    if schedule_cron_expression is not None:
        _update_task_schedule(cmd, registry, schedule_cron_expression, resource_group, dry_run)


def _eval_trigger_run(cmd, registry, resource_group, defer_immediate_run):
    if not defer_immediate_run:
        logger.warning('Triggering the %s to run immediately', CONTINUOSPATCH_TASK_SCANREGISTRY_NAME)
        # Seen Managed Identity taking time, see if there can be an alternative (one alternative is to schedule the cron expression with delay)
        # NEED TO SKIP THE TIME.SLEEP IN UNIT TEST CASE OR FIND AN ALTERNATIVE SOLUITION TO MI COMPLETE
        time.sleep(30)
        _trigger_task_run(cmd, registry, resource_group, CONTINUOSPATCH_TASK_SCANREGISTRY_NAME)


def delete_continuous_patch_v1(cmd, registry, dryrun):
    logger.debug("Entering delete_continuous_patch_v1")
    cssc_tasks_exists = check_continuous_task_exists(cmd, registry)
    if not dryrun and cssc_tasks_exists:
        cssc_tasks = ', '.join(CONTINUOSPATCH_ALL_TASK_NAMES)
        logger.warning("All of these tasks will be deleted: %s", cssc_tasks)
        for taskname in CONTINUOSPATCH_ALL_TASK_NAMES:
            # bug: if one of the deletion fails, the others will not be attempted, we need to attempt to delete all of them
            _delete_task(cmd, registry, taskname, dryrun)
            logger.warning("Task %s deleted.", taskname)

    if not cssc_tasks_exists:
        logger.warning("%s workflow task does not exist", CONTINUOUS_PATCHING_WORKFLOW_NAME)

    logger.warning("Deleting %s/%s:%s", CSSC_WORKFLOW_POLICY_REPOSITORY, CONTINUOSPATCH_OCI_ARTIFACT_CONFIG, CONTINUOSPATCH_OCI_ARTIFACT_CONFIG_TAG_V1)
    delete_oci_artifact_continuous_patch(cmd, registry, dryrun)


def list_continuous_patch_v1(cmd, registry):
    logger.debug("Entering list_continuous_patch_v1")

    if not check_continuous_task_exists(cmd, registry):
        logger.warning("%s workflow task does not exist. Run 'az acr supply-chain workflow create' to create workflow tasks", CONTINUOUS_PATCHING_WORKFLOW_NAME)
        return

    acr_task_client = cf_acr_tasks(cmd.cli_ctx)
    resource_group_name = parse_resource_id(registry.id)[RESOURCE_GROUP]
    tasks_list = acr_task_client.list(resource_group_name, registry.name)
    filtered_cssc_tasks = _transform_task_list(tasks_list)
    return filtered_cssc_tasks


def acr_cssc_dry_run(cmd, registry, config_file_path):
    logger.debug("Entering acr_cssc_dry_run with parameters: %s %s", registry, config_file_path)

    if config_file_path is None:
        logger.error("--config parameter is needed to perform dry-run check.")
        return
    try:
        file_name = os.path.basename(config_file_path)
        tmp_folder = os.path.join(os.getcwd(), tempfile.mkdtemp(prefix="cli_temp_cssc"))
        create_temporary_dry_run_file(config_file_path, tmp_folder)

        resource_group_name = parse_resource_id(registry.id)[RESOURCE_GROUP]
        acr_registries_task_client = cf_acr_registries_tasks(cmd.cli_ctx)
        acr_run_client = cf_acr_runs(cmd.cli_ctx)
        source_location = prepare_source_location(
            cmd,
            tmp_folder,
            acr_registries_task_client,
            registry.name,
            resource_group_name)

        OS = acr_run_client.models.OS
        Architecture = acr_run_client.models.Architecture

        # TODO: when the extension merges back into the acr module, we need to reuse the 'get_validate_platform()' from ACR modules (src\azure-cli\azure\cli\command_modules\acr\_utils.py)
        platform_os = OS.linux.value
        platform_arch = Architecture.amd64.value
        platform_variant = None

        value_pair = [{"name": "CONFIGPATH", "value": f"{file_name}"}]
        request = acr_registries_task_client.models.FileTaskRunRequest(
            task_file_path=TMP_DRY_RUN_FILE_NAME,
            values_file_path=None,
            values=value_pair,
            source_location=source_location,
            timeout=None,
            platform=acr_registries_task_client.models.PlatformProperties(
                os=platform_os,
                architecture=platform_arch,
                variant=platform_variant
            ),
            credentials=_get_custom_registry_credentials(cmd, auth_mode=None),
            agent_pool_name=None,
            log_template=None
        )

        queued = LongRunningOperation(cmd.cli_ctx)(acr_registries_task_client.begin_schedule_run(
            resource_group_name=resource_group_name,
            registry_name=registry.name,
            run_request=request))
        run_id = queued.run_id
        logger.warning("Performing dry-run check for filter policy using acr task run id: %s", run_id)
        return WorkflowTaskStatus.generate_logs(cmd, acr_run_client, run_id, registry.name, resource_group_name)
    finally:
        delete_temporary_dry_run_file(tmp_folder)


def cancel_continuous_patch_runs(cmd, resource_group_name, registry_name):
    logger.debug("Entering cancel_continuous_patch_v1")
    acr_task_run_client = cf_acr_runs(cmd.cli_ctx)
    list_filter_str = f"(contains(['Running', 'Queued', 'Started'], Status)) and (contains(['{CONTINUOSPATCH_TASK_SCANREGISTRY_NAME}', '{CONTINUOSPATCH_TASK_SCANIMAGE_NAME}', '{CONTINUOSPATCH_TASK_PATCHIMAGE_NAME}'], TaskName))"
    top = 1000
    running_tasks = acr_task_run_client.list(resource_group_name, registry_name, filter=list_filter_str, top=top)
    for task in running_tasks:
        logger.warning("Sending request to cancel task %s", task.name)
        acr_task_run_client.begin_cancel(resource_group_name, registry_name, task.name)
    logger.warning("All active running patching tasks with have been cancelled.")


def track_scan_progress(cmd, resource_group_name, registry_name, status):
    logger.debug("Entering track_scan_progress")
    acr_task_run_client = cf_acr_runs(cmd.cli_ctx)
    list_filter_str = f"Status eq '{status}' and (contains(['{CONTINUOSPATCH_TASK_SCANREGISTRY_NAME}', '{CONTINUOSPATCH_TASK_SCANIMAGE_NAME}', '{CONTINUOSPATCH_TASK_PATCHIMAGE_NAME}'], TaskName))"
    top = 1000
    return acr_task_run_client.list(resource_group_name, registry_name, filter=list_filter_str, top=top)


# don't want to destory existing work, will use this function to fix functionality and then point to the new function if it works
def track_scan_progress_newer(cmd, resource_group_name, registry, status):
    logger.debug("Entering track_scan_progress_newer")

    config = get_oci_artifact_continuous_patch(cmd, registry)
    enabled_images = config.get_enabled_images()
    return _retrieve_logs_for_image(cmd, registry, resource_group_name, enabled_images, config.cadence)


def _retrieve_logs_for_image(cmd, registry, resource_group_name, images, cadence):
    image_status = []
    acr_task_run_client = cf_acr_runs(cmd.cli_ctx)

    # get all the tasks executed since the last cadence, add a day to make sure we are not running into and edge case with the date
    today = datetime.date.today()
    # delta = datetime.timedelta(days=int(cadence) + 1)
    delta = datetime.timedelta(days=int(cadence)) # use the cadence as is, we are running into issues we are querying too much and take time to filter
    previous_date = today - delta
    previous_date_filter = previous_date.strftime('%Y-%m-%dT%H:%M:%SZ')

    list_filter_str = f"TaskName in ('{CONTINUOSPATCH_TASK_SCANIMAGE_NAME}', '{CONTINUOSPATCH_TASK_PATCHIMAGE_NAME}') and createTime ge {previous_date_filter}"
    # filters found in ACR.BuildRP.DataModels\src\v18_09_GA\RunFilter.cs

    top = 1000
    # th API returns an iterator, if we want to be able to modify it, we need to convert it to a list
    taskruns = acr_task_run_client.list(resource_group_name, registry.name, filter=list_filter_str, top=top)
    taskruns_list = list(taskruns)

    ## another way that we might speed things up is, instead of doing n^2 check while matching image to task, is to get all the logs, and start populating with the logs that we have, and use that to match the image to the task, instead than the task to the image
    ## one more way, get all the logs first (multiple threads), and then match the logs to the images, this way we can get the logs faster, and then match them to the images however we want
    ## another thing, separate the patch and the scan logs, no need to double the search space just for the hell of it. The scanning task will have info on the patching task (runid it trigerred), so we can use that to match the logs and make it easier
    start_time = time.time()

    for image in images:
        image_status += WorkflowTaskStatus.from_taskrun(cmd, acr_task_run_client, registry, image, taskruns_list)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time} seconds / images processed: {len(images)} / tasks filtered: {len(taskruns_list)}")
    return image_status


def _trigger_task_run(cmd, registry, resource_group, task_name):
    acr_task_registries_client = cf_acr_registries_tasks(cmd.cli_ctx)
    request = acr_task_registries_client.models.TaskRunRequest(
        task_id=f"{registry.id}/tasks/{task_name}")
    queued_run = LongRunningOperation(cmd.cli_ctx)(
        acr_task_registries_client.begin_schedule_run(
            resource_group,
            registry.name,
            request))
    run_id = queued_run.run_id
    print(f"Queued {CONTINUOUS_PATCHING_WORKFLOW_NAME} workflow task '{task_name}' with run ID: {run_id}. Use 'az acr task logs' to view the logs.")


def _create_encoded_task(task_file):
    # this is a bit of a hack, but we need to fix the path to the task's yaml,
    # relative paths don't work because we don't control where the az cli is running from
    templates_path = os.path.dirname(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../templates/"))

    with open(os.path.join(templates_path, task_file), "rb") as f:
        base64_content = base64.b64encode(f.read())
        return base64_content.decode('utf-8')


def _update_task_schedule(cmd, registry, cron_expression, resource_group_name, dryrun):
    logger.debug("converted cadence to cron_expression: %s", cron_expression)
    acr_task_client = cf_acr_tasks(cmd.cli_ctx)
    taskUpdateParameters = acr_task_client.models.TaskUpdateParameters(
        trigger=acr_task_client.models.TriggerUpdateParameters(
            timer_triggers=[
                acr_task_client.models.TimerTriggerUpdateParameters(
                    name='azcli_defined_schedule',
                    schedule=cron_expression
                )
            ]
        )
    )

    if dryrun:
        logger.debug("Dry run, skipping the update of the task schedule")
        return None
    try:
        acr_task_client.begin_update(resource_group_name, registry.name,
                                     CONTINUOSPATCH_TASK_SCANREGISTRY_NAME,
                                     taskUpdateParameters)
        print("Cadence has been successfully updated.")
    except Exception as exception:
        raise AzCLIError(f"Failed to update the task schedule: {exception}")


def _delete_task(cmd, registry, task_name, dryrun):
    logger.debug("Entering delete_task")
    resource_group = parse_resource_id(registry.id)[RESOURCE_GROUP]

    try:
        acr_tasks_client = cf_acr_tasks(cmd.cli_ctx)
        _delete_task_role_assignment(cmd.cli_ctx, acr_tasks_client, registry, resource_group, task_name, dryrun)

        if dryrun:
            logger.debug("Dry run, skipping deletion of the task: %s ", task_name)
            return None
        logger.debug("Deleting task %s", task_name)
        LongRunningOperation(cmd.cli_ctx)(
            acr_tasks_client.begin_delete(
                resource_group,
                registry.name,
                task_name))

    except Exception as exception:
        raise AzCLIError("Failed to delete task %s from registry %s : %s" % task_name, registry.name, exception)

    logger.debug("Task %s deleted successfully", task_name)


def _delete_task_role_assignment(cli_ctx, acrtask_client, registry, resource_group, task_name, dryrun):
    role_client = cf_authorization(cli_ctx)
    acrtask_client = cf_acr_tasks(cli_ctx)

    task = acrtask_client.get(resource_group, registry.name, task_name)
    identity = task.identity

    if identity:
        assigned_roles = role_client.role_assignments.list_for_scope(
            registry.id,
            filter=f"principalId eq '{identity.principal_id}'"
        )

        for role in assigned_roles:
            if dryrun:
                logger.debug("Dry run, skipping deletion of role assignments, task: %s, role name: %s", task_name, role.name)
                return None
            logger.debug("Deleting role assignments of task %s from the registry", task_name)
            role_client.role_assignments.delete(
                scope=registry.id,
                role_assignment_name=role.name
            )


def _transform_task_list(tasks):
    transformed = []
    for task in tasks:
        transformed_obj = {
            "creationDate": task.creation_date,
            "location": task.location,
            "name": task.name,
            "provisioningState": task.provisioning_state,
            "systemData": task.system_data,
            "cadence": None,
            "description": CONTINUOUS_PATCH_WORKFLOW[task.name][DESCRIPTION]
        }

        # Extract cadence from trigger.timerTriggers if available
        trigger = task.trigger
        if trigger and trigger.timer_triggers:
            transformed_obj["cadence"] = transform_cron_to_cadence(trigger.timer_triggers[0].schedule)
        transformed.append(transformed_obj)

    return transformed


def _get_custom_registry_credentials(cmd,
                                     auth_mode=None,
                                     login_server=None,
                                     username=None,
                                     password=None,
                                     identity=None,
                                     is_remove=False):
    """Get the credential object from the input
    :param str auth_mode: The login mode for the source registry
    :param str login_server: The login server of custom registry
    :param str username: The username for custom registry (plain text or a key vault secret URI)
    :param str password: The password for custom registry (plain text or a key vault secret URI)
    :param str identity: The task managed identity used for the credential
    """
    acr_tasks_client = cf_acr_tasks(cmd.cli_ctx)
    source_registry_credentials = None
    if auth_mode:
        source_registry_credentials = acr_tasks_client.models.SourceRegistryCredentials(
            login_mode=auth_mode)

    custom_registries = None
    if login_server:
        # if null username and password (or identity), then remove the credential
        custom_reg_credential = None

        is_identity_credential = False
        if not username and not password:
            is_identity_credential = identity is not None

        if not is_remove:
            if is_identity_credential:
                custom_reg_credential = acr_tasks_client.models.CustomRegistryCredentials(
                    identity=identity
                )
            else:
                custom_reg_credential = acr_tasks_client.models.CustomRegistryCredentials(
                    user_name=acr_tasks_client.models.SecretObject(
                        type=acr_tasks_client.models.SecretObjectType.vaultsecret if _is_vault_secret(
                            cmd, username)else acr_tasks_client.models.SecretObjectType.opaque,
                        value=username
                    ),
                    password=acr_tasks_client.models.SecretObject(
                        type=acr_tasks_client.models.SecretObjectType.vaultsecret if _is_vault_secret(
                            cmd, password) else acr_tasks_client.models.SecretObjectType.opaque,
                        value=password
                    ),
                    identity=identity
                )

        custom_registries = {login_server: custom_reg_credential}

    return acr_tasks_client.models.Credentials(
        source_registry=source_registry_credentials,
        custom_registries=custom_registries
    )


def _is_vault_secret(cmd, credential):
    keyvault_dns = None
    try:
        keyvault_dns = cmd.cli_ctx.cloud.suffixes.keyvault_dns
    except Exception:
        return False
    if credential is not None:
        return keyvault_dns.upper() in credential.upper()
    return False
