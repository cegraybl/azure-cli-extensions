# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

# want it to be something where we can store the status of a image processed by the workflow
# contain both the image, scanning status and patching status

import time
import re
from ._constants import (
    CONTINUOSPATCH_TASK_PATCHIMAGE_NAME,
    CONTINUOSPATCH_TASK_SCANIMAGE_NAME,
    TASK_RUN_STATUS_RUNNING,
    TASK_RUN_STATUS_FAILED,
    TASK_RUN_STATUS_SUCCESS,
    RESOURCE_GROUP,
    CSSCTaskTypes)
from azure.cli.command_modules.acr._constants import ACR_RUN_DEFAULT_TIMEOUT_IN_SEC
from azure.cli.command_modules.acr._stream_utils import _get_run_status
from azure.cli.core.profiles import ResourceType, get_sdk
from azure.cli.command_modules.acr._azure_utils import get_blob_info
from azure.cli.core.azclierror import AzCLIError
from knack.log import get_logger
from enum import Enum
from msrestazure.azure_exceptions import CloudError
from azure.mgmt.core.tools import parse_resource_id

logger = get_logger(__name__)


class WorkflowTaskState(Enum):
    RUNNING = "Running"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    QUEUED = "Queued"
    UNKNOWN = "Unknown"


class WorkflowTaskStatus:
    def __init__(self, image):
        repo = image.split(':')
        # string to represent the repository
        self.repository = repo[0]

        # string to represent the tag, can be '*', but for that we should be splitting it to a specific tag per repository
        # if tag == '*':
        #     raise ValueError('Unsupported, for tag "*" please split it to a specific tag per repository')
        self.tag = repo[1]

        # latest taskrun object for the scan task, if none, means no scan task has been run
        self.scan_task = None

        # not sure if we should be proactive to get the logs for the scan task, but it will also be that we don't know that the scan task has been run until we check for logs
        self.scan_logs = ""

        # latest taskrun object for the patch task, if none, means no patch task has been run
        self.patch_task = None

        # ditto for patch logs, we don't know if the patch task has been run until we check for logs
        self.patch_logs = ""

    def is_wildcard(self):
        return self.tag == '*'

    def image(self):
        if self.is_wildcard():
            return self.repository
        return f"{self.repository}:{self.tag}"

    # task run status from src\ACR.Build.Contracts\src\Status.cs
    @staticmethod
    def _task_status_to_workflow_status(task):
        if task.status == "succeeded":
            return WorkflowTaskState.SUCCEEDED

        if task.status == "running" or task.status == "started":
            return WorkflowTaskState.RUNNING

        if task.status == "queued":
            return WorkflowTaskState.QUEUED

        if task.status == "failed" or task.status == "canceled" or task.status == "error" or task.status == "timeout":
            return WorkflowTaskState.FAILED

        return WorkflowTaskState.UNKNOWN

    def scan_status(self):
        return WorkflowTaskStatus._task_status_to_workflow_status(self.scan_task)

    def patch_status(self):
        return WorkflowTaskStatus._task_status_to_workflow_status(self.patch_task)

    # this extracts the image from the copacetic task logs, using this when we only have a repository name and a wildcard tag
    @staticmethod
    def _get_image_from_tasklog(logs, task_type):
        if task_type == CONTINUOSPATCH_TASK_SCANIMAGE_NAME:
            match = re.search(r'Scanning image for vulnerability and patch (\S+)', logs)
        if task_type == CONTINUOSPATCH_TASK_PATCHIMAGE_NAME:
            match = re.search(r'Scan, Upload scan report and Schedule Patch for (\S+)', logs)

        if match:
            return match.group(1)
        return None

    @staticmethod
    def _from_taskrun_wildcard(cmd, taskrun_client, registry, repository, taskruns):
        all_status = {}

        for taskrun in taskruns:
            if hasattr(taskrun, 'task_log_result'):
                tasklog = taskrun.task_log_result
            else:
                tasklog = WorkflowTaskStatus.get_logs_for_taskrun(cmd, taskrun_client, taskrun, registry)
                taskrun.task_log_result = tasklog

            if repository in tasklog:
                image = WorkflowTaskStatus._get_image_from_tasklog(tasklog, taskrun.task)
                if all_status.get(image) is None:
                    all_status[image] = WorkflowTaskStatus(image)

                status = all_status[image]
                if taskrun.task == CONTINUOSPATCH_TASK_SCANIMAGE_NAME:
                    latest_run, latest_log = WorkflowTaskStatus._latest_task(status.scan_task, status.scan_logs, taskrun, tasklog)
                    all_status[image].scan_task = latest_run
                    all_status[image].scan_logs = latest_log
                elif taskrun.task == CONTINUOSPATCH_TASK_PATCHIMAGE_NAME:
                    latest_run = WorkflowTaskStatus._latest_task(status.patch_task, status.patch_logs, taskrun, tasklog)
                    all_status[image].patch_task = latest_run
                    all_status[image].patch_logs = latest_log

        return all_status.values()

    @staticmethod
    def _latest_task(this_task, this_log, that_task, that_log):
        if this_task is None:
            return (that_task, that_log)
        if that_task is None:
            return (this_task, this_log)
        return (this_task, this_log) if this_task.create_time > that_task.create_time else (that_task, that_log)
        
    # a different/better approach might be to have this functions just get all tasks/logs related to the image, and then have a different function to filter it to the latest logs
    # that way with images with a '*' tag we can deffer the filtering to a later stage
    # for now leave it as is, get a single log, we will figure it out later. Just want to see it working
    @staticmethod
    def from_taskrun(cmd, taskrun_client, registry, image, taskruns):
        if str(image).endswith(':*'):
            repository = image.split(':')[0]
            return WorkflowTaskStatus._from_taskrun_wildcard(cmd, taskrun_client, registry, repository, taskruns)
        
        status = WorkflowTaskStatus(image)
        has_scan = False
        has_patch = False
        
        ####
        #how to model/represent when we have a '*' tag and have to serve multiple status?
        for taskrun in taskruns:
            if hasattr(taskrun, 'task_log_result'):
                tasklog = taskrun.task_log_result
            else:
                tasklog = WorkflowTaskStatus.get_logs_for_taskrun(cmd, taskrun_client, taskrun, registry)
                taskrun.task_log_result = tasklog
            # we also need to check if we already have logs, and if they are newer than the ones we have found previously
            # need to match the status from the run to the Enum we have here. mostly matching what is in C:\git\build1\src\ACR.Build.Contracts\src\Status.cs -- taskrun.status
            image = status.image()
            if image in tasklog:
                taskruns.remove(taskrun)
                if taskrun.task == CONTINUOSPATCH_TASK_SCANIMAGE_NAME:
                    latest_run, latest_log = WorkflowTaskStatus._latest_task(status.scan_task, status.scan_logs, taskrun, tasklog)
                    status.scan_task = latest_run
                    status.scan_logs = latest_log
                    has_scan = True
                elif taskrun.task == CONTINUOSPATCH_TASK_PATCHIMAGE_NAME:
                    latest_run = WorkflowTaskStatus._latest_task(status.patch_task, status.patch_logs, taskrun, tasklog)
                    status.patch_task = latest_run
                    status.patch_logs = latest_log
                    has_patch = True

            if has_scan and has_patch:
                break

        # possible optimization, remove the tasks we have already processed from the list to reduce the number of taskruns we have to go through for future iterations/objects
        # the problem is that taskruns is an iterator, so we can't remove elements from it
        ## copy it to a list?
        return status

    def __str__(self) -> str:
        scan_status = self.scan_status()
        scan_date = "" if self.scan_task is None else self.scan_task.create_time
        scan_task_id = "" if self.scan_task is None else self.scan_task.run_id
        patch_status = self.patch_status()
        patch_date = "" if self.patch_task is None else self.patch_task.create_time
        patch_task_id = "" if self.patch_task is None else self.patch_task.run_id
        patched_image = f"{self.repository}:{self.tag}-1" if self.patch_task is not None else "" # fix this one
        workflow_type = CSSCTaskTypes.ContinuousPatchV1.value

        return f"image: {self.repository}:{self.tag}\n" \
               f"\tscan status: {scan_status}\n" \
               f"\tscan date: {scan_date}\n" \
               f"\tscan task ID: {scan_task_id}\n" \
               f"\tpatch status: {patch_status}\n" \
               f"\tpatch date: {patch_date}\n" \
               f"\tpatch task ID: {patch_task_id}\n" \
               f"\tpatched image: {patched_image}\n" \
               f"\tworkflow type: {workflow_type}"
        

    # i want to improve on this, don't want to do an output redirect always, need change it to have it both ways (stream and string)
    @staticmethod
    def get_logs_for_taskrun(cmd, taskrun_client, taskrun, registry):
        import io
        from contextlib import redirect_stdout

        f = io.StringIO()
        with redirect_stdout(f):
            resource_group = parse_resource_id(registry.id)[RESOURCE_GROUP]
            WorkflowTaskStatus.generate_logs(cmd, taskrun_client, taskrun.run_id, registry.name, resource_group)
        return f.getvalue()


    @staticmethod
    def generate_logs(
        cmd,
        client,
        run_id,
        registry_name,
        resource_group_name,
        timeout=ACR_RUN_DEFAULT_TIMEOUT_IN_SEC,
        print_output=True):

        log_file_sas = None
        error_msg = "Could not get logs for ID: {}".format(run_id)
        try:
            response = client.get_log_sas_url(
                resource_group_name=resource_group_name,
                registry_name=registry_name,
                run_id=run_id)
            log_file_sas = response.log_link
        except (AttributeError, CloudError) as e:
            logger.debug("%s Exception: %s", error_msg, e)
            raise AzCLIError(error_msg)

        account_name, endpoint_suffix, container_name, blob_name, sas_token = get_blob_info(
            log_file_sas)
        AppendBlobService = get_sdk(cmd.cli_ctx, ResourceType.DATA_STORAGE, 'blob#AppendBlobService')
        if not timeout:
            timeout = ACR_RUN_DEFAULT_TIMEOUT_IN_SEC

        run_status = TASK_RUN_STATUS_RUNNING
        while WorkflowTaskStatus._evaluate_task_run_nonterminal_state(run_status):
            run_status = WorkflowTaskStatus._get_run_status_local(client, resource_group_name, registry_name, run_id)
            if WorkflowTaskStatus._evaluate_task_run_nonterminal_state(run_status):
                logger.debug("Waiting for the task run to complete. Current status: %s", run_status)
                time.sleep(2)

        WorkflowTaskStatus._download_logs(
            AppendBlobService(
                account_name=account_name,
                sas_token=sas_token,
                endpoint_suffix=endpoint_suffix),
            container_name,
            blob_name,
            print_output=print_output)
    
    @staticmethod
    def _evaluate_task_run_nonterminal_state(run_status):
        return run_status != TASK_RUN_STATUS_SUCCESS and run_status != TASK_RUN_STATUS_FAILED
    
    @staticmethod
    def _get_run_status_local(client, resource_group_name, registry_name, run_id):
        try:
            response = client.get(resource_group_name, registry_name, run_id)
            return response.status
        except (AttributeError, CloudError):
            return None

    @staticmethod
    def _download_logs(blob_service,
                       container_name,
                       blob_name,
                       print_output):
        blob_text = blob_service.get_blob_to_text(
            container_name=container_name,
            blob_name=blob_name)
        WorkflowTaskStatus._remove_internal_acr_statements(blob_text.content, print_output)

    @staticmethod
    def _remove_internal_acr_statements(blob_content, print_output):
        lines = blob_content.split("\n")
        starting_identifier = "DRY RUN mode enabled"
        terminating_identifier = "Total matches found"
        print_line = False
        output = ""

        for line in lines:
            if line.startswith(starting_identifier):
                print_line = True
            elif line.startswith(terminating_identifier):
                if print_output:
                    print(line)
                else:
                    output = output + "\n" + line
                print_line = False

            if print_output:
                print(line)
            else:
                output = output + "\n" + line
