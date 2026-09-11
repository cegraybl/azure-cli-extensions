Microsoft Azure CLI 'acrcssc' Extension
==========================================

Azure Container Registry - Container Secure Supply Chain (Continuous Patching)
==========================================

Overview
========
The `acrcssc` extension for Azure CLI provides continuous patching capabilities for Azure Container Registry (ACR). This extension helps automate the process of scanning and patching container images to ensure they are up-to-date with the latest security patches. Scans your configured list of images for vulnerabilities (CVEs) using Trivy and patch them using Copacetic.

Preview Limitations
===================
Continuous Patching is currently in preview. The following limitations apply:

- Windows-based container images aren’t supported
- Only "OS-level" vulnerabilities will be patched. This includes packages in the image managed by a package manager such as “apt” and “yum”. Vulnerabilities at the “application level” are unable to be patched, such as compiled languages like Go, Python, NodeJS
- Patching is only supported in Public regions, not in Sovereign regions
- CSSC patching is not supported for registries or in regions where Tasks are unavailable.

Features
========
- **Continuous Patching Workflow**: Automates the process of scanning and patching container images.
- **Task Management**: Create, update, delete, show, and cancel continuous patch tasks in the registry.
- **Dry Run Mode**: Validate the configuration without making any changes.
- **Immediate Run**: Trigger the patching workflow immediately.
- **Run Status**: Monitor the status of the scanning and patching tasks.
- **Network-restricted Registries**: Configure task system identities, credentials,
  and least-privilege roles for ACR Tasks network-rule bypass.

Commands
========
- `az acr supply-chain workflow create`: Create a continuous patch task in the registry.
- `az acr supply-chain workflow configure-network-bypass`: Configure an existing
  workflow for ACR Tasks network-rule bypass.
- `az acr supply-chain workflow update`: Update an existing continuous patch task.
- `az acr supply-chain workflow delete`: Delete a continuous patch task.
- `az acr supply-chain workflow list`: List all continuous patch tasks in the registry.
- `az acr supply-chain workflow show`: Show details of a specific continuous patch task.
- `az acr supply-chain workflow cancel-run`: Cancel all running scan and patch tasks.

Usage
=====
1. **Create a Continuous Patch Task**:
   ```sh
   az acr supply-chain workflow create --resource-group <resource-group> --registry <registry-name> --type continuouspatchv1 --schedule <schedule> --config <config-file>
   ```

1. **Create on a network-restricted registry**:

   The ``--enable-network-bypass`` flag explicitly enables
   ``networkRuleBypassAllowedForTasks`` on the registry. This permits ACR Tasks
   that authenticate with their system-assigned identities to bypass registry
   network rules; it does not enable public network access.

   ```sh
   az acr supply-chain workflow create --resource-group <resource-group> --registry <registry-name> --type continuouspatchv1 --schedule <schedule> --config <config-file> --enable-network-bypass
   ```

1. **Repair an existing workflow on a network-restricted registry**:

   This command is idempotent. It explicitly enables the registry bypass policy
   and updates the three existing CSSC tasks in place. It preserves unrelated
   task credentials and role assignments and selects classic RBAC or RBAC+ABAC
   repository roles based on the registry authorization mode.

   ```sh
   az acr supply-chain workflow configure-network-bypass --resource-group <resource-group> --registry <registry-name> --type continuouspatchv1
   ```

   Successful output reports the bypass policy, authorization mode, task
   principal IDs, credential readiness, and role readiness. Afterward, validate
   the workflow without triggering patch execution:

   ```sh
   az acr supply-chain workflow update --resource-group <resource-group> --registry <registry-name> --type continuouspatchv1 --config <config-file> --dry-run
   ```

1. **Update a Continuous Patch Task**:
   ```sh
   az acr supply-chain workflow update --resource-group <resource-group> --registry <registry-name> --type continuouspatchv1 --schedule <schedule> --config <config-file>
   ```

1. **Update with dryrun to test configuration changes**:
   ```sh
   az acr supply-chain workflow update --resource-group <resource-group> --registry <registry-name> --type continuouspatchv1 --config <config-file> --dryrun
   ```

1. **Delete a Continuous Patch Task**:
   ```sh
   az acr supply-chain workflow delete --resource-group <resource-group> --registry <registry-name> --type continuouspatchv1
   ```

1. **List Continuous Patch Tasks**:
   ```sh
   az acr supply-chain workflow list --resource-group <resource-group> --registry <registry-name> --type continuouspatchv1 --run-status <status>
   ```

1. **Show a Continuous Patch Task**:
   ```sh
   az acr supply-chain workflow show --resource-group <resource-group> --registry <registry-name> --type continuouspatchv1
   ```

1. **Cancel all Scan and Patch Running Tasks**:
   ```sh
   az acr supply-chain workflow cancel-run --resource-group <resource-group> --registry <registry-name> --type continuouspatchv1
   ```

Configuration
=============
The configuration file for the continuous patch task should define the repositories to be scanned and patched, the schedule for the task, and any other relevant settings.

Example Configuration:

```JSON
{
  "repositories": [
    {
      "repository": "alpine",
      "tags": ["tag1", "tag2"],
      "enabled": true
    },
    {
      "repository": "python",
      "tags": ["*"],
      "enabled": false
    }
  ],
  "version": "v1",
  "tag-convention": "floating"
}
```

Tag Convention
==============
The `tag-convention` property in the configuration file determines how the tags for patched images are managed. It can have the following values:

- **incremental**: This is the default behavior. It increases the patch version of the tag. For example, if the original tag is `1.0`, the patched tags will be `1.0-1`, `1.0-2`, etc.
- **floating**: This reuses the tag postfix `patched` for patching. For example, if the original tag is `1.0`, the patched tag will be `1.0-patched`.

Manual Network-Bypass Recovery
==============================

Use the extension command above when possible. For an existing workflow that
must be repaired before the updated extension can be installed, an administrator
can apply the equivalent Azure CLI configuration manually. Review the security
impact before enabling the registry policy.

1. Enable the registry policy:

   ```sh
   REGISTRY_ID=$(az acr show -n <registry-name> -g <resource-group> --query id -o tsv)
   LOGIN_SERVER=$(az acr show -n <registry-name> -g <resource-group> --query loginServer -o tsv)
   az resource update --ids "$REGISTRY_ID" --api-version 2025-06-01-preview --set properties.networkRuleBypassAllowedForTasks=true
   ```

1. For each of ``cssc-trigger-workflow``, ``cssc-scan-image``, and
   ``cssc-patch-image``, assign a system identity, disable default source
   authentication, and configure identity login:

   ```sh
   az acr task update -r <registry-name> -n <task-name> --assign-identity '[system]' --auth-mode None --source-acr-auth-id '[system]'
   az acr task credential add -r <registry-name> -n <task-name> --login-server "$LOGIN_SERVER" --use-identity '[system]'
   ```

   If the login-server credential already exists with the wrong identity,
   remove that credential before adding it again. Do not remove unrelated
   custom-registry credentials.

1. Assign registry-scoped roles to each task principal. For classic RBAC,
   trigger and scan require ``AcrPull`` plus ``Container Registry Tasks
   Contributor``; patch requires ``AcrPush``. For RBAC+ABAC, trigger requires
   ``Container Registry Repository Reader``, ``Container Registry Repository
   Catalog Lister``, and ``Container Registry Tasks Contributor``; scan requires
   ``Container Registry Repository Reader`` plus ``Container Registry Tasks
   Contributor``; patch requires ``Container Registry Repository Writer``.

   ```sh
   PRINCIPAL_ID=$(az acr task show -r <registry-name> -n <task-name> --query identity.principalId -o tsv)
   az role assignment create --assignee-object-id "$PRINCIPAL_ID" --assignee-principal-type ServicePrincipal --role <required-role> --scope "$REGISTRY_ID"
   ```

1. Verify the resulting task and policy state:

   ```sh
   az resource show --ids "$REGISTRY_ID" --api-version 2025-06-01-preview --query properties.networkRuleBypassAllowedForTasks
   az acr task show -r <registry-name> -n <task-name> --query "{identity:identity,credentials:credentials}"
   az role assignment list --assignee-object-id "$PRINCIPAL_ID" --scope "$REGISTRY_ID"
   ```
