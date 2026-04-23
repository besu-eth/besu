# Burn-In Reference

## Starting Burn-In Nodes

### Fresh Syncs

Use the Jenkins job to create burn-in nodes:
https://jenkins.ops.protocols.consensys.io/job/Besu/job/elc-yml-create-burnin-nodes/

Configure the job with:
- **RC_BESU_GIT_COMMIT**: the new RC tag (e.g. `26.1.0-RC1`)
- **CTRL_COMPARE_BESU_GIT_COMMIT**: the previous (latest) release tag

The job automatically appends the commit tags to burn-in node names, so they should not conflict with existing nodes. You should not need to change the YML.

### Update Jobs

Existing canaries and validators need to be updated. Since the release binary has not been created yet, you generally need to build from source. Update the inventory accordingly (example: https://github.com/Consensys/protocols-aws-inventories/commit/fa99d963a56ad724518b438a3edd11169f1b63f9).

Update jobs:
- **Sepolia**: https://jenkins.ops.protocols.consensys.io/job/Besu/job/elc-update-Sepolia/
- **Hoodi**: https://jenkins.ops.protocols.consensys.io/job/Besu/job/elc-update-Hoodi/
- **Mainnet**: https://jenkins.ops.protocols.consensys.io/job/Besu/job/elc-update-Mainnet/

## Performance Burn-In

The burn-in nodes job creates 3 identical "perf-test" nodes and 3 identical "perf-control" nodes.

Everything is kept identical except the Besu version. Three test and three control nodes provides a good average for metrics and helps avoid anomalies due to bad peers. These nodes are analyzed for performance regressions since the last release.

## Burn-In Analysis

Once nodes have synced and run for a sufficient period, use the [protocols-node-analysis](https://github.com/Consensys/protocols-node-analysis) tooling to generate reports and verify database integrity. See [NODE-ANALYSIS.md](NODE-ANALYSIS.md) for the full analysis workflow.

## Troubleshooting Failed Node Launches

### Identifying the failure
- Use the "Open Blue Ocean" menu item in Jenkins to identify which node failed
- Alternatively, check if all nodes were provisioned in Azure console or CLI

### If the node failed to provision
Rerun the Jenkins job, removing the nodes that succeeded from the YML.

### If all nodes provisioned but Jenkins shows red
Check if EL and CL launched in Grafana. If Besu is syncing and looks healthy, it may just be a flaky Ansible health check at the end of the job.

### If a client did not launch properly
Rerun the job (removing successful nodes from YML) and add `provisioned_hosts` to the overrides:

```yaml
create-elcnode:
- stack_name: burn-snap
  stack_type: dev
  network: holesky
  cl_client_name: lodestar
  cl_client_version: latest
  overrides: |
    besu_sync_mode: SNAP
    besu_data_storage_format: BONSAI
    provisioned_hosts: ["10.0.0.122"]
```

See the Consensys internal documentation for further details on the `provisioned_hosts` override.
