# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tool to manage the status (start, stop, reboot) of a DB cluster."""

import asyncio
from ...common.connection import RDSConnectionManager
from ...common.decorator import handle_exceptions
from ...common.server import mcp
from ...common.utils import (
    format_aws_response,
    format_cluster_info,
    get_operation_impact,
)
from ...constants import (
    CONFIRM_REBOOT,
    CONFIRM_START,
    CONFIRM_STOP,
    ERROR_READONLY_MODE,
    SUCCESS_REBOOTED,
    SUCCESS_STARTED,
    SUCCESS_STOPPED,
)
from loguru import logger
from mcp.server.fastmcp import Context
from pydantic import Field
from typing import Any, Dict, Optional
from typing_extensions import Annotated


STATUS_CLUSTER_TOOL_DESCRIPTION = """Manage the status of an RDS database cluster.

<use_case>
Use this tool to change the operational status of an Amazon RDS database cluster.
You can start a stopped cluster, stop a running cluster, or reboot a cluster to apply
configuration changes or resolve certain issues.
</use_case>

<important_notes>
1. Each action requires explicit confirmation with a specific confirmation string
2. Stopping a cluster will make it unavailable but will continue to incur storage charges
3. Starting a cluster will resume full billing charges
4. Rebooting causes a brief interruption but preserves cluster settings and data
5. Aurora Serverless v1 clusters cannot be stopped manually
6. When run with readonly=True (default), this operation will be simulated but not actually performed
</important_notes>

## Response structure
If called without confirmation:
- `requires_confirmation`: Always true
- `warning`: Warning message about the action
- `impact`: Description of the impact of the action
- `message`: Instructions for confirming the action

If called with valid confirmation:
- `message`: Success message confirming the action
- `formatted_cluster`: A simplified representation of the cluster in its new state
- `DBCluster`: The full AWS API response containing cluster details including:
  - `DBClusterIdentifier`: The cluster identifier
  - `Status`: The current status (e.g., "starting", "stopping", "rebooting")
  - Other cluster details

<examples>
Example usage scenarios:
1. Stop a development cluster (first call to get warning):
   - db_cluster_identifier="dev-db-cluster"
   - action="stop"

2. Confirm stopping the cluster:
   - db_cluster_identifier="dev-db-cluster"
   - action="stop"
   - confirmation="CONFIRM_STOP"

3. Reboot a cluster that's experiencing issues:
   - db_cluster_identifier="prod-db-cluster"
   - action="reboot"
   - confirmation="CONFIRM_REBOOT"

4. Start a previously stopped cluster:
   - db_cluster_identifier="dev-db-cluster"
   - action="start"
   - confirmation="CONFIRM_START"
</examples>
"""


@mcp.tool(
    name='ManageDBClusterStatus',
    description=STATUS_CLUSTER_TOOL_DESCRIPTION,
)
@handle_exceptions
async def status_db_cluster(
    db_cluster_identifier: Annotated[str, Field(description='The identifier for the DB cluster')],
    action: Annotated[str, Field(description='Action to perform: "start", "stop", or "reboot"')],
    confirmation: Annotated[
        Optional[str], Field(description='Confirmation text for destructive operations')
    ] = None,
    ctx: Context = None,
) -> Dict[str, Any]:
    """Manage the status of an RDS database cluster.

    Args:
        db_cluster_identifier: The identifier for the DB cluster
        action: Action to perform: "start", "stop", or "reboot"
        confirmation: Confirmation text for destructive operations
        ctx: MCP context for logging and state management

    Returns:
        Dict[str, Any]: The response from the AWS API
    """
    rds_client = RDSConnectionManager.get_connection()

    action = action.lower()

    if action not in ['start', 'stop', 'reboot']:
        return {'error': f'Invalid action: {action}. Must be one of: start, stop, reboot'}

    # check read-only mode
    if not Context.check_operation_allowed(action, ctx):
        return {'error': ERROR_READONLY_MODE}

    # define confirmation requirements and warning messages for each action
    confirmation_requirements = {
        'start': {
            'required_confirmation': CONFIRM_START,
            'warning_message': f'WARNING: You are about to start DB cluster {db_cluster_identifier}. Starting a stopped cluster will resume billing charges and may incur costs. To confirm, please provide the confirmation parameter with the value "{CONFIRM_START}".',
            'impact': get_operation_impact('start_db_cluster'),
        },
        'stop': {
            'required_confirmation': CONFIRM_STOP,
            'warning_message': f'WARNING: You are about to stop DB cluster {db_cluster_identifier}. This will make the database unavailable until it is started again. To confirm, please provide the confirmation parameter with the value "{CONFIRM_STOP}".',
            'impact': get_operation_impact('stop_db_cluster'),
        },
        'reboot': {
            'required_confirmation': CONFIRM_REBOOT,
            'warning_message': f'WARNING: You are about to reboot DB cluster {db_cluster_identifier}. This will cause a brief interruption in database availability. To confirm, please provide the confirmation parameter with the value "{CONFIRM_REBOOT}".',
            'impact': get_operation_impact('reboot_db_cluster'),
        },
    }

    # get confirmation requirements for the current action
    required_confirmation = confirmation_requirements[action]['required_confirmation']
    warning_message = confirmation_requirements[action]['warning_message']
    impact = confirmation_requirements[action]['impact']

    # if no confirmation provided, return warning without executing operation
    if not confirmation:
        return {
            'requires_confirmation': True,
            'warning': warning_message,
            'impact': impact,
            'message': warning_message,
        }

    # if confirmation provided but doesn't match the required confirmation string, return error
    if confirmation != required_confirmation:
        return {
            'error': f'Confirmation value must be exactly "{required_confirmation}" to proceed with this operation. Operation aborted.'
        }

    if action == 'start':
        logger.info(f'Starting DB cluster {db_cluster_identifier}')
        response = await asyncio.to_thread(
            rds_client.start_db_cluster, DBClusterIdentifier=db_cluster_identifier
        )
        logger.success(f'Successfully started DB cluster {db_cluster_identifier}')

        result = format_aws_response(response)
        result['message'] = SUCCESS_STARTED.format(f'DB cluster {db_cluster_identifier}')

    elif action == 'stop':
        logger.info(f'Stopping DB cluster {db_cluster_identifier}')
        response = await asyncio.to_thread(
            rds_client.stop_db_cluster, DBClusterIdentifier=db_cluster_identifier
        )
        logger.success(f'Successfully stopped DB cluster {db_cluster_identifier}')

        result = format_aws_response(response)
        result['message'] = SUCCESS_STOPPED.format(f'DB cluster {db_cluster_identifier}')

    elif action == 'reboot':
        logger.info(f'Rebooting DB cluster {db_cluster_identifier}')
        response = await asyncio.to_thread(
            rds_client.reboot_db_cluster, DBClusterIdentifier=db_cluster_identifier
        )
        logger.success(f'Successfully initiated reboot of DB cluster {db_cluster_identifier}')

        result = format_aws_response(response)
        result['message'] = SUCCESS_REBOOTED.format(f'DB cluster {db_cluster_identifier}')

    # add formatted cluster info to the result
    result['formatted_cluster'] = format_cluster_info(result.get('DBCluster', {}))

    return result
