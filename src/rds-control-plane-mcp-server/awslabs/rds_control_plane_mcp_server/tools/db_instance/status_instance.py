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

"""Tool to manage the status (start, stop, reboot) of a DB instance."""

import asyncio
from ...common.connection import RDSConnectionManager
from ...common.decorator import handle_exceptions
from ...common.server import mcp
from ...common.utils import (
    format_aws_response,
    format_instance_info,
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
from ...context import RDSContext
from loguru import logger
from mcp.server.fastmcp import Context as FastMCPContext
from pydantic import Field
from typing import Any, Dict, Optional
from typing_extensions import Annotated


STATUS_INSTANCE_TOOL_DESCRIPTION = """Manage the status of an Amazon RDS database instance.

This tool allows you to change the status of an RDS database instance:

- **Start**: Starts a stopped instance, making it available for connections
- **Stop**: Stops a running instance, making it unavailable until started again
- **Reboot**: Reboots a running instance, causing a brief interruption in availability

<warning>
These operations affect the availability of your database:
- Starting a stopped instance will resume billing charges
- Stopping an instance makes it unavailable until it's started again
- Rebooting an instance causes a brief service interruption
</warning>
"""


@mcp.tool(
    name='ManageDBInstanceStatus',
    description=STATUS_INSTANCE_TOOL_DESCRIPTION,
)
@handle_exceptions
async def status_db_instance(
    db_instance_identifier: Annotated[
        str, Field(description='The identifier for the DB instance')
    ],
    action: Annotated[str, Field(description='Action to perform: "start", "stop", or "reboot"')],
    force_failover: Annotated[
        Optional[bool],
        Field(description='When rebooting, whether to force a failover to another AZ'),
    ] = False,
    confirmation: Annotated[
        Optional[str], Field(description='Confirmation text for destructive operations')
    ] = None,
    ctx: Optional[FastMCPContext] = None,
) -> Dict[str, Any]:
    """Manage the status of an RDS database instance.

    Args:
        db_instance_identifier: The identifier for the DB instance
        action: Action to perform: "start", "stop", or "reboot"
        force_failover: When rebooting, whether to force a failover to another AZ
        confirmation: Confirmation text for destructive operations
        ctx: MCP context for logging and state management

    Returns:
        Dict[str, Any]: The response from the AWS API
    """
    # Get RDS client
    rds_client = RDSConnectionManager.get_connection()

    action = action.lower()

    if action not in ['start', 'stop', 'reboot']:
        return {'error': f'Invalid action: {action}. Must be one of: start, stop, reboot'}

    # check read-only mode
    if not RDSContext.check_operation_allowed(action, ctx):
        return {'error': ERROR_READONLY_MODE}

    # define confirmation requirements and warning messages for each action
    confirmation_requirements = {
        'start': {
            'required_confirmation': CONFIRM_START,
            'warning_message': f'WARNING: You are about to start DB instance {db_instance_identifier}. Starting a stopped instance will resume billing charges and may incur costs. To confirm, please provide the confirmation parameter with the value "{CONFIRM_START}".',
            'impact': get_operation_impact('start_db_instance'),
        },
        'stop': {
            'required_confirmation': CONFIRM_STOP,
            'warning_message': f'WARNING: You are about to stop DB instance {db_instance_identifier}. This will make the database unavailable until it is started again. To confirm, please provide the confirmation parameter with the value "{CONFIRM_STOP}".',
            'impact': get_operation_impact('stop_db_instance'),
        },
        'reboot': {
            'required_confirmation': CONFIRM_REBOOT,
            'warning_message': f'WARNING: You are about to reboot DB instance {db_instance_identifier}. This will cause a brief interruption in database availability. To confirm, please provide the confirmation parameter with the value "{CONFIRM_REBOOT}".',
            'impact': get_operation_impact('reboot_db_instance'),
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

    result = {}

    if action == 'start':
        logger.info(f'Starting DB instance {db_instance_identifier}')
        response = await asyncio.to_thread(
            rds_client.start_db_instance, DBInstanceIdentifier=db_instance_identifier
        )
        logger.success(f'Successfully started DB instance {db_instance_identifier}')

        result = format_aws_response(response)
        result['message'] = SUCCESS_STARTED.format(f'DB instance {db_instance_identifier}')

    elif action == 'stop':
        logger.info(f'Stopping DB instance {db_instance_identifier}')
        response = await asyncio.to_thread(
            rds_client.stop_db_instance, DBInstanceIdentifier=db_instance_identifier
        )
        logger.success(f'Successfully stopped DB instance {db_instance_identifier}')

        result = format_aws_response(response)
        result['message'] = SUCCESS_STOPPED.format(f'DB instance {db_instance_identifier}')

    elif action == 'reboot':
        logger.info(f'Rebooting DB instance {db_instance_identifier}')
        response = await asyncio.to_thread(
            rds_client.reboot_db_instance,
            DBInstanceIdentifier=db_instance_identifier,
            ForceFailover=force_failover,
        )
        logger.success(f'Successfully initiated reboot of DB instance {db_instance_identifier}')

        result = format_aws_response(response)
        result['message'] = SUCCESS_REBOOTED.format(f'DB instance {db_instance_identifier}')

    # add formatted instance info to the result
    result['formatted_instance'] = format_instance_info(result.get('DBInstance', {}))

    return result
