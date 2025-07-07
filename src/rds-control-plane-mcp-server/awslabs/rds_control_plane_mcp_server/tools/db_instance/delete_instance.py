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

"""Tool to delete an Amazon RDS database instance."""

import asyncio
from ...common.connection import RDSConnectionManager
from ...common.decorator import handle_exceptions
from ...common.server import mcp
from ...common.utils import (
    add_pending_operation,
    format_aws_response,
    format_instance_info,
    get_operation_impact,
    get_pending_operation,
    remove_pending_operation,
)
from ...constants import (
    ERROR_READONLY_MODE,
    SUCCESS_DELETED,
)
from loguru import logger
from mcp.server.fastmcp import Context
from pydantic import Field
from typing import Any, Dict, Optional
from typing_extensions import Annotated


DELETE_INSTANCE_TOOL_DESCRIPTION = """Delete an Amazon RDS database instance.

This tool deletes an RDS database instance. By default, a final snapshot will be created
unless explicitly disabled. This operation cannot be undone.

<warning>
This is a destructive operation that permanently deletes the database instance and all its data.
Without a final snapshot, all data will be permanently lost.
</warning>
"""


@mcp.tool(
    name='DeleteDBInstance',
    description=DELETE_INSTANCE_TOOL_DESCRIPTION,
)
@handle_exceptions
async def delete_db_instance(
    db_instance_identifier: Annotated[
        str, Field(description='The identifier for the DB instance')
    ],
    skip_final_snapshot: Annotated[
        bool,
        Field(
            description='Determines whether a final DB snapshot is created before the DB instance is deleted'
        ),
    ] = False,
    final_db_snapshot_identifier: Annotated[
        Optional[str],
        Field(
            description='The DB snapshot identifier of the new DB snapshot created when SkipFinalSnapshot is false'
        ),
    ] = None,
    confirmation_token: Annotated[
        Optional[str], Field(description='The confirmation token for the operation')
    ] = None,
    ctx: Context = None,
) -> Dict[str, Any]:
    """Delete an RDS database instance.

    Args:
        db_instance_identifier: The identifier for the DB instance
        skip_final_snapshot: Determines whether a final DB snapshot is created
        final_db_snapshot_identifier: The DB snapshot identifier if creating final snapshot
        confirmation_token: The confirmation token for the operation
        ctx: MCP context for logging and state management

    Returns:
        Dict[str, Any]: The response from the AWS API
    """
    rds_client = RDSConnectionManager.get_connection()

    # if server is in readonly mode
    if not Context.check_operation_allowed('delete', ctx):
        return {'error': ERROR_READONLY_MODE}

    # confirmation message and impact
    impact = get_operation_impact('delete_db_instance')
    confirmation_msg = (
        f"WARNING: You are about to delete the database instance '{db_instance_identifier}'."
    )

    # if no confirmation token provided, create a pending operation and return a token
    if not confirmation_token:
        # create parameters for the operation
        params = {
            'db_instance_identifier': db_instance_identifier,
            'skip_final_snapshot': skip_final_snapshot,
        }

        if not skip_final_snapshot and final_db_snapshot_identifier:
            params['final_db_snapshot_identifier'] = final_db_snapshot_identifier

        # add the pending operation and get a token
        token = add_pending_operation('delete_db_instance', params)

        # return the token directly in the response
        return {
            'requires_confirmation': True,
            'warning': confirmation_msg,
            'impact': impact,
            'confirmation_token': token,
            'message': f'WARNING: You are about to delete DB instance {db_instance_identifier}. This operation cannot be undone.\n\nTo confirm, please call this function again with the confirmation_token parameter set to this token.',
        }

    # if confirmation token provided, check if it's valid
    pending_op = get_pending_operation(confirmation_token)
    if not pending_op:
        return {
            'error': 'Invalid or expired confirmation token. Please request a new token by calling this function without a confirmation_token parameter.'
        }

    # extract operation details
    op_type, params, _ = pending_op

    # verify that this is the correct operation type
    if op_type != 'delete_db_instance':
        return {
            'error': f'Invalid operation type. Expected "delete_db_instance", got "{op_type}".'
        }

    # verify that the parameters match
    if params.get('db_instance_identifier') != db_instance_identifier:
        return {
            'error': 'Parameter mismatch. The confirmation token is for a different DB instance.'
        }

    try:
        # remove the pending operation
        remove_pending_operation(confirmation_token)

        # AWS API parameters
        aws_params = {
            'DBInstanceIdentifier': db_instance_identifier,
            'SkipFinalSnapshot': skip_final_snapshot,
        }

        if not skip_final_snapshot and final_db_snapshot_identifier:
            aws_params['FinalDBSnapshotIdentifier'] = final_db_snapshot_identifier

        logger.info(f'Deleting DB instance {db_instance_identifier}')
        response = await asyncio.to_thread(rds_client.delete_db_instance, **aws_params)
        logger.success(f'Successfully initiated deletion of DB instance {db_instance_identifier}')

        result = format_aws_response(response)
        result['message'] = SUCCESS_DELETED.format(f'DB instance {db_instance_identifier}')
        result['formatted_instance'] = format_instance_info(result.get('DBInstance', {}))

        return result
    except Exception as e:
        raise e
