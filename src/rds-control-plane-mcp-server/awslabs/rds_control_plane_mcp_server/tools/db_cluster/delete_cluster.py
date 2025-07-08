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

"""Tool to delete an Amazon RDS database cluster."""

import asyncio
from ...common.connection import RDSConnectionManager
from ...common.decorator import handle_exceptions
from ...common.server import mcp
from ...common.utils import (
    add_pending_operation,
    format_aws_response,
    format_cluster_info,
    get_operation_impact,
    get_pending_operation,
    remove_pending_operation,
)
from loguru import logger
from mcp.server.fastmcp import Context
from pydantic import Field
from typing import Any, Dict, Optional
from typing_extensions import Annotated


DELETE_CLUSTER_TOOL_DESCRIPTION = """Delete an RDS database cluster.

<use_case>
Use this tool to permanently remove an Amazon RDS database cluster and optionally
create a final snapshot. This operation cannot be undone, so a confirmation token is
required to prevent accidental deletion.
</use_case>

<important_notes>
1. This is a destructive operation that permanently deletes data
2. A confirmation token is required for safety - first call without token to receive one
3. By default, a final snapshot is created (skip_final_snapshot=False)
4. When creating a final snapshot (default behavior), you must provide final_db_snapshot_identifier
5. The operation may take several minutes to complete
6. All associated instances, automated backups and continuous backups (PITR) will be deleted
7. When run with readonly=True (default), this operation will be simulated but not actually performed
</important_notes>

## Response structure
If called without a confirmation token:
- `requires_confirmation`: Always true
- `warning`: Warning message about the deletion
- `impact`: Description of the impact of deletion
- `confirmation_token`: Token to use in a subsequent call
- `message`: Instructions for confirming the deletion

If called with a valid confirmation token:
- `message`: Success message confirming deletion
- `formatted_cluster`: A simplified representation of the deleted cluster
- `DBCluster`: The full AWS API response containing cluster details including:
  - `DBClusterIdentifier`: The cluster identifier
  - `Status`: The current status (usually "deleting")
  - Other cluster details

<examples>
Example usage scenarios:
1. Start deletion process (get confirmation token):
   - db_cluster_identifier="test-db-cluster"
   - skip_final_snapshot=true

2. Confirm deletion (with confirmation token):
   - db_cluster_identifier="test-db-cluster"
   - skip_final_snapshot=true
   - confirmation_token="abc123xyz" (token received from step 1)

3. Delete with final snapshot:
   - db_cluster_identifier="prod-db-cluster"
   - skip_final_snapshot=false
   - final_db_snapshot_identifier="prod-final-snapshot-20230625"
</examples>
"""


@mcp.tool(
    name='DeleteDBCluster',
    description=DELETE_CLUSTER_TOOL_DESCRIPTION,
)
@handle_exceptions
async def delete_db_cluster(
    db_cluster_identifier: Annotated[str, Field(description='The identifier for the DB cluster')],
    skip_final_snapshot: Annotated[
        bool,
        Field(
            description='Determines whether a final DB snapshot is created before the DB cluster is deleted'
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
    """Delete an RDS database cluster.

    Args:
        db_cluster_identifier: The identifier for the DB cluster
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
        return {
            'error': 'This operation requires write access. The server is currently in read-only mode.'
        }

    # confirmation message and impact
    impact = get_operation_impact('delete_db_cluster')
    confirmation_msg = f"""
⚠️ WARNING: You are about to delete the database cluster '{db_cluster_identifier}'.

This action will:
- Permanently delete all data in the cluster (unless a final snapshot is created)
- Terminate all instances in the cluster
- Cause downtime for any applications using this database
- Remove all automated backups after the retention period

This operation cannot be undone.
"""

    # if no confirmation token provided, create a pending operation and return a token
    if not confirmation_token:
        # create parameters for the operation
        params = {
            'db_cluster_identifier': db_cluster_identifier,
            'skip_final_snapshot': skip_final_snapshot,
        }

        if not skip_final_snapshot and final_db_snapshot_identifier:
            params['final_db_snapshot_identifier'] = final_db_snapshot_identifier

        # add the pending operation and get a token
        token = add_pending_operation('delete_db_cluster', params)

        # return the token directly in the response
        return {
            'requires_confirmation': True,
            'warning': confirmation_msg,
            'impact': impact,
            'confirmation_token': token,
            'message': f'WARNING: You are about to delete DB cluster {db_cluster_identifier}. This operation cannot be undone.\n\nTo confirm, please call this function again with the confirmation_token parameter set to this token.',
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
    if op_type != 'delete_db_cluster':
        return {'error': f'Invalid operation type. Expected "delete_db_cluster", got "{op_type}".'}

    # verify that the parameters match
    if params.get('db_cluster_identifier') != db_cluster_identifier:
        return {
            'error': 'Parameter mismatch. The confirmation token is for a different DB cluster.'
        }

    # remove the pending operation
    remove_pending_operation(confirmation_token)

    # AWS API parameters
    aws_params = {
        'DBClusterIdentifier': db_cluster_identifier,
        'SkipFinalSnapshot': skip_final_snapshot,
    }

    if not skip_final_snapshot and final_db_snapshot_identifier:
        aws_params['FinalDBSnapshotIdentifier'] = final_db_snapshot_identifier

    logger.info(f'Deleting DB cluster {db_cluster_identifier}')
    response = await asyncio.to_thread(rds_client.delete_db_cluster, **aws_params)
    logger.success(f'Successfully initiated deletion of DB cluster {db_cluster_identifier}')

    result = format_aws_response(response)
    result['message'] = f'Successfully deleted DB cluster {db_cluster_identifier}'
    result['formatted_cluster'] = format_cluster_info(result.get('DBCluster', {}))

    return result
