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

"""Tool to modify an existing Amazon RDS database cluster."""

import asyncio
from ...common.connection import RDSConnectionManager
from ...common.decorator import handle_exceptions
from ...common.server import mcp
from ...common.utils import (
    format_aws_response,
    format_cluster_info,
)
from ...constants import (
    ERROR_READONLY_MODE,
    SUCCESS_MODIFIED,
)
from loguru import logger
from mcp.server.fastmcp import Context
from pydantic import Field
from typing import Any, Dict, List, Optional
from typing_extensions import Annotated


MODIFY_CLUSTER_TOOL_DESCRIPTION = """Modify an existing RDS database cluster configuration.

<use_case>
Use this tool to update the configuration of an existing Amazon RDS database cluster.
This allows changing various settings like backup retention, parameter groups, security groups,
and upgrading database engine versions without recreating the cluster.
</use_case>

<important_notes>
1. Setting apply_immediately=True applies changes immediately but may cause downtime
2. Setting apply_immediately=False (default) applies changes during the next maintenance window
3. Major version upgrades require allow_major_version_upgrade=True
4. Changing the port may require updates to security groups and application configurations
5. When run with readonly=True (default), this operation will be simulated but not actually performed
</important_notes>

## Response structure
Returns a dictionary with the following keys:
- `message`: Success message confirming the modification
- `formatted_cluster`: A simplified representation of the modified cluster in standard format
- `DBCluster`: The full AWS API response containing all cluster details including:
  - `DBClusterIdentifier`: The cluster identifier
  - `Status`: The current status (may show "modifying")
  - `PendingModifiedValues`: Values that will be applied if not immediate
  - Other updated cluster configuration details

<examples>
Example usage scenarios:
1. Increase backup retention period:
   - db_cluster_identifier="production-db-cluster"
   - backup_retention_period=14
   - apply_immediately=True

2. Change security groups and apply during maintenance window:
   - db_cluster_identifier="production-db-cluster"
   - vpc_security_group_ids=["sg-87654321", "sg-12348765"]
   - apply_immediately=False

3. Upgrade database engine version:
   - db_cluster_identifier="production-db-cluster"
   - engine_version="5.7.mysql_aurora.2.10.2"
   - allow_major_version_upgrade=True
   - apply_immediately=False
</examples>
"""


@mcp.tool(
    name='ModifyDBCluster',
    description=MODIFY_CLUSTER_TOOL_DESCRIPTION,
)
@handle_exceptions
async def modify_db_cluster(
    db_cluster_identifier: Annotated[str, Field(description='The identifier for the DB cluster')],
    apply_immediately: Annotated[
        Optional[bool],
        Field(
            description='Specifies whether the modifications are applied immediately, or during the next maintenance window'
        ),
    ] = None,
    backup_retention_period: Annotated[
        Optional[int],
        Field(description='The number of days for which automated backups are retained'),
    ] = None,
    db_cluster_parameter_group_name: Annotated[
        Optional[str],
        Field(description='The name of the DB cluster parameter group to use for the DB cluster'),
    ] = None,
    vpc_security_group_ids: Annotated[
        Optional[List[str]],
        Field(description='A list of EC2 VPC security groups to associate with this DB cluster'),
    ] = None,
    port: Annotated[
        Optional[int],
        Field(description='The port number on which the DB cluster accepts connections'),
    ] = None,
    engine_version: Annotated[
        Optional[str], Field(description='The version number of the database engine to upgrade to')
    ] = None,
    allow_major_version_upgrade: Annotated[
        Optional[bool], Field(description='Indicates whether major version upgrades are allowed')
    ] = None,
    ctx: Context = None,
) -> Dict[str, Any]:
    """Modify an existing RDS database cluster configuration.

    Args:
        db_cluster_identifier: The identifier for the DB cluster
        apply_immediately: Specifies whether the modifications are applied immediately
        backup_retention_period: The number of days for which automated backups are retained
        db_cluster_parameter_group_name: The name of the DB cluster parameter group to use
        vpc_security_group_ids: A list of EC2 VPC security groups to associate with this DB cluster
        port: The port number on which the DB cluster accepts connections
        engine_version: The version number of the database engine to upgrade to
        allow_major_version_upgrade: Indicates whether major version upgrades are allowed
        ctx: MCP context for logging and state management

    Returns:
        Dict[str, Any]: The response from the AWS API
    """
    rds_client = RDSConnectionManager.get_connection()

    # if server is in readonly mode
    if not Context.check_operation_allowed('modify', ctx):
        return {'error': ERROR_READONLY_MODE}

    params = {
        'DBClusterIdentifier': db_cluster_identifier,
    }

    # Add optional parameters if provided
    if apply_immediately is not None:
        params['ApplyImmediately'] = apply_immediately
    if backup_retention_period is not None:
        params['BackupRetentionPeriod'] = backup_retention_period
    if db_cluster_parameter_group_name:
        params['DBClusterParameterGroupName'] = db_cluster_parameter_group_name
    if vpc_security_group_ids:
        params['VpcSecurityGroupIds'] = vpc_security_group_ids
    if port is not None:
        params['Port'] = port
    if engine_version:
        params['EngineVersion'] = engine_version
    if allow_major_version_upgrade is not None:
        params['AllowMajorVersionUpgrade'] = allow_major_version_upgrade

    logger.info(f'Modifying DB cluster {db_cluster_identifier}')
    response = await asyncio.to_thread(rds_client.modify_db_cluster, **params)
    logger.success(f'Successfully modified DB cluster {db_cluster_identifier}')

    result = format_aws_response(response)
    result['message'] = SUCCESS_MODIFIED.format(f'DB cluster {db_cluster_identifier}')
    result['formatted_cluster'] = format_cluster_info(result.get('DBCluster', {}))

    return result
