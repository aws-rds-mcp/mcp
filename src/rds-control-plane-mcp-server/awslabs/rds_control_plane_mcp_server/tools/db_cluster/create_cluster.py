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

"""Tool to create a new Amazon RDS database cluster."""

import asyncio
from ...common.connection import RDSConnectionManager
from ...common.decorator import handle_exceptions
from ...common.server import mcp
from ...common.utils import (
    add_mcp_tags,
    format_aws_response,
    format_cluster_info,
    get_engine_port,
    validate_db_identifier,
)
from ...constants import (
    ERROR_INVALID_PARAMS,
    ERROR_READONLY_MODE,
    SUCCESS_CREATED,
)
from ...context import RDSContext
from loguru import logger
from mcp.server.fastmcp import Context as FastMCPContext
from pydantic import Field
from typing import Any, Dict, List, Optional
from typing_extensions import Annotated


CREATE_CLUSTER_TOOL_DESCRIPTION = """Create a new Amazon RDS database cluster.

<use_case>
Use this tool to provision a new Amazon RDS database cluster in your AWS account.
This creates the cluster control plane but doesn't automatically provision database instances.
You'll need to create DB instances separately after the cluster is available.
</use_case>

<important_notes>
1. Cluster identifiers must follow naming rules: 1-63 alphanumeric characters, must begin with a letter
2. The tool will automatically determine default port numbers based on the engine if not specified
3. Master user passwords are stored securely in AWS Secrets Manager
4. Not all parameter combinations are valid for all database engines
5. When run with readonly=True (default), this operation will be simulated but not actually performed
</important_notes>

## Response structure
Returns a dictionary with the following keys:
- `message`: Success message confirming the creation
- `formatted_cluster`: A simplified representation of the cluster in standard format
- `DBCluster`: The full AWS API response containing all cluster details including:
  - `DBClusterIdentifier`: The cluster identifier
  - `Status`: The current status (usually "creating" initially)
  - `Engine`: The database engine
  - `EngineVersion`: The engine version
  - `Endpoint`: The connection endpoint
  - `MasterUsername`: The admin username
  - `AvailabilityZones`: List of AZs where the cluster operates
  - Other cluster configuration details and settings

<examples>
Example usage scenarios:
1. Create a basic Aurora PostgreSQL cluster:
   - db_cluster_identifier="my-postgres-cluster"
   - engine="aurora-postgresql"
   - master_username="admin"

2. Create a MySQL-compatible Aurora cluster with custom settings:
   - db_cluster_identifier="production-aurora"
   - engine="aurora-mysql"
   - master_username="dbadmin"
   - database_name="appdb"
   - backup_retention_period=7
   - vpc_security_group_ids=["sg-12345678"]
</examples>
"""


@mcp.tool(
    name='CreateDBCluster',
    description=CREATE_CLUSTER_TOOL_DESCRIPTION,
)
@handle_exceptions
async def create_db_cluster(
    db_cluster_identifier: Annotated[str, Field(description='The identifier for the DB cluster')],
    engine: Annotated[
        str,
        Field(
            description='The name of the database engine to be used for this DB cluster (e.g., aurora, aurora-mysql, aurora-postgresql, mysql, postgres, mariadb, oracle, sqlserver)'
        ),
    ],
    master_username: Annotated[
        str, Field(description='The name of the master user for the DB cluster')
    ],
    database_name: Annotated[
        Optional[str], Field(description='The name for your database')
    ] = None,
    vpc_security_group_ids: Annotated[
        Optional[List[str]],
        Field(description='A list of EC2 VPC security groups to associate with this DB cluster'),
    ] = None,
    db_subnet_group_name: Annotated[
        Optional[str], Field(description='A DB subnet group to associate with this DB cluster')
    ] = None,
    availability_zones: Annotated[
        Optional[List[str]],
        Field(
            description='A list of Availability Zones (AZs) where instances in the DB cluster can be created'
        ),
    ] = None,
    backup_retention_period: Annotated[
        Optional[int],
        Field(description='The number of days for which automated backups are retained'),
    ] = None,
    port: Annotated[
        Optional[int],
        Field(
            description='The port number on which the instances in the DB cluster accept connections'
        ),
    ] = None,
    engine_version: Annotated[
        Optional[str], Field(description='The version number of the database engine to use')
    ] = None,
    ctx: FastMCPContext = None,
) -> Dict[str, Any]:
    """Create a new RDS database cluster.

    Args:
        db_cluster_identifier: The identifier for the DB cluster
        engine: The name of the database engine to be used for this DB cluster
        master_username: The name of the master user for the DB cluster
        database_name: The name for your database
        vpc_security_group_ids: A list of EC2 VPC security groups to associate with this DB cluster
        db_subnet_group_name: A DB subnet group to associate with this DB cluster
        availability_zones: A list of Availability Zones (AZs) where instances in the DB cluster can be created
        backup_retention_period: The number of days for which automated backups are retained
        port: The port number on which the instances in the DB cluster accept connections
        engine_version: The version number of the database engine to use
        ctx: MCP context for logging and state management

    Returns:
        Dict[str, Any]: The response from the AWS API
    """
    # Get RDS client
    rds_client = RDSConnectionManager.get_connection()

    # Check if server is in readonly mode
    if not RDSContext.check_operation_allowed('create', ctx):
        return {'error': ERROR_READONLY_MODE}

    # validate identifier
    if not validate_db_identifier(db_cluster_identifier):
        error_msg = ERROR_INVALID_PARAMS.format(
            'db_cluster_identifier must be 1-63 characters, begin with a letter, and contain only alphanumeric characters and hyphens'
        )
        if ctx:
            await ctx.error(error_msg)
        return {'error': error_msg}

    params = {
        'DBClusterIdentifier': db_cluster_identifier,
        'Engine': engine,
        'MasterUsername': master_username,
        'ManageMasterUserPassword': True,
    }

    # add optional parameters if provided
    if database_name:
        params['DatabaseName'] = database_name
    if vpc_security_group_ids:
        params['VpcSecurityGroupIds'] = vpc_security_group_ids
    if db_subnet_group_name:
        params['DBSubnetGroupName'] = db_subnet_group_name
    if availability_zones:
        params['AvailabilityZones'] = availability_zones
    if backup_retention_period is not None:
        params['BackupRetentionPeriod'] = backup_retention_period
    if port is not None:
        params['Port'] = port
    else:
        params['Port'] = get_engine_port(engine)
    if engine_version:
        params['EngineVersion'] = engine_version

    # MCP tags
    params = add_mcp_tags(params)

    logger.info(f'Creating DB cluster {db_cluster_identifier} with engine {engine}')
    response = await asyncio.to_thread(rds_client.create_db_cluster, **params)
    logger.success(f'Successfully created DB cluster {db_cluster_identifier}')

    result = format_aws_response(response)
    result['message'] = SUCCESS_CREATED.format(f'DB cluster {db_cluster_identifier}')
    result['formatted_cluster'] = format_cluster_info(result.get('DBCluster', {}))

    return result
