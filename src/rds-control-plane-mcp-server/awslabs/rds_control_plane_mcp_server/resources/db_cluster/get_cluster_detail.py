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

"""Resource for retrieving detailed information about RDS DB Clusters."""

import asyncio
from ...common.connection import RDSConnectionManager
from ...common.decorator import handle_exceptions
from ...common.server import mcp
from datetime import datetime
from loguru import logger
from mypy_boto3_rds.type_defs import DBClusterTypeDef
from pydantic import BaseModel, Field
from typing import Dict, List, Optional


GET_CLUSTER_DETAIL_RESOURCE_DESCRIPTION = """Get detailed information about a specific Amazon RDS cluster.

<use_case>
Use this resource to retrieve comprehensive details about a specific RDS database cluster
identified by its cluster ID. This provides deeper insights than the cluster list resource.
</use_case>

<important_notes>
1. The cluster ID must exist in your AWS account and region
2. The response contains full configuration details about the specified cluster
3. This resource includes information not available in the list view such as parameter groups,
   backup configuration, and maintenance windows
4. Use the cluster list resource first to identify valid cluster IDs
5. Error responses will be returned if the cluster doesn't exist or there are permission issues
</important_notes>

## Response structure
Returns a JSON document containing detailed cluster information:
- All fields from the list view plus:
- `endpoint`: The primary endpoint for connecting to the cluster
- `reader_endpoint`: The reader endpoint for read operations (if applicable)
- `port`: The port the database engine is listening on
- `parameter_group`: Database parameter group information
- `backup_retention_period`: How long backups are retained (in days)
- `preferred_backup_window`: When automated backups occur
- `preferred_maintenance_window`: When maintenance operations can occur
- `resource_uri`: The full resource URI for this specific cluster
"""

# Data Models


class ClusterMember(BaseModel):
    """DB cluster member model."""

    instance_id: str = Field(description='The instance identifier of the DB cluster member')
    is_writer: bool = Field(description='Whether the cluster member is a writer instance')
    status: Optional[str] = Field(
        None, description='The status of the DB cluster parameter group for this member'
    )


class Cluster(BaseModel):
    """DB cluster model."""

    cluster_id: str = Field(description='The DB cluster identifier')
    status: str = Field(description='The current status of the DB cluster')
    engine: str = Field(description='The database engine')
    engine_version: Optional[str] = Field(None, description='The version of the database engine')
    endpoint: Optional[str] = Field(
        None, description='The connection endpoint for the primary instance'
    )
    reader_endpoint: Optional[str] = Field(
        None, description='The reader endpoint for the DB cluster'
    )
    multi_az: bool = Field(
        description='Whether the DB cluster has instances in multiple Availability Zones'
    )
    backup_retention: int = Field(description='The retention period for automated backups')
    preferred_backup_window: Optional[str] = Field(
        None, description='The daily time range during which automated backups are created'
    )
    preferred_maintenance_window: Optional[str] = Field(
        None, description='The weekly time range during which system maintenance can occur'
    )
    created_time: Optional[datetime] = Field(
        None, description='The time when the DB cluster was created'
    )
    members: List[ClusterMember] = Field(
        default_factory=list, description='A list of DB cluster members'
    )
    vpc_security_groups: List[Dict[str, str]] = Field(
        default_factory=list, description='A list of VPC security groups the DB cluster belongs to'
    )
    tags: Dict[str, str] = Field(default_factory=dict, description='A list of tags')
    resource_uri: Optional[str] = Field(None, description='The resource URI for this cluster')

    @classmethod
    def from_DBClusterTypeDef(cls, cluster: DBClusterTypeDef) -> 'Cluster':
        """Format cluster information from AWS API response into a detailed structured model.

        Args:
            cluster: Raw cluster data from AWS API response

        Returns:
            Formatted cluster information as a Cluster object with comprehensive details
        """
        members = []
        for member in cluster.get('DBClusterMembers', []):
            members.append(
                ClusterMember(
                    instance_id=member.get('DBInstanceIdentifier', ''),
                    is_writer=member.get('IsClusterWriter', False),
                    status=member.get('DBClusterParameterGroupStatus'),
                )
            )

        vpc_security_groups = []
        for sg in cluster.get('VpcSecurityGroups', []):
            vpc_security_groups.append(
                {'id': sg.get('VpcSecurityGroupId', ''), 'status': sg.get('Status', '')}
            )

        tags = {}
        if cluster.get('TagList'):
            for tag in cluster.get('TagList', []):
                if 'Key' in tag and 'Value' in tag:
                    tags[tag['Key']] = tag['Value']

        cluster_id = cluster.get('DBClusterIdentifier', '')

        return cls(
            cluster_id=cluster_id,
            status=cluster.get('Status', ''),
            engine=cluster.get('Engine', ''),
            engine_version=cluster.get('EngineVersion'),
            endpoint=cluster.get('Endpoint'),
            reader_endpoint=cluster.get('ReaderEndpoint'),
            multi_az=cluster.get('MultiAZ', False),
            backup_retention=cluster.get('BackupRetentionPeriod', 0),
            preferred_backup_window=cluster.get('PreferredBackupWindow'),
            preferred_maintenance_window=cluster.get('PreferredMaintenanceWindow'),
            created_time=cluster.get('ClusterCreateTime'),
            members=members,
            vpc_security_groups=vpc_security_groups,
            tags=tags,
            resource_uri=f'aws-rds://db-cluster/{cluster_id}',
        )


@mcp.resource(
    uri='aws-rds://db-cluster/{cluster_id}',
    name='GetDBClusterDetail',
    description=GET_CLUSTER_DETAIL_RESOURCE_DESCRIPTION,
    mime_type='application/json',
)
@handle_exceptions
async def get_cluster_detail(
    cluster_id: str = Field(
        ..., description='The unique identifier of the RDS DB cluster to retrieve details for'
    ),
) -> Cluster:
    """Retrieve detailed information about a specific RDS cluster.

    Args:
        cluster_id: The unique identifier of the DB cluster to retrieve

    Returns:
        Formatted Cluster object with comprehensive details
    """
    logger.info(f'Getting cluster detail resource for {cluster_id}')
    rds_client = RDSConnectionManager.get_connection()
    response = await asyncio.to_thread(
        rds_client.describe_db_clusters, DBClusterIdentifier=cluster_id
    )

    clusters = response.get('DBClusters', [])
    if not clusters:
        raise ValueError(f'Cluster {cluster_id} not found')
    cluster = Cluster.from_DBClusterTypeDef(clusters[0])

    return cluster
