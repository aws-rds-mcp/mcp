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

"""Resource for listing available RDS DB Instances."""

from ...common.connection import RDSConnectionManager
from ...common.decorator import handle_exceptions
from ...common.server import mcp
from ...common.utils import handle_paginated_aws_api_call
from loguru import logger
from mypy_boto3_rds.type_defs import DBInstanceTypeDef
from pydantic import BaseModel, Field
from typing import Dict, List, Optional


class InstanceSummary(BaseModel):
    """Simplified DB instance model for list views."""

    instance_id: str = Field(description='The DB instance identifier')
    dbi_resource_id: Optional[str] = Field(
        None, description='The AWS Region-unique, immutable identifier for the DB instance'
    )
    status: str = Field(description='The current status of the DB instance')
    engine: str = Field(description='The database engine')
    engine_version: Optional[str] = Field(None, description='The version of the database engine')
    instance_class: str = Field(
        description='The compute and memory capacity class of the DB instance'
    )
    availability_zone: Optional[str] = Field(
        None, description='The Availability Zone of the DB instance'
    )
    multi_az: bool = Field(description='Whether the DB instance is a Multi-AZ deployment')
    publicly_accessible: bool = Field(description='Whether the DB instance is publicly accessible')
    db_cluster: Optional[str] = Field(
        None, description='The DB cluster identifier, if this is a member of a DB cluster'
    )
    tag_list: Dict[str, str] = Field(default_factory=dict, description='A list of tags')
    resource_uri: Optional[str] = Field(None, description='The resource URI for this instance')

    @classmethod
    def from_DBInstanceTypeDef(cls, instance: DBInstanceTypeDef) -> 'InstanceSummary':
        """Format instance information into a simplified model for list views.

        Args:
            instance: Raw instance data from AWS API response

        Returns:
            Formatted instance information as an InstanceSummary object
        """
        tags = {}
        if instance.get('TagList'):
            for tag in instance.get('TagList', []):
                if 'Key' in tag and 'Value' in tag:
                    tags[tag['Key']] = tag['Value']

        return cls(
            instance_id=instance.get('DBInstanceIdentifier', ''),
            dbi_resource_id=instance.get('DbiResourceId'),
            status=instance.get('DBInstanceStatus', ''),
            engine=instance.get('Engine', ''),
            engine_version=instance.get('EngineVersion', ''),
            instance_class=instance.get('DBInstanceClass', ''),
            availability_zone=instance.get('AvailabilityZone'),
            multi_az=instance.get('MultiAZ', False),
            publicly_accessible=instance.get('PubliclyAccessible', False),
            db_cluster=instance.get('DBClusterIdentifier'),
            tag_list=tags,
            resource_uri=None,
        )


class InstanceSummaryList(BaseModel):
    """DB instance list model."""

    instances: List[InstanceSummary] = Field(description='List of DB instances')
    count: int = Field(description='Number of DB instances')
    resource_uri: str = Field(description='The resource URI for instances')


LIST_INSTANCES_RESOURCE_DESCRIPTION = """List all available Amazon RDS instances in your account.

<use_case>
Use this resource to discover all available RDS database instances in your AWS account.
</use_case>

<important_notes>
1. The response provides essential information about each instance
2. Instance identifiers returned can be used with other tools and resources in this MCP server
3. Keep note of the instance_id and dbi_resource_id for use with other tools
4. Instances are filtered to the AWS region specified in your environment configuration
5. Use the `aws-rds://db-instance/{instance_id}` to get more information about a specific instance
</important_notes>

## Response structure
Returns a JSON document containing:
- `instances`: Array of DB instance objects
- `count`: Number of instances found
- `resource_uri`: Base URI for accessing instances

Each instance object contains:
- `instance_id`: Unique identifier for the instance
- `dbi_resource_id`: The unique resource identifier for this instance
- `status`: Current status of the instance
- `engine`: Database engine type
- `engine_version`: The version of the database engine
- `instance_class`: The instance type (e.g., db.t3.medium)
- `availability_zone`: The AZ where the instance is located
- `multi_az`: Whether the instance has Multi-AZ deployment
- `publicly_accessible`: Whether the instance is publicly accessible
- `db_cluster`: The DB cluster identifier (if applicable)
- `tag_list`: Dictionary of instance tags
- `resource_uri`: The resource URI for this instance
"""


@mcp.resource(
    uri='aws-rds://db-instance',
    name='ListDBInstances',
    mime_type='application/json',
    description=LIST_INSTANCES_RESOURCE_DESCRIPTION,
)
@handle_exceptions
async def list_instances() -> InstanceSummaryList:
    """List all RDS instances.

    Retrieves a complete list of all RDS database instances in the current AWS region,
    including Aurora instances and standard RDS instances, with pagination handling
    for large result sets.

    Returns:
        InstanceSummaryList containing formatted instance information including identifiers,
        status, engine details, and other relevant metadata
    """
    logger.info('Getting instance list resource')
    rds_client = RDSConnectionManager.get_connection()

    instances = handle_paginated_aws_api_call(
        client=rds_client,
        paginator_name='describe_db_instances',
        operation_parameters={},
        format_function=InstanceSummary.from_DBInstanceTypeDef,
        result_key='DBInstances',
    )

    result = InstanceSummaryList(
        instances=instances, count=len(instances), resource_uri='aws-rds://db-instance'
    )

    return result
