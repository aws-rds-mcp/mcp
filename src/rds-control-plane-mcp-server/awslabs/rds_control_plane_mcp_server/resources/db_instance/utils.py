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

"""Instance specific utility functions for the RDS Control Plane MCP Server."""

from ...common.models import (
    InstanceEndpoint,
    InstanceModel,
    InstanceStorage,
    InstanceSummaryModel,
    VpcSecurityGroup,
)
from typing import Dict
from mypy_boto3_rds.type_defs import DBInstanceTypeDef




def format_instance_summary(instance: DBInstanceTypeDef) -> InstanceSummaryModel:
    """Format instance information into a simplified model for list views.
    
    This returns a simplified InstanceSummaryModel with only essential fields
    to provide a high-level overview of instances.

    Args:
        instance: Raw instance data from AWS API response

    Returns:
        Formatted instance information as an InstanceSummaryModel object
    """
    tags = {}
    if instance.get('TagList'):
        for tag in instance.get('TagList', []):
            if 'Key' in tag and 'Value' in tag:
                tags[tag['Key']] = tag['Value']
                
    return InstanceSummaryModel(
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


def format_instance_detail(instance: DBInstanceTypeDef) -> InstanceModel:
    """Format instance information into a detailed model with comprehensive information.

    Args:
        instance: Raw instance data from AWS

    Returns:
        Formatted instance information as an InstanceModel with comprehensive details
    """
    endpoint = InstanceEndpoint(
        address=instance.get('Endpoint', {}).get('Address'),
        port=instance.get('Endpoint', {}).get('Port'),
        hosted_zone_id=instance.get('Endpoint', {}).get('HostedZoneId'),
    )

    storage = InstanceStorage(
        type=instance.get('StorageType'),
        allocated=instance.get('AllocatedStorage'),
        encrypted=instance.get('StorageEncrypted'),
    )

    vpc_security_groups = []
    for sg in instance.get('VpcSecurityGroups', []):
        vpc_security_groups.append(
            VpcSecurityGroup(id=sg.get('VpcSecurityGroupId', ''), status=sg.get('Status', ''))
        )

    tags = {}
    if instance.get('TagList'):
        for tag in instance.get('TagList', []):
            if 'Key' in tag and 'Value' in tag:
                tags[tag['Key']] = tag['Value']

    return InstanceModel(
        instance_id=instance.get('DBInstanceIdentifier', ''),
        status=instance.get('DBInstanceStatus', ''),
        engine=instance.get('Engine', ''),
        engine_version=instance.get('EngineVersion', ''),
        instance_class=instance.get('DBInstanceClass', ''),
        endpoint=endpoint,
        availability_zone=instance.get('AvailabilityZone'),
        multi_az=instance.get('MultiAZ', False),
        storage=storage,
        preferred_backup_window=instance.get('PreferredBackupWindow'),
        preferred_maintenance_window=instance.get('PreferredMaintenanceWindow'),
        publicly_accessible=instance.get('PubliclyAccessible', False),
        vpc_security_groups=vpc_security_groups,
        db_cluster=instance.get('DBClusterIdentifier'),
        tags=tags,
        dbi_resource_id=instance.get('DbiResourceId'),
        resource_uri=None,
    )
