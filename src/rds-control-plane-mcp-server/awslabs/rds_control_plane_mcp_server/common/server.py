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

from mcp.server.fastmcp import FastMCP


"""Common MCP server configuration."""

SERVER_VERSION = '0.1.0'

SERVER_INSTRUCTIONS = """
This server provides access to Amazon RDS database instances and clusters.

Key capabilities:
- View detailed information about RDS DB instances
- View detailed information about RDS DB clusters
- Access connection endpoints, configuration, and status information
- List performance reports and log files for DB instances

The server operates in read-only mode by default, providing safe access to RDS resources.
"""

SERVER_DEPENDENCIES = ['pydantic', 'loguru', 'boto3', 'mypy-boto3-rds']

mcp = FastMCP(
    'awslabs.rds-control-plane-mcp-server',
    version=SERVER_VERSION,
    instructions=SERVER_INSTRUCTIONS,
    dependencies=SERVER_DEPENDENCIES,
)
