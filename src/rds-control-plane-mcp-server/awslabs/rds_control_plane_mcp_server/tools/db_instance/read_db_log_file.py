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

"""read_rds_db_logs data models, helpers and tool implementation."""

import asyncio
import json
from ...common.connection import RDSConnectionManager
from ...common.decorator import handle_exceptions
from ...common.server import mcp
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field
from typing import Optional


TOOL_DESCRIPTION = """Read database log files from RDS instances.

This tool retrieves contents of database log files from Amazon RDS instances.
It allows you to download log file portions, search for specific patterns,
and paginate through large log files.

<use_case>
Use this tool to troubleshoot database issues by examining log files for errors,
warnings, or specific events. Log analysis can help identify performance problems,
configuration issues, failed operations, and security concerns.
</use_case>

<important_notes>
1. Returns up to 1MB of log content per call due to API limitations
2. Use the marker parameter for pagination through large log files
3. The pattern parameter can filter logs to show only relevant entries
4. For error logs, "error/postgresql.log" or "error/log" are common file names
5. Use list_db_logs resource first to identify available log files
6. Log file content may contain sensitive information like connection details
</important_notes>
"""


class DBLogFileResponse(BaseModel):
    """Data model for the response from read_rds_db_logs API.

    This model represents the structure of the database log file data
    returned when requesting log file contents from an RDS instance.
    """

    log_content: str = Field(
        ...,
        description='The content of the log file. May be empty if the file exists but has no content.',
    )

    next_marker: Optional[str] = Field(
        None,
        description="The pagination marker that can be used in a subsequent request to read the next portion of the log file. Will be None when there's no more data to retrieve.",
    )

    additional_data_pending: bool = Field(
        False,
        description='Indicates whether there is additional data available in the log file. If True, more data can be retrieved using the provided marker.',
    )


def preprocess_log_content(log_file_content: str, pattern: Optional[str] = None) -> str:
    """Preprocess and filter the log content before returning it.

    This function processes the raw log file content and applies any specified pattern filtering.
    If a pattern is provided, only lines containing that pattern will be included in the result.

    Args:
        log_file_content: Raw log content from the RDS instance
        pattern: Optional filter pattern; when provided, only lines containing this pattern are returned

    Returns:
        str: The processed log content, potentially filtered by the pattern
    """
    if not pattern or not log_file_content:
        return log_file_content

    filtered_lines = []
    for line in log_file_content.splitlines():
        if pattern in line:
            filtered_lines.append(line)

    return '\n'.join(filtered_lines)


@mcp.tool(
    name='ReadDBLogFiles',
    description=TOOL_DESCRIPTION,
    annotations=ToolAnnotations(
        title='Read RDS DB Log Files',
        readOnlyHint=True,
    ),
)
@handle_exceptions
async def read_db_log_file(
    db_instance_identifier: str = Field(
        ...,
        description='The identifier of the RDS instance (DBInstanceIdentifier, not DbiResourceId) to read logs from.',
    ),
    log_file_name: str = Field(
        ...,
        description='The name of the log file to read (e.g., "error/postgresql.log").',
    ),
    marker: Optional[str] = Field(
        '0',
        description='The pagination marker returned by a previous call to this tool for reading the next portion of a log file. Set to the first page by default.',
    ),
    number_of_lines: Optional[int] = Field(
        100,
        description='The number of lines to read from the log file (default: 100).',
        ge=1,
        lt=10000,
    ),
    pattern: Optional[str] = Field(
        None,
        description='The pattern to filter log entries. Only returns lines that contain the specified pattern string.',
    ),
) -> str:
    """Retrieve RDS database log file contents.

    Args:
        db_instance_identifier: The identifier of the RDS instance to retrieve logs from
        log_file_name: The name of the log file to read
        marker: The pagination marker from a previous call (set to '0' for first page)
        number_of_lines: Number of lines to retrieve (1-9999)
        pattern: Optional filter pattern to only return matching lines

    Returns:
        str: A JSON string containing the log content, pagination marker, and pending data flag
    """
    rds_client = RDSConnectionManager.get_connection()

    params = {
        'DBInstanceIdentifier': db_instance_identifier,
        'LogFileName': log_file_name,
    }

    if number_of_lines is not None:
        params['NumberOfLines'] = number_of_lines  # type: ignore

    if marker:
        params['Marker'] = marker

    response = await asyncio.to_thread(rds_client.download_db_log_file_portion, **params)

    result = DBLogFileResponse(
        log_content=preprocess_log_content(response.get('LogFileData', ''), pattern=pattern),
        next_marker=response.get('NextToken', None),
        additional_data_pending=response.get('AdditionalDataPending', False),
    )

    serializable_dict = result.model_dump()
    return json.dumps(serializable_dict, indent=2)
