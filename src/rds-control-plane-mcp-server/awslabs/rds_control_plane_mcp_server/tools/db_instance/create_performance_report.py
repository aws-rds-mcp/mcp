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

"""Performance report creation tool for RDS instances."""

from ...common.connection import PIConnectionManager
from ...common.decorator import handle_exceptions
from ...common.server import mcp
from ...common.utils import add_mcp_tags
from ...context import RDSContext
from datetime import datetime, timedelta
from loguru import logger
from pydantic import Field
from typing import Optional, Tuple


# Constants
MIN_DURATION_MINUTES = 5
MAX_DURATION_DAYS = 6
DEFAULT_START_DAYS_AGO = 5
DEFAULT_END_DAYS_AGO = 2

REPORT_CREATION_SUCCESS_RESPONSE = """Performance analysis report creation has been initiated successfully.

The report ID is: {}

This process is asynchronous and will take some time to complete. Once generated,
you can access the report details using the Performance Insights dashboard or the aws-rds://db-instance/{}/performance_report resource.

Note: Report generation typically takes a few minutes depending on the time range selected.
"""

CREATE_PERF_REPORT_TOOL_DESCRIPTION = """Create a performance report for an RDS instance.

    This tool creates a performance analysis report for a specific RDS instance over a time period
    that can range from 5 minutes to 6 days. Creating performance reports is an asynchronous process.
    The created reports will be tagged for identification.

    <use_case>
    Use this tool to generate detailed performance analysis reports for your RDS instances.
    These reports can help identify performance bottlenecks, analyze database behavior patterns,
    and support optimization efforts.
    </use_case>

    <important_notes>
    1. The analysis period can range from 5 minutes to 6 days
    2. There must be at least 24 hours of performance data before the analysis start time
    3. Time parameters must be in ISO8601 format (e.g., '2025-06-01T00:00:00Z')
    4. This operation will fail if the --read-only flag is True
    5. For region, DB engine, and instance class support information, see Amazon RDS documentation
    </important_notes>

    Args:
        dbi_resource_identifier: The DbiResourceId of a RDS Instance (e.g., db-EXAMPLEDBIID)
        start_time: The beginning of the time interval for the report (ISO8601 format)
        end_time: The end of the time interval for the report (ISO8601 format)

    Returns:
        str: A confirmation message with the report ID and instructions to access the report

    <examples>
    Example usage scenarios:
    1. Performance troubleshooting:
       - Generate a report for a period where performance issues were observed
       - Analyze the detailed metrics to identify bottlenecks

    2. Capacity planning:
       - Create reports for peak usage periods
       - Use the insights to make informed scaling decisions

    3. Performance optimization:
       - Generate reports before and after configuration changes
       - Compare the results to validate improvements
    </examples>
"""


def _parse_iso_datetime(time_str: str) -> datetime:
    """Parse ISO8601 datetime string, handling Z suffix.

    Args:
        time_str: ISO8601 formatted datetime string

    Returns:
        datetime: Parsed datetime object
    """
    if time_str.endswith('Z'):
        time_str = time_str.replace('Z', '+00:00')
    return datetime.fromisoformat(time_str)


def _get_default_time_range() -> Tuple[datetime, datetime]:
    """Get default start and end times for the report.

    Returns:
        Tuple[datetime, datetime]: Default start and end times based on configured defaults
    """
    now = datetime.now()
    start = now - timedelta(days=DEFAULT_START_DAYS_AGO)
    end = now - timedelta(days=DEFAULT_END_DAYS_AGO)
    return start, end


def _parse_time_parameters(
    start_time: Optional[str], end_time: Optional[str]
) -> Tuple[datetime, datetime]:
    """Parse and validate start and end time parameters.

    Args:
        start_time: Optional ISO8601 formatted start time string
        end_time: Optional ISO8601 formatted end time string

    Returns:
        Tuple[datetime, datetime]: Parsed start and end datetime objects

    Raises:
        ValueError: If time string formats are invalid
    """
    default_start, default_end = _get_default_time_range()

    if start_time:
        try:
            start = _parse_iso_datetime(start_time)
        except ValueError as e:
            raise ValueError(f'Invalid start_time format: {e}')
    else:
        start = default_start

    if end_time:
        try:
            end = _parse_iso_datetime(end_time)
        except ValueError as e:
            raise ValueError(f'Invalid end_time format: {e}')
    else:
        end = default_end

    return start, end


def _validate_time_range(start: datetime, end: datetime) -> None:
    """Validate that the time range meets requirements.

    Args:
        start: Start datetime to validate
        end: End datetime to validate

    Raises:
        ValueError: If time range is invalid
    """
    if start >= end:
        raise ValueError('start_time must be before end_time')

    duration = end - start
    if duration < timedelta(minutes=MIN_DURATION_MINUTES):
        raise ValueError(f'Time range must be at least {MIN_DURATION_MINUTES} minutes')
    if duration > timedelta(days=MAX_DURATION_DAYS):
        raise ValueError(f'Time range cannot exceed {MAX_DURATION_DAYS} days')


@mcp.tool(name='CreatePerformanceReport', description=CREATE_PERF_REPORT_TOOL_DESCRIPTION)
@handle_exceptions
async def create_performance_report(
    dbi_resource_identifier: str = Field(
        ...,
        description='The DbiResourceId of a RDS Instance (e.g., db-EXAMPLEDBIID) for the data source where PI should get its metrics.',
    ),
    start_time: Optional[str] = Field(
        None,
        description='The beginning of the time interval for the report (ISO8601 format). This must be within 5 minutes to 6 days of the end_time. There must be at least 24 hours of performance data before the analysis start time.',
    ),
    end_time: Optional[str] = Field(
        None,
        description='The end of the time interval for the report (ISO8601 format). This must be within 5 minutes to 6 days of the start_time.',
    ),
) -> str:
    """Create a performance analysis report for a specific RDS instance.

    Args:
        dbi_resource_identifier: The DbiResourceId of the RDS instance to analyze
        start_time: The beginning of the time interval for the report (ISO8601 format)
        end_time: The end of the time interval for the report (ISO8601 format)

    Returns:
        str: A confirmation message with the report ID and access instructions

    Raises:
        ValueError: If running in readonly mode or if parameters are invalid
    """
    if RDSContext.readonly_mode():
        logger.warning('You are running this tool in readonly mode. This operation is not allowed')
        raise ValueError(
            'You have configured this tool in readonly mode. To make this change you will have to update your configuration.'
        )

    start, end = _parse_time_parameters(start_time, end_time)
    _validate_time_range(start, end)

    params = {
        'ServiceType': 'RDS',
        'Identifier': dbi_resource_identifier,
        'StartTime': start,
        'EndTime': end,
    }

    params = add_mcp_tags(params)

    pi_client = PIConnectionManager.get_connection()
    response = pi_client.create_performance_analysis_report(**params)

    report_id = response.get('AnalysisReportId')
    if not report_id:
        raise ValueError('Failed to create performance report: No report ID returned')

    return REPORT_CREATION_SUCCESS_RESPONSE.format(report_id, dbi_resource_identifier)
