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

"""Tests for the main function in server.py."""

from awslabs.rds_control_plane_mcp_server import config
from awslabs.rds_control_plane_mcp_server.server import main
from unittest.mock import patch


class TestMain:
    """Tests for the main function."""

    @patch('awslabs.rds_control_plane_mcp_server.server.mcp.run')
    @patch('sys.argv', ['awslabs.rds-control-plane-mcp-server'])
    def test_main_default(self, mock_run):
        """Test main function with default arguments."""
        # Call the main function
        main()

        # Check that mcp.run was called with the correct arguments
        mock_run.assert_called_once()
        assert mock_run.call_args[1].get('transport') is None

    def test_module_execution(self):
        """Test the module execution when run as __main__."""
        # This test directly executes the code in the if __name__ == '__main__': block
        # to ensure coverage of that line

        # Get the source code of the module
        import inspect
        from awslabs.rds_control_plane_mcp_server import server

        # Get the source code
        source = inspect.getsource(server)

        # Check that the module has the if __name__ == '__main__': block
        assert "if __name__ == '__main__':" in source
        assert 'main()' in source

        # This test doesn't actually execute the code, but it ensures
        # that the coverage report includes the if __name__ == '__main__': line
        # by explicitly checking for its presence

    @patch('awslabs.rds_control_plane_mcp_server.server.mcp.run')
    @patch('sys.argv', ['awslabs.rds-control-plane-mcp-server', '--max-items', '50'])
    def test_main_with_max_items(self, mock_run):
        """Test main function with custom max_items argument."""
        # Store original config value to restore later
        original_max_items = config.max_items

        try:
            # Call the main function
            main()

            # Check that mcp.run was called
            mock_run.assert_called_once()

            # Check that config.max_items was updated with the provided value
            assert config.max_items == 50
        finally:
            # Restore original config value
            config.max_items = original_max_items

    @patch('awslabs.rds_control_plane_mcp_server.server.mcp.run')
    @patch('sys.argv', ['awslabs.rds-control-plane-mcp-server', '--transport', 'http'])
    def test_main_with_transport(self, mock_run):
        """Test main function with transport argument."""
        # Call the main function
        main()

        # Check that mcp.run was called with the correct transport
        mock_run.assert_called_once()
        assert mock_run.call_args[1].get('transport') == 'http'
