from unittest.mock import AsyncMock

import pytest


@pytest.fixture
def mock_rpc():
    rpc = AsyncMock()
    rpc.get_logs = AsyncMock(return_value=[])
    rpc.latest_block = AsyncMock(return_value=100)
    rpc.aclose = AsyncMock()
    return rpc
