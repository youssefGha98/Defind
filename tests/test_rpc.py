from typing import Any, cast
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from defind.clients.rpc import RPC, topics_param


def _http_response(status: int, payload: dict[str, Any]) -> httpx.Response:
    req = httpx.Request("POST", "http://localhost:8545")
    return httpx.Response(status, request=req, json=payload)


@pytest.mark.asyncio
async def test_post_json_retries_on_http_429_then_succeeds() -> None:
    rpc = RPC("http://localhost:8545", max_retries=2, retry_backoff_s=0.01)
    post_mock = AsyncMock(
        side_effect=[
            _http_response(429, {"error": {"code": -32000, "message": "rate limited"}}),
            _http_response(200, {"result": "ok"}),
        ]
    )
    cast(Any, rpc.client).post = post_mock

    with patch("defind.clients.rpc.asyncio.sleep", new=AsyncMock()) as sleep_mock:
        out = await rpc._post_json({"jsonrpc": "2.0", "id": 1, "method": "x", "params": []})

    assert out == {"result": "ok"}
    assert post_mock.await_count == 2
    sleep_mock.assert_awaited_once()
    await rpc.aclose()


@pytest.mark.asyncio
async def test_post_json_does_not_retry_on_http_400() -> None:
    rpc = RPC("http://localhost:8545", max_retries=3, retry_backoff_s=0.01)
    post_mock = AsyncMock(return_value=_http_response(400, {"error": {"code": -32602, "message": "bad request"}}))
    cast(Any, rpc.client).post = post_mock

    with patch("defind.clients.rpc.asyncio.sleep", new=AsyncMock()) as sleep_mock:
        with pytest.raises(httpx.HTTPStatusError):
            await rpc._post_json({"jsonrpc": "2.0", "id": 1, "method": "x", "params": []})

    assert post_mock.await_count == 1
    sleep_mock.assert_not_awaited()
    await rpc.aclose()


@pytest.mark.asyncio
async def test_post_json_retries_on_transport_error_then_succeeds() -> None:
    rpc = RPC("http://localhost:8545", max_retries=2, retry_backoff_s=0.01)
    req = httpx.Request("POST", "http://localhost:8545")
    post_mock = AsyncMock(
        side_effect=[
            httpx.ConnectError("network down", request=req),
            _http_response(200, {"result": "ok"}),
        ]
    )
    cast(Any, rpc.client).post = post_mock

    with patch("defind.clients.rpc.asyncio.sleep", new=AsyncMock()) as sleep_mock:
        out = await rpc._post_json({"jsonrpc": "2.0", "id": 1, "method": "x", "params": []})

    assert out == {"result": "ok"}
    assert post_mock.await_count == 2
    sleep_mock.assert_awaited_once()
    await rpc.aclose()


@pytest.mark.asyncio
async def test_post_json_raises_after_max_retries_exhausted() -> None:
    rpc = RPC("http://localhost:8545", max_retries=2, retry_backoff_s=0.0)
    req = httpx.Request("POST", "http://localhost:8545")
    post_mock = AsyncMock(side_effect=httpx.ReadTimeout("timeout", request=req))
    cast(Any, rpc.client).post = post_mock

    with pytest.raises(httpx.ReadTimeout):
        await rpc._post_json({"jsonrpc": "2.0", "id": 1, "method": "x", "params": []})

    # initial try + 2 retries
    assert post_mock.await_count == 3
    await rpc.aclose()


def test_topics_param_lowercases_values() -> None:
    assert topics_param(["0xAbC", "0xDEF"]) == [["0xabc", "0xdef"]]


@pytest.mark.asyncio
async def test_latest_block_raises_on_rpc_error_payload() -> None:
    rpc = RPC("http://localhost:8545")
    cast(Any, rpc)._post_json = AsyncMock(return_value={"error": {"code": -32000, "message": "oops"}})

    with pytest.raises(RuntimeError, match="RPC error: -32000 oops"):
        await rpc.latest_block()

    await rpc.aclose()


@pytest.mark.asyncio
async def test_get_logs_parses_fields_and_fallback_hash_keys() -> None:
    rpc = RPC("http://localhost:8545")
    cast(Any, rpc)._post_json = AsyncMock(
        return_value={
            "result": [
                {
                    "address": "0xABCDEF0000000000000000000000000000000001",
                    "topics": ["0xAbC", b"0xDEF"],
                    "data": "0x",
                    "blockNumber": "0x10",
                    "transactionHash": "0xTX1",
                    "logIndex": "0x2",
                    "blockTimestamp": "0x64",
                },
                {
                    "address": "0xABCDEF0000000000000000000000000000000002",
                    "topics": ["0x123"],
                    "data": "0x00",
                    "blockNumber": "0x11",
                    "transaction_hash": "0xTX2",
                    "logIndex": "0x3",
                    "blockTimestamp": 123,
                },
            ]
        }
    )

    out = await rpc.get_logs(
        address="0xABCDEF0000000000000000000000000000000000",
        topic0s=["0xAbC"],
        from_block=1,
        to_block=2,
    )

    assert len(out) == 2
    assert out[0].address == "0xabcdef0000000000000000000000000000000001"
    assert out[0].topics == ("0xabc", "0xdef")
    assert out[0].block_number == 16
    assert out[0].tx_hash == "0xtx1"
    assert out[0].block_timestamp == 100
    assert out[1].tx_hash == "0xtx2"
    assert out[1].block_timestamp == 123
    await rpc.aclose()


@pytest.mark.asyncio
async def test_get_logs_raises_on_rpc_error_payload() -> None:
    rpc = RPC("http://localhost:8545")
    cast(Any, rpc)._post_json = AsyncMock(return_value={"error": {"code": -32005, "message": "too many"}})

    with pytest.raises(RuntimeError, match="RPC error: -32005 too many"):
        await rpc.get_logs(
            address="0xabc",
            topic0s=["0x1"],
            from_block=1,
            to_block=2,
        )

    await rpc.aclose()


@pytest.mark.asyncio
async def test_aclose_closes_underlying_client() -> None:
    rpc = RPC("http://localhost:8545")
    aclose_mock = AsyncMock()
    cast(Any, rpc.client).aclose = aclose_mock

    await rpc.aclose()

    aclose_mock.assert_awaited_once()
