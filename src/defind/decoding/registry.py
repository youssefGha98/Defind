from __future__ import annotations
from typing import Iterable

from .specs import (
    EventRegistry, EventSpec, TopicFieldSpec, DataFieldSpec, Projection
)
from defind.core.constants import CLAIM_REWARDS_T0, DEPOSIT_T0, MINT_T0, BURN_T0, COLLECT_T0, COLLECTFEES_T0, WITHDRAW_T0

def make_default_registry() -> EventRegistry:
    reg: EventRegistry = {}

    # Mint
    reg[MINT_T0] = EventSpec(
        topic0=MINT_T0,
        name="Mint",
        topic_fields=[
            TopicFieldSpec("owner", 1, "address"),
            TopicFieldSpec("tick_lower", 2, "int24"),
            TopicFieldSpec("tick_upper", 3, "int24"),
        ],
        data_fields=[
            DataFieldSpec("sender",   0, "address"),
            DataFieldSpec("liquidity",1, "uint128"),
            DataFieldSpec("amount0",  2, "uint256"),
            DataFieldSpec("amount1",  3, "uint256"),
        ],
        projection={
            "owner": "topic.owner",
            "sender": "data.sender",
            "tick_lower": "topic.tick_lower",
            "tick_upper": "topic.tick_upper",
            "liquidity": "data.liquidity",
            "amount0": "data.amount0",
            "amount1": "data.amount1",
        },
        fast_zero_words=(1,2,3),
        drop_if_all_zero_fields=("liquidity","amount0","amount1"),
    )

    # Burn
    reg[BURN_T0] = EventSpec(
        topic0=BURN_T0,
        name="Burn",
        topic_fields=[
            TopicFieldSpec("owner", 1, "address"),
            TopicFieldSpec("tick_lower", 2, "int24"),
            TopicFieldSpec("tick_upper", 3, "int24"),
        ],
        data_fields=[
            DataFieldSpec("liquidity",0, "uint128"),
            DataFieldSpec("amount0",  1, "uint256"),
            DataFieldSpec("amount1",  2, "uint256"),
        ],
        projection={
            "owner": "topic.owner",
            "tick_lower": "topic.tick_lower",
            "tick_upper": "topic.tick_upper",
            "liquidity": "data.liquidity",
            "amount0": "data.amount0",
            "amount1": "data.amount1",
        },
        fast_zero_words=(0,1,2),
        drop_if_all_zero_fields=("liquidity","amount0","amount1"),
    )

    # Collect
    reg[COLLECT_T0] = EventSpec(
        topic0=COLLECT_T0,
        name="Collect",
        topic_fields=[
            TopicFieldSpec("owner", 1, "address"),
            TopicFieldSpec("tick_lower", 2, "int24"),
            TopicFieldSpec("tick_upper", 3, "int24"),
        ],
        data_fields=[
            DataFieldSpec("recipient",0, "address"),
            DataFieldSpec("amount0",  1, "uint128"),
            DataFieldSpec("amount1",  2, "uint128"),
        ],
        projection={
            "owner": "topic.owner",
            "recipient": "data.recipient",
            "tick_lower": "topic.tick_lower",
            "tick_upper": "topic.tick_upper",
            "amount0": "data.amount0",
            "amount1": "data.amount1",
        },
        fast_zero_words=(1,2),
        drop_if_all_zero_fields=("amount0","amount1"),
    )

    # CollectFees
    reg[COLLECTFEES_T0] = EventSpec(
        topic0=COLLECTFEES_T0,
        name="CollectFees",
        topic_fields=[
            TopicFieldSpec("recipient", 1, "address"),
        ],
        data_fields=[
            DataFieldSpec("amount0", 0, "uint128"),
            DataFieldSpec("amount1", 1, "uint128"),
        ],
        projection={
            "recipient": "topic.recipient",
            "amount0": "data.amount0",
            "amount1": "data.amount1",
        },
        fast_zero_words=(0,1),
        drop_if_all_zero_fields=("amount0","amount1"),
    )

    # Deposit
    reg[DEPOSIT_T0] = EventSpec(
        topic0=DEPOSIT_T0,
        name="Deposit",
        model_tag="clgauge",
        topic_fields=[
            TopicFieldSpec("user",              1, "address"),
            TopicFieldSpec("tokenId",           2, "uint256"),
            TopicFieldSpec("liquidityToStake",  3, "uint128"),
        ],
        data_fields=[],
        projection={
            "owner":     "topic.user",               # adapter maps owner→GaugeRow.user
            "liquidity": "topic.liquidityToStake",   # adapter maps liquidity→GaugeRow.amount
            "token_id":   "topic.tokenId",            # stash tokenId; adapter maps to GaugeRow.token_id
        },
        fast_zero_words=(),
        drop_if_all_zero_fields=(),
    )

    # Withdraw
    reg[WITHDRAW_T0] = EventSpec(
        topic0=WITHDRAW_T0,
        name="Withdraw",
        model_tag="clgauge",
        topic_fields=[
            TopicFieldSpec("user",              1, "address"),
            TopicFieldSpec("tokenId",           2, "uint256"),
            TopicFieldSpec("liquidityToStake",  3, "uint128"),
        ],
        data_fields=[],
        projection={
            "owner":     "topic.user",
            "liquidity": "topic.liquidityToStake",
            "token_id":   "topic.tokenId",
        },
        fast_zero_words=(),
        drop_if_all_zero_fields=(),
    )


    reg[CLAIM_REWARDS_T0] = EventSpec(
        topic0=CLAIM_REWARDS_T0,
        name="ClaimRewards",
        model_tag="clgauge",
        topic_fields=[TopicFieldSpec("from", 1, "address")],
        data_fields=[DataFieldSpec("amount", 0, "uint256")],
        projection={
            "owner":     "topic.from",
            "amount": "data.amount",
        },
        fast_zero_words=(),
        drop_if_all_zero_fields=(),
    )

    return reg

def add_event_spec(registry: EventRegistry, spec: EventSpec) -> None:
    registry[spec.topic0.lower()] = spec

def add_many(registry: EventRegistry, specs: Iterable[EventSpec]) -> None:
    for s in specs:
        add_event_spec(registry, s)
