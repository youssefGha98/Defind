"""All event registries for DefInd - CL pools, Gauges, VFAT, and NFPM.

This module consolidates all domain-specific registry functions using signature-based
creation via the registry_builder module. All registries are composable and can be
merged with `{**a, **b}` syntax.

Available registries:
- CL Pool events: make_clpool_registry(), make_clpool_factory_registry()
- Gauge events: make_gauge_registry(), make_gauge_factory_registry()  
- VFAT/Sickle events: make_vfat_registry()
- NFPM events: make_nfpm_registry()

Example
-------
>>> from defind.decoding.registries import make_clpool_registry, make_gauge_registry
>>> reg = {**make_clpool_registry(), **make_gauge_registry()}
"""

from __future__ import annotations

from .specs import EventRegistry
from .registry_builder import make_registry


# -------------------------
# CL Pool registry (Mint/Burn/Collect/CollectFees)
# -------------------------

def make_clpool_registry() -> EventRegistry:
    """Return registry for concentrated-liquidity pool events."""
    return make_registry([
        "Mint(address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, address sender, uint128 liquidity, uint256 amount0, uint256 amount1)",
        "Burn(address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 liquidity, uint256 amount0, uint256 amount1)",
        "Collect(address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, address recipient, uint128 amount0, uint128 amount1)",
        "CollectFees(address indexed recipient, uint128 indexed amount0, uint128 indexed amount1)",
    ])


def make_clpool_factory_registry() -> EventRegistry:
    """Return registry for CLPool factory events (PoolCreated)."""
    return make_registry(
        "PoolCreated(address indexed token0, address indexed token1, int24 indexed tickSpacing, address pool)"
    )


# -------------------------
# Gauge registry (Deposit/Withdraw/ClaimRewards)
# -------------------------

def make_gauge_registry() -> EventRegistry:
    """Return registry for standard gauge events (deposit/withdraw/claim)."""
    return make_registry([
        "Deposit(address indexed user, uint256 indexed tokenId, uint128 indexed liquidityToStake)",
        "ClaimRewards(address indexed from, uint256 amount)",
        "Withdraw(address indexed user, uint256 indexed tokenId, uint128 indexed liquidityToStake)",
    ])


def make_gauge_factory_registry() -> EventRegistry:
    """Return registry for standard gauge factory events (GaugeCreated)."""
    return make_registry(
        "GaugeCreated(address indexed pool, address indexed gauge, address indexed reward)"
    )


# -------------------------
# VFAT / Sickle registry
# -------------------------

def make_vfat_registry() -> EventRegistry:
    """Return registry for VFAT/Sickle-related events."""
    return make_registry(
        "Deploy(address indexed admin, address sickle)"
    )

# -------------------------
# NFPM (NonFungiblePositionManager) registry
# -------------------------

def make_nfpm_registry() -> EventRegistry:
    """Return registry for NonFungiblePositionManager events."""
    return make_registry([
        "Collect(uint256 indexed tokenId, address recipient, uint256 amount0, uint256 amount1)",
        "IncreaseLiquidity(uint256 indexed tokenId, uint128 liquidity, uint256 amount0, uint256 amount1)",
        "DecreaseLiquidity(uint256 indexed tokenId, uint128 liquidity, uint256 amount0, uint256 amount1)",
        "Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
    ])
