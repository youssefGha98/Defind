"""Test script for the new event-based storage structure.

This script demonstrates how events are now stored in separate parquet files:
Data/{event_name}/shard_*.parquet
Data/{event_name}/manifests/run_*.jsonl
"""

import asyncio
from defind.orchestration.orchestrator import fetch_decode
from defind.decoding.registries import make_clpool_registry


async def main():
    """Test fetch_decode with new event-based storage."""
    
    # Create a registry with multiple event types
    registry = make_clpool_registry()
    
    # Configure the fetch
    result = await fetch_decode(
        rpc_url="https://mainnet.optimism.io",
        address="0x25CbdDb98b35ab1FF77413456B31EC81A6B6B746",  # Example CLPool Factory
        topic0s=list(registry.keys()),  # Fetch all events in registry
        start_block=120_000_000,
        end_block=120_001_000,  # Small range for testing
        step=1_000,
        concurrency=8,
        out_root="./test_data",  # New structure will be created here
        rows_per_shard=10_000,
        registry=registry,
    )
    
    print("\n" + "="*60)
    print("FETCH RESULTS")
    print("="*60)
    for key, value in result.items():
        print(f"{key:25s}: {value:,}")
    
    print("\n" + "="*60)
    print("NEW STRUCTURE CREATED")
    print("="*60)
    print("Data/")
    print("├── _manifests/")
    print("│   └── run_*.jsonl")
    print("├── {event_name_1}/")
    print("│   └── shard_*.parquet")
    print("├── {event_name_2}/")
    print("│   └── shard_*.parquet")
    print("└── ...")
    print()


if __name__ == "__main__":
    asyncio.run(main())
