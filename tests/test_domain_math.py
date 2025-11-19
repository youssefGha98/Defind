"""Unit tests for domain math functions."""

import pytest
from define.domain.math import (
    Q96, Q128, amounts_from_liquidity, fee_growth_inside_x128, uncollected_fees,
    sqrt_ratio_at_tick, tick_to_price_base1
)


class TestAmountsFromLiquidity:
    """Test canonical CL math for 3 cases: below, above, in-range."""
    
    def test_entirely_below_range(self):
        """When current price is below position range: all token0, no token1."""
        L = 1000000  # 1M liquidity
        tick_lower, tick_upper = 0, 1000  # Range: ticks 0-1000
        current_tick = -500  # Below range
        sqrt_price_x96 = int(sqrt_ratio_at_tick(current_tick) * Q96)
        
        amt0, amt1 = amounts_from_liquidity(L, sqrt_price_x96, tick_lower, tick_upper)
        
        assert amt0 > 0, "Should have token0 when below range"
        assert amt1 == 0.0, "Should have no token1 when below range"
    
    def test_entirely_above_range(self):
        """When current price is above position range: no token0, all token1."""
        L = 1000000
        tick_lower, tick_upper = 0, 1000
        current_tick = 1500  # Above range
        sqrt_price_x96 = int(sqrt_ratio_at_tick(current_tick) * Q96)
        
        amt0, amt1 = amounts_from_liquidity(L, sqrt_price_x96, tick_lower, tick_upper)
        
        assert amt0 == 0.0, "Should have no token0 when above range"
        assert amt1 > 0, "Should have token1 when above range"
    
    def test_in_range(self):
        """When current price is in position range: both tokens present."""
        L = 1000000
        tick_lower, tick_upper = 0, 1000
        current_tick = 500  # In range
        sqrt_price_x96 = int(sqrt_ratio_at_tick(current_tick) * Q96)
        
        amt0, amt1 = amounts_from_liquidity(L, sqrt_price_x96, tick_lower, tick_upper)
        
        assert amt0 > 0, "Should have token0 when in range"
        assert amt1 > 0, "Should have token1 when in range"


class TestFeeGrowthInside:
    """Test fee growth inside calculation."""
    
    def test_fee_growth_increases_uncollected_fees(self):
        """When global fee growth increases, uncollected fees should increase."""
        L = 1000000
        fei_last_x128 = 100 * Q128  # Previous fee growth inside
        fei_now_x128 = 200 * Q128   # Current fee growth inside (doubled)
        tokens_owed_last = 1000     # Previous uncollected fees
        
        fees = uncollected_fees(L, fei_last_x128, fei_now_x128, tokens_owed_last)
        
        expected = float(tokens_owed_last) + L * ((fei_now_x128 - fei_last_x128) / Q128)
        assert abs(fees - expected) < 1e-10, f"Expected {expected}, got {fees}"
        assert fees > tokens_owed_last, "Fees should increase when fee growth increases"
    
    def test_fee_growth_inside_below_range(self):
        """Test fee growth calculation when current tick is below range."""
        current_tick = -100
        fg_global_x128 = 1000 * Q128
        lower_fg_out_x128 = 100 * Q128
        upper_fg_out_x128 = 200 * Q128
        tick_lower, tick_upper = 0, 1000
        
        result = fee_growth_inside_x128(
            current_tick, fg_global_x128, lower_fg_out_x128, upper_fg_out_x128,
            tick_lower, tick_upper
        )
        
        expected = (lower_fg_out_x128 - upper_fg_out_x128) % (1 << 256)
        assert result == expected


class TestPriceFunctions:
    """Test price conversion functions."""
    
    def test_tick_to_price_base1(self):
        """Test basic tick to price conversion."""
        # Tick 0 should give price 1.0
        assert abs(tick_to_price_base1(0) - 1.0) < 1e-10
        
        # Positive tick should give price > 1
        assert tick_to_price_base1(1000) > 1.0
        
        # Negative tick should give price < 1
        assert tick_to_price_base1(-1000) < 1.0
    
    def test_sqrt_ratio_consistency(self):
        """Test that sqrt_ratio_at_tick is consistent with tick_to_price_base1."""
        tick = 1000
        price = tick_to_price_base1(tick)
        sqrt_price = sqrt_ratio_at_tick(tick)
        
        # sqrt_price^2 should equal price
        assert abs(sqrt_price ** 2 - price) < 1e-10


if __name__ == "__main__":
    pytest.main([__file__])
