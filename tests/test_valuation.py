"""Unit tests for valuation logic."""

import pytest
from define.domain.models import (
    Pool, Token, PositionStatic, PositionEventState, PoolSnapshot, PricePoint
)
from define.application.valuation import value_snapshot


class TestValueSnapshot:
    """Test position valuation at a snapshot."""
    
    def setup_method(self):
        """Set up test data."""
        self.token0 = Token(address="0x123", symbol="USDC", decimals=6)
        self.token1 = Token(address="0x456", symbol="WETH", decimals=18)
        self.pool = Pool(id="pool1", token0=self.token0, token1=self.token1, fee=3000)
        
        self.pos_static = PositionStatic(
            token_id=12345,
            pool_id="pool1",
            tick_lower=-1000,
            tick_upper=1000,
            entry_amount0=1000.0,  # 1000 USDC
            entry_amount1=0.5      # 0.5 WETH
        )
        
        self.pos_state = PositionEventState(
            token_id=12345,
            liquidity=1000000,
            fee_growth_inside0_last_x128=100 * (2**128),
            fee_growth_inside1_last_x128=50 * (2**128),
            tokens_owed0=10,  # 10 USDC in uncollected fees
            tokens_owed1=5    # 5 WETH in uncollected fees
        )
        
        self.snapshot = PoolSnapshot(
            pool_id="pool1",
            block_number=18000000,
            timestamp=1700000000,
            sqrt_price_x96=int(1.0001 ** (0 / 2) * (2**96)),  # At tick 0
            tick=0,
            fg0_x128=200 * (2**128),  # Global fee growth increased
            fg1_x128=100 * (2**128),
            lower_fg0_out_x128=0,
            lower_fg1_out_x128=0,
            upper_fg0_out_x128=0,
            upper_fg1_out_x128=0
        )
        
        self.price_point = PricePoint(
            pool_id="pool1",
            timestamp=1700000000,
            token0_usd=1.0,    # USDC = $1
            token1_usd=2000.0  # WETH = $2000
        )
    
    def test_basic_valuation(self):
        """Test basic position valuation."""
        result = value_snapshot(
            pool=self.pool,
            pos_static=self.pos_static,
            pos_state=self.pos_state,
            snap=self.snapshot,
            px=self.price_point
        )
        
        # Should have valid results
        assert result.token_id == 12345
        assert result.pool_id == "pool1"
        assert result.timestamp == 1700000000
        
        # Should have positive inventory values (position is in range at tick 0)
        assert result.amount0_from_L > 0
        assert result.amount1_from_L > 0
        
        # Should have positive uncollected fees (fee growth increased)
        assert result.uncollected_fee0 > 10  # More than initial tokens_owed0
        assert result.uncollected_fee1 > 5   # More than initial tokens_owed1
        
        # Should have positive USD values
        assert result.value_inventory_usd > 0
        assert result.value_uncollected_fees_usd > 0
        assert result.value_total_usd > 0
        assert result.hodl_value_usd > 0
        
        # Total should equal inventory + fees
        expected_total = result.value_inventory_usd + result.value_uncollected_fees_usd
        assert abs(result.value_total_usd - expected_total) < 1e-10
    
    def test_il_calculation(self):
        """Test impermanent loss calculation."""
        result = value_snapshot(
            pool=self.pool,
            pos_static=self.pos_static,
            pos_state=self.pos_state,
            snap=self.snapshot,
            px=self.price_point
        )
        
        # IL should be inventory value minus HODL value
        expected_il = result.value_inventory_usd - result.hodl_value_usd
        assert abs(result.il_usd - expected_il) < 1e-10
        
        # Capital gain should equal IL (no mint USD baseline)
        assert abs(result.capital_gain_usd - result.il_usd) < 1e-10


if __name__ == "__main__":
    pytest.main([__file__])
