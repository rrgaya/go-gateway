package hotbit

import (
	"math"

	"github.com/herenow/atomic-gtw/gateway"
)

func marketsToCommonMarket(markets []Market) []gateway.Market {
	commonMarkets := make([]gateway.Market, 0, len(markets))

	for _, market := range markets {
		if market.IsOpenTrade {
			commonMarkets = append(commonMarkets, marketToCommonMarket(market))
		}
	}

	return commonMarkets
}

func marketToCommonMarket(market Market) gateway.Market {
	return gateway.Market{
		Exchange: Exchange,
		Pair: gateway.Pair{
			Base:  market.Base(),
			Quote: market.Quote(),
		},
		Symbol:           market.Symbol(),
		TakerFee:         0.0025,
		MakerFee:         0.00,
		PriceTick:        1 / math.Pow10(int(market.QuoteCoinPrecision)),
		AmountTick:       market.MinBuyCount,
		MinimumOrderSize: market.MinBuyCount,
		PaysFeeInStock:   true,
	}
}
